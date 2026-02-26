# -*- coding: utf-8 -*-
"""
UpSeller Web Scraper - Automacao com Playwright.
Baixa etiquetas de envio (PDFs), exporta XMLs de NF-e, e extrai dados de pedidos
do UpSeller ERP para gerar XLSX compativel com ProcessadorEtiquetasShopee.

Fluxo:
  1. Login no app.upseller.com (ou reutiliza sessao)
  2. Extrai dados de pedidos (order_sn, tracking, produtos) → gera XLSX
  3. Navega para Pedidos > Para Imprimir > Baixa PDFs de etiquetas
  4. Navega para Brasil NF-e > Exporta XMLs por data
  5. Move tudo para pasta_entrada do usuario
"""

import os
import re
import glob
import shutil
import logging
import asyncio
import zipfile
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from pathlib import Path

logger = logging.getLogger(__name__)

# URLs do UpSeller (mapeadas em 2026-02-24)
UPSELLER_BASE = "https://app.upseller.com"
UPSELLER_LOGIN = f"{UPSELLER_BASE}/pt/login"
UPSELLER_PEDIDOS = f"{UPSELLER_BASE}/order/to-ship"
UPSELLER_PEDIDOS_TODOS = f"{UPSELLER_BASE}/order/all-orders"
UPSELLER_PARA_IMPRIMIR = f"{UPSELLER_BASE}/pt/order/in-process"
UPSELLER_NFE = f"{UPSELLER_BASE}/order/invoice-manage/brazil-nf-e/issued/recent"


class UpSellerScraper:
    """
    Automatiza o UpSeller ERP via Playwright para baixar etiquetas e XMLs.

    Usa sessao persistente para evitar login repetido.
    Roda headless em producao, com navegador visivel em debug.
    """

    def __init__(self, config: dict):
        """
        Args:
            config: {
                "email": str,           # Login UpSeller
                "password": str,        # Senha (decriptada)
                "profile_dir": str,     # Pasta de sessao persistente
                "headless": bool,       # True em producao
                "download_dir": str,    # Pasta destino dos downloads
            }
        """
        self.email = config.get("email", "")
        self.password = config.get("password", "")
        self.profile_dir = config.get("profile_dir", "")
        self.headless = config.get("headless", True)
        self.download_dir = config.get("download_dir", "")

        # Garantir que pastas existem
        if self.profile_dir:
            os.makedirs(self.profile_dir, exist_ok=True)
        if self.download_dir:
            os.makedirs(self.download_dir, exist_ok=True)

        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

    async def _iniciar_navegador(self):
        """Inicia Playwright com contexto persistente."""
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()

        # Garantir diretorio de sessao valido
        profile = self.profile_dir or os.path.join(os.path.expanduser("~"), ".upseller_session")
        os.makedirs(profile, exist_ok=True)
        logger.info(f"[UpSeller] Profile dir: {profile}")

        # Args extras para modo visivel: abrir na frente, centralizado
        extra_args = [
            "--disable-blink-features=AutomationControlled",
            "--no-first-run",
            "--disable-gpu",
        ]
        if not self.headless:
            # Abrir janela na frente, posicao centralizada para chamar atencao
            extra_args.extend([
                "--window-position=200,50",
                "--window-size=1100,750",
                "--auto-open-devtools-for-tabs=false",
            ])

        # Contexto persistente = salva cookies, localStorage, etc.
        try:
            self._context = await self._playwright.chromium.launch_persistent_context(
                user_data_dir=profile,
                headless=self.headless,
                accept_downloads=True,
                viewport={"width": 1100, "height": 700},
                locale="pt-BR",
                timezone_id="America/Sao_Paulo",
                args=extra_args,
                ignore_default_args=["--enable-automation"],
                timeout=30000,
            )
        except Exception as e:
            logger.error(f"[UpSeller] Erro ao abrir navegador persistente: {e}")
            # Fallback: tentar sem contexto persistente
            logger.info("[UpSeller] Tentando sem contexto persistente...")
            browser = await self._playwright.chromium.launch(
                headless=self.headless,
                args=["--disable-blink-features=AutomationControlled", "--no-first-run", "--disable-gpu"],
                timeout=30000,
            )
            self._context = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                locale="pt-BR",
                timezone_id="America/Sao_Paulo",
                accept_downloads=True,
            )
            self._browser = browser

        # Usar primeira pagina ou criar nova
        if self._context.pages:
            self._page = self._context.pages[0]
        else:
            self._page = await self._context.new_page()

        logger.info(f"[UpSeller] Navegador iniciado (headless={self.headless})")

    async def _esta_logado(self) -> bool:
        """Verifica se ja esta logado no UpSeller."""
        try:
            await self._page.goto(UPSELLER_BASE, wait_until="domcontentloaded", timeout=15000)
            # Se redirecionou para login, nao esta logado
            url_atual = self._page.url
            if "/login" in url_atual or "/sign" in url_atual:
                return False
            # Verificar se tem elemento de dashboard/menu
            try:
                await self._page.wait_for_selector(
                    'nav, .sidebar, .menu, [class*="sidebar"], [class*="menu"]',
                    timeout=5000
                )
                return True
            except:
                return False
        except Exception as e:
            logger.warning(f"[UpSeller] Erro verificando login: {e}")
            return False

    async def login(self) -> bool:
        """
        Verifica se ja esta logado via sessao persistente.
        Se nao estiver, preenche email/senha e aguarda o usuario resolver o CAPTCHA.

        O UpSeller tem CAPTCHA no login, entao o fluxo eh:
        1. Primeira vez: usuario faz login manual no navegador visivel
        2. Proximas vezes: sessao persistente reutilizada (14 dias)

        Retorna: True se logado com sucesso
        """
        if not self._page:
            await self._iniciar_navegador()

        # Verificar se ja esta logado (sessao persistente)
        if await self._esta_logado():
            logger.info("[UpSeller] Sessao existente reutilizada")
            return True

        logger.info("[UpSeller] Nao esta logado - tentando login...")
        try:
            await self._page.goto(UPSELLER_LOGIN, wait_until="domcontentloaded", timeout=30000)

            # Preencher email (campo eh type="text", nao type="email")
            email_input = await self._page.wait_for_selector(
                'input[type="text"]:first-of-type, input[name="email"], input[placeholder*="email" i]',
                timeout=10000
            )
            await email_input.fill(self.email)

            # Preencher senha
            password_input = await self._page.wait_for_selector(
                'input[type="password"]',
                timeout=5000
            )
            await password_input.fill(self.password)

            # Marcar "Mantenha-me conectado"
            try:
                checkbox = await self._page.query_selector('input[type="checkbox"]')
                if checkbox:
                    is_checked = await checkbox.is_checked()
                    if not is_checked:
                        await checkbox.click()
            except Exception:
                pass

            # CAPTCHA: se estiver headless, nao pode resolver
            if self.headless:
                logger.warning("[UpSeller] CAPTCHA detectado - headless nao pode resolver. Use login manual primeiro.")
                return False

            # Modo visivel: aguardar usuario resolver CAPTCHA e clicar Login (max 120s)
            logger.info("[UpSeller] Aguardando usuario resolver CAPTCHA e clicar Login...")
            try:
                await self._page.wait_for_url(
                    lambda url: "/login" not in url and "/sign" not in url,
                    timeout=120000  # 2 minutos para resolver CAPTCHA
                )
                logger.info("[UpSeller] Login realizado com sucesso!")
                return True
            except Exception:
                logger.error("[UpSeller] Timeout aguardando login manual (CAPTCHA)")
                return False

        except Exception as e:
            logger.error(f"[UpSeller] Erro no login: {e}")
            return False

    async def login_manual(self, timeout_seconds: int = 180) -> bool:
        """
        Abre navegador VISIVEL NA FRENTE para o usuario fazer login manual.
        Sessao persistente fica salva por 14 dias.
        Nao precisa de email/senha — o usuario preenche tudo direto no navegador.

        Args:
            timeout_seconds: Tempo maximo para o usuario completar o login

        Retorna: True se logado com sucesso
        """
        # Forcar modo visivel
        old_headless = self.headless
        self.headless = False

        if not self._page:
            await self._iniciar_navegador()

        # Verificar se ja esta logado
        if await self._esta_logado():
            logger.info("[UpSeller] Ja esta logado!")
            self.headless = old_headless
            return True

        logger.info("[UpSeller] Abrindo pagina de login para login manual...")
        await self._page.goto(UPSELLER_LOGIN, wait_until="domcontentloaded", timeout=30000)

        # Trazer janela para FRENTE (Playwright bring_to_front)
        try:
            await self._page.bring_to_front()
        except Exception:
            pass

        # Marcar "Mantenha-me conectado" se existir
        try:
            checkbox = await self._page.query_selector('input[type="checkbox"]')
            if checkbox and not await checkbox.is_checked():
                await checkbox.click()
        except Exception:
            pass

        # Aguardar usuario preencher tudo (email, senha, CAPTCHA) e clicar Login
        logger.info(f"[UpSeller] Aguardando login manual (max {timeout_seconds}s)...")
        logger.info("[UpSeller] >>> Preencha email, senha e CAPTCHA, depois clique Login <<<")
        try:
            await self._page.wait_for_url(
                lambda url: "/login" not in url and "/sign" not in url,
                timeout=timeout_seconds * 1000
            )
            logger.info("[UpSeller] Login concluido com sucesso! Sessao salva.")
            self.headless = old_headless
            return True
        except Exception:
            logger.error("[UpSeller] Timeout - login nao concluido a tempo")
            self.headless = old_headless
            return False

    async def _fechar_popups(self, max_tentativas: int = 5):
        """
        Fecha popups/modais/tutoriais que bloqueiam a pagina do UpSeller.

        O UpSeller mostra um popup tutorial "Introducao de Controle de Pedidos"
        com video do YouTube toda vez que acessa /pt/order/in-process.
        Esse popup NAO e ant-modal — e um popup customizado com X no canto.

        Estrategia (em ordem):
        1. Detectar overlay #myNav e esconder via JS
        2. Buscar botao X no popup tutorial (varios seletores)
        3. Tentar fechar via JavaScript (click no X, remove overlay)
        4. Pressionar ESC para fechar
        5. Clicar fora do popup para fechar
        """
        for tentativa in range(max_tentativas):
            popup_encontrado = False

            # ---- Estrategia 1: Esconder overlay #myNav e .my_nav_bg ----
            try:
                removidos = await self._page.evaluate("""
                    (() => {
                        let count = 0;
                        const nav = document.getElementById('myNav');
                        if (nav && nav.style.display !== 'none') {
                            nav.style.display = 'none';
                            count++;
                        }
                        document.querySelectorAll('.my_nav_bg').forEach(el => {
                            if (el.style.display !== 'none') {
                                el.style.display = 'none';
                                count++;
                            }
                        });
                        return count;
                    })()
                """)
                if removidos > 0:
                    popup_encontrado = True
                    logger.info(f"[UpSeller] Overlay removido via JS ({removidos} elementos)")
            except Exception:
                pass

            # ---- Estrategia 2: Popup tutorial "Introducao de Controle de Pedidos" ----
            # Esse popup tem um X no canto superior direito e contem video YouTube
            try:
                fechou_tutorial = await self._page.evaluate("""
                    (() => {
                        // Buscar popup que contem "Introdução" ou "Controle de Pedidos" ou video YouTube
                        const allEls = document.querySelectorAll('div, section, aside, [class*="modal"], [class*="popup"], [class*="dialog"], [class*="tutorial"], [class*="intro"]');
                        for (const el of allEls) {
                            const text = el.textContent || '';
                            const hasYoutube = el.querySelector('iframe[src*="youtube"], iframe[src*="youtu.be"]');
                            const isTutorial = text.includes('Introdução') || text.includes('Controle de Pedidos') || text.includes('Ver mais vídeos tutoriais') || hasYoutube;

                            if (isTutorial && el.offsetWidth > 200 && el.offsetHeight > 100) {
                                // Encontrou popup tutorial! Buscar botao fechar
                                // 1. Buscar X explicito (botao ou span com × ou X)
                                const closeSelectors = [
                                    'button[class*="close"]', 'span[class*="close"]', 'a[class*="close"]',
                                    'button[aria-label="Close"]', 'button[aria-label="Fechar"]',
                                    '.ant-modal-close', '.close-btn', '[class*="close-icon"]',
                                    'svg[class*="close"]'
                                ];
                                for (const sel of closeSelectors) {
                                    const btn = el.querySelector(sel);
                                    if (btn) { btn.click(); return 'close_btn'; }
                                }

                                // 2. Buscar qualquer elemento pequeno no topo-direito que pareca X
                                const children = el.querySelectorAll('*');
                                for (const child of children) {
                                    const t = (child.textContent || '').trim();
                                    if ((t === '×' || t === 'X' || t === 'x' || t === '✕' || t === '✖') && child.offsetWidth < 60) {
                                        child.click();
                                        return 'x_char';
                                    }
                                }

                                // 3. Buscar SVG close icon
                                const svgs = el.querySelectorAll('svg');
                                for (const svg of svgs) {
                                    const rect = svg.getBoundingClientRect();
                                    // SVG pequeno no canto superior direito = provavel close button
                                    if (rect.width < 30 && rect.width > 5) {
                                        const parent = svg.closest('button, a, span, div');
                                        if (parent) { parent.click(); return 'svg_close'; }
                                        svg.click();
                                        return 'svg_click';
                                    }
                                }

                                // 4. Ultima tentativa: remover o popup inteiro via DOM
                                el.style.display = 'none';
                                el.remove();
                                return 'removed';
                            }
                        }
                        return null;
                    })()
                """)
                if fechou_tutorial:
                    popup_encontrado = True
                    logger.info(f"[UpSeller] Popup tutorial fechado via: {fechou_tutorial}")
                    await self._page.wait_for_timeout(500)
            except Exception as e:
                logger.debug(f"[UpSeller] Erro ao fechar tutorial via JS: {e}")

            # ---- Estrategia 3: Popups com botoes "Ignorar", "Pular" ----
            # IMPORTANTE: NAO clicar "Entendido" pois ele AVANCA o tutorial passo-a-passo
            # e pode navegar para outra pagina (ex: "Para Emitir" em vez de "Para Enviar")
            # Preferir "Ignorar"/"Pular" que FECHAM o tutorial inteiro
            for btn_text in ['Ignorar', 'Pular']:
                try:
                    btn = await self._page.query_selector(f'button:has-text("{btn_text}")')
                    if btn and await btn.is_visible():
                        await btn.click()
                        popup_encontrado = True
                        logger.info(f"[UpSeller] Popup fechado via botao '{btn_text}'")
                        await self._page.wait_for_timeout(500)
                except Exception:
                    pass

            # Se so tem "Entendido" (sem Ignorar/Pular), remover o popup via JS ao inves de clicar
            try:
                removeu = await self._page.evaluate("""
                    (() => {
                        // Buscar popovers/tooltips do tutorial
                        const pops = document.querySelectorAll('.ant-popover:not(.ant-popover-hidden), [class*="popover"], [class*="tooltip"], [class*="guide"]');
                        let removed = 0;
                        for (const p of pops) {
                            const text = (p.textContent || '');
                            if (text.includes('Entendido') || text.includes('Nota Fiscal') || text.includes('Clique aqui')) {
                                p.style.display = 'none';
                                p.remove();
                                removed++;
                            }
                        }
                        // Remover overlays de tutorial
                        const overlays = document.querySelectorAll('.ant-popover-mask, [class*="mask"], [class*="backdrop"]');
                        for (const o of overlays) {
                            o.style.display = 'none';
                        }
                        return removed;
                    })()
                """)
                if removeu and removeu > 0:
                    popup_encontrado = True
                    logger.info(f"[UpSeller] Tutorial 'Entendido' removido via JS ({removeu})")
            except Exception:
                pass

            # Guias passo-a-passo com "Ignorar" e "Proximo"
            try:
                ignorar_btn = await self._page.query_selector(
                    'button:has-text("Ignorar"), a:has-text("Ignorar"), '
                    'span:has-text("Ignorar"), div:has-text("Ignorar"):not(:has(div))'
                )
                if ignorar_btn and await ignorar_btn.is_visible():
                    await ignorar_btn.click()
                    popup_encontrado = True
                    logger.info("[UpSeller] Popup guia passo-a-passo fechado via 'Ignorar'")
                    await self._page.wait_for_timeout(500)
                    # Pode haver mais guias em sequencia, continuar loop
            except Exception:
                pass

            # Tambem tentar fechar via JS (guias do UpSeller podem ser divs flutuantes)
            try:
                fechou_guia = await self._page.evaluate("""
                    (() => {
                        // Buscar divs que parecem guias/tooltips com "Ignorar" ou "Próximo"
                        const allEls = document.querySelectorAll('div, section');
                        for (const el of allEls) {
                            const text = el.textContent || '';
                            const hasIgnorar = text.includes('Ignorar');
                            const hasProximo = text.includes('Próximo');
                            const hasSteps = text.match(/[0-9]+[/][0-9]+/);
                            if (hasIgnorar && (hasProximo || hasSteps) && el.offsetWidth > 100 && el.offsetWidth < 600) {
                                // Clicar em "Ignorar"
                                const btns = el.querySelectorAll('button, a, span, div');
                                for (const btn of btns) {
                                    if ((btn.textContent || '').trim() === 'Ignorar') {
                                        btn.click();
                                        return 'ignorar_clicked';
                                    }
                                }
                                // Ou remover o guia inteiro
                                el.style.display = 'none';
                                return 'guide_hidden';
                            }
                        }
                        return null;
                    })()
                """)
                if fechou_guia:
                    popup_encontrado = True
                    logger.info(f"[UpSeller] Guia passo-a-passo: {fechou_guia}")
                    await self._page.wait_for_timeout(500)
            except Exception:
                pass

            # ---- Estrategia 4: ant-modal genericos ----
            try:
                modals = await self._page.query_selector_all('.ant-modal-wrap:not([style*="display: none"])')
                for modal in modals:
                    close_btn = await modal.query_selector('.ant-modal-close')
                    if close_btn:
                        await close_btn.click()
                        popup_encontrado = True
                        logger.info("[UpSeller] ant-modal fechado")
                        await self._page.wait_for_timeout(500)
            except Exception:
                pass

            # ---- Estrategia 4: Seletores CSS diretos para X/fechar ----
            if not popup_encontrado:
                for selector in [
                    # X do popup tutorial (baseado no screenshot)
                    'div:has(iframe[src*="youtube"]) ~ *:has-text("×")',
                    'div:has(iframe[src*="youtube"]) ~ button',
                    '[class*="close"]:visible',
                    'button[aria-label="Close"]',
                    'button[aria-label="Fechar"]',
                    'a[aria-label="Close"]',
                ]:
                    try:
                        btn = await self._page.query_selector(selector)
                        if btn and await btn.is_visible():
                            await btn.click()
                            popup_encontrado = True
                            logger.info(f"[UpSeller] Popup fechado via seletor: {selector}")
                            await self._page.wait_for_timeout(500)
                            break
                    except Exception:
                        continue

            # ---- Estrategia 5: Pressionar ESC ----
            if not popup_encontrado:
                try:
                    await self._page.keyboard.press("Escape")
                    await self._page.wait_for_timeout(500)
                    # Verificar se algo mudou
                    has_popup = await self._page.evaluate("""
                        (() => {
                            const iframes = document.querySelectorAll('iframe[src*="youtube"]');
                            for (const iframe of iframes) {
                                if (iframe.offsetParent !== null) return true;
                            }
                            const modals = document.querySelectorAll('.ant-modal-wrap:not([style*="display: none"])');
                            return modals.length > 0;
                        })()
                    """)
                    if not has_popup:
                        if tentativa > 0:
                            logger.info("[UpSeller] ESC fechou popup")
                        break
                except Exception:
                    pass

            # Se nao encontrou nenhum popup nesta tentativa, parar
            if not popup_encontrado:
                break

            await self._page.wait_for_timeout(300)

        # Garantia final: esconder qualquer coisa que possa estar bloqueando
        try:
            await self._page.evaluate("""
                (() => {
                    // Remover iframes de YouTube visíveis (tutoriais)
                    document.querySelectorAll('iframe[src*="youtube"], iframe[src*="youtu.be"]').forEach(iframe => {
                        const container = iframe.closest('div[style], div[class*="modal"], div[class*="popup"], div[class*="tutorial"], div[class*="intro"]');
                        if (container) container.style.display = 'none';
                    });
                    // Esconder overlays
                    const nav = document.getElementById('myNav');
                    if (nav) nav.style.display = 'none';
                })()
            """)
        except Exception:
            pass

    async def listar_lojas_pendentes(self) -> dict:
        """
        Le os pedidos pendentes no UpSeller e agrupa por loja.
        NAO faz nenhuma acao - apenas leitura.

        Retorna:
            {
                "lojas": [
                    {"nome": "Loja X", "marketplace": "Shopee", "pedidos": 12,
                     "orders": ["260224ABC...", ...]}
                ],
                "total_pedidos": 34,
                "sucesso": True
            }
        """
        if not self._page:
            await self._iniciar_navegador()

        if not await self._esta_logado():
            if not await self.login():
                return {"lojas": [], "total_pedidos": 0, "sucesso": False, "erro": "Nao logado"}

        logger.info("[UpSeller] Listando lojas com pedidos pendentes...")
        resultado = {"lojas": [], "total_pedidos": 0, "sucesso": False}

        try:
            # ===== NAVEGAR para pagina "Processando Pedidos" =====
            # Usar /order/to-ship que mostra "Para Enviar" (onde ficam os pedidos pendentes)
            # NAO usar /pt/order/in-process que mostra "Para Imprimir" (vazio antes de programar)
            await self._page.goto(UPSELLER_PEDIDOS, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(3000)

            # ===== FECHAR POPUPS/TUTORIAIS que bloqueiam a pagina =====
            await self._fechar_popups()
            await self._page.wait_for_timeout(500)
            await self._fechar_popups()
            await self._page.wait_for_timeout(500)

            # ===== VERIFICAR se popup redirecionou para pagina errada =====
            # O tutorial "Emitir Nota Fiscal" pode navegar para "Para Emitir"
            current_url = self._page.url
            if '/order/to-ship' not in current_url:
                logger.warning(f"[UpSeller] Popup redirecionou para {current_url}, re-navegando...")
                await self._page.goto(UPSELLER_PEDIDOS, wait_until="domcontentloaded", timeout=30000)
                await self._page.wait_for_timeout(2000)
                await self._fechar_popups()

            # Screenshot APOS fechar popups
            await self.screenshot("listar_00_apos_fechar_popups")

            # ===== EXTRAIR contagem de pedidos do sidebar =====
            # O sidebar mostra: Para Reservar 0, Para Emitir 13, Para Enviar 66, Para Imprimir 0, etc.
            sidebar_info = await self._page.evaluate("""
                (() => {
                    const info = {};
                    // Buscar TODOS os elementos que possam conter contagens do sidebar
                    const allEls = document.querySelectorAll('li, a, div, span');
                    for (const el of allEls) {
                        const text = (el.textContent || '').trim();
                        // Padroes: "Para Enviar 66" ou "Para Enviar (66)"
                        const patterns = [
                            /^(Para (?:Reservar|Emitir|Enviar|Imprimir|Retirada))\s+(\d+)$/,
                            /^(Para (?:Reservar|Emitir|Enviar|Imprimir|Retirada))\s*[(\[]\s*(\d+)/,
                            /^(Programando|Enviado|Fatura Pendente)\s+(\d+)$/,
                        ];
                        for (const p of patterns) {
                            const m = text.match(p);
                            if (m) {
                                const key = m[1].trim();
                                const val = parseInt(m[2]);
                                if (!info[key] || val > info[key]) info[key] = val;
                                break;
                            }
                        }
                    }
                    return info;
                })()
            """)
            logger.info(f"[UpSeller] Sidebar info: {sidebar_info}")

            # Total de pedidos pendentes = Para Enviar + Para Emitir (ambos precisam ser processados)
            total_para_enviar = 0
            total_para_emitir = 0
            if isinstance(sidebar_info, dict):
                total_para_enviar = sidebar_info.get('Para Enviar', 0)
                total_para_emitir = sidebar_info.get('Para Emitir', 0)

            para_enviar_count = total_para_enviar  # Guardar para usar no resultado final
            total_sidebar = total_para_enviar + total_para_emitir
            logger.info(f"[UpSeller] Sidebar: Para Enviar={total_para_enviar}, Para Emitir={total_para_emitir}, total={total_sidebar}")

            # ===== GARANTIR que estamos na pagina "Para Enviar" =====
            # IMPORTANTE: usar JS preciso que clica no elemento FOLHA (nao pai)
            # porque has-text() do Playwright faz substring e pode pegar container pai
            clicou_para_enviar = False
            try:
                clicou_para_enviar = await self._page.evaluate("""
                    (() => {
                        // Estrategia: encontrar o MENOR elemento que contem EXATAMENTE "Para Enviar"
                        // no sidebar esquerdo (excluindo elementos de conteudo principal)
                        const sidebar = document.querySelector('.ant-menu, [class*="sidebar"], [class*="menu"], nav');
                        const searchIn = sidebar || document;

                        // Buscar spans/divs/a com texto que COMECA com "Para Enviar"
                        const candidates = searchIn.querySelectorAll('span, a, div, li');
                        let bestMatch = null;
                        let bestSize = Infinity;

                        for (const el of candidates) {
                            // Usar innerText direto (nao textContent que inclui filhos)
                            const directText = el.childNodes.length <= 3
                                ? Array.from(el.childNodes).map(n => n.nodeType === 3 ? n.textContent.trim() : '').join('').trim()
                                : '';
                            const fullText = (el.textContent || '').trim();

                            // Match exato "Para Enviar" (com ou sem numero)
                            const isMatch = directText === 'Para Enviar' ||
                                            fullText === 'Para Enviar' ||
                                            /^Para Enviar$/.test(directText) ||
                                            /^Para Enviar\s+\d+$/.test(fullText);

                            // Nao deve conter "Para Emitir" no mesmo elemento
                            const hasOther = fullText.includes('Para Emitir') ||
                                             fullText.includes('Para Reservar') ||
                                             fullText.includes('Para Imprimir');

                            if (isMatch && !hasOther && el.offsetWidth > 5) {
                                const size = el.offsetWidth * el.offsetHeight;
                                if (size < bestSize && size > 0) {
                                    bestSize = size;
                                    bestMatch = el;
                                }
                            }
                        }

                        if (bestMatch) {
                            bestMatch.click();
                            return true;
                        }
                        return false;
                    })()
                """)
                if clicou_para_enviar:
                    logger.info("[UpSeller] Clicou 'Para Enviar' no sidebar (JS preciso)")
                    await self._page.wait_for_timeout(2000)
            except Exception as e:
                logger.warning(f"[UpSeller] Erro ao clicar Para Enviar: {e}")

            if not clicou_para_enviar:
                # Fallback: usar Playwright text= selector (mais preciso que has-text)
                try:
                    el = self._page.locator('text="Para Enviar"').first
                    if await el.is_visible():
                        await el.click()
                        clicou_para_enviar = True
                        logger.info("[UpSeller] Clicou 'Para Enviar' via locator text=")
                        await self._page.wait_for_timeout(2000)
                except Exception:
                    pass

            # Fechar popups novamente (sidebar click pode re-abrir)
            await self._fechar_popups()

            # ===== VERIFICAR se estamos na pagina "Para Enviar" e nao "Para Emitir" =====
            # O tutorial "Entendido" pode ter navegado para "Para Emitir"
            pagina_check = await self._page.evaluate("""
                (() => {
                    // Verificar qual item do sidebar esta ativo/selecionado
                    const menuItems = document.querySelectorAll('li.ant-menu-item-selected, li.ant-menu-item-active, li[class*="selected"]');
                    for (const item of menuItems) {
                        const text = (item.textContent || '').trim();
                        if (text.includes('Para Emitir')) return 'Para Emitir';
                        if (text.includes('Para Enviar')) return 'Para Enviar';
                        if (text.includes('Para Imprimir')) return 'Para Imprimir';
                    }
                    // Fallback: verificar URL
                    return window.location.pathname;
                })()
            """)
            logger.info(f"[UpSeller] Pagina atual: {pagina_check}")

            if pagina_check and 'Para Emitir' in str(pagina_check):
                logger.warning("[UpSeller] Pagina esta em 'Para Emitir'! Re-navegando para 'Para Enviar'...")
                await self._page.goto(UPSELLER_PEDIDOS, wait_until="domcontentloaded", timeout=30000)
                await self._page.wait_for_timeout(3000)
                await self._fechar_popups()
                await self._page.wait_for_timeout(500)
                await self._fechar_popups()
                # Re-clicar "Para Enviar" no sidebar
                try:
                    await self._page.evaluate("""
                        (() => {
                            const sidebar = document.querySelector('.ant-menu, [class*="sidebar"], [class*="menu"], nav');
                            const searchIn = sidebar || document;
                            const candidates = searchIn.querySelectorAll('span, a, div, li');
                            let bestMatch = null;
                            let bestSize = Infinity;
                            for (const el of candidates) {
                                const fullText = (el.textContent || '').trim();
                                const isMatch = /^Para Enviar(\\s+\\d+)?$/.test(fullText);
                                const hasOther = fullText.includes('Para Emitir') || fullText.includes('Para Reservar') || fullText.includes('Para Imprimir');
                                if (isMatch && !hasOther && el.offsetWidth > 5) {
                                    const size = el.offsetWidth * el.offsetHeight;
                                    if (size < bestSize && size > 0) { bestSize = size; bestMatch = el; }
                                }
                            }
                            if (bestMatch) { bestMatch.click(); return true; }
                            return false;
                        })()
                    """)
                    await self._page.wait_for_timeout(2000)
                except Exception:
                    pass

            await self.screenshot("listar_01_para_enviar")

            # ===== EXTRAIR contagem do texto da pagina =====
            page_text = await self._page.evaluate("document.body.innerText")

            # Se pagina mostra 0 e sidebar mostra 0 tambem, nada a fazer
            total_efetivo = total_sidebar  # Usar sidebar como referencia principal
            logger.info(f"[UpSeller] Sidebar total (Para Enviar + Para Emitir): {total_efetivo}")

            if total_efetivo == 0:
                if "Nenhum Dado" in page_text:
                    resultado["sucesso"] = True
                    resultado["total_pedidos"] = 0
                    return resultado

            # ===== EXTRAIR lojas ITERANDO por todas as sub-abas com pedidos =====
            # Sub-abas dentro de "Para Enviar": Para Programar, Programando, Falha, etc.
            # Precisamos ler de TODAS para nao perder pedidos
            lojas_dict = {}  # {loja_nome: {marketplace, orders: set()}}

            # Identificar sub-abas com pedidos > 0
            sub_tabs_info = await self._page.evaluate("""
                (() => {
                    const tabs = [];
                    const candidates = document.querySelectorAll('[role="tab"], .ant-tabs-tab, [class*="ant-tabs-tab"]');
                    for (const el of candidates) {
                        const text = (el.textContent || '').trim();
                        const match = text.match(/^(.+?)\\s+(\\d+)$/);
                        if (match) {
                            const name = match[1].trim();
                            const count = parseInt(match[2]);
                            if (count > 0) {
                                const rect = el.getBoundingClientRect();
                                tabs.push({ name, count, y: Math.round(rect.y), w: Math.round(rect.width) });
                            }
                        }
                    }
                    return tabs;
                })()
            """)
            logger.info(f"[UpSeller] Sub-abas com pedidos: {sub_tabs_info}")

            # Lista de abas para iterar (priorizar Para Programar, depois outras)
            abas_para_ler = []
            if sub_tabs_info:
                for tab in sub_tabs_info:
                    abas_para_ler.append(tab)
            else:
                # Fallback: ler da aba atual
                abas_para_ler.append({"name": "atual", "count": 0})

            for tab_info in abas_para_ler:
                tab_name = tab_info.get("name", "atual")
                tab_count = tab_info.get("count", 0)

                # Clicar na sub-aba (se nao for a primeira que ja esta ativa)
                if tab_name != "atual" and len(abas_para_ler) > 1:
                    try:
                        clicou_aba = await self._page.evaluate("""
                            (tabName) => {
                                const candidates = document.querySelectorAll('[role="tab"], .ant-tabs-tab, [class*="ant-tabs-tab"]');
                                for (const el of candidates) {
                                    const text = (el.textContent || '').trim();
                                    if (text.startsWith(tabName)) {
                                        el.click();
                                        return true;
                                    }
                                }
                                return false;
                            }
                        """, tab_name)
                        if clicou_aba:
                            await self._page.wait_for_timeout(1500)
                            logger.info(f"[UpSeller] Clicou sub-aba '{tab_name}' ({tab_count})")
                    except Exception:
                        logger.debug(f"[UpSeller] Erro ao clicar aba {tab_name}")
                        continue

                # Ler TODAS as paginas (paginacao: 50/pagina)
                pagina_atual = 1
                max_paginas = 20  # Limite de seguranca
                while pagina_atual <= max_paginas:
                    # Scroll para carregar todas as rows desta pagina
                    await self._scroll_carregar_todos(max_scrolls=10)

                    # Extrair lojas da tabela atual
                    await self._extrair_lojas_da_tabela(lojas_dict)

                    # Verificar se tem proxima pagina
                    tem_proxima = await self._page.evaluate("""
                        (() => {
                            // Botao ">" de proxima pagina (ant-pagination)
                            const nextBtn = document.querySelector(
                                '.ant-pagination-next:not(.ant-pagination-disabled), ' +
                                'li.ant-pagination-next:not(.ant-pagination-disabled) button, ' +
                                'button.ant-pagination-item-link[aria-label="next"]'
                            );
                            if (nextBtn && !nextBtn.closest('.ant-pagination-disabled')) {
                                // Verificar texto "X/Y" para saber se tem mais
                                const pageInfo = document.body.innerText.match(/(\\d+)\\/(\\d+)/);
                                if (pageInfo) {
                                    const current = parseInt(pageInfo[1]);
                                    const total = parseInt(pageInfo[2]);
                                    if (current < total) {
                                        nextBtn.click();
                                        return { clicked: true, page: current + 1, totalPages: total };
                                    }
                                    return { clicked: false, reason: 'last_page' };
                                }
                                // Sem info de pagina, tentar clicar e ver se funciona
                                nextBtn.click();
                                return { clicked: true, page: 'unknown' };
                            }
                            return { clicked: false, reason: 'no_button' };
                        })()
                    """)
                    logger.info(f"[UpSeller] Paginacao pagina {pagina_atual}: {tem_proxima}")

                    if not tem_proxima or not tem_proxima.get('clicked'):
                        break  # Ultima pagina

                    pagina_atual += 1
                    await self._page.wait_for_timeout(2000)  # Esperar proxima pagina carregar
                    await self._page.evaluate("window.scrollTo(0, 0)")  # Voltar ao topo

            # Se nenhuma loja encontrada nas sub-abas, tentar JS generico
            if not lojas_dict:
                try:
                    lojas_js = await self._page.evaluate("""
                        (() => {
                            const lojas = {};
                            const rows = document.querySelectorAll('tr.top_row, tr[class*="top_row"]');
                            rows.forEach((row, idx) => {
                                let loja = 'Desconhecida';
                                let mp = '';
                                const spans = row.querySelectorAll('span');
                                for (const span of spans) {
                                    const t = (span.textContent || '').trim();
                                    if (['Shopee', 'Shein', 'Mercado Livre', 'TikTok', 'Amazon', 'Magalu', 'Kwai'].includes(t)) {
                                        mp = t;
                                    } else if (t.length > 2 && t.length < 50 && !t.startsWith('#') && !t.includes('NF-e') && !t.match(/^\\d/) && !t.match(/^(Combinado|Pendente)/)) {
                                        loja = t;
                                    }
                                }
                                if (!lojas[loja]) lojas[loja] = { marketplace: mp, count: 0 };
                                lojas[loja].count++;
                            });
                            return lojas;
                        })()
                    """)
                    if lojas_js and isinstance(lojas_js, dict) and len(lojas_js) > 0:
                        for nome, info in lojas_js.items():
                            lojas_dict[nome] = {
                                'marketplace': info.get('marketplace', ''),
                                'orders': set([f'_js_{i}' for i in range(info.get('count', 1))])
                            }
                        logger.info(f"[UpSeller] Estrategia JS fallback: {len(lojas_dict)} lojas")
                except Exception:
                    pass

            # Fallback generico se ainda vazio
            if not lojas_dict and total_efetivo > 0:
                lojas_dict['Todas as Lojas'] = {
                    'marketplace': '',
                    'orders': set([f'_pedido_{i}' for i in range(total_efetivo)])
                }
                logger.info(f"[UpSeller] Fallback generico: {total_efetivo} pedidos")

            # Montar resultado
            resultado["lojas"] = [
                {
                    "nome": nome,
                    "marketplace": info['marketplace'],
                    "pedidos": len(info['orders']),
                    "orders": list(info['orders'])[:50],
                }
                for nome, info in sorted(lojas_dict.items())
            ]
            total_lojas = sum(l["pedidos"] for l in resultado["lojas"])

            # Sempre usar o MAIOR entre tabela e sidebar
            resultado["total_pedidos"] = max(total_lojas, para_enviar_count)
            resultado["sucesso"] = True

            logger.info(f"[UpSeller] {len(resultado['lojas'])} lojas, {resultado['total_pedidos']} pedidos (tabela={total_lojas}, sidebar={para_enviar_count})")

            # Incluir info do sidebar no resultado para o frontend
            resultado["sidebar_info"] = sidebar_info if isinstance(sidebar_info, dict) else {}

        except Exception as e:
            logger.error(f"[UpSeller] Erro ao listar lojas pendentes: {e}")
            resultado["erro"] = str(e)
            await self.screenshot("listar_erro")

        return resultado

    # ===== HELPERS: Filtro de loja e configuracao de etiqueta =====

    async def _filtrar_por_loja(self, nome_loja: str) -> bool:
        """
        Filtra pedidos por loja no dropdown multi-checkbox do UpSeller.

        O UpSeller usa um componente customizado (nao ant-select padrao):
        - Trigger: .select_multiple_box > .inp_box com texto "Todas Lojas"
        - Popup: .my_select_dropdown_wrap com search, checkboxes, Cancelar/Salvar
        - Cada loja: label.ant-checkbox-wrapper dentro de .option_list
        - Confirmacao: div "Salvar" dentro de .option_action

        Args:
            nome_loja: Nome da loja para filtrar (ex: "DAHIANE")

        Retorna: True se filtrou com sucesso
        """
        if not nome_loja:
            return True  # Sem filtro = mostra todas

        logger.info(f"[UpSeller] Filtrando por loja: '{nome_loja}'")

        try:
            # 1. Abrir o popup clicando no trigger "Todas Lojas"
            abriu = await self._page.evaluate("""
                (() => {
                    // Buscar o trigger do multi-select de lojas
                    const triggers = document.querySelectorAll('.inp_box.ant-select-selection, .select_multiple_box .inp_box');
                    for (const trigger of triggers) {
                        const text = (trigger.textContent || '').trim();
                        if (text.includes('Todas Lojas') || text.includes('Todas as Lojas') || text.includes('Loja')) {
                            trigger.click();
                            return { found: true, text: text.substring(0, 50) };
                        }
                    }
                    // Fallback: buscar por classe do container
                    const selectBox = document.querySelector('.select_multiple_box');
                    if (selectBox) {
                        const inp = selectBox.querySelector('.inp_box');
                        if (inp) { inp.click(); return { found: true, text: 'select_multiple_box fallback' }; }
                    }
                    return { found: false };
                })()
            """)

            if not abriu or not abriu.get("found"):
                logger.warning("[UpSeller] Trigger 'Todas Lojas' nao encontrado")
                return False

            logger.info(f"[UpSeller] Popup de lojas aberto: {abriu}")
            await self._page.wait_for_timeout(800)

            # 2. Desmarcar "Tudo" se estiver marcado (queremos apenas 1 loja)
            await self._page.evaluate("""
                (() => {
                    const wrap = document.querySelector('.my_select_dropdown_wrap');
                    if (!wrap) return;
                    const allCheck = wrap.querySelector('.all_check label.ant-checkbox-wrapper');
                    if (allCheck && allCheck.classList.contains('ant-checkbox-wrapper-checked')) {
                        allCheck.click(); // Desmarcar "Tudo"
                    }
                })()
            """)
            await self._page.wait_for_timeout(300)

            # 3. Desmarcar todas as lojas que possam estar selecionadas
            await self._page.evaluate("""
                (() => {
                    const wrap = document.querySelector('.my_select_dropdown_wrap');
                    if (!wrap) return;
                    const checked = wrap.querySelectorAll('.option_list label.ant-checkbox-wrapper-checked');
                    for (const cb of checked) { cb.click(); }
                })()
            """)
            await self._page.wait_for_timeout(300)

            # 4. Usar o campo de busca para filtrar (facilita encontrar a loja)
            search_input = await self._page.query_selector('.my_select_dropdown_wrap .option_search input.ant-input')
            if search_input:
                await search_input.fill(nome_loja)
                await self._page.wait_for_timeout(500)
                logger.info(f"[UpSeller] Buscou '{nome_loja}' no campo de pesquisa")

            # 5. Selecionar a loja desejada pelo nome
            selecionou = await self._page.evaluate("""
                (nomeLoja) => {
                    const wrap = document.querySelector('.my_select_dropdown_wrap');
                    if (!wrap) return { selected: false, error: 'wrap not found' };

                    const labels = wrap.querySelectorAll('.option_list label.ant-checkbox-wrapper');
                    const normalizar = (s) => s.toLowerCase().normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').trim();
                    const target = normalizar(nomeLoja);

                    // Match exato primeiro
                    for (const label of labels) {
                        const text = normalizar(label.textContent || '');
                        if (text === target) {
                            if (!label.classList.contains('ant-checkbox-wrapper-checked')) {
                                label.click();
                            }
                            return { selected: true, text: label.textContent.trim(), method: 'exact' };
                        }
                    }

                    // Match parcial (contem)
                    for (const label of labels) {
                        const text = normalizar(label.textContent || '');
                        if (text.includes(target) || target.includes(text)) {
                            if (!label.classList.contains('ant-checkbox-wrapper-checked')) {
                                label.click();
                            }
                            return { selected: true, text: label.textContent.trim(), method: 'partial' };
                        }
                    }

                    return {
                        selected: false,
                        available: Array.from(labels).map(l => l.textContent.trim())
                    };
                }
            """, nome_loja)

            if not selecionou or not selecionou.get("selected"):
                lojas_disp = selecionou.get("available", []) if selecionou else []
                logger.warning(f"[UpSeller] Loja '{nome_loja}' nao encontrada. Disponiveis: {lojas_disp}")
                # Cancelar e fechar popup
                await self._page.evaluate("""
                    (() => {
                        const cancel = document.querySelector('.my_select_dropdown_wrap .option_action .d_ib');
                        if (cancel && cancel.textContent.trim() === 'Cancelar') cancel.click();
                    })()
                """)
                await self._page.wait_for_timeout(500)
                return False

            logger.info(f"[UpSeller] Loja '{selecionou.get('text')}' marcada ({selecionou.get('method')})")

            # 6. Clicar em "Salvar" para aplicar o filtro
            await self._page.evaluate("""
                (() => {
                    const wrap = document.querySelector('.my_select_dropdown_wrap');
                    if (!wrap) return;
                    const actionDivs = wrap.querySelectorAll('.option_action .d_ib, .option_action div');
                    for (const div of actionDivs) {
                        if (div.textContent.trim() === 'Salvar') {
                            div.click();
                            return;
                        }
                    }
                })()
            """)

            await self._page.wait_for_timeout(2000)  # Esperar tabela recarregar
            await self.screenshot(f"filtro_loja_{nome_loja[:20]}")
            logger.info(f"[UpSeller] Filtro por loja '{nome_loja}' aplicado e salvo")
            return True

        except Exception as e:
            logger.error(f"[UpSeller] Erro ao filtrar por loja: {e}")
            return False

    async def _configurar_formato_etiqueta(self) -> bool:
        """
        Verifica se a configuracao de etiqueta ja esta salva no UpSeller.

        As configuracoes de etiqueta sao salvas globalmente em:
        UpSeller > Configuracoes > Configuracoes de Envio > Shopee
        - Tipo: Etiqueta de Envio Personalizada
        - Formato: PDF
        - Tamanho: 10x15cm
        - Lista de Separacao: Habilitada (SKU + Variante)

        Como as configs ja estao salvas, este metodo apenas loga confirmacao.
        Se precisar configurar no futuro, acessar /pt/settings/shipping-shopee.

        Retorna: True (configs ja salvas no UpSeller)
        """
        logger.info("[UpSeller] Formato de etiqueta ja configurado no UpSeller (Personalizada + Lista Sep. 10x15cm)")
        return True

    async def _aguardar_tracking(self, timeout_segundos: int = 120) -> bool:
        """
        Apos programar envio, aguarda ate que os pedidos recebam tracking number.
        Verifica periodicamente se os pedidos sairam de "Para Programar" para "Programando"
        e depois para "Para Imprimir" (com etiquetas prontas).

        Args:
            timeout_segundos: Tempo maximo de espera (default: 120s)

        Retorna: True se etiquetas ficaram disponiveis
        """
        logger.info(f"[UpSeller] Aguardando tracking numbers (timeout: {timeout_segundos}s)...")

        inicio = datetime.now()
        tentativa = 0
        max_tentativas = timeout_segundos // 10  # Verifica a cada 10 segundos

        while tentativa < max_tentativas:
            tentativa += 1
            elapsed = (datetime.now() - inicio).total_seconds()
            logger.info(f"[UpSeller] Verificando tracking... tentativa {tentativa}/{max_tentativas} ({elapsed:.0f}s)")

            # Recarregar pagina Para Imprimir para verificar se ha etiquetas
            try:
                await self._page.goto(UPSELLER_PARA_IMPRIMIR, wait_until="domcontentloaded", timeout=30000)
                await self._page.wait_for_timeout(2000)
                await self._fechar_popups()

                # Verificar contador "Para Imprimir" no sidebar
                count = await self._page.evaluate("""
                    (() => {
                        const items = document.querySelectorAll('li, span, div');
                        for (const item of items) {
                            const text = (item.textContent || '').trim();
                            const m = text.match(/^Para Imprimir\\s+(\\d+)$/);
                            if (m && parseInt(m[1]) > 0) return parseInt(m[1]);
                        }
                        // Verificar tambem se ha linhas na tabela
                        const rows = document.querySelectorAll('tr.top_row, tbody tr');
                        if (rows.length > 0) {
                            const bodyText = document.body.innerText;
                            if (!bodyText.includes('Nenhum Dado')) return rows.length;
                        }
                        return 0;
                    })()
                """)

                if count and count > 0:
                    logger.info(f"[UpSeller] {count} etiquetas disponiveis para impressao!")
                    return True

            except Exception as e:
                logger.warning(f"[UpSeller] Erro ao verificar tracking: {e}")

            # Esperar antes da proxima tentativa
            await self._page.wait_for_timeout(10000)

        logger.warning(f"[UpSeller] Timeout aguardando tracking apos {timeout_segundos}s")
        return False

    async def programar_envio(self, filtro_loja: str = None) -> dict:
        """
        Programa envio dos pedidos pendentes em 'Para Programar'.

        Fluxo CORRETO (atualizado 2026-02-26):
          1. Navega para /order/to-ship (pagina "Para Enviar")
          2. Clica no item "Para Enviar" no sidebar (preciso, evita parent match)
          3. Fecha popups/tutoriais
          4. FILTRA POR LOJA se especificado (dropdown de filtro)
          5. Clica na sub-aba 'Para Programar' dentro do conteudo
          6. Seleciona todos os pedidos
          7. Clica em 'Programar Envio'
          8. Aguarda processamento (pedidos movem para 'Para Imprimir')

        Args:
            filtro_loja: Nome da loja para filtrar (opcional, None = todas)

        Retorna: dict com {total_programados, sucesso, mensagem}
        """
        if not self._page:
            await self._iniciar_navegador()

        if not await self._esta_logado():
            if not await self.login():
                return {"total_programados": 0, "sucesso": False, "mensagem": "Nao logado"}

        logger.info("[UpSeller] Iniciando programacao de envio...")
        resultado = {"total_programados": 0, "sucesso": False, "mensagem": ""}

        try:
            # 1. Navegar para pagina "Para Enviar" (contem sub-aba "Para Programar")
            await self._page.goto(UPSELLER_PEDIDOS, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(3000)

            # 2. Fechar popups/tutoriais
            await self._fechar_popups()
            await self._page.wait_for_timeout(1000)
            await self._fechar_popups()

            # 3. Clicar em "Para Enviar" no sidebar com JS preciso (menor elemento)
            clicou_sidebar = await self._page.evaluate("""
                (() => {
                    const sidebar = document.querySelector('.ant-menu, [class*="sidebar"], [class*="menu"], nav');
                    const searchIn = sidebar || document;
                    const candidates = searchIn.querySelectorAll('span, a, div, li');
                    let bestMatch = null;
                    let bestSize = Infinity;
                    for (const el of candidates) {
                        const directText = el.childNodes.length <= 3
                            ? Array.from(el.childNodes).map(n => n.nodeType === 3 ? n.textContent.trim() : '').join('').trim()
                            : '';
                        const fullText = (el.textContent || '').trim();
                        const isMatch = directText === 'Para Enviar' ||
                                        fullText === 'Para Enviar' ||
                                        /^Para Enviar$/.test(directText) ||
                                        /^Para Enviar\\s+\\d+$/.test(fullText);
                        const hasOther = fullText.includes('Para Emitir') ||
                                         fullText.includes('Para Reservar') ||
                                         fullText.includes('Para Imprimir');
                        if (isMatch && !hasOther && el.offsetWidth > 5) {
                            const size = el.offsetWidth * el.offsetHeight;
                            if (size < bestSize && size > 0) { bestSize = size; bestMatch = el; }
                        }
                    }
                    if (bestMatch) { bestMatch.click(); return true; }
                    return false;
                })()
            """)
            if clicou_sidebar:
                logger.info("[UpSeller] Clicou em 'Para Enviar' no sidebar via JS preciso")
                await self._page.wait_for_timeout(2000)
            else:
                logger.warning("[UpSeller] Sidebar 'Para Enviar' nao encontrado via JS, usando URL direta")

            # Fechar popups novamente (sidebar click pode ativar novos tutoriais)
            await self._fechar_popups()

            # 3.5. FILTRAR POR LOJA se especificado
            if filtro_loja:
                filtrou = await self._filtrar_por_loja(filtro_loja)
                if filtrou:
                    logger.info(f"[UpSeller] Filtrado por loja: {filtro_loja}")
                    await self._page.wait_for_timeout(2000)
                else:
                    logger.warning(f"[UpSeller] Nao conseguiu filtrar por loja '{filtro_loja}', continuando sem filtro")

            await self.screenshot("programar_01_pagina_para_enviar")

            # 4. Clicar na sub-aba "Para Programar" (dentro do conteudo, nao sidebar)
            clicou_tab = await self._page.evaluate("""
                (() => {
                    // Sub-tabs ficam na area de conteudo (role=tab, ou divs dentro do main)
                    const candidates = document.querySelectorAll(
                        '[role="tab"], .ant-tabs-tab, [class*="tab"], span, div, a'
                    );
                    for (const el of candidates) {
                        const text = (el.textContent || '').trim();
                        // Match "Para Programar" ou "Para Programar 94" etc
                        if (/^Para Programar(\\s+\\d+)?$/.test(text)) {
                            // Excluir se contem outros textos (parent container)
                            if (text.includes('Programando') || text.includes('Enviado'))
                                continue;
                            // Verificar que e visivel e pequeno (tab, nao container)
                            const rect = el.getBoundingClientRect();
                            if (rect.width > 5 && rect.width < 500 && rect.height < 100) {
                                el.click();
                                return { clicked: true, text: text, tag: el.tagName };
                            }
                        }
                    }
                    return { clicked: false };
                })()
            """)
            if clicou_tab and clicou_tab.get("clicked"):
                logger.info(f"[UpSeller] Clicou na aba 'Para Programar': {clicou_tab}")
                await self._page.wait_for_timeout(2000)
            else:
                # Fallback: tentar Playwright locators
                logger.info("[UpSeller] Tentando Playwright locator para 'Para Programar'")
                try:
                    tab_loc = self._page.locator('div[role="tab"]:has-text("Para Programar")').first
                    if await tab_loc.count() > 0:
                        await tab_loc.click(timeout=5000)
                        await self._page.wait_for_timeout(2000)
                        logger.info("[UpSeller] Clicou aba 'Para Programar' via locator")
                    else:
                        logger.info("[UpSeller] Aba 'Para Programar' nao encontrada, pode ja estar ativa")
                except Exception as e:
                    logger.warning(f"[UpSeller] Erro ao clicar aba Para Programar: {e}")

            await self._fechar_popups()
            await self.screenshot("programar_02_aba_para_programar")

            # 5. Extrair quantidade de pedidos para programar
            page_text = await self._page.evaluate("document.body.innerText")
            total_match = re.search(r'Para Programar\s*(\d+)', page_text)
            total_para_programar = int(total_match.group(1)) if total_match else 0
            logger.info(f"[UpSeller] Pedidos Para Programar: {total_para_programar}")

            if total_para_programar == 0:
                # Verificar se "Nenhum Dado" ou tabela vazia
                if "Nenhum Dado" in page_text or "Total 0" in page_text:
                    resultado["sucesso"] = True
                    resultado["mensagem"] = "Nenhum pedido para programar"
                    return resultado

            # 6. Selecionar todos os pedidos
            selecionados = 0
            try:
                # Checkbox "selecionar todos" no header da tabela
                select_all = await self._page.wait_for_selector(
                    'thead .ant-checkbox-wrapper, th .ant-checkbox-input, '
                    'thead input[type="checkbox"], '
                    '.ant-table-header .ant-checkbox-wrapper',
                    timeout=5000
                )
                await select_all.click()
                await self._page.wait_for_timeout(1000)
                logger.info("[UpSeller] Checkbox 'selecionar todos' clicado")

                # Extrair quantos foram selecionados
                sel_text = await self._page.evaluate("document.body.innerText")
                sel_match = re.search(r'Selecionado\s*(\d+)', sel_text)
                selecionados = int(sel_match.group(1)) if sel_match else total_para_programar

            except Exception:
                logger.warning("[UpSeller] Checkbox 'selecionar todos' nao encontrado, tentando individuais")
                checkboxes = await self._page.query_selector_all(
                    'tbody .ant-checkbox-input, tr.top_row .ant-checkbox-input, '
                    'tbody input[type="checkbox"]'
                )
                for cb in checkboxes[:100]:
                    try:
                        await cb.click()
                        selecionados += 1
                    except:
                        pass
                await self._page.wait_for_timeout(500)

            logger.info(f"[UpSeller] {selecionados} pedidos selecionados")
            await self.screenshot("programar_03_selecionados")

            if selecionados == 0:
                resultado["mensagem"] = "Nenhum pedido selecionado"
                return resultado

            # 7. Clicar no botao BATCH "Programar Envio" na barra de acoes do topo
            # IMPORTANTE: Ha 2 tipos de "Programar Envio" no DOM:
            #   - Botao BATCH (topo): <button class="ant-btn ant-btn-link"> dentro de #orderArrangeShipmentStep1
            #   - Links PER-ROW: <a class="ant-dropdown-link"> em cada linha da tabela
            # Precisamos clicar APENAS no botao BATCH do topo
            clicou_btn = await self._page.evaluate("""
                (() => {
                    // Estrategia 1: Botao dentro de #orderArrangeShipmentStep1
                    const step1 = document.querySelector('#orderArrangeShipmentStep1');
                    if (step1) {
                        const btn = step1.querySelector('button') || step1;
                        btn.click();
                        return { clicked: true, method: 'orderArrangeShipmentStep1', tag: btn.tagName };
                    }
                    // Estrategia 2: button.ant-btn.ant-btn-link com texto "Programar Envio" no topo
                    const btns = document.querySelectorAll('button.ant-btn.ant-btn-link');
                    for (const btn of btns) {
                        const text = (btn.textContent || '').trim();
                        if (text === 'Programar Envio') {
                            const rect = btn.getBoundingClientRect();
                            if (rect.y < 350 && rect.width > 50) {
                                btn.click();
                                return { clicked: true, method: 'ant-btn-link-top', tag: 'BUTTON', y: Math.round(rect.y) };
                            }
                        }
                    }
                    // Estrategia 3: Botao dentro de .list_btn ou .list_operation
                    const actionBar = document.querySelector('.list_btn, .list_operation');
                    if (actionBar) {
                        const btn = actionBar.querySelector('button');
                        if (btn && btn.textContent.includes('Programar Envio')) {
                            btn.click();
                            return { clicked: true, method: 'list_btn', tag: 'BUTTON' };
                        }
                    }
                    return { clicked: false };
                })()
            """)

            if not clicou_btn or not clicou_btn.get("clicked"):
                # Fallback Playwright - buscar especificamente o botao (nao links <a>)
                logger.warning("[UpSeller] JS nao encontrou botao batch, tentando Playwright")
                btn_programar = await self._page.query_selector(
                    '#orderArrangeShipmentStep1 button, '
                    'button.ant-btn-link:has-text("Programar Envio")'
                )
                if btn_programar:
                    await btn_programar.click()
                    logger.info("[UpSeller] Clicou 'Programar Envio' via Playwright fallback")
                else:
                    logger.error("[UpSeller] Botao 'Programar Envio' BATCH nao encontrado")
                    await self.screenshot("programar_erro_sem_botao")
                    resultado["mensagem"] = "Botao 'Programar Envio' nao encontrado"
                    return resultado
            else:
                logger.info(f"[UpSeller] Clicou 'Programar Envio' BATCH: {clicou_btn}")

            await self._page.wait_for_timeout(2000)
            await self.screenshot("programar_04_apos_click")

            # 8. Modal de "Programar Envio" com tabs de logistica (Entregar na Agencia/Retirada)
            # O modal tem: tabs por metodo de envio, endereco/data por loja, e botao "Programar Envio"
            try:
                modal_btn = await self._page.wait_for_selector(
                    '.ant-modal button.ant-btn-primary',
                    timeout=5000
                )
                if modal_btn:
                    # Verificar texto do botao para garantir que e "Programar Envio" e nao outro
                    btn_text = await modal_btn.evaluate("el => el.textContent.trim()")
                    logger.info(f"[UpSeller] Modal encontrado, botao: '{btn_text}'")
                    await modal_btn.click()
                    logger.info("[UpSeller] Clicou 'Programar Envio' no modal de confirmacao")
                    await self._page.wait_for_timeout(3000)
            except Exception:
                logger.info("[UpSeller] Sem modal de confirmacao de envio")

            # Fechar popups que possam aparecer apos confirmar
            await self._fechar_popups()
            await self.screenshot("programar_05_apos_confirmar_modal")

            # 9. Aguardar processamento - recarregar pagina e verificar se count diminuiu
            # O tab counter NAO atualiza dinamicamente, entao recarregamos a pagina
            logger.info("[UpSeller] Aguardando processamento do UpSeller (10s)...")
            await self._page.wait_for_timeout(10000)

            # Recarregar pagina para ver contadores atualizados
            await self._page.goto(UPSELLER_PEDIDOS, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(3000)
            await self._fechar_popups()

            # Verificar novo total "Para Programar"
            new_text = await self._page.evaluate("document.body.innerText")
            new_match = re.search(r'Para Programar\s*(\d+)', new_text)
            novo_total = int(new_match.group(1)) if new_match else 0
            programados_real = total_para_programar - novo_total
            if programados_real < 0:
                programados_real = selecionados  # fallback

            logger.info(f"[UpSeller] Antes: {total_para_programar}, Agora: {novo_total}, Programados: {programados_real}")
            await self.screenshot("programar_06_finalizado")

            resultado["total_programados"] = programados_real if programados_real > 0 else selecionados
            resultado["sucesso"] = True
            resultado["mensagem"] = f"{resultado['total_programados']} pedidos programados para envio"
            logger.info(f"[UpSeller] Programacao concluida: {resultado['total_programados']} pedidos")

        except Exception as e:
            logger.error(f"[UpSeller] Erro ao programar envio: {e}")
            resultado["mensagem"] = str(e)
            await self.screenshot("programar_erro")

        return resultado

    async def baixar_etiquetas(self, filtro_loja: str = None) -> List[str]:
        """
        Navega para pagina "Para Enviar" e usa "Imprimir em Massa" para baixar PDFs.

        O UpSeller ja esta configurado globalmente para:
        - Etiqueta de Envio Personalizada, PDF, 10x15cm
        - Imprimir Lista de Separacao (SKU + Variante, ordenado por Nome)
        Portanto o "Imprimir em Massa" gera o PDF com etiqueta+lista ja intercalados.

        Fluxo (atualizado 2026-02-26):
          1. Navega para /pt/order/in-process (Para Imprimir)
          2. FILTRA POR LOJA se especificado
          3. Seleciona todos os pedidos
          4. Clica em "Imprimir em Massa" (botao dropdown na barra de acoes)
          5. Aguarda download do PDF (as configs ja estao salvas globalmente)

        Args:
            filtro_loja: Filtrar por nome de loja (opcional)

        Retorna: Lista de caminhos dos PDFs baixados
        """
        if not self._page:
            await self._iniciar_navegador()

        if not await self._esta_logado():
            if not await self.login():
                return []

        logger.info("[UpSeller] Navegando para Para Imprimir (/pt/order/in-process)...")
        pdfs_baixados = []

        try:
            # 1. Navegar diretamente para pagina "Para Imprimir"
            await self._page.goto(UPSELLER_PARA_IMPRIMIR, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(3000)

            # Fechar popup tutorial se existir
            await self._fechar_popups()

            # 2. FILTRAR POR LOJA se especificado
            if filtro_loja:
                filtrou = await self._filtrar_por_loja(filtro_loja)
                if filtrou:
                    logger.info(f"[UpSeller] Etiquetas filtradas por loja: {filtro_loja}")
                    await self._page.wait_for_timeout(2000)
                else:
                    logger.warning(f"[UpSeller] Nao filtrou por loja '{filtro_loja}' em Para Imprimir")

            # Verificar se ha pedidos para imprimir
            page_text = await self._page.evaluate("document.body.innerText")
            if "Nenhum Dado" in page_text or "Total 0" in page_text:
                logger.info("[UpSeller] Para Imprimir tem 0 pedidos, pulando download de etiquetas.")
                return []

            await self.screenshot("etiquetas_01_para_imprimir")

            # 3. Selecionar todos os pedidos
            try:
                select_all = await self._page.wait_for_selector(
                    'thead .ant-checkbox-wrapper, th .ant-checkbox-input, '
                    'thead input[type="checkbox"], '
                    '.ant-table-header .ant-checkbox-wrapper',
                    timeout=5000
                )
                await select_all.click()
                await self._page.wait_for_timeout(1000)
                logger.info("[UpSeller] Checkbox 'selecionar todos' clicado")
            except Exception:
                logger.warning("[UpSeller] Checkbox 'selecionar todos' nao encontrado, tentando individuais")
                has_data = await self._page.query_selector('tr.row, tr.top_row, tbody tr')
                if not has_data:
                    logger.info("[UpSeller] Nenhuma linha de pedido encontrada, pulando.")
                    return []
                checkboxes = await self._page.query_selector_all(
                    'tbody .ant-checkbox-input, tr.top_row .ant-checkbox-input'
                )
                for cb in checkboxes[:50]:
                    try:
                        await cb.click()
                    except:
                        pass

            await self.screenshot("etiquetas_02_selecionados")

            # 4. Clicar em "Imprimir em Massa" (dropdown trigger na barra de acoes)
            # Este botao e: a.ant-btn.ant-btn-link.my_btn.ant-dropdown-trigger
            # O download acontece DIRETAMENTE ao clicar, pois as configs ja estao salvas
            try:
                async with self._page.expect_download(timeout=120000) as download_info:
                    # Clicar "Imprimir em Massa"
                    clicou = await self._page.evaluate("""
                        (() => {
                            // Buscar o botao "Imprimir em Massa" na barra de acoes
                            const btns = document.querySelectorAll(
                                'a.ant-btn.ant-btn-link, a.ant-dropdown-trigger, ' +
                                'button.ant-btn-link, span'
                            );
                            for (const btn of btns) {
                                const text = (btn.textContent || '').trim();
                                if (text === 'Imprimir em Massa' || text.includes('Imprimir em Massa')) {
                                    const rect = btn.getBoundingClientRect();
                                    // Garantir que esta na barra de acoes (topo da pagina)
                                    if (rect.width > 30 && rect.y < 400) {
                                        btn.click();
                                        return { clicked: true, text: text, y: Math.round(rect.y) };
                                    }
                                }
                            }
                            return { clicked: false };
                        })()
                    """)

                    if not clicou or not clicou.get("clicked"):
                        # Fallback: Playwright locator
                        logger.warning("[UpSeller] JS nao encontrou 'Imprimir em Massa', tentando locator")
                        btn_massa = self._page.locator('a:has-text("Imprimir em Massa")').first
                        if await btn_massa.count() > 0:
                            await btn_massa.click(timeout=5000)
                            logger.info("[UpSeller] Clicou 'Imprimir em Massa' via locator")
                        else:
                            logger.error("[UpSeller] Botao 'Imprimir em Massa' nao encontrado")
                            await self.screenshot("etiquetas_sem_botao_imprimir")
                            return []
                    else:
                        logger.info(f"[UpSeller] Clicou 'Imprimir em Massa': {clicou}")

                    await self._page.wait_for_timeout(2000)
                    await self.screenshot("etiquetas_03_apos_imprimir_massa")

                    # O "Imprimir em Massa" pode abrir um dropdown com sub-opcoes
                    # ou iniciar o download direto. Verificar se ha dropdown aberto.
                    dropdown_items = await self._page.evaluate("""
                        (() => {
                            const items = document.querySelectorAll(
                                '.ant-dropdown:not(.ant-dropdown-hidden) li, ' +
                                '.ant-dropdown-menu-item'
                            );
                            if (items.length === 0) return { hasDropdown: false };
                            const options = Array.from(items).map(i => i.textContent.trim());
                            return { hasDropdown: true, options: options };
                        })()
                    """)

                    if dropdown_items and dropdown_items.get("hasDropdown"):
                        logger.info(f"[UpSeller] Dropdown aberto com opcoes: {dropdown_items.get('options')}")
                        # Clicar na primeira opcao (Imprimir Etiquetas ou similar)
                        await self._page.evaluate("""
                            (() => {
                                const items = document.querySelectorAll(
                                    '.ant-dropdown:not(.ant-dropdown-hidden) li'
                                );
                                for (const item of items) {
                                    const text = (item.textContent || '').trim().toLowerCase();
                                    // Priorizar opcao com DDC/casada
                                    if (text.includes('casada') || text.includes('ddc')) {
                                        item.click();
                                        return text;
                                    }
                                }
                                // Fallback: primeira opcao de impressao
                                for (const item of items) {
                                    const text = (item.textContent || '').trim().toLowerCase();
                                    if (text.includes('imprimir') || text.includes('etiqueta')) {
                                        item.click();
                                        return text;
                                    }
                                }
                                // Clicar na primeira opcao
                                if (items.length > 0) { items[0].click(); return items[0].textContent.trim(); }
                                return null;
                            })()
                        """)
                        await self._page.wait_for_timeout(2000)

                    # Se aparece modal de confirmacao, clicar no botao primario
                    try:
                        modal_btn = await self._page.wait_for_selector(
                            '.ant-modal button.ant-btn-primary',
                            timeout=5000
                        )
                        if modal_btn:
                            btn_text = await modal_btn.evaluate("el => el.textContent.trim()")
                            logger.info(f"[UpSeller] Modal encontrado, botao: '{btn_text}'")
                            await modal_btn.click()
                            await self._page.wait_for_timeout(2000)
                    except Exception:
                        logger.info("[UpSeller] Sem modal de confirmacao, aguardando download direto...")

                download = await download_info.value
                filename = download.suggested_filename or f"etiquetas_{filtro_loja or 'todas'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                save_path = os.path.join(self.download_dir, filename)
                await download.save_as(save_path)
                pdfs_baixados.append(save_path)
                logger.info(f"[UpSeller] PDF baixado: {save_path}")

            except Exception as e:
                logger.warning(f"[UpSeller] Download nao capturado via expect_download: {e}")
                # Fallback: verificar filesystem por downloads recentes
                await self._page.wait_for_timeout(5000)
                downloads_novos = self._verificar_downloads_novos("*.pdf")
                if downloads_novos:
                    pdfs_baixados.extend(downloads_novos)
                    logger.info(f"[UpSeller] PDFs encontrados via filesystem: {len(downloads_novos)}")
                else:
                    logger.error("[UpSeller] Nenhum PDF baixado por nenhum metodo")

            await self.screenshot("etiquetas_04_finalizado")

        except Exception as e:
            logger.error(f"[UpSeller] Erro ao baixar etiquetas: {e}")

        logger.info(f"[UpSeller] Total de PDFs baixados: {len(pdfs_baixados)}")
        return pdfs_baixados

    async def exportar_xmls(self, data_inicio: str = None, data_fim: str = None) -> List[str]:
        """
        Exporta XMLs de NF-e do UpSeller por data.

        URL: /order/invoice-manage/brazil-nf-e/issued/recent

        Fluxo REAL do UpSeller (mapeado em 2026-02-25):
          1. Clicar botao "Exportar" → dropdown abre
          2. Selecionar "Exportar por Data (XML)" → modal abre
          3. Selecionar mes no date picker (clicar input, depois clicar <a> no month panel)
          4. Clicar "Exportar" no modal → POST /api/invoice/invoice-export (async)
          5. Aguardar progresso (polling GET /api/check-process) ate 100%
          6. Botao "Baixar" aparece no dialogo de progresso
          7. Clicar "Baixar" → download real do ZIP de XMLs

        Args:
            data_inicio: nao usado (filtro por mes)
            data_fim: nao usado (filtro por mes)

        Retorna: Lista de caminhos dos ZIPs/XMLs baixados
        """
        if not self._page:
            await self._iniciar_navegador()

        if not await self._esta_logado():
            if not await self.login():
                return []

        print("[UpSeller] Exportando XMLs de NF-e...", flush=True)
        xmls_baixados = []

        # Screenshots de debug em pasta permanente (nao e apagada pelo pipeline)
        debug_dir = os.path.join(os.path.dirname(self.download_dir or "/tmp"), "_debug_screenshots")
        os.makedirs(debug_dir, exist_ok=True)

        async def _debug_screenshot(nome):
            path = os.path.join(debug_dir, f"nfe_{nome}_{datetime.now().strftime('%H%M%S')}.png")
            try:
                await self._page.screenshot(path=path, full_page=False)
                print(f"[UpSeller] Screenshot: {path}", flush=True)
            except Exception as ex:
                print(f"[UpSeller] Erro screenshot: {ex}", flush=True)

        try:
            # ====== ETAPA 1: Navegar para pagina NF-e ======
            print(f"[UpSeller] Navegando para {UPSELLER_NFE}...", flush=True)
            await self._page.goto(UPSELLER_NFE, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(3000)
            print(f"[UpSeller] URL atual: {self._page.url}", flush=True)

            # Verificar se ha dados na pagina
            page_text = await self._page.evaluate("document.body.innerText")
            if "Nenhum Dado" in page_text and "Exportar" not in page_text:
                print("[UpSeller] Pagina NF-e sem dados, pulando exportacao.", flush=True)
                return []

            # Esconder overlay #myNav que bloqueia cliques
            await self._page.evaluate("""
                (() => {
                    const nav = document.getElementById('myNav');
                    if (nav) nav.style.display = 'none';
                    document.querySelectorAll('.my_nav_bg').forEach(el => el.style.display = 'none');
                })()
            """)

            # ====== ETAPA 2: Abrir dropdown e selecionar "Exportar por Data (XML)" ======
            try:
                btn_exportar = await self._page.wait_for_selector(
                    'button.ant-btn-primary:has-text("Exportar")',
                    timeout=8000
                )
                await btn_exportar.click(force=True)
                await self._page.wait_for_timeout(2000)
                print("[UpSeller] Clicou botao Exportar", flush=True)
            except Exception as e:
                print(f"[UpSeller] Botao 'Exportar' nao encontrado: {e}", flush=True)
                return []

            # Selecionar "Exportar por Data (XML)" no dropdown
            try:
                btn_por_data = await self._page.wait_for_selector(
                    'li.ant-dropdown-menu-item:has-text("Exportar por Data"), '
                    '.ant-dropdown-menu-item:has-text("por Data"), '
                    'li.f_title:has-text("Exportar por Data")',
                    timeout=5000
                )
                await btn_por_data.click(force=True)
                await self._page.wait_for_timeout(2000)
                print("[UpSeller] Selecionou 'Exportar por Data (XML)'", flush=True)
            except Exception as e:
                print(f"[UpSeller] Dropdown 'Exportar por Data' nao encontrado: {e}", flush=True)
                await _debug_screenshot("02_dropdown_falhou")
                return []

            # ====== ETAPA 3: Selecionar mes no modal ======
            await _debug_screenshot("03_modal_aberto")
            print("[UpSeller] Modal aberto, selecionando mes...", flush=True)

            try:
                # Clicar no campo "Filtrar por mes" para abrir o month picker
                mes_input = await self._page.wait_for_selector(
                    '.ant-modal input[placeholder*="mês"], '
                    '.ant-modal input[placeholder*="mes"], '
                    '.ant-modal .ant-calendar-picker-input:not(.ant-input-disabled)',
                    timeout=5000
                )
                await mes_input.click(force=True)
                await self._page.wait_for_timeout(1000)
                print("[UpSeller] Abriu month picker", flush=True)

                # Mapear mes atual para portugues abreviado
                mes_pt = {
                    1: "jan", 2: "fev", 3: "mar", 4: "abr",
                    5: "mai", 6: "jun", 7: "jul", 8: "ago",
                    9: "set", 10: "out", 11: "nov", 12: "dez"
                }.get(datetime.now().month, "fev")

                # Clicar no <a> dentro do td do mes (Ant Design month panel)
                # IMPORTANTE: clicar no <a>, nao no <td> — o <a> dispara o evento corretamente
                try:
                    await self._page.evaluate(f"""
                        (() => {{
                            const cells = document.querySelectorAll('.ant-calendar-month-panel-cell');
                            for (const cell of cells) {{
                                if (cell.textContent.trim().toLowerCase() === '{mes_pt}') {{
                                    const link = cell.querySelector('a.ant-calendar-month-panel-month');
                                    if (link) {{ link.click(); return true; }}
                                    cell.click();
                                    return true;
                                }}
                            }}
                            // Fallback: clicar no current-cell
                            const current = document.querySelector('.ant-calendar-month-panel-selected-cell a, .ant-calendar-month-panel-current-cell a');
                            if (current) {{ current.click(); return true; }}
                            return false;
                        }})()
                    """)
                    await self._page.wait_for_timeout(1000)
                    print(f"[UpSeller] Selecionou mes: {mes_pt}", flush=True)
                except Exception:
                    # Fallback: digitar data diretamente
                    mes_str = datetime.now().strftime("%m/%Y")
                    print(f"[UpSeller] Fallback: digitando {mes_str}...", flush=True)
                    await mes_input.triple_click()
                    await self._page.keyboard.type(mes_str)
                    await self._page.keyboard.press("Enter")
                    await self._page.wait_for_timeout(1000)

                # Verificar se o mes foi preenchido
                mes_valor = await self._page.evaluate("""
                    (() => {
                        const input = document.querySelector('.ant-modal input[placeholder*="mês"], .ant-modal input[placeholder*="mes"]');
                        return input ? input.value : '';
                    })()
                """)
                print(f"[UpSeller] Valor do campo mes: '{mes_valor}'", flush=True)

            except Exception as e_mes:
                print(f"[UpSeller] Erro ao selecionar mes: {e_mes}", flush=True)
                # Continuar mesmo sem mes — talvez exporte tudo

            await _debug_screenshot("04_mes_selecionado")

            # ====== ETAPA 4: Clicar "Exportar" no modal (inicia processamento async) ======
            try:
                btn_confirm = await self._page.wait_for_selector(
                    '.ant-modal-footer button.ant-btn-primary, '
                    '.ant-modal button.ant-btn-primary:has-text("Exportar")',
                    timeout=8000
                )
                await btn_confirm.click(force=True)
                print("[UpSeller] Clicou Exportar no modal (inicia processamento async)", flush=True)
            except Exception as e:
                print(f"[UpSeller] Botao Exportar do modal nao encontrado: {e}", flush=True)
                await _debug_screenshot("05_sem_btn_exportar_modal")
                return []

            # ====== ETAPA 5: Aguardar processamento (barra de progresso + polling) ======
            # O UpSeller faz POST /api/invoice/invoice-export e depois
            # polling GET /api/check-process ate terminar.
            # Quando termina, aparece botao "Baixar" no dialogo de progresso.
            print("[UpSeller] Aguardando processamento async (max 5min)...", flush=True)

            baixar_btn = None
            max_espera = 300  # 5 minutos maximo
            inicio_espera = datetime.now()

            while (datetime.now() - inicio_espera).total_seconds() < max_espera:
                await self._page.wait_for_timeout(3000)

                # Verificar se botao "Baixar" apareceu
                baixar_btn = await self._page.query_selector(
                    'button:has-text("Baixar"):not([disabled])'
                )
                if baixar_btn:
                    print("[UpSeller] Botao 'Baixar' apareceu! Processamento concluido.", flush=True)
                    break

                # Verificar progresso via texto da pagina
                try:
                    progress_text = await self._page.evaluate("""
                        (() => {
                            // Buscar textos de progresso/sucesso no dialogo
                            const els = document.querySelectorAll('.ant-modal, [class*="dialog"], [class*="modal"]');
                            for (const el of els) {
                                const text = el.textContent || '';
                                if (text.includes('Total') || text.includes('Sucesso') || text.includes('Baixar')) {
                                    return text.substring(0, 300);
                                }
                            }
                            return '';
                        })()
                    """)
                    if progress_text:
                        elapsed = int((datetime.now() - inicio_espera).total_seconds())
                        # Extrair porcentagem se disponivel
                        if "Sucesso" in progress_text and "Baixar" in progress_text:
                            print(f"[UpSeller] Progresso ({elapsed}s): completo!", flush=True)
                        elif "Total" in progress_text:
                            print(f"[UpSeller] Progresso ({elapsed}s): processando...", flush=True)
                except Exception:
                    pass

                # Verificar se houve erro
                try:
                    erro_el = await self._page.query_selector(
                        '.ant-message-error, .ant-notification-notice-error, '
                        '[class*="error"]:has-text("Erro"), [class*="error"]:has-text("falha")'
                    )
                    if erro_el:
                        erro_text = await erro_el.inner_text()
                        print(f"[UpSeller] Erro detectado: {erro_text}", flush=True)
                        await _debug_screenshot("06_erro_processamento")
                        break
                except Exception:
                    pass

            if not baixar_btn:
                # Ultima tentativa — buscar mais amplamente
                baixar_btn = await self._page.query_selector(
                    'button:has-text("Baixar")'
                )

            if not baixar_btn:
                print("[UpSeller] Botao 'Baixar' nao apareceu apos timeout.", flush=True)
                await _debug_screenshot("07_sem_baixar")
                # Tentar via filesystem (download pode ter ido para pasta padrao)
                downloads_novos = self._verificar_downloads_novos("*.zip")
                if downloads_novos:
                    xmls_baixados.extend(downloads_novos)
                    print(f"[UpSeller] ZIPs encontrados via filesystem: {len(downloads_novos)}", flush=True)
                return xmls_baixados

            await _debug_screenshot("08_antes_baixar")

            # ====== ETAPA 6: Clicar "Baixar" para fazer o download real ======
            try:
                async with self._page.expect_download(timeout=120000) as download_info:
                    await baixar_btn.click(force=True)
                    print("[UpSeller] Clicou 'Baixar' - aguardando download...", flush=True)

                download = await download_info.value
                filename = download.suggested_filename or f"xmls_upseller_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
                save_path = os.path.join(self.download_dir, filename)
                await download.save_as(save_path)
                xmls_baixados.append(save_path)
                print(f"[UpSeller] XML ZIP baixado: {save_path}", flush=True)

            except Exception as e:
                print(f"[UpSeller] Download via expect_download falhou: {e}", flush=True)
                await _debug_screenshot("09_download_falhou")
                # Fallback: verificar pasta de downloads do sistema
                await self._page.wait_for_timeout(10000)  # Esperar download finalizar
                downloads_novos = self._verificar_downloads_novos("*.zip", segundos_atras=60)
                if downloads_novos:
                    xmls_baixados.extend(downloads_novos)
                    print(f"[UpSeller] ZIPs encontrados via filesystem: {len(downloads_novos)}", flush=True)
                else:
                    # Tentar na pasta de downloads padrao do Windows
                    pasta_downloads = os.path.join(os.path.expanduser("~"), "Downloads")
                    if os.path.isdir(pasta_downloads):
                        for f in glob.glob(os.path.join(pasta_downloads, "xml_nfe_*.zip")):
                            mtime = datetime.fromtimestamp(os.path.getmtime(f))
                            if (datetime.now() - mtime).total_seconds() < 120:
                                # Mover para download_dir
                                dest = os.path.join(self.download_dir, os.path.basename(f))
                                shutil.copy2(f, dest)
                                xmls_baixados.append(dest)
                                print(f"[UpSeller] ZIP encontrado em Downloads: {f} -> {dest}", flush=True)

            # Fechar dialogo de progresso (se ainda aberto)
            try:
                fechar_btn = await self._page.query_selector(
                    'button:has-text("Fechar"), .ant-modal-close'
                )
                if fechar_btn:
                    await fechar_btn.click(force=True)
            except Exception:
                pass

        except Exception as e:
            print(f"[UpSeller] Erro ao exportar XMLs: {e}", flush=True)
            import traceback
            traceback.print_exc()

        print(f"[UpSeller] Total de XMLs baixados: {len(xmls_baixados)}", flush=True)
        return xmls_baixados

    def _verificar_downloads_novos(self, padrao: str = "*", segundos_atras: int = 120) -> List[str]:
        """Verifica arquivos recentes na pasta de downloads."""
        if not self.download_dir:
            return []
        agora = datetime.now()
        novos = []
        for f in glob.glob(os.path.join(self.download_dir, padrao)):
            mtime = datetime.fromtimestamp(os.path.getmtime(f))
            if (agora - mtime).total_seconds() < segundos_atras:
                novos.append(f)
        return novos

    # ----------------------------------------------------------------
    # EXTRACAO DE DADOS DE PEDIDOS (XLSX)
    # ----------------------------------------------------------------

    async def extrair_dados_pedidos(self, status_filter: str = "para_enviar") -> str:
        """
        Scrapa a lista de pedidos do UpSeller e gera XLSX compativel com
        ProcessadorEtiquetasShopee.

        Navega para: /order/to-ship (Processando Pedidos)
        Sidebar: Para Reservar, Para Emitir, Para Enviar, Para Imprimir, Para Retirada

        Para cada pedido, extrai via tabela my_custom_table:
          - order_sn (cell3: Nº Pedido da Plataforma)
          - tracking_number (cell5: codigo de rastreio quando disponivel)
          - product_info (cell0: nome, variacao, quantidade)

        Gera XLSX na pasta download_dir com colunas:
          order_sn | tracking_number | product_info

        Args:
            status_filter: "para_enviar", "para_imprimir", "para_retirada", "para_emitir"

        Retorna: caminho do XLSX gerado (ou "" se falhou)
        """
        if not self._page:
            await self._iniciar_navegador()

        if not await self._esta_logado():
            if not await self.login():
                return ""

        logger.info("[UpSeller] Extraindo dados de pedidos...")
        pedidos = []

        try:
            # Navegar para pagina de pedidos processando
            await self._page.goto(UPSELLER_PEDIDOS, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(3000)

            # Clicar na aba correta no sidebar esquerdo
            tab_text = {
                "para_enviar": "Para Enviar",
                "para_imprimir": "Para Imprimir",
                "para_retirada": "Para Retirada",
                "para_emitir": "Para Emitir",
                "para_reservar": "Para Reservar",
            }.get(status_filter, "Para Enviar")

            try:
                # Sidebar usa links com texto exato
                tab = await self._page.wait_for_selector(
                    f'a:has-text("{tab_text}"), '
                    f'div:has-text("{tab_text}"):not(:has(div)), '
                    f'span:has-text("{tab_text}"), '
                    f'li:has-text("{tab_text}")',
                    timeout=10000
                )
                await tab.click()
                await self._page.wait_for_load_state("domcontentloaded", timeout=15000)
                await self._page.wait_for_timeout(2000)
            except Exception:
                logger.warning(f"[UpSeller] Aba '{tab_text}' nao encontrada, usando pagina atual...")

            # Aguardar tabela carregar
            try:
                await self._page.wait_for_selector(
                    'table.my_custom_table, tr.top_row, tr.row.my_table_border',
                    timeout=10000
                )
            except Exception:
                logger.warning("[UpSeller] Tabela de pedidos nao carregou, tentando mesmo assim...")

            # Screenshot para debug
            await self.screenshot("pedidos_lista")

            # Extrair pedidos de TODAS as paginas
            pagina_num = 1
            while True:
                logger.info(f"[UpSeller] Processando pagina {pagina_num} de pedidos...")

                # Extrair pedidos desta pagina
                pedidos_pagina = await self._extrair_pedidos_pagina()
                if pedidos_pagina:
                    pedidos.extend(pedidos_pagina)
                    logger.info(f"[UpSeller] Pagina {pagina_num}: {len(pedidos_pagina)} pedidos extraidos")
                else:
                    logger.info(f"[UpSeller] Pagina {pagina_num}: nenhum pedido encontrado")
                    if pagina_num == 1:
                        # Se primeira pagina vazia, tentar metodo alternativo (API XHR)
                        pedidos_alt = await self._extrair_pedidos_alternativo()
                        if pedidos_alt:
                            pedidos.extend(pedidos_alt)
                            logger.info(f"[UpSeller] Metodo alternativo: {len(pedidos_alt)} pedidos")
                    break

                # Tentar navegar para proxima pagina
                proximo = await self._ir_proxima_pagina()
                if not proximo:
                    break
                pagina_num += 1
                await self._page.wait_for_timeout(2000)

        except Exception as e:
            logger.error(f"[UpSeller] Erro ao extrair dados de pedidos: {e}")
            await self.screenshot("pedidos_erro")

        if not pedidos:
            logger.warning("[UpSeller] Nenhum pedido encontrado para gerar XLSX")
            return ""

        # Gerar XLSX
        xlsx_path = self._gerar_xlsx_pedidos(pedidos)
        logger.info(f"[UpSeller] XLSX gerado com {len(pedidos)} pedidos: {xlsx_path}")
        return xlsx_path

    async def _extrair_lojas_da_tabela(self, lojas_dict: dict):
        """
        Extrai lojas e pedidos das rows VISIVEIS da tabela atual.
        Agrega no lojas_dict existente (para combinar multiplas sub-abas).
        """
        top_rows = await self._page.query_selector_all('tr.top_row')
        data_rows = await self._page.query_selector_all('tr.row.my_table_border')
        logger.info(f"[UpSeller] Tabela: {len(top_rows)} top_rows, {len(data_rows)} data_rows")

        if not top_rows:
            return

        for i, top_row in enumerate(top_rows):
            try:
                top_text = await top_row.inner_text()
                loja = ''
                marketplace = ''

                # Buscar nome da loja em spans especificos
                loja_el = await top_row.query_selector('span.d_ib.max_w_160, span[class*="max_w_160"]')
                if loja_el:
                    loja = (await loja_el.inner_text()).strip()

                # Buscar marketplace de img alt/src
                try:
                    mp_from_img = await self._page.evaluate("""
                        (row) => {
                            const imgs = row.querySelectorAll('img');
                            for (const img of imgs) {
                                const alt = (img.alt || '').toLowerCase();
                                const src = (img.src || '').toLowerCase();
                                const title = (img.title || '').toLowerCase();
                                const all = alt + ' ' + src + ' ' + title;
                                if (all.includes('shopee')) return 'Shopee';
                                if (all.includes('shein')) return 'Shein';
                                if (all.includes('mercado') || all.includes('meli')) return 'Mercado Livre';
                                if (all.includes('tiktok')) return 'TikTok';
                                if (all.includes('amazon')) return 'Amazon';
                                if (all.includes('magalu')) return 'Magalu';
                                if (all.includes('kwai')) return 'Kwai';
                            }
                            const spans = row.querySelectorAll('span');
                            const mps = ['Shopee', 'Shein', 'Mercado Livre', 'TikTok', 'Amazon', 'Magalu', 'Kwai'];
                            for (const span of spans) {
                                const t = (span.textContent || '').trim();
                                if (mps.includes(t)) return t;
                            }
                            return '';
                        }
                    """, top_row)
                    if mp_from_img:
                        marketplace = mp_from_img
                except Exception:
                    pass

                # Fallback: regex no texto
                if not marketplace:
                    for mp in ['Shopee', 'Shein', 'Mercado Livre', 'TikTok', 'Amazon', 'Magalu', 'Kwai']:
                        if mp.lower() in top_text.lower():
                            marketplace = mp
                            break

                # Fallback loja: buscar em spans
                if not loja:
                    spans = await top_row.query_selector_all('span')
                    for span in spans:
                        try:
                            t = (await span.inner_text()).strip()
                            if (len(t) > 2 and len(t) < 50 and
                                  not t.startswith('#') and not t.startswith('NF') and
                                  not re.match(r'^\d', t) and
                                  t != marketplace and
                                  t not in ['NF-e', 'NFe', 'Combinado', 'Pendente']):
                                loja = t
                                break
                        except:
                            continue

                # Extrair order_sn do data_row correspondente
                order_sn = ''
                if i < len(data_rows):
                    cells = await data_rows[i].query_selector_all('td')
                    if len(cells) > 3:
                        cell3_text = (await cells[3].inner_text()).strip()
                        linhas_cell = [l.strip() for l in cell3_text.split('\n') if l.strip()]
                        if linhas_cell:
                            order_sn = re.sub(r'\s*(Combinado|Pendente|Processando).*$', '', linhas_cell[0]).strip()

                if not loja:
                    loja = 'Desconhecida'

                if loja not in lojas_dict:
                    lojas_dict[loja] = {'marketplace': marketplace, 'orders': set()}
                elif marketplace and not lojas_dict[loja]['marketplace']:
                    lojas_dict[loja]['marketplace'] = marketplace

                if order_sn:
                    lojas_dict[loja]['orders'].add(order_sn)
                else:
                    lojas_dict[loja]['orders'].add(f'_pedido_{len(lojas_dict[loja]["orders"])}_{i}')

            except Exception as e:
                logger.debug(f"[UpSeller] Erro ao ler linha {i}: {e}")

    async def _scroll_carregar_todos(self, max_scrolls: int = 30):
        """
        Scrolla a pagina para baixo progressivamente para forcar o carregamento
        de todas as rows da tabela (o UpSeller pode usar virtual scroll/lazy load).

        Espera ate que o numero de rows pare de crescer.
        """
        last_count = 0
        for i in range(max_scrolls):
            current_count = await self._page.evaluate(
                "document.querySelectorAll('tr.row.my_table_border').length"
            )
            if current_count == last_count and i > 0:
                # Nao carregou mais, parar
                logger.info(f"[UpSeller] Scroll completo: {current_count} rows carregadas apos {i} scrolls")
                break
            last_count = current_count
            # Scrollar para baixo
            await self._page.evaluate("window.scrollBy(0, 800)")
            await self._page.wait_for_timeout(500)
        # Voltar ao topo
        await self._page.evaluate("window.scrollTo(0, 0)")
        await self._page.wait_for_timeout(500)

    async def _extrair_pedidos_pagina(self) -> List[Dict]:
        """
        Extrai pedidos da pagina atual do UpSeller.

        Estrutura real mapeada (2026-02-24):
        - Tabela: table.my_custom_table
        - Cada pedido = 2 linhas:
          * tr.top_row → #UP_ID, NFe badge, logistica, loja/marketplace
          * tr.row.my_table_border → 8 colunas:
            cell0: produto (nome a.break_spaces, qtd b.nowrap, preco, variacao)
            cell1: valor total
            cell2: destinatario
            cell3: order_sn (Nº Pedido Plataforma) ex: 260224JN1F006K, GSH16336A00NDXQ
            cell4: datas
            cell5: metodo envio + tracking
            cell6: status
            cell7: acoes
        """
        pedidos = []

        # Primeiro: scroll progressivo para carregar todas as rows (lazy load)
        await self._scroll_carregar_todos()

        # Estrategia 1: Estrutura real mapeada do UpSeller
        try:
            top_rows = await self._page.query_selector_all('tr.top_row')
            data_rows = await self._page.query_selector_all('tr.row.my_table_border')

            if top_rows and data_rows:
                count = min(len(top_rows), len(data_rows))
                logger.info(f"[UpSeller] Encontrados {count} pedidos (top_row + data_row)")

                for i in range(count):
                    try:
                        pedido = await self._extrair_dados_par_linhas(top_rows[i], data_rows[i])
                        if pedido and pedido.get('order_sn'):
                            pedidos.append(pedido)
                    except Exception as e:
                        logger.debug(f"[UpSeller] Erro no pedido {i}: {e}")

                if pedidos:
                    logger.info(f"[UpSeller] Estrategia 1 (real): {len(pedidos)} pedidos extraidos")
                    return pedidos
        except Exception as e:
            logger.debug(f"[UpSeller] Estrategia 1 (real mapeada) falhou: {e}")

        # Estrategia 2: Tabela generica (fallback)
        try:
            rows = await self._page.query_selector_all(
                'table tbody tr, .ant-table-tbody tr'
            )
            if rows:
                logger.info(f"[UpSeller] Fallback: {len(rows)} linhas de tabela")
                for row in rows:
                    pedido = await self._extrair_dados_linha_generica(row)
                    if pedido and pedido.get('order_sn'):
                        pedidos.append(pedido)
                if pedidos:
                    return pedidos
        except Exception as e:
            logger.debug(f"[UpSeller] Estrategia 2 (tabela generica) falhou: {e}")

        # Estrategia 3: Extrair do texto inteiro da pagina com regex
        try:
            texto_pagina = await self._page.inner_text('body')
            pedidos = self._parsear_pedidos_texto(texto_pagina)
            if pedidos:
                return pedidos
        except Exception as e:
            logger.debug(f"[UpSeller] Estrategia 3 (texto) falhou: {e}")

        return pedidos

    async def _extrair_dados_par_linhas(self, top_row, data_row) -> Optional[Dict]:
        """
        Extrai dados de um pedido a partir do par (top_row, data_row)
        da tabela real do UpSeller.

        top_row contem: #UP_ID, NFe badge, loja, marketplace
        data_row contem: produto, valor, destinatario, order_sn, tempo, envio, status, acoes
        """
        try:
            # --- TOP ROW: extrair loja e marketplace ---
            top_text = await top_row.inner_text()
            loja = ''
            marketplace = ''
            try:
                loja_el = await top_row.query_selector('span.d_ib.max_w_160, span[class*="max_w_160"]')
                if loja_el:
                    loja = (await loja_el.inner_text()).strip()
                mp_el = await top_row.query_selector('span.f_cl_59:last-of-type')
                if mp_el:
                    marketplace = (await mp_el.inner_text()).strip()
            except Exception:
                pass

            # --- DATA ROW: extrair dados das 8 celulas ---
            cells = await data_row.query_selector_all('td')
            if len(cells) < 6:
                return None

            # Cell 3: order_sn (Nº Pedido da Plataforma)
            order_sn = ''
            try:
                cell3 = cells[3]
                cell3_text = (await cell3.inner_text()).strip()
                # Primeira linha eh o order_sn, pode ter "Combinado" embaixo
                linhas = [l.strip() for l in cell3_text.split('\n') if l.strip()]
                if linhas:
                    order_sn = linhas[0]
                    # Remover "Combinado" ou outros status do order_sn
                    order_sn = re.sub(r'\s*(Combinado|Pendente|Processando).*$', '', order_sn).strip()
            except Exception:
                pass

            # Cell 5: Metodo de envio + tracking
            tracking = ''
            try:
                cell5 = cells[5]
                cell5_text = (await cell5.inner_text()).strip()
                # Tracking: formato GC... (Shein), BR... (Shopee/Correios), etc
                # Procurar codigos longos de rastreio
                m = re.search(r'(GC\d{10,25})', cell5_text)
                if m:
                    tracking = m.group(1)
                else:
                    m = re.search(r'(BR\w{10,25})', cell5_text)
                    if m:
                        tracking = m.group(1)
                    else:
                        # Outros formatos: codigo alfanumerico longo
                        m = re.search(r'\b([A-Z]{2}\d{9,25})\b', cell5_text)
                        if m:
                            tracking = m.group(1)
            except Exception:
                pass

            # Cell 0: Produto(s) - pode ter multiplos .row_item ou multi-prod no mesmo item
            produtos = []
            try:
                cell0 = cells[0]
                row_items = await cell0.query_selector_all('.row_item')
                for item in row_items:
                    prods = await self._extrair_produtos_de_row_item(item)
                    if prods:
                        produtos.extend(prods)

                # Se nao achou via row_item, tentar extrair do texto
                if not produtos:
                    cell0_text = (await cell0.inner_text()).strip()
                    produtos = self._extrair_produtos_do_texto_celula(cell0_text)
            except Exception:
                pass

            if order_sn or tracking:
                return {
                    'order_sn': order_sn,
                    'tracking_number': tracking,
                    'produtos': produtos,
                    'product_info': self._formatar_product_info(produtos),
                    'loja': loja,
                    'marketplace': marketplace,
                }

        except Exception as e:
            logger.debug(f"[UpSeller] Erro ao extrair par de linhas: {e}")

        return None

    async def _extrair_produto_de_row_item(self, item) -> Optional[Dict]:
        """
        Extrai dados de produto de um elemento .row_item da tabela UpSeller.
        NOTA: Este metodo retorna apenas o PRIMEIRO produto.
        Para multi-produto, use _extrair_produtos_de_row_item().

        Estrutura real:
        - a.break_spaces / span.break_spaces → nome do produto
        - b.nowrap.ml_20.f_16 → quantidade (ex: "× 1")
        - span com R$ → preco
        - div texto final → variacao (ex: "Esquerdo +5cm,39/40")
        """
        produtos = await self._extrair_produtos_de_row_item(item)
        return produtos[0] if produtos else None

    async def _extrair_produtos_de_row_item(self, item) -> List[Dict]:
        """
        Extrai TODOS os produtos de um elemento .row_item da tabela UpSeller.
        Suporta pedidos multi-produto (ex: 2 produtos no mesmo row_item).

        Pedidos multi-produto tem multiplos a/span.break_spaces e b.nowrap:
        "H3.TAZ.27/28 × 1 R$ 19.99 TAZ,27/28  H3.CREME/CAFÉ.25/26 × 1 R$ 16.99 CREME/CAFÉ,25/26"
        """
        produtos = []

        try:
            # Buscar TODOS os nomes de produto (Shopee: a.break_spaces, Shein: span.break_spaces)
            nome_els = await item.query_selector_all('a.break_spaces, span.break_spaces')
            qtd_els = await item.query_selector_all('b.nowrap, b[class*="nowrap"]')

            if not nome_els:
                # Fallback: tentar .line_overflow_2 ou .line_overflow
                nome_els = await item.query_selector_all('.line_overflow_2, .line_overflow')

            if not nome_els:
                return produtos

            # Para cada produto encontrado
            for idx, nome_el in enumerate(nome_els):
                nome = (await nome_el.inner_text()).strip()
                if not nome:
                    continue

                # Quantidade correspondente (mesmo indice)
                qtd = '1'
                if idx < len(qtd_els):
                    qtd_text = (await qtd_els[idx].inner_text()).strip()
                    m = re.search(r'(\d+)', qtd_text)
                    if m:
                        qtd = m.group(1)

                # Variacao: extrair do texto completo do item
                # Para multi-produto, usar regex para pegar variacao apos cada produto
                variacao = ''
                item_text = (await item.inner_text()).strip()
                # Procurar variacao APOS o nome deste produto
                # Formato: "NOME × N R$ XX.XX VARIACAO"
                escaped_nome = re.escape(nome)
                m_var = re.search(
                    escaped_nome + r'.*?[×xX]\s*\d+.*?R\$.*?\n\s*(.+?)(?:\n|$)',
                    item_text, re.DOTALL
                )
                if m_var:
                    variacao_candidata = m_var.group(1).strip()
                    # Verificar que nao e outro produto
                    if (not re.match(r'^[×xX]\s*\d', variacao_candidata) and
                        not re.match(r'^R\$', variacao_candidata) and
                        len(variacao_candidata) > 1 and
                        len(variacao_candidata) < 80):
                        variacao = variacao_candidata

                # Se nao achou variacao via regex, tentar posicional
                if not variacao:
                    linhas = [l.strip() for l in item_text.split('\n') if l.strip()]
                    # Encontrar a variacao correspondente ao produto
                    # Padrao: nome, × N, R$ XX, variacao, (proximo produto ou fim)
                    found_nome = False
                    for linha in linhas:
                        if nome in linha:
                            found_nome = True
                            continue
                        if found_nome:
                            if (not re.match(r'^[×xX]\s*\d', linha) and
                                not re.match(r'^R\$', linha) and
                                not any(nome_el2 != nome_el and linha in (await nome_el2.inner_text()) for nome_el2 in nome_els[:0]) and
                                len(linha) > 1):
                                variacao = linha
                                break

                produtos.append({
                    'sku': nome,  # Usar nome como SKU
                    'nome': nome,
                    'variacao': variacao,
                    'qtd': qtd,
                })

        except Exception as e:
            logger.debug(f"[UpSeller] Erro ao extrair produtos de row_item: {e}")

        return produtos

    def _extrair_produtos_do_texto_celula(self, texto: str) -> List[Dict]:
        """
        Extrai produtos do texto de uma celula de produto (cell0).
        Fallback quando .row_item nao funciona.

        Texto tipico: "DMEVA-Esquerdo +5cm-39/40 × 1 R$ 117.01 Esquerdo +5cm,39/40"
        """
        produtos = []
        if not texto:
            return produtos

        linhas = [l.strip() for l in texto.split('\n') if l.strip()]

        i = 0
        while i < len(linhas):
            nome = ''
            qtd = '1'
            variacao = ''

            # Linha com nome do produto (nao comeca com R$, x, ou numero puro)
            if (not re.match(r'^[×xX]\s*\d', linhas[i]) and
                not re.match(r'^R\$', linhas[i]) and
                len(linhas[i]) > 3):
                nome = linhas[i]

                # Proximas linhas: quantidade, preco, variacao
                j = i + 1
                while j < len(linhas) and j < i + 4:
                    m_qtd = re.match(r'^[×xX]\s*(\d+)', linhas[j])
                    if m_qtd:
                        qtd = m_qtd.group(1)
                    elif not re.match(r'^R\$', linhas[j]):
                        # Provavel variacao
                        variacao = linhas[j]
                    j += 1

                if nome:
                    produtos.append({
                        'sku': nome,
                        'nome': nome,
                        'variacao': variacao,
                        'qtd': qtd,
                    })
                i = j
            else:
                i += 1

        return produtos

    async def _extrair_dados_linha_generica(self, row) -> Optional[Dict]:
        """Extrai dados de uma linha de tabela generica (fallback)."""
        try:
            texto = await row.inner_text()
            if not texto or len(texto.strip()) < 10:
                return None

            order_sn = ''
            # Shopee: 260210A88XUUY8 (6 digitos + alfanum)
            m = re.search(r'\b(\d{6}[A-Z0-9]{6,10})\b', texto)
            if m:
                order_sn = m.group(1)
            else:
                # Shein: GSH...
                m = re.search(r'\b(GSH\w{10,20})\b', texto)
                if m:
                    order_sn = m.group(1)
                else:
                    # Mercado Livre: numero longo
                    m = re.search(r'\b(\d{10,13})\b', texto)
                    if m:
                        order_sn = m.group(1)

            tracking = ''
            m = re.search(r'(BR\w{10,25})', texto)
            if m:
                tracking = m.group(1)
            else:
                m = re.search(r'(GC\d{10,25})', texto)
                if m:
                    tracking = m.group(1)

            produtos = self._extrair_produtos_do_texto(texto)

            if order_sn or tracking:
                return {
                    'order_sn': order_sn,
                    'tracking_number': tracking,
                    'produtos': produtos,
                    'product_info': self._formatar_product_info(produtos),
                }

        except Exception as e:
            logger.debug(f"[UpSeller] Erro ao extrair linha generica: {e}")

        return None

    # Os metodos antigos _extrair_dados_card e _extrair_produtos_de_elemento
    # foram substituidos por _extrair_dados_par_linhas e _extrair_produto_de_row_item
    # que usam os seletores reais mapeados da pagina UpSeller.

    def _extrair_produtos_do_texto(self, texto: str) -> List[Dict]:
        """
        Extrai dados de produto a partir de texto bruto.
        Tenta reconhecer padroes comuns de listagem de produtos.
        """
        import re
        produtos = []

        if not texto:
            return produtos

        linhas = [l.strip() for l in texto.split('\n') if l.strip()]

        # Padrao 1: "SKU: XXX - Produto - Variacao x Qtd"
        for linha in linhas:
            m = re.match(
                r'(?:SKU[:\s]*)?([A-Za-z0-9_-]+)\s*[-|]\s*(.+?)\s*[-|]\s*(.+?)\s*[xX×]\s*(\d+)',
                linha
            )
            if m:
                produtos.append({
                    'sku': m.group(1).strip(),
                    'nome': m.group(2).strip(),
                    'variacao': m.group(3).strip(),
                    'qtd': m.group(4),
                })
                continue

            # Padrao 2: "NomeProduto (Variacao) x2"
            m = re.match(r'(.+?)\s*\((.+?)\)\s*[xX×]\s*(\d+)', linha)
            if m:
                produtos.append({
                    'sku': '',
                    'nome': m.group(1).strip(),
                    'variacao': m.group(2).strip(),
                    'qtd': m.group(3),
                })
                continue

            # Padrao 3: "x2 NomeProduto"
            m = re.match(r'[xX×](\d+)\s+(.+)', linha)
            if m:
                produtos.append({
                    'sku': '',
                    'nome': m.group(2).strip(),
                    'variacao': '',
                    'qtd': m.group(1),
                })

        return produtos

    def _parsear_pedidos_texto(self, texto: str) -> List[Dict]:
        """
        Metodo de fallback: extrai pedidos do texto inteiro da pagina usando regex.
        Util quando a estrutura DOM nao e facilmente parseavel.
        """
        import re
        pedidos = []

        # Encontrar blocos de pedidos separados por order_sn ou tracking
        # Shopee order_sn: YYMMDD + alfanumerico
        order_sns = re.findall(r'\b(\d{6}[A-Z0-9]{6,10})\b', texto)
        trackings = re.findall(r'(BR\w{10,25})', texto)

        # Criar pedidos a partir dos matches
        for i, osn in enumerate(order_sns):
            tracking = trackings[i] if i < len(trackings) else ''
            pedidos.append({
                'order_sn': osn,
                'tracking_number': tracking,
                'produtos': [],
                'product_info': '',
            })

        return pedidos

    async def _extrair_pedidos_alternativo(self) -> List[Dict]:
        """
        Metodo alternativo: tenta interceptar requests XHR/API internas do UpSeller
        para obter dados de pedidos em formato JSON.
        """
        pedidos = []

        try:
            # Interceptar respostas de API
            respostas = []

            async def capturar_resposta(response):
                url = response.url
                # APIs comuns de listagem de pedidos
                if any(kw in url for kw in ['order', 'pedido', 'list', 'api']):
                    if response.status == 200:
                        try:
                            body = await response.json()
                            respostas.append({'url': url, 'data': body})
                        except Exception:
                            pass

            self._page.on("response", capturar_resposta)

            # Recarregar pagina para capturar requests
            await self._page.reload(wait_until="domcontentloaded")
            await self._page.wait_for_timeout(5000)

            # Remover listener
            self._page.remove_listener("response", capturar_resposta)

            # Analisar respostas capturadas
            for resp in respostas:
                data = resp['data']
                logger.info(f"[UpSeller] API capturada: {resp['url']}")

                # Tentar extrair pedidos do JSON
                items = []
                if isinstance(data, dict):
                    # Procurar lista de pedidos em varias posicoes do JSON
                    for key in ['data', 'items', 'orders', 'list', 'results', 'records']:
                        if key in data and isinstance(data[key], list):
                            items = data[key]
                            break
                    # Se data.data e dict, tentar data.data.items etc
                    if not items and 'data' in data and isinstance(data['data'], dict):
                        inner = data['data']
                        for key in ['items', 'orders', 'list', 'results', 'records']:
                            if key in inner and isinstance(inner[key], list):
                                items = inner[key]
                                break
                elif isinstance(data, list):
                    items = data

                for item in items:
                    if not isinstance(item, dict):
                        continue

                    # Extrair campos - nomes de campos podem variar
                    order_sn = ''
                    tracking = ''
                    produtos = []

                    for field in ['order_sn', 'orderSn', 'order_id', 'orderId',
                                  'platform_order_id', 'marketplace_order_id', 'reference_no']:
                        if field in item and item[field]:
                            order_sn = str(item[field]).strip()
                            break

                    for field in ['tracking_number', 'trackingNumber', 'tracking_no',
                                  'shipping_tracking', 'logistics_tracking']:
                        if field in item and item[field]:
                            tracking = str(item[field]).strip()
                            break

                    # Extrair produtos do JSON
                    for field in ['products', 'items', 'order_items', 'orderItems',
                                  'line_items', 'goods', 'skus']:
                        if field in item and isinstance(item[field], list):
                            for prod in item[field]:
                                if not isinstance(prod, dict):
                                    continue
                                sku = ''
                                nome = ''
                                variacao = ''
                                qtd = '1'
                                for sf in ['sku', 'SKU', 'sku_code', 'parent_sku',
                                           'seller_sku', 'item_sku', 'sku_id']:
                                    if sf in prod and prod[sf]:
                                        sku = str(prod[sf]).strip()
                                        break
                                for sf in ['name', 'product_name', 'item_name',
                                           'title', 'goods_name']:
                                    if sf in prod and prod[sf]:
                                        nome = str(prod[sf]).strip()
                                        break
                                for sf in ['variation', 'variant', 'spec',
                                           'variation_name', 'option']:
                                    if sf in prod and prod[sf]:
                                        variacao = str(prod[sf]).strip()
                                        break
                                for sf in ['quantity', 'qty', 'count', 'num']:
                                    if sf in prod and prod[sf]:
                                        qtd = str(int(float(str(prod[sf]))))
                                        break
                                produtos.append({
                                    'sku': sku,
                                    'nome': nome,
                                    'variacao': variacao,
                                    'qtd': qtd,
                                })
                            break

                    if order_sn or tracking:
                        pedidos.append({
                            'order_sn': order_sn,
                            'tracking_number': tracking,
                            'produtos': produtos,
                            'product_info': self._formatar_product_info(produtos),
                        })

            if pedidos:
                logger.info(f"[UpSeller] {len(pedidos)} pedidos extraidos via API interna")

        except Exception as e:
            logger.error(f"[UpSeller] Erro no metodo alternativo (XHR): {e}")

        return pedidos

    async def _ir_proxima_pagina(self) -> bool:
        """Tenta navegar para a proxima pagina da lista de pedidos."""
        try:
            # Seletores comuns de paginacao
            next_btn = await self._page.query_selector(
                'button[class*="next"], a[class*="next"], '
                'li.next a, li.next button, '
                '.pagination .next, .ant-pagination-next, '
                'button:has-text(">"), a:has-text(">"), '
                'button:has-text("Proximo"), a:has-text("Proximo"), '
                'button:has-text("Próximo"), a:has-text("Próximo")'
            )

            if next_btn:
                is_disabled = await next_btn.get_attribute('disabled')
                class_name = await next_btn.get_attribute('class') or ''
                if is_disabled or 'disabled' in class_name:
                    return False

                await next_btn.click()
                await self._page.wait_for_load_state("domcontentloaded", timeout=15000)
                return True

        except Exception as e:
            logger.debug(f"[UpSeller] Sem proxima pagina: {e}")

        return False

    def _formatar_product_info(self, produtos: List[Dict]) -> str:
        """
        Converte lista de produtos no formato que ProcessadorEtiquetasShopee
        espera no campo product_info do XLSX.

        Entrada: [{"sku": "SKU-001", "nome": "Camiseta", "variacao": "Preta,M", "qtd": "2"}]
        Saida: "[1] Parent SKU Reference No.: SKU-001; Quantity: 2; Product Name: Camiseta; Variation Name: Preta,M;"
        """
        if not produtos:
            return ''

        partes = []
        for i, prod in enumerate(produtos, 1):
            sku = prod.get('sku', '') or ''
            nome = prod.get('nome', '') or prod.get('descricao', '') or ''
            variacao = prod.get('variacao', '') or ''
            qtd = prod.get('qtd', '1') or '1'

            parte = (
                f"[{i}] Parent SKU Reference No.: {sku}; "
                f"Quantity: {qtd}; "
                f"Product Name: {nome}; "
                f"Variation Name: {variacao};"
            )
            partes.append(parte)

        return ' '.join(partes)

    def _gerar_xlsx_pedidos(self, pedidos: List[Dict]) -> str:
        """
        Gera XLSX no formato compativel com ProcessadorEtiquetasShopee.

        Colunas: order_sn | tracking_number | product_info

        O formato de product_info segue o padrao de exportacao da Shopee:
        "[1] Parent SKU Reference No.: XXX; Quantity: N; Product Name: YYY; Variation Name: ZZZ;"
        """
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Pedidos"

        # Cabecalho (mesmo formato que export Shopee)
        ws.append(['order_sn', 'tracking_number', 'product_info'])

        pedidos_validos = 0
        for p in pedidos:
            order_sn = p.get('order_sn', '')
            tracking = p.get('tracking_number', '')
            product_info = p.get('product_info', '')

            if not order_sn and not tracking:
                continue

            ws.append([order_sn, tracking, product_info])
            pedidos_validos += 1

        # Salvar
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"pedidos_upseller_{timestamp}.xlsx"
        path = os.path.join(self.download_dir, filename)
        wb.save(path)
        wb.close()

        logger.info(f"[UpSeller] XLSX gerado: {pedidos_validos} pedidos em {path}")
        return path

    # ----------------------------------------------------------------
    # MOVER ARQUIVOS PARA PASTA DE ENTRADA
    # ----------------------------------------------------------------

    def mover_para_pasta_entrada(self, resultado: dict, pasta_entrada: str) -> dict:
        """
        Move todos os arquivos baixados/gerados para pasta_entrada do usuario.

        Args:
            resultado: dict com chaves "pdfs", "xmls", "xlsx"
            pasta_entrada: caminho da pasta de entrada do processador

        Operacoes:
            - PDFs → copia para pasta_entrada/
            - ZIPs → extrai XMLs para pasta_entrada/
            - XLSX → copia para pasta_entrada/

        Retorna: {"pdfs_movidos": int, "xmls_extraidos": int, "xlsx_copiado": bool, "erros": []}
        """
        os.makedirs(pasta_entrada, exist_ok=True)
        resumo = {
            "pdfs_movidos": 0,
            "xmls_extraidos": 0,
            "xlsx_copiado": False,
            "erros": [],
        }

        # Mover PDFs
        for pdf_path in resultado.get("pdfs", []):
            try:
                if os.path.exists(pdf_path):
                    destino = os.path.join(pasta_entrada, os.path.basename(pdf_path))
                    shutil.copy2(pdf_path, destino)
                    resumo["pdfs_movidos"] += 1
                    logger.info(f"[UpSeller] PDF copiado: {destino}")
            except Exception as e:
                erro = f"Erro ao copiar PDF {pdf_path}: {e}"
                resumo["erros"].append(erro)
                logger.error(f"[UpSeller] {erro}")

        # Extrair XMLs dos ZIPs
        for zip_path in resultado.get("xmls", []):
            try:
                if os.path.exists(zip_path) and zip_path.lower().endswith('.zip'):
                    # Copiar o ZIP inteiro (o processador sabe ler ZIPs)
                    destino = os.path.join(pasta_entrada, os.path.basename(zip_path))
                    shutil.copy2(zip_path, destino)
                    # Contar XMLs dentro
                    with zipfile.ZipFile(zip_path, 'r') as zf:
                        xml_count = sum(1 for n in zf.namelist() if n.lower().endswith('.xml'))
                        resumo["xmls_extraidos"] += xml_count
                    logger.info(f"[UpSeller] ZIP copiado ({xml_count} XMLs): {destino}")
                elif os.path.exists(zip_path) and zip_path.lower().endswith('.xml'):
                    # XML individual
                    destino = os.path.join(pasta_entrada, os.path.basename(zip_path))
                    shutil.copy2(zip_path, destino)
                    resumo["xmls_extraidos"] += 1
            except Exception as e:
                erro = f"Erro ao processar XML/ZIP {zip_path}: {e}"
                resumo["erros"].append(erro)
                logger.error(f"[UpSeller] {erro}")

        # Copiar XLSX
        xlsx_path = resultado.get("xlsx", "")
        if xlsx_path and os.path.exists(xlsx_path):
            try:
                destino = os.path.join(pasta_entrada, os.path.basename(xlsx_path))
                shutil.copy2(xlsx_path, destino)
                resumo["xlsx_copiado"] = True
                logger.info(f"[UpSeller] XLSX copiado: {destino}")
            except Exception as e:
                erro = f"Erro ao copiar XLSX {xlsx_path}: {e}"
                resumo["erros"].append(erro)
                logger.error(f"[UpSeller] {erro}")

        logger.info(
            f"[UpSeller] Resumo: {resumo['pdfs_movidos']} PDFs, "
            f"{resumo['xmls_extraidos']} XMLs, "
            f"XLSX={'sim' if resumo['xlsx_copiado'] else 'nao'}"
        )
        return resumo

    async def fechar(self):
        """Fecha navegador preservando sessao persistente."""
        try:
            if self._context:
                try:
                    await self._context.close()
                except Exception:
                    pass
            if self._browser:
                try:
                    await self._browser.close()
                except Exception:
                    pass
            if self._playwright:
                try:
                    await self._playwright.stop()
                except Exception:
                    pass
            self._page = None
            self._context = None
            self._browser = None
            self._playwright = None
            logger.info("[UpSeller] Navegador fechado (sessao preservada)")
        except Exception as e:
            logger.warning(f"[UpSeller] Erro ao fechar navegador: {e}")

    async def screenshot(self, nome: str = "debug") -> str:
        """Tira screenshot para debug. Retorna caminho do arquivo."""
        if not self._page:
            return ""
        path = os.path.join(self.download_dir or "/tmp", f"screenshot_{nome}_{datetime.now().strftime('%H%M%S')}.png")
        await self._page.screenshot(path=path, full_page=True)
        return path


# =============================================
# FUNCAO UTILITARIA para uso standalone
# =============================================

async def executar_download_completo(config: dict, incluir_xlsx: bool = True) -> dict:
    """
    Executa download completo (dados de pedidos + etiquetas + XMLs) e retorna resultado.

    Pipeline:
    1. Login (sessao persistente)
    2. Extrair dados de pedidos → gerar XLSX (se incluir_xlsx=True)
    3. Baixar etiquetas (PDFs)
    4. Exportar NF-e XMLs (ZIPs)

    Args:
        config: Configuracao do scraper (email, password, etc.)
        incluir_xlsx: Se True, extrai dados de pedidos e gera XLSX

    Retorna:
        {"pdfs": [...], "xmls": [...], "xlsx": str, "sucesso": bool, "erro": str}
    """
    scraper = UpSellerScraper(config)
    resultado = {"pdfs": [], "xmls": [], "xlsx": "", "sucesso": False, "erro": ""}

    try:
        logado = await scraper.login()
        if not logado:
            resultado["erro"] = "Falha no login do UpSeller"
            return resultado

        # 1. Extrair dados de pedidos e gerar XLSX (ANTES de baixar etiquetas)
        if incluir_xlsx:
            try:
                xlsx_path = await scraper.extrair_dados_pedidos()
                resultado["xlsx"] = xlsx_path
                logger.info(f"[UpSeller] XLSX de pedidos: {xlsx_path}")
            except Exception as e:
                logger.warning(f"[UpSeller] Erro ao extrair dados de pedidos (continuando): {e}")

        # 2. Baixar etiquetas (PDFs)
        resultado["pdfs"] = await scraper.baixar_etiquetas()

        # 3. Exportar XMLs
        resultado["xmls"] = await scraper.exportar_xmls()

        resultado["sucesso"] = True

    except Exception as e:
        resultado["erro"] = str(e)
        logger.error(f"[UpSeller] Erro no download completo: {e}")

    finally:
        await scraper.fechar()

    return resultado


async def executar_pipeline_completo(config: dict, pasta_entrada: str) -> dict:
    """
    Pipeline completo: download + mover para pasta_entrada.

    Ideal para uso com agendamento ou botao 'Sincronizar Agora'.

    Args:
        config: Configuracao do scraper
        pasta_entrada: Pasta de entrada do processador

    Retorna:
        {
            "download": {pdfs, xmls, xlsx, sucesso, erro},
            "movidos": {pdfs_movidos, xmls_extraidos, xlsx_copiado, erros},
            "sucesso": bool,
            "erro": str,
        }
    """
    resultado_final = {
        "download": {},
        "movidos": {},
        "sucesso": False,
        "erro": "",
    }

    scraper = UpSellerScraper(config)

    try:
        # Download completo
        logado = await scraper.login()
        if not logado:
            resultado_final["erro"] = "Falha no login do UpSeller"
            return resultado_final

        download = {"pdfs": [], "xmls": [], "xlsx": "", "sucesso": False, "erro": ""}

        # Extrair dados de pedidos
        try:
            xlsx_path = await scraper.extrair_dados_pedidos()
            download["xlsx"] = xlsx_path
        except Exception as e:
            logger.warning(f"[UpSeller] Erro ao extrair pedidos: {e}")

        # Baixar etiquetas
        download["pdfs"] = await scraper.baixar_etiquetas()

        # Exportar XMLs
        download["xmls"] = await scraper.exportar_xmls()

        download["sucesso"] = True
        resultado_final["download"] = download

        # Mover para pasta de entrada
        resumo = scraper.mover_para_pasta_entrada(download, pasta_entrada)
        resultado_final["movidos"] = resumo

        resultado_final["sucesso"] = True

    except Exception as e:
        resultado_final["erro"] = str(e)
        logger.error(f"[UpSeller] Erro no pipeline completo: {e}")

    finally:
        await scraper.fechar()

    return resultado_final
