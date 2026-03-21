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
import hashlib
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Union, Any
from pathlib import Path

logger = logging.getLogger(__name__)

# URLs do UpSeller (mapeadas em 2026-02-24)
UPSELLER_BASE = "https://app.upseller.com"
UPSELLER_LOGIN = f"{UPSELLER_BASE}/pt/login"
UPSELLER_PEDIDOS = f"{UPSELLER_BASE}/order/to-ship"
UPSELLER_PEDIDOS_TODOS = f"{UPSELLER_BASE}/order/all-orders"
UPSELLER_PARA_IMPRIMIR = f"{UPSELLER_BASE}/pt/order/in-process"
UPSELLER_NFE = f"{UPSELLER_BASE}/order/invoice-manage/brazil-nf-e/issued/recent"
UPSELLER_PARA_EMITIR = f"{UPSELLER_BASE}/pt/order/pending-invoice"
UPSELLER_PRINT_SETTING = f"{UPSELLER_BASE}/pt/settings/order/print-setting"


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
        self._ultima_config_etiqueta_ts = None
        self._ultimo_check_ordenacao = 0  # timestamp do ultimo check de dropdown ordenacao

    @staticmethod
    def _arquivo_tabulado_valido(caminho_arquivo: str) -> bool:
        """Valida rapidamente se um arquivo e planilha/csv real (nao HTML da SPA)."""
        try:
            if not caminho_arquivo or not os.path.exists(caminho_arquivo):
                return False

            ext = os.path.splitext(caminho_arquivo)[1].lower()
            with open(caminho_arquivo, "rb") as f:
                head = f.read(4096)

            if not head:
                return False

            head_strip = head.lstrip()
            head_low = head_strip.lower()

            # UpSeller SPA retornando HTML com extensao .xlsx/.csv
            if head_low.startswith(b"<!doctype html") or head_low.startswith(b"<html"):
                return False
            if b"<title>upseller" in head_low[:700]:
                return False

            if ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
                return head.startswith(b"PK\x03\x04")
            if ext == ".xls":
                return head.startswith(b"\xD0\xCF\x11\xE0")

            if ext == ".csv":
                txt = head.decode("utf-8", errors="ignore")
                return ("\n" in txt or "\r" in txt) and ("," in txt or ";" in txt or "\t" in txt)

            # Fallback permissivo para extensoes nao padrao, mas com assinatura conhecida.
            if head.startswith(b"PK\x03\x04") or head.startswith(b"\xD0\xCF\x11\xE0"):
                return True
            txt = head.decode("utf-8", errors="ignore")
            return ("\n" in txt or "\r" in txt) and ("," in txt or ";" in txt or "\t" in txt)
        except Exception:
            return False

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
            await self._page.wait_for_timeout(1200)

            # Se redirecionou para login, nao esta logado.
            url_atual = (self._page.url or "").lower()
            if "/login" in url_atual or "/sign" in url_atual:
                return False

            # Guard-rail: evitar falso positivo quando ainda esta na tela de login.
            eh_tela_login = await self._page.evaluate("""
                (() => {
                    const txt = (document.body?.innerText || '').toLowerCase();
                    const hasPwd = !!document.querySelector('input[type="password"]');
                    const hasCaptcha = txt.includes('captcha');
                    const hasLoginBtn = Array.from(document.querySelectorAll('button, a, span, div'))
                        .some((el) => {
                            const t = (el.textContent || '').trim().toLowerCase();
                            return t === 'login' || t === 'entrar';
                        });
                    const hasLoginHints =
                        txt.includes('esqueci minha senha') ||
                        txt.includes('cadastre-se') ||
                        txt.includes('mantenha-me conectado');
                    return hasPwd && (hasCaptcha || hasLoginBtn || hasLoginHints);
                })()
            """)
            if eh_tela_login:
                return False

            # Verificar marcadores de app autenticado (menus/telas internas).
            app_auth_ok = await self._page.evaluate("""
                (() => {
                    const hasSelectors = !!(
                        document.querySelector('a[href*="/order/"], a[href*="/pt/order/"]') ||
                        document.querySelector('.my_layout_l, .ant-layout-sider, .ant-menu-item') ||
                        document.querySelector('[class*="sidebar"], [class*="menu"]')
                    );
                    if (hasSelectors) return true;
                    const txt = (document.body?.innerText || '').toLowerCase();
                    const keys = [
                        'pedidos', 'compras', 'estoque', 'sac', 'analises', 'financeiro',
                        'para enviar', 'para emitir', 'para imprimir'
                    ];
                    return keys.some((k) => txt.includes(k));
                })()
            """)

            if not app_auth_ok:
                return False
            return True
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

            # ---- Estrategia 1b: remover overlay/tutorial do driver.js ----
            # Esse overlay intercepta ponteiro e bloqueia clique no filtro de loja.
            try:
                removed_driver = await self._page.evaluate("""
                    (() => {
                        let count = 0;
                        const selectors = [
                            'svg.driver-overlay',
                            '.driver-overlay',
                            '.driver-popover',
                            '.driver-stage',
                            '.driver-highlighted-element',
                            '.driver-active-element',
                            '[class*="driver-overlay"]',
                            '[class*="driver-popover"]'
                        ];
                        for (const sel of selectors) {
                            for (const el of document.querySelectorAll(sel)) {
                                try {
                                    el.style.pointerEvents = 'none';
                                    el.style.display = 'none';
                                } catch (e) {}
                                try { el.remove(); } catch (e) {}
                                count++;
                            }
                        }
                        try {
                            document.body.classList.remove(
                                'driver-active',
                                'driver-open',
                                'driver-fix-stacking',
                                'driver-no-interaction'
                            );
                        } catch (e) {}
                        return count;
                    })()
                """)
                if removed_driver and removed_driver > 0:
                    popup_encontrado = True
                    logger.info(f"[UpSeller] Overlay driver removido ({removed_driver})")
            except Exception:
                pass

            # ---- Estrategia 2: Popup tutorial "Introducao de Controle de Pedidos" ----
            # Esse popup tem um X no canto superior direito e contem video YouTube
            try:
                fechou_tutorial = await self._page.evaluate("""
                    (() => {
                        const isVisible = (el) => {
                            if (!el) return false;
                            const st = window.getComputedStyle(el);
                            const r = el.getBoundingClientRect();
                            return st.display !== 'none' && st.visibility !== 'hidden' && r.width > 120 && r.height > 80;
                        };
                        const txtNorm = (s) => (s || '').toLowerCase();

                        // Apenas contêineres realmente de modal/tutorial.
                        const roots = Array.from(document.querySelectorAll(
                            '#myNav, .my_nav_bg, .ant-modal-wrap, .ant-popover, ' +
                            '[class*="tutorial"], [class*="intro"], [class*="guide"], [class*="popup"]'
                        ));

                        for (const el of roots) {
                            if (!isVisible(el)) continue;
                            const text = txtNorm(el.textContent || '');
                            const hasYoutube = !!el.querySelector('iframe[src*="youtube"], iframe[src*="youtu.be"]');
                            const isTutorial =
                                hasYoutube ||
                                text.includes('introdu') ||
                                text.includes('controle de pedidos') ||
                                text.includes('videos tutoriais') ||
                                text.includes('ignorar') ||
                                text.includes('pular');
                            if (!isTutorial) continue;

                            // Fechamento seguro: apenas botões/ações explícitas.
                            const explicitClose = el.querySelector(
                                '.ant-modal-close, button[aria-label="Close"], button[aria-label="Fechar"], .ant-popover-close, .close-btn'
                            );
                            if (explicitClose) {
                                explicitClose.click();
                                return 'close_safe';
                            }

                            const textButtons = Array.from(el.querySelectorAll('button, a, span, div'));
                            for (const node of textButtons) {
                                const t = txtNorm(node.textContent || '').trim();
                                if (t === 'ignorar' || t === 'pular' || t === 'fechar' || t === 'cancelar') {
                                    node.click();
                                    return 'close_text';
                                }
                            }

                            // Fallback seguro: esconder apenas o root do tutorial visível.
                            el.style.display = 'none';
                            return 'hidden_safe';
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

            # ---- Estrategia 3e: Popups de Avisos/Anuncios do UpSeller ----
            # Modal de "Avisos" com anuncios de webinars, novidades etc.
            # Tem botao "Proximo" para paginar mas nenhum "Ignorar"/"Pular".
            # Solucao: fechar via X, botao do footer, ou esconder via JS.
            try:
                fechou_avisos = await self._page.evaluate("""
                    (() => {
                        const modals = document.querySelectorAll('.ant-modal-wrap:not([style*="display: none"])');
                        for (const wrap of modals) {
                            const modal = wrap.querySelector('.ant-modal');
                            if (!modal) continue;
                            const title = (modal.querySelector('.ant-modal-title, .ant-modal-header') || {}).textContent || '';
                            const body = (modal.querySelector('.ant-modal-body') || {}).textContent || '';
                            const combined = (title + ' ' + body).toLowerCase()
                                .normalize('NFD').replace(/[\\u0300-\\u036f]/g, '');
                            // Detectar popups de avisos/anuncios/novidades/webinar
                            const isAviso = combined.includes('aviso') ||
                                            combined.includes('anuncio') ||
                                            combined.includes('novidade') ||
                                            combined.includes('webinar') ||
                                            combined.includes('atualizac') ||
                                            combined.includes('comunicado') ||
                                            combined.includes('noticia') ||
                                            combined.includes('newsletter');
                            if (!isAviso) continue;

                            // Tentar fechar via X
                            const closeBtn = modal.querySelector('.ant-modal-close, button[aria-label="Close"], button[aria-label="Fechar"]');
                            if (closeBtn) {
                                closeBtn.click();
                                return 'close_x';
                            }
                            // Tentar fechar via botao no footer
                            const allBtns = modal.querySelectorAll('button, a.ant-btn');
                            for (const btn of allBtns) {
                                const t = (btn.textContent || '').trim().toLowerCase();
                                if (t === 'fechar' || t === 'ok' || t === 'cancelar' || t === 'entendi' || t === 'got it' || t === 'close') {
                                    btn.click();
                                    return 'close_btn_' + t;
                                }
                            }
                            // Fallback: esconder o modal e a mask via JS
                            wrap.style.display = 'none';
                            const masks = document.querySelectorAll('.ant-modal-mask');
                            masks.forEach(m => m.style.display = 'none');
                            document.body.style.removeProperty('overflow');
                            document.body.classList.remove('ant-scrolling-effect');
                            return 'hidden_js';
                        }
                        return null;
                    })()
                """)
                if fechou_avisos:
                    popup_encontrado = True
                    logger.info(f"[UpSeller] Popup de avisos/anuncios fechado via: {fechou_avisos}")
                    await self._page.wait_for_timeout(500)
            except Exception as e:
                logger.debug(f"[UpSeller] Erro ao fechar popup avisos: {e}")

            # ---- Estrategia 4: ant-modal genericos ----
            try:
                modals = await self._page.query_selector_all('.ant-modal-wrap:not([style*="display: none"])')
                for modal in modals:
                    # Pular modals de negocio (confirmar impressao, configurar etc.)
                    try:
                        modal_text = await modal.inner_text()
                        modal_lower = (modal_text or '').lower()
                        is_business = any(kw in modal_lower for kw in [
                            'marcar como impresso', 'configurar', 'imprimir etiqueta',
                            'confirmar', 'selecionar logistica', 'enviar pedido'
                        ])
                        if is_business:
                            continue
                    except Exception:
                        pass
                    close_btn = await modal.query_selector('.ant-modal-close')
                    if close_btn:
                        await close_btn.click()
                        popup_encontrado = True
                        logger.info("[UpSeller] ant-modal fechado via X")
                        await self._page.wait_for_timeout(500)
                        continue
                    # Fallback: esconder modal sem X via JS
                    try:
                        await modal.evaluate("""
                            (wrap) => {
                                wrap.style.display = 'none';
                                const masks = document.querySelectorAll('.ant-modal-mask');
                                masks.forEach(m => m.style.display = 'none');
                                document.body.style.removeProperty('overflow');
                                document.body.classList.remove('ant-scrolling-effect');
                            }
                        """)
                        popup_encontrado = True
                        logger.info("[UpSeller] ant-modal sem X escondido via JS")
                        await self._page.wait_for_timeout(500)
                    except Exception:
                        pass
            except Exception:
                pass

            # ---- Estrategia 4: Seletores CSS diretos para X/fechar ----
            # IMPORTANTE: evitar seletores genéricos de "close", pois podem
            # clicar no "x" de filtros (ex.: chip da loja) e remover o filtro.
            if not popup_encontrado:
                for selector in [
                    # X do popup tutorial (baseado no screenshot)
                    'div:has(iframe[src*="youtube"]) ~ *:has-text("×")',
                    'div:has(iframe[src*="youtube"]) ~ button',
                    '.ant-modal-wrap .ant-modal-close',
                    '.ant-drawer .ant-drawer-close',
                    '.ant-popover .ant-popover-close',
                    '.ant-modal-wrap button[aria-label="Close"]',
                    '.ant-modal-wrap button[aria-label="Fechar"]',
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

        # Garantia final: NUCLEAR — remover QUALQUER coisa que bloqueie a pagina
        # Isso inclui modais, overlays, masks, drawers, notificacoes, tutoriais etc.
        # So preserva elementos de negocio (confirmar impressao, logistica etc.)
        try:
            resultado_nuclear = await self._page.evaluate("""
                (() => {
                    const log = [];
                    const BUSINESS_KW = [
                        'marcar como impresso', 'configurar impressao',
                        'selecionar logistica', 'enviar pedido',
                        'confirmar envio'
                    ];
                    const isBusiness = (text) => {
                        const t = (text || '').toLowerCase();
                        return BUSINESS_KW.some(kw => t.includes(kw));
                    };

                    // 1. Fechar TODOS os ant-modal-wrap visiveis (exceto business)
                    document.querySelectorAll('.ant-modal-wrap').forEach(wrap => {
                        if (wrap.style.display === 'none') return;
                        const text = wrap.textContent || '';
                        if (isBusiness(text)) return;
                        wrap.style.display = 'none';
                        log.push('modal-wrap');
                    });

                    // 2. Esconder TODAS as masks de modal
                    document.querySelectorAll('.ant-modal-mask, .ant-image-preview-mask').forEach(m => {
                        m.style.display = 'none';
                        log.push('mask');
                    });

                    // 3. Fechar ant-drawer
                    document.querySelectorAll('.ant-drawer:not(.ant-drawer-hidden)').forEach(d => {
                        const text = d.textContent || '';
                        if (isBusiness(text)) return;
                        d.style.display = 'none';
                        log.push('drawer');
                    });

                    // 4. Remover iframes de YouTube (tutoriais)
                    document.querySelectorAll('iframe[src*="youtube"], iframe[src*="youtu.be"]').forEach(iframe => {
                        const container = iframe.closest('div[style], div[class*="modal"], div[class*="popup"], div[class*="tutorial"], div[class*="intro"]');
                        if (container) { container.style.display = 'none'; log.push('youtube'); }
                    });

                    // 5. Esconder overlay #myNav e driver.js
                    const nav = document.getElementById('myNav');
                    if (nav && nav.style.display !== 'none') { nav.style.display = 'none'; log.push('myNav'); }
                    const driverSels = [
                        'svg.driver-overlay', '.driver-overlay', '.driver-popover',
                        '.driver-stage', '.driver-highlighted-element',
                        '[class*="driver-overlay"]', '[class*="driver-popover"]'
                    ];
                    for (const sel of driverSels) {
                        document.querySelectorAll(sel).forEach(el => {
                            try { el.style.pointerEvents = 'none'; el.style.display = 'none'; } catch(e){}
                            try { el.remove(); } catch(e){}
                            log.push('driver');
                        });
                    }

                    // 6. Remover classes de bloqueio do body
                    try {
                        document.body.classList.remove(
                            'driver-active', 'driver-open',
                            'driver-fix-stacking', 'driver-no-interaction',
                            'ant-scrolling-effect'
                        );
                        document.body.style.removeProperty('overflow');
                        document.body.style.removeProperty('padding-right');
                        document.body.style.removeProperty('touch-action');
                    } catch(e){}

                    // 7. Remover qualquer div flutuante com z-index alto que bloqueie cliques
                    // (popups customizados do UpSeller que nao sao ant-modal)
                    document.querySelectorAll('div[style*="z-index"]').forEach(el => {
                        const st = window.getComputedStyle(el);
                        const z = parseInt(st.zIndex || '0');
                        if (z < 1000) return;
                        const r = el.getBoundingClientRect();
                        if (r.width < 200 || r.height < 100) return;
                        // Elemento grande com z-index alto — provavelmente popup/overlay
                        const text = (el.textContent || '').toLowerCase();
                        if (isBusiness(text)) return;
                        // Se cobre mais de 30% da viewport, e um blocker
                        const vpW = window.innerWidth;
                        const vpH = window.innerHeight;
                        if (r.width > vpW * 0.3 && r.height > vpH * 0.3) {
                            el.style.display = 'none';
                            log.push('zindex_overlay:' + z);
                        }
                    });

                    // 8. Remover ant-notification/ant-message que podem cobrir botoes
                    document.querySelectorAll('.ant-notification, .ant-message').forEach(n => {
                        n.style.pointerEvents = 'none';
                    });

                    return log.length > 0 ? log.join(',') : null;
                })()
            """)
            if resultado_nuclear:
                logger.info(f"[UpSeller] Garantia final removeu: {resultado_nuclear}")
        except Exception:
            pass

        # ESC final por seguranca
        try:
            await self._page.keyboard.press("Escape")
            await self._page.wait_for_timeout(200)
            await self._page.keyboard.press("Escape")
        except Exception:
            pass
        # Alguns layouts deixam o menu de ordenacao ("Order") aberto.
        # Isso bloqueia cliques no filtro de loja; fechamos aqui por seguranca.
        try:
            await self._fechar_dropdown_ordenacao()
        except Exception:
            pass

    async def _fechar_dropdown_ordenacao(self, max_tentativas: int = 3, force: bool = False) -> bool:
        """
        Fecha menu de ordenacao (ex.: "Order" com opcoes "Hora do Pagamento")
        quando estiver aberto e interferindo no fluxo.
        Cooldown de 5s para evitar loop de re-verificacoes.
        """
        if not self._page:
            return False
        import time as _time
        agora = _time.time()
        if not force and (agora - self._ultimo_check_ordenacao) < 5:
            return True  # Ja verificado recentemente, pular
        self._ultimo_check_ordenacao = agora
        try:
            detector_js = """
                () => {
                    const norm = (s) => (s || '')
                        .toLowerCase()
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim();
                    const isVisible = (el) => {
                        if (!el) return false;
                        const st = window.getComputedStyle(el);
                        const r = el.getBoundingClientRect();
                        return st.display !== 'none' && st.visibility !== 'hidden' &&
                            r.width > 140 && r.height > 90 && r.width < 560 && r.height < 620;
                    };
                    const nodes = Array.from(document.querySelectorAll('div, ul, section, aside'));
                    for (const n of nodes) {
                        if (!isVisible(n)) continue;
                        const t = norm(n.textContent || '');
                        if (!t) continue;
                        const hasSort =
                            t.includes('hora do pagamento') &&
                            t.includes('expira em') &&
                            (t.includes('anuncio/sku') || t.includes('anuncio / sku') || t.includes('sku (armazem)'));
                        if (hasSort) return true;
                    }
                    return false;
                }
            """

            fechou_algum = False
            aberto = bool(await self._page.evaluate(detector_js))
            if not aberto:
                return True

            # Fechar via JS clicando em area neutra do body (evita clicar em botoes)
            fechou_algum = True
            try:
                await self._page.evaluate("""
                    (() => {
                        // Fechar dropdown clicando em area neutra
                        const overlay = document.querySelector('.ant-dropdown-trigger, .ant-select-open');
                        if (overlay) overlay.click();
                        // Fallback: click no body em area segura
                        document.body.click();
                    })()
                """)
            except Exception:
                pass
            try:
                await self._page.keyboard.press("Escape")
            except Exception:
                pass
            await self._page.wait_for_timeout(300)

            aberto = bool(await self._page.evaluate(detector_js))
            if not aberto:
                logger.info("[UpSeller] Dropdown de ordenacao fechado automaticamente")
            return not aberto
        except Exception:
            return False

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

            # Sempre iniciar leitura global sem filtro de loja ativo.
            # Evita subcontagem quando a sessao ficou presa em uma loja especifica.
            try:
                await self._limpar_filtro_loja()
                await self._page.wait_for_timeout(500)
            except Exception:
                pass
            # Contagem base deve usar a sub-aba "Para Programar" (escopo real de Gerar Pedidos).
            try:
                await self._abrir_subaba_para_programar()
                await self._page.wait_for_timeout(700)
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

            para_programar_count = 0
            try:
                para_programar_count = int(await self._ler_contagem_para_programar() or 0)
            except Exception:
                para_programar_count = 0
            logger.info(
                f"[UpSeller] Contagem da sub-aba Para Programar: {para_programar_count} "
                f"(sidebar Para Enviar={para_enviar_count})"
            )

            # ===== EXTRAIR contagem por loja (metodo manual agregado, blindado) =====
            # Regras:
            # - contagem na sub-aba "Para Programar"
            # - mapeamento de loja por igualdade normalizada (sem contains), para nao misturar BEKA/Beka Shein
            # - sempre incluir todas as lojas do dropdown com 0
            lojas_agregadas = await self._contar_lojas_via_pedidos(
                url=UPSELLER_PEDIDOS,
                nome_aba="Para Programar",
                clicar_para_enviar=True
            )
            nomes_lojas = await self._listar_nomes_lojas_filtro()
            logger.info(
                f"[UpSeller] Lojas dropdown={len(nomes_lojas)} | lojas agregadas={len(lojas_agregadas or [])}"
            )

            def _norm_nome(v):
                return re.sub(r"\s+", " ", (v or "").strip()).casefold()

            mapa_agregado = {}
            for item in (lojas_agregadas or []):
                nome_item = (item.get("nome") or "").strip()
                if not nome_item:
                    continue
                key = _norm_nome(nome_item)
                if not key:
                    continue
                try:
                    pedidos_item = max(0, int(item.get("pedidos", 0) or 0))
                except Exception:
                    pedidos_item = 0
                cur = mapa_agregado.get(key)
                if (not cur) or (pedidos_item > int(cur.get("pedidos", 0) or 0)):
                    mapa_agregado[key] = {
                        "nome": nome_item,
                        "marketplace": (item.get("marketplace") or "").strip(),
                        "pedidos": pedidos_item,
                    }

            lojas_consolidadas = []
            vistos = set()

            # Primeiro: lojas conhecidas no dropdown (lista persistente)
            for nome in (nomes_lojas or []):
                nome_ref = (nome or "").strip()
                if not nome_ref:
                    continue
                key = _norm_nome(nome_ref)
                if not key or key in vistos:
                    continue
                base = mapa_agregado.get(key) or {}
                lojas_consolidadas.append({
                    "nome": nome_ref,
                    "marketplace": (base.get("marketplace") or "").strip(),
                    "pedidos": int(base.get("pedidos", 0) or 0),
                    "orders": [],
                })
                vistos.add(key)

            # Depois: lojas detectadas que nao existem no dropdown (seguranca)
            for key, base in mapa_agregado.items():
                if key in vistos:
                    continue
                lojas_consolidadas.append({
                    "nome": (base.get("nome") or "").strip() or "Desconhecida",
                    "marketplace": (base.get("marketplace") or "").strip(),
                    "pedidos": int(base.get("pedidos", 0) or 0),
                    "orders": [],
                })
                vistos.add(key)

            resultado["lojas"] = sorted(
                lojas_consolidadas,
                key=lambda x: (-(int(x.get("pedidos", 0) or 0)), (x.get("nome") or "").lower())
            )

            total_lojas = sum(int(l.get("pedidos", 0) or 0) for l in (resultado.get("lojas") or []))
            total_ref_pedidos = para_programar_count
            if total_ref_pedidos <= 0 and para_enviar_count > 0 and total_lojas == 0:
                # fallback de conexao em caso de contagem da sub-aba indisponivel
                total_ref_pedidos = para_enviar_count
            resultado["total_pedidos"] = max(total_lojas, total_ref_pedidos)
            resultado["sucesso"] = True

            logger.info(
                f"[UpSeller] Consolidado Para Programar: lojas={len(resultado['lojas'])}, "
                f"soma_lojas={total_lojas}, total_ref={total_ref_pedidos}, total_final={resultado['total_pedidos']}"
            )

            # Incluir info do sidebar no resultado para o frontend
            resultado["sidebar_info"] = sidebar_info if isinstance(sidebar_info, dict) else {}

            # Completar com TODAS as lojas cadastradas no UpSeller (inclusive com 0 pedidos).
            # Isso garante lista persistente completa no backend.
            try:
                nomes_todas = await self._listar_nomes_lojas_filtro()
                if nomes_todas:
                    def _norm(s):
                        return (s or "").strip().casefold()

                    # Base de fallback (scrape agregado) para lojas onde filtro individual falhar.
                    mapa_pendentes = {_norm(l.get("nome", "")): l for l in resultado["lojas"] if l.get("nome")}

                    nomes_unicos = []
                    vistos_nomes = set()
                    for nome in nomes_todas:
                        key = _norm(nome)
                        if key and key not in vistos_nomes:
                            vistos_nomes.add(key)
                            nomes_unicos.append(nome)

                    lojas_completas = []
                    vistos = set()
                    for nome in nomes_unicos:
                        key = _norm(nome)
                        if not key or key in vistos:
                            continue

                        # Fonte unica: mapa agregado da extracao completa.
                        # O filtro loja-a-loja do UpSeller esta instavel e pode contaminar contagens.
                        item_base = mapa_pendentes.get(key) or {}
                        marketplace = (item_base.get("marketplace") or "").strip()
                        if not marketplace:
                            marketplace = ((mapa_pendentes.get(key) or {}).get("marketplace") or "").strip()
                        try:
                            pedidos = int(item_base.get("pedidos", 0) or 0)
                        except Exception:
                            pedidos = 0

                        lojas_completas.append({
                            "nome": nome,
                            "marketplace": marketplace,
                            "pedidos": max(0, pedidos),
                            "orders": [],
                        })
                        vistos.add(key)

                    # Inclui lojas pendentes que nao apareceram no dropdown (fallback de seguranca).
                    for item in resultado["lojas"]:
                        key = _norm(item.get("nome", ""))
                        if key and key not in vistos:
                            try:
                                pedidos_item = int(item.get("pedidos", 0) or 0)
                            except Exception:
                                pedidos_item = 0
                            lojas_completas.append({
                                "nome": item.get("nome", ""),
                                "marketplace": item.get("marketplace", ""),
                                "pedidos": max(0, pedidos_item),
                                "orders": [],
                            })
                            vistos.add(key)

                    resultado["lojas"] = sorted(
                        lojas_completas,
                        key=lambda x: (-(int(x.get("pedidos", 0) or 0)), (x.get("nome") or "").lower())
                    )
                    total_merge = sum(int(l.get("pedidos", 0) or 0) for l in resultado["lojas"])
                    resultado["total_pedidos"] = max(
                        total_merge,
                        int(locals().get("total_ref_pedidos", 0) or 0)
                    )
                    logger.info(
                        f"[UpSeller] Lista completa consolidada: "
                        f"{len(resultado['lojas'])} lojas, total={resultado['total_pedidos']} "
                        f"(merge={total_merge}, ref={int(locals().get('total_ref_pedidos', 0) or 0)})"
                    )
            except Exception as e_merge:
                logger.warning(f"[UpSeller] Nao foi possivel mesclar lista completa de lojas: {e_merge}")

        except Exception as e:
            logger.error(f"[UpSeller] Erro ao listar lojas pendentes: {e}")
            resultado["erro"] = str(e)
            await self.screenshot("listar_erro")

        # === Coletar contagens per-status por loja ===
        # Regras:
        # - Notas pendentes = Para Emitir + Falha na Emissao + Falha ao subir
        # - Etiquetas pendentes = Etiqueta para Impressao (in-process)
        def _norm_nome_local(v):
            return re.sub(r"\s+", " ", (v or "").strip()).casefold()

        def _somar_contagens(*maps):
            soma = {}
            nome_ref = {}
            for mp in maps:
                if not isinstance(mp, dict):
                    continue
                for nome, val in mp.items():
                    nome_limpo = (nome or "").strip()
                    if not nome_limpo:
                        continue
                    key = _norm_nome_local(nome_limpo)
                    if not key:
                        continue
                    try:
                        qtd = max(0, int(val or 0))
                    except Exception:
                        qtd = 0
                    if key not in nome_ref:
                        nome_ref[key] = nome_limpo
                    soma[key] = int(soma.get(key, 0) or 0) + qtd
            return {nome_ref[k]: int(v or 0) for k, v in soma.items()}

        # Emitir: somar abas de falha tambem.
        try:
            logger.info("[UpSeller] Coletando contagens de NF-e por loja (Para Emitir + falhas)...")
            cont_emitir = {}
            cont_falha_emissao = {}
            cont_falha_subir = {}

            # Leitura rapida dos contadores para evitar varrer abas zeradas.
            cont_tabs_nfe = {}
            try:
                await self._page.goto(UPSELLER_PARA_EMITIR, wait_until="domcontentloaded", timeout=30000)
                await self._page.wait_for_timeout(1500)
                await self._fechar_popups()
                cont_tabs_nfe = await self._ler_contadores_tabs_nfe()
            except Exception:
                cont_tabs_nfe = {}

            qtd_emitir = int((cont_tabs_nfe or {}).get("para_emitir", 0) or 0)
            qtd_falha_emissao = int((cont_tabs_nfe or {}).get("falha_na_emissao", 0) or 0)
            qtd_falha_subir = int((cont_tabs_nfe or {}).get("falha_ao_subir", 0) or 0)
            logger.info(
                "[UpSeller] Contadores NF-e (snapshot): "
                f"para_emitir={qtd_emitir}, falha_emissao={qtd_falha_emissao}, falha_subir={qtd_falha_subir}"
            )

            if qtd_emitir > 0:
                cont_emitir = await self._contar_lojas_em_pagina(
                    UPSELLER_PARA_EMITIR, nome_aba="Para Emitir", exigir_aba=True
                )
                if not cont_emitir:
                    cont_emitir = await self._contar_lojas_em_pagina(
                        UPSELLER_PARA_EMITIR, nome_aba="Para Emitir", exigir_aba=False
                    )
            else:
                logger.info("[UpSeller] Aba 'Para Emitir' zerada; pulando contagem por loja.")

            if qtd_falha_emissao > 0:
                cont_falha_emissao = await self._contar_lojas_em_pagina(
                    UPSELLER_PARA_EMITIR, nome_aba="Falha na Emissão", exigir_aba=True
                )
                if not cont_falha_emissao:
                    cont_falha_emissao = await self._contar_lojas_em_pagina(
                        UPSELLER_PARA_EMITIR, nome_aba="Falha na Emissao", exigir_aba=True
                    )
            else:
                logger.info("[UpSeller] Aba 'Falha na Emissao' zerada; pulando contagem por loja.")

            if qtd_falha_subir > 0:
                cont_falha_subir = await self._contar_lojas_em_pagina(
                    UPSELLER_PARA_EMITIR, nome_aba="Falha ao subir", exigir_aba=True
                )
            else:
                logger.info("[UpSeller] Aba 'Falha ao subir' zerada; pulando contagem por loja.")

            contagem_emitir = _somar_contagens(cont_emitir, cont_falha_emissao, cont_falha_subir)
            resultado["contagem_para_emitir"] = contagem_emitir
            logger.info(
                f"[UpSeller] NF-e por loja: base={sum(cont_emitir.values())}, "
                f"falha_emissao={sum(cont_falha_emissao.values())}, "
                f"falha_subir={sum(cont_falha_subir.values())}, "
                f"total={sum(contagem_emitir.values())}"
            )
        except Exception as e:
            logger.warning(f"[UpSeller] Falha ao contar NF-e por loja: {e}")
            resultado["contagem_para_emitir"] = {}

        try:
            logger.info("[UpSeller] Coletando contagens de etiquetas por loja...")
            contagem_imprimir = {}
            sidebar_local = resultado.get("sidebar_info", {}) if isinstance(resultado.get("sidebar_info", {}), dict) else {}
            tem_chave_imprimir = "Para Imprimir" in sidebar_local
            qtd_para_imprimir = int(sidebar_local.get("Para Imprimir", 0) or 0) if tem_chave_imprimir else -1
            if tem_chave_imprimir and qtd_para_imprimir <= 0:
                logger.info("[UpSeller] Sidebar 'Para Imprimir' zerado; pulando contagem de etiquetas por loja.")
            else:
                for aba in ["Etiqueta para Impressão", "Etiqueta para Impressao", "Para Imprimir"]:
                    cont_tmp = await self._contar_lojas_em_pagina(
                        UPSELLER_PARA_IMPRIMIR, nome_aba=aba, exigir_aba=True
                    )
                    if cont_tmp:
                        contagem_imprimir = cont_tmp
                        break
                if not contagem_imprimir:
                    contagem_imprimir = await self._contar_lojas_em_pagina(
                        UPSELLER_PARA_IMPRIMIR, nome_aba=None, exigir_aba=False
                    )
            resultado["contagem_para_imprimir"] = contagem_imprimir
        except Exception as e:
            logger.warning(f"[UpSeller] Falha ao contar etiquetas por loja: {e}")
            resultado["contagem_para_imprimir"] = {}

        # Adicionar contagens per-status a cada loja do resultado, com match normalizado.
        map_emitir_norm = {
            _norm_nome_local(nome): int(val or 0)
            for nome, val in (resultado.get("contagem_para_emitir", {}) or {}).items()
        }
        map_imprimir_norm = {
            _norm_nome_local(nome): int(val or 0)
            for nome, val in (resultado.get("contagem_para_imprimir", {}) or {}).items()
        }
        for loja in resultado.get("lojas", []):
            nome = (loja.get("nome") or "").strip()
            key = _norm_nome_local(nome)
            loja["notas_pendentes"] = int(map_emitir_norm.get(key, 0) or 0)
            loja["etiquetas_pendentes"] = int(map_imprimir_norm.get(key, 0) or 0)

        return resultado

    async def _contar_lojas_em_pagina(
        self, url: str, nome_aba: str = None, exigir_aba: bool = False
    ) -> Dict[str, int]:
        """
        Navega para uma pagina do UpSeller (ex: Para Emitir, Para Imprimir)
        e conta lojas de forma agregada na tabela (sem filtrar loja por loja).

        Retorna: {nome_loja: quantidade}
        """
        contagem = {}
        try:
            lojas = await self._contar_lojas_via_pedidos(
                url=url,
                nome_aba=nome_aba,
                clicar_para_enviar=False,
                exigir_aba=exigir_aba
            )
            for item in lojas or []:
                nome = (item.get("nome") or "").strip()
                if not nome:
                    continue
                try:
                    contagem[nome] = max(0, int(item.get("pedidos", 0) or 0))
                except Exception:
                    contagem[nome] = 0
            logger.info(f"[UpSeller] Contagem agregada '{nome_aba or url}': {contagem}")
        except Exception as e:
            logger.warning(f"[UpSeller] Erro ao contar lojas em '{nome_aba or url}' (agregado): {e}")
        return contagem

    async def _ler_total_sub_abas(self) -> int:
        """
        Le os numeros de TODAS as sub-abas visiveis (ex: "Para Programar 4", "Programando 0")
        e retorna a SOMA. Usado apos filtrar por loja para obter a contagem exata.
        """
        try:
            total = await self._page.evaluate("""
                (() => {
                    let soma = 0;
                    const candidates = document.querySelectorAll('[role="tab"], .ant-tabs-tab, [class*="ant-tabs-tab"]');
                    for (const el of candidates) {
                        const text = (el.textContent || '').trim();
                        const match = text.match(/(\d+)\s*$/);
                        if (match) {
                            soma += parseInt(match[1]);
                        }
                    }
                    return soma;
                })()
            """)
            return int(total) if isinstance(total, (int, float)) else 0
        except Exception as e:
            logger.debug(f"[UpSeller] Erro ao ler total sub-abas: {e}")
            return 0

    async def _limpar_filtro_loja(self):
        """
        Remove todos os filtros de loja selecionados (clica nos X dos chips).
        Restaura a visao de 'todas as lojas'.
        """
        try:
            for _ in range(20):
                removed = await self._page.evaluate("""
                    (() => {
                        const boxes = document.querySelectorAll('.select_multiple_box');
                        for (const box of boxes) {
                            const closeBtn = box.querySelector(
                                '.tag_item .anticon-close, ' +
                                'i[aria-label="icon: close"], ' +
                                '.ant-select-selection-item-remove, ' +
                                '.icon_clear.icon_item.anticon-close-circle'
                            );
                            if (closeBtn) {
                                closeBtn.click();
                                return true;
                            }
                        }
                        return false;
                    })()
                """)
                if not removed:
                    break
                await self._page.wait_for_timeout(300)
            await self._page.wait_for_timeout(1500)
            logger.info("[UpSeller] Filtro de loja limpo (todas as lojas)")
        except Exception as e:
            logger.warning(f"[UpSeller] Erro ao limpar filtro de loja: {e}")

    async def _listar_nomes_lojas_filtro(self) -> List[str]:
        """
        Le o dropdown customizado de lojas e retorna TODOS os nomes cadastrados.
        Nao altera selecoes; abre o dropdown, coleta e fecha.
        """
        nomes = []
        try:
            abriu = await self._page.evaluate("""
                (() => {
                    const trigger =
                        document.querySelector('.select_multiple_box .inp_box') ||
                        document.querySelector('.inp_box.ant-select-selection');
                    if (!trigger) return false;
                    trigger.click();
                    return true;
                })()
            """)
            if not abriu:
                return []

            await self._page.wait_for_timeout(800)

            nomes = await self._page.evaluate("""
                (() => {
                    const out = new Set();
                    const wrap = document.querySelector('.my_select_dropdown_wrap');
                    if (!wrap) return [];

                    const normalize = (s) => (s || '').replace(/\\s+/g, ' ').trim();

                    const lerVisiveis = () => {
                        const labels = wrap.querySelectorAll('.option_list label.ant-checkbox-wrapper, label.ant-checkbox-wrapper');
                        for (const lb of labels) {
                            const txt = normalize(lb.textContent || '');
                            if (!txt) continue;
                            const low = txt.toLowerCase();
                            if (low === 'tudo' || low.includes('selecionar tudo')) continue;
                            out.add(txt);
                        }
                    };

                    // Algumas listas sao virtualizadas; rolar para coletar todos os itens.
                    const scrollBox = wrap.querySelector('.option_list') || wrap;
                    let prevTop = -1;
                    for (let i = 0; i < 30; i++) {
                        lerVisiveis();
                        if (!scrollBox || scrollBox.scrollHeight <= scrollBox.clientHeight) break;
                        if (scrollBox.scrollTop === prevTop) break;
                        prevTop = scrollBox.scrollTop;
                        scrollBox.scrollTop = Math.min(scrollBox.scrollTop + scrollBox.clientHeight, scrollBox.scrollHeight);
                    }
                    scrollBox.scrollTop = 0;
                    lerVisiveis();

                    return Array.from(out);
                })()
            """)

        except Exception as e:
            logger.warning(f"[UpSeller] Erro ao listar lojas do filtro: {e}")
        finally:
            # Fechar dropdown sem alterar estado.
            try:
                fechou = await self._page.evaluate("""
                    (() => {
                        const wrap = document.querySelector('.my_select_dropdown_wrap');
                        if (!wrap) return true;
                        const cancel = Array.from(wrap.querySelectorAll('.option_action .d_ib, .option_action button, .option_action a, .option_action div'))
                            .find(el => ((el.textContent || '').trim().toLowerCase() === 'cancelar'));
                        if (cancel) {
                            cancel.click();
                            return true;
                        }
                        return false;
                    })()
                """)
                if not fechou:
                    await self._page.keyboard.press("Escape")
                    await self._page.wait_for_timeout(150)
            except Exception:
                pass

        return nomes or []

    async def _abrir_subaba_para_programar(self) -> None:
        """Garante que a sub-aba 'Para Programar' esta ativa."""
        try:
            clicou_tab = await self._page.evaluate("""
                (() => {
                    const candidates = document.querySelectorAll(
                        '[role="tab"], .ant-tabs-tab, [class*="tab"], span, div, a'
                    );
                    for (const el of candidates) {
                        const text = (el.textContent || '').trim();
                        if (/^Para Programar(\\s+\\d+)?$/.test(text)) {
                            if (text.includes('Programando') || text.includes('Enviado')) continue;
                            const rect = el.getBoundingClientRect();
                            if (rect.width > 5 && rect.width < 500 && rect.height < 100) {
                                el.click();
                                return true;
                            }
                        }
                    }
                    return false;
                })()
            """)
            if not clicou_tab:
                tab_loc = self._page.locator('div[role="tab"]:has-text("Para Programar")').first
                if await tab_loc.count() > 0:
                    await tab_loc.click(timeout=5000)
            await self._page.wait_for_timeout(900)
        except Exception:
            pass

    async def _ler_contagem_para_programar(self) -> int:
        """Le a contagem da aba 'Para Programar' de forma robusta."""
        try:
            count = await self._page.evaluate("""
                (() => {
                    const parseNum = (t) => {
                        const m1 = (t || '').match(/Para Programar\\s*\\(?\\s*(\\d+)\\s*\\)?/i);
                        if (m1) return parseInt(m1[1], 10);
                        return null;
                    };

                    const candidates = document.querySelectorAll('[role="tab"], .ant-tabs-tab, [class*="ant-tabs-tab"], span, div, a');
                    for (const el of candidates) {
                        const text = (el.textContent || '').trim();
                        if (!/Para Programar/i.test(text)) continue;
                        const n = parseNum(text);
                        if (n !== null) return n;
                    }

                    const body = (document.body && document.body.innerText) || '';
                    const nBody = parseNum(body);
                    if (nBody !== null) return nBody;

                    const rows = document.querySelectorAll('tr.top_row, tr[class*="top_row"], .order_item');
                    return rows ? rows.length : 0;
                })()
            """)
            return int(count or 0)
        except Exception:
            return 0

    async def _ler_marketplace_primeira_linha(self) -> str:
        """Tenta ler marketplace da primeira linha visivel da tabela."""
        try:
            mp = await self._page.evaluate("""
                (() => {
                    const row = document.querySelector('tr.top_row, tr[class*="top_row"], .order_item');
                    if (!row) return '';
                    const txt = (row.textContent || '').toLowerCase();
                    if (txt.includes('shopee')) return 'Shopee';
                    if (txt.includes('shein')) return 'Shein';
                    if (txt.includes('mercado livre') || txt.includes('mercado')) return 'Mercado Livre';
                    if (txt.includes('tiktok')) return 'TikTok';
                    if (txt.includes('amazon')) return 'Amazon';
                    if (txt.includes('magalu')) return 'Magalu';
                    if (txt.includes('kwai')) return 'Kwai';
                    return '';
                })()
            """)
            return (mp or '').strip()
        except Exception:
            return ''

    async def _ler_sidebar_info(self) -> dict:
        """Le contadores do sidebar (Para Enviar, Para Emitir, etc.)."""
        try:
            info = await self._page.evaluate("""
                (() => {
                    const out = {};
                    const allEls = document.querySelectorAll('li, a, div, span');
                    const patterns = [
                        /^(Para (?:Reservar|Emitir|Enviar|Imprimir|Retirada))\\s+(\\d+)$/i,
                        /^(Para (?:Reservar|Emitir|Enviar|Imprimir|Retirada))\\s*[(\\[]\\s*(\\d+)/i,
                        /^(Programando|Enviado|Fatura Pendente)\\s+(\\d+)$/i,
                    ];
                    for (const el of allEls) {
                        const text = (el.textContent || '').trim();
                        for (const p of patterns) {
                            const m = text.match(p);
                            if (!m) continue;
                            const key = (m[1] || '').replace(/\\s+/g, ' ').trim();
                            const val = parseInt(m[2] || '0', 10) || 0;
                            if (!out[key] || val > out[key]) out[key] = val;
                            break;
                        }
                    }
                    return out;
                })()
            """)
            return info if isinstance(info, dict) else {}
        except Exception:
            return {}

    async def _ler_total_subabas(self) -> int:
        """Soma os contadores das sub-abas de pedidos (ex.: Para Programar, Programando, etc.)."""
        try:
            total = await self._page.evaluate("""
                (() => {
                    let sum = 0;
                    const tabs = document.querySelectorAll('[role="tab"], .ant-tabs-tab, [class*="ant-tabs-tab"]');
                    for (const el of tabs) {
                        const text = (el.textContent || '').trim();
                        const m = text.match(/^(.+?)\\s+(\\d+)$/);
                        if (!m) continue;
                        const nome = (m[1] || '').toLowerCase();
                        if (nome.includes('para enviar') || nome.includes('para imprimir') || nome.includes('para emitir')) {
                            continue;
                        }
                        sum += parseInt(m[2] || '0', 10) || 0;
                    }
                    return sum;
                })()
            """)
            return int(total or 0)
        except Exception:
            return 0

    async def _ler_contadores_programacao_envio(self) -> Dict[str, int]:
        """
        Le contadores das sub-abas da tela de programacao de envio:
        - Para Programar
        - Programando
        - Falha na Programacao
        - Obtendo N° de Rastreio
        - Erro ao Obter N° de Rastreio
        """
        base = {
            "para_programar": 0,
            "programando": 0,
            "falha_na_programacao": 0,
            "obtendo_rastreio": 0,
            "erro_obter_rastreio": 0,
        }
        try:
            data = await self._page.evaluate("""
                (() => {
                    const out = {
                        para_programar: 0,
                        programando: 0,
                        falha_na_programacao: 0,
                        obtendo_rastreio: 0,
                        erro_obter_rastreio: 0
                    };
                    const normalize = (s) => (s || '')
                        .toLowerCase()
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim();
                    const setMax = (k, v) => {
                        const n = parseInt(v || 0, 10) || 0;
                        if (n > (out[k] || 0)) out[k] = n;
                    };
                    const nodes = document.querySelectorAll('[role="tab"], .ant-tabs-tab, .ant-tabs-tab-btn, span, div, a, li');
                    for (const el of nodes) {
                        const txtRaw = (el.textContent || '').trim();
                        if (!txtRaw) continue;
                        const txt = normalize(txtRaw);
                        const m = txt.match(/(\\d+)\\s*$/);
                        if (!m) continue;
                        const count = parseInt(m[1] || '0', 10) || 0;

                        if (txt.startsWith('para programar')) {
                            setMax('para_programar', count);
                            continue;
                        }
                        if (txt.startsWith('programando')) {
                            setMax('programando', count);
                            continue;
                        }
                        if (txt.startsWith('falha na programacao') || txt.includes('falha na programacao')) {
                            setMax('falha_na_programacao', count);
                            continue;
                        }
                        if (txt.startsWith('obtendo n') && txt.includes('rastreio')) {
                            setMax('obtendo_rastreio', count);
                            continue;
                        }
                        if (txt.startsWith('erro ao obter n') && txt.includes('rastreio')) {
                            setMax('erro_obter_rastreio', count);
                            continue;
                        }
                    }
                    return out;
                })()
            """)
            if isinstance(data, dict):
                for k in list(base.keys()):
                    try:
                        base[k] = int(data.get(k, 0) or 0)
                    except Exception:
                        base[k] = 0
            return base
        except Exception as e:
            logger.debug(f"[UpSeller] Erro ao ler contadores da programacao: {e}")
            return base

    async def _aguardar_conclusao_programacao(self, timeout_segundos: int = 240) -> Dict[str, int]:
        """
        Aguarda finalizar filas transitorias apos "Programar Envio":
        Programando / Obtendo rastreio / Erro obter rastreio.
        """
        inicio = datetime.now()
        ultimo = {
            "para_programar": 0,
            "programando": 0,
            "falha_na_programacao": 0,
            "obtendo_rastreio": 0,
            "erro_obter_rastreio": 0,
        }
        while (datetime.now() - inicio).total_seconds() < max(20, int(timeout_segundos or 240)):
            cont = await self._ler_contadores_programacao_envio()
            if isinstance(cont, dict):
                ultimo = cont
            transientes = int(ultimo.get("programando", 0) or 0) + int(ultimo.get("obtendo_rastreio", 0) or 0)
            if transientes <= 0:
                return ultimo
            await self._page.wait_for_timeout(7000)
            try:
                await self._page.reload(wait_until="domcontentloaded")
                await self._page.wait_for_timeout(1200)
                await self._fechar_popups()
                await self._abrir_subaba_para_programar()
            except Exception:
                pass
        return ultimo

    async def _status_subaba_etiquetas(self, alvo: str = "nao_impressa") -> Dict[str, Any]:
        """
        Le o estado da sub-aba de etiquetas (Todos / Etiqueta nao impressa / Etiqueta impressa).
        """
        base = {
            "exists": False,
            "active": False,
            "active_text": "",
            "target_text": "",
            "target_count": 0,
        }
        try:
            data = await self._page.evaluate(
                """
                (alvoTab) => {
                    const normalize = (s) => (s || '')
                        .toLowerCase()
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim();
                    const alvo = normalize(alvoTab || 'nao_impressa');
                    const isVisible = (el) => {
                        if (!el) return false;
                        const st = window.getComputedStyle(el);
                        const r = el.getBoundingClientRect();
                        return st.display !== 'none' && st.visibility !== 'hidden' &&
                            r.width > 12 && r.height > 8 && r.y >= 0 && r.y < (window.innerHeight - 20);
                    };
                    const eAlvo = (txt) => {
                        if (!txt) return false;
                        if (alvo.includes('nao_impressa') || alvo.includes('nao')) {
                            return txt.includes('etiqueta nao impressa') || txt.startsWith('nao impressa');
                        }
                        if (alvo.includes('impressa') && !alvo.includes('nao')) {
                            return txt.includes('etiqueta impressa') || txt.startsWith('impressa');
                        }
                        if (alvo.includes('todos')) {
                            return txt.startsWith('todos');
                        }
                        return false;
                    };
                    const uniq = [];
                    const seen = new Set();
                    const nodes = document.querySelectorAll('.ant-tabs-tab, [role="tab"], .ant-tabs-tab-btn');
                    for (const n of nodes) {
                        const root = n.closest('.ant-tabs-tab, [role="tab"]') || n;
                        if (!root || seen.has(root)) continue;
                        seen.add(root);
                        if (!isVisible(root)) continue;
                        uniq.push(root);
                    }

                    const out = {
                        exists: false,
                        active: false,
                        active_text: '',
                        target_text: '',
                        target_count: 0,
                    };

                    for (const tab of uniq) {
                        const raw = (tab.textContent || '').replace(/\\s+/g, ' ').trim();
                        if (!raw) continue;
                        const txt = normalize(raw);
                        const m = raw.match(/(\\d+)\\s*$/);
                        const c = m ? (parseInt(m[1], 10) || 0) : 0;
                        const isActive = tab.classList.contains('ant-tabs-tab-active') ||
                            tab.getAttribute('aria-selected') === 'true' ||
                            !!tab.querySelector('[aria-selected="true"]');
                        if (isActive) {
                            out.active_text = raw;
                        }
                        if (eAlvo(txt)) {
                            out.exists = true;
                            out.target_text = raw;
                            if (c > out.target_count) out.target_count = c;
                            if (isActive) out.active = true;
                        }
                    }
                    return out;
                }
                """,
                (alvo or "nao_impressa"),
            )
            if isinstance(data, dict):
                base.update(data)
            return base
        except Exception:
            return base

    async def _ativar_subaba_etiquetas(
        self,
        alvo: str = "nao_impressa",
        tentativas: int = 3,
        estrito: bool = False
    ) -> bool:
        """
        Tenta ativar uma sub-aba de etiquetas e confirma se ela ficou ativa.
        """
        try:
            alvo_txt = (alvo or "nao_impressa")
            tentativas = max(1, int(tentativas or 1))
            for _ in range(tentativas):
                status = await self._status_subaba_etiquetas(alvo_txt)
                if status.get("active"):
                    return True
                if not status.get("exists"):
                    return False

                clicou = await self._page.evaluate(
                    """
                    (alvoTab) => {
                        const normalize = (s) => (s || '')
                            .toLowerCase()
                            .normalize('NFD')
                            .replace(/[\\u0300-\\u036f]/g, '')
                            .replace(/\\s+/g, ' ')
                            .trim();
                        const alvo = normalize(alvoTab || 'nao_impressa');
                        const isVisible = (el) => {
                            if (!el) return false;
                            const st = window.getComputedStyle(el);
                            const r = el.getBoundingClientRect();
                            return st.display !== 'none' && st.visibility !== 'hidden' &&
                                r.width > 12 && r.height > 8 && r.y >= 0 && r.y < (window.innerHeight - 20);
                        };
                        const eAlvo = (txt) => {
                            if (!txt) return false;
                            if (alvo.includes('nao_impressa') || alvo.includes('nao')) {
                                return txt.includes('etiqueta nao impressa') || txt.startsWith('nao impressa');
                            }
                            if (alvo.includes('impressa') && !alvo.includes('nao')) {
                                return txt.includes('etiqueta impressa') || txt.startsWith('impressa');
                            }
                            if (alvo.includes('todos')) {
                                return txt.startsWith('todos');
                            }
                            return false;
                        };
                        const clickNode = (el) => {
                            if (!el) return false;
                            try {
                                el.click();
                                return true;
                            } catch (_) {}
                            try {
                                ['mouseover', 'mouseenter', 'mousedown', 'mouseup', 'click'].forEach(ev =>
                                    el.dispatchEvent(new MouseEvent(ev, { bubbles: true, cancelable: true, view: window }))
                                );
                                return true;
                            } catch (_) {}
                            return false;
                        };
                        const seen = new Set();
                        const nodes = document.querySelectorAll('.ant-tabs-tab, [role="tab"], .ant-tabs-tab-btn');
                        for (const n of nodes) {
                            const root = n.closest('.ant-tabs-tab, [role="tab"]') || n;
                            if (!root || seen.has(root)) continue;
                            seen.add(root);
                            if (!isVisible(root)) continue;
                            const raw = (root.textContent || '').replace(/\\s+/g, ' ').trim();
                            const txt = normalize(raw);
                            if (!eAlvo(txt)) continue;
                            const btn = root.querySelector('.ant-tabs-tab-btn, [role="tab"]') || root;
                            if (clickNode(btn) || clickNode(root)) {
                                return { clicked: true, text: raw };
                            }
                        }
                        return { clicked: false, text: '' };
                    }
                    """,
                    alvo_txt,
                )
                await self._page.wait_for_timeout(900)
                if isinstance(clicou, dict) and clicou.get("clicked"):
                    status2 = await self._status_subaba_etiquetas(alvo_txt)
                    if status2.get("active"):
                        return True
            status_fim = await self._status_subaba_etiquetas(alvo_txt)
            if estrito and status_fim.get("exists") and not status_fim.get("active"):
                logger.error(
                    "[UpSeller] Nao foi possivel ativar sub-aba de etiquetas '%s' (ativa atual: '%s')",
                    alvo_txt, status_fim.get("active_text", "")
                )
            return bool(status_fim.get("active"))
        except Exception as e:
            if estrito:
                logger.error(f"[UpSeller] Falha ao ativar sub-aba de etiquetas '{alvo}': {e}")
            return False

    async def _ler_contadores_tabs_nfe(self) -> Dict[str, int]:
        """
        Le contadores das sub-abas de NF-e na tela /pending-invoice.

        Retorna sempre as chaves:
        - para_emitir
        - emitindo
        - falha_na_emissao
        - subindo
        - falha_ao_subir
        """
        base = {
            "para_emitir": 0,
            "emitindo": 0,
            "falha_na_emissao": 0,
            "subindo": 0,
            "falha_ao_subir": 0,
        }
        try:
            info = await self._page.evaluate("""
                (() => {
                    const out = {
                        para_emitir: 0,
                        emitindo: 0,
                        falha_na_emissao: 0,
                        subindo: 0,
                        falha_ao_subir: 0,
                    };
                    const normalize = (s) => (s || '')
                        .toLowerCase()
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim();
                    const isVisible = (el) => {
                        if (!el) return false;
                        const st = window.getComputedStyle(el);
                        const r = el.getBoundingClientRect();
                        return st.display !== 'none' && st.visibility !== 'hidden' &&
                            r.width > 10 && r.height > 10 && r.width < 520 && r.height < 120 && r.y >= 0 && r.y < 440;
                    };
                    const parseCount = (el, raw) => {
                        const m = (raw || '').match(/(\\d+)\\s*$/);
                        if (m) return parseInt(m[1], 10) || 0;
                        const badge = el.querySelector('.ant-badge-count, .ant-tabs-tab-count');
                        if (badge) {
                            const n = parseInt((badge.textContent || '').replace(/\\D+/g, ''), 10);
                            if (!Number.isNaN(n)) return n;
                        }
                        return 0;
                    };
                    const setMax = (k, v) => {
                        const n = parseInt(v || 0, 10) || 0;
                        if (n > (out[k] || 0)) out[k] = n;
                    };

                    const nodes = Array.from(document.querySelectorAll('[role="tab"], .ant-tabs-tab'));
                    for (const el of nodes) {
                        if (!isVisible(el)) continue;
                        const raw = (el.textContent || '').replace(/\\s+/g, ' ').trim();
                        if (!raw) continue;
                        const txt = normalize(raw);
                        if (!txt) continue;
                        const count = parseCount(el, raw);

                        if (txt.startsWith('para emitir')) {
                            setMax('para_emitir', count);
                            continue;
                        }
                        if (txt.startsWith('emitindo')) {
                            setMax('emitindo', count);
                            continue;
                        }
                        if (txt.startsWith('falha na emissao') || txt.includes('falha na emissao')) {
                            setMax('falha_na_emissao', count);
                            continue;
                        }
                        if (txt.startsWith('subindo')) {
                            setMax('subindo', count);
                            continue;
                        }
                        if (txt.startsWith('falha ao subir') || txt.includes('falha ao subir')) {
                            setMax('falha_ao_subir', count);
                            continue;
                        }
                    }
                    return out;
                })()
            """)
            if isinstance(info, dict):
                for k in base.keys():
                    try:
                        base[k] = max(0, int(info.get(k, 0) or 0))
                    except Exception:
                        base[k] = 0
            return base
        except Exception:
            return base

    async def _tabela_filtrada_para_loja(self, nome_loja: str) -> bool:
        """
        Valida se as linhas visiveis da tabela pertencem majoritariamente a loja alvo.
        Evita contagem errada quando o filtro de loja nao aplica de fato.
        """
        try:
            valid = await self._page.evaluate("""
                (nomeLoja) => {
                    const normalize = (s) => (s || '')
                        .toLowerCase()
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .replace(/[^a-z0-9 ]+/g, ' ')
                        .replace(/\\s+/g, ' ')
                        .trim();
                    const target = normalize(nomeLoja);
                    const marketRx = /(shopee|shein|mercado livre|tiktok|amazon|magalu|kwai)/i;

                    // Identificar o box de loja (evita pegar "Métodos de Envio", etc).
                    const boxes = Array.from(document.querySelectorAll('.select_multiple_box'));
                    let txtBox = '';
                    for (const box of boxes) {
                        const trigger = box.querySelector('.inp_box') || box;
                        const t = normalize(trigger ? trigger.textContent : (box.textContent || ''));
                        if (!t) continue;
                        if (
                            t.includes(target) ||
                            t.includes('loja') ||
                            t.includes('todas lojas') ||
                            t.includes('todas as lojas')
                        ) {
                            txtBox = t;
                            break;
                        }
                    }
                    // Fallback: alguns layouts usam ant-select no filtro de loja.
                    if (!txtBox) {
                        const selects = Array.from(document.querySelectorAll('.ant-select'));
                        for (const sel of selects) {
                            const r = sel.getBoundingClientRect();
                            if (r.width < 100 || r.height < 20 || r.y < 30 || r.y > 280) continue;
                            const t = normalize(sel.textContent || '');
                            if (!t) continue;
                            if (
                                t.includes(target) ||
                                t.includes('loja') ||
                                t.includes('todas lojas') ||
                                t.includes('todas as lojas')
                            ) {
                                txtBox = t;
                                break;
                            }
                        }
                    }

                    // Se o seletor indicar explicitamente "todas/tudo", filtro nao aplicado.
                    if (txtBox && (txtBox.includes('todas') || txtBox.includes('tudo'))) {
                        return false;
                    }

                    const rows = Array.from(document.querySelectorAll('tr.top_row, tr[class*="top_row"], .order_item')).slice(0, 25);
                    if (rows.length === 0) {
                        const bodyTxt = normalize(document.body.innerText || '');
                        if (bodyTxt.includes('nenhum dado') || bodyTxt.includes('total 0')) {
                            // Loja filtrada sem pedidos visiveis -> filtro valido.
                            return true;
                        }
                        return false;
                    }

                    let comparadas = 0;
                    let ok = 0;
                    for (const row of rows) {
                        const rowTxt = normalize(row.textContent || '');
                        const lojaEl =
                            row.querySelector('span.d_ib.max_w_160, span[class*="max_w_160"], [class*="shop"], [class*="store"]');
                        const txtLoja = normalize(lojaEl ? lojaEl.textContent : '');
                        let lojaLinha = txtLoja;
                        if (!lojaLinha && rowTxt) {
                            const m = rowTxt.match(/([a-z0-9][a-z0-9 ._-]{1,60})\\s*\\|\\s*(shopee|shein|mercado livre|tiktok|amazon|magalu|kwai)/i);
                            if (m && m[1]) lojaLinha = normalize(m[1]);
                        }
                        if (!lojaLinha && rowTxt && rowTxt.includes(target)) {
                            lojaLinha = target;
                        }
                        if (!lojaLinha) continue;
                        comparadas++;
                        if (lojaLinha === target || lojaLinha.includes(target) || target.includes(lojaLinha)) {
                            ok++;
                            continue;
                        }
                        // Se a linha nao carrega nome da loja de forma clara, mas contem marketplace,
                        // considerar mismatch para evitar falso positivo.
                        if (!marketRx.test(rowTxt)) {
                            // Sem marketplace identificavel, nao penaliza.
                            comparadas--;
                        }
                    }

                    // Quando a UI nao expõe nome da loja nas linhas, usar o trigger como fallback.
                    if (comparadas === 0) {
                        if (!txtBox) return false;
                        if (txtBox.includes(target) && !txtBox.includes('todas') && !txtBox.includes('tudo')) {
                            return true;
                        }
                        return false;
                    }
                    const ratio = ok / comparadas;
                    return ratio >= 0.75;
                }
            """, nome_loja)
            return bool(valid)
        except Exception:
            return False

    @staticmethod
    def _normalizar_lista_lojas_filtro(filtro_loja: Union[str, List[str], tuple, set, None]) -> List[str]:
        """Normaliza filtro de loja aceitando string unica ou lista de lojas."""
        if filtro_loja is None:
            return []
        if isinstance(filtro_loja, str):
            nome = filtro_loja.strip()
            return [nome] if nome else []

        out = []
        vistos = set()
        for item in list(filtro_loja or []):
            nome = str(item or "").strip()
            if not nome:
                continue
            key = re.sub(r"\s+", " ", nome.casefold()).strip()
            if key in vistos:
                continue
            vistos.add(key)
            out.append(nome)
        return out

    async def _tabela_filtrada_para_lojas(self, nomes_lojas: List[str]) -> bool:
        """
        Valida se as linhas visiveis da tabela pertencem ao conjunto de lojas informado.
        """
        nomes = self._normalizar_lista_lojas_filtro(nomes_lojas)
        if not nomes:
            return True
        if len(nomes) == 1:
            return await self._tabela_filtrada_para_loja(nomes[0])

        try:
            valid = await self._page.evaluate("""
                (nomesLojas) => {
                    const normalize = (s) => (s || '')
                        .toLowerCase()
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .replace(/[^a-z0-9 ]+/g, ' ')
                        .replace(/\\s+/g, ' ')
                        .trim();
                    const targets = (nomesLojas || []).map(normalize).filter(Boolean);
                    if (!targets.length) return true;
                    const marketRx = /(shopee|shein|mercado livre|tiktok|amazon|magalu|kwai)/i;

                    // Trigger do seletor de loja nao pode indicar "todas".
                    const boxes = Array.from(document.querySelectorAll('.select_multiple_box'));
                    let txtBox = '';
                    for (const box of boxes) {
                        const trigger = box.querySelector('.inp_box') || box;
                        const t = normalize(trigger ? trigger.textContent : (box.textContent || ''));
                        if (!t) continue;
                        if (
                            t.includes('loja') ||
                            t.includes('todas lojas') ||
                            t.includes('todas as lojas') ||
                            targets.some((x) => t.includes(x))
                        ) {
                            txtBox = t;
                            break;
                        }
                    }
                    // Fallback: alguns layouts usam ant-select no filtro de loja.
                    if (!txtBox) {
                        const selects = Array.from(document.querySelectorAll('.ant-select'));
                        for (const sel of selects) {
                            const r = sel.getBoundingClientRect();
                            if (r.width < 100 || r.height < 20 || r.y < 30 || r.y > 280) continue;
                            const t = normalize(sel.textContent || '');
                            if (!t) continue;
                            if (
                                t.includes('loja') ||
                                t.includes('todas lojas') ||
                                t.includes('todas as lojas') ||
                                targets.some((x) => t.includes(x))
                            ) {
                                txtBox = t;
                                break;
                            }
                        }
                    }
                    if (txtBox && (txtBox.includes('todas') || txtBox.includes('tudo'))) {
                        return false;
                    }

                    const rows = Array.from(
                        document.querySelectorAll('tr.top_row, tr[class*="top_row"], .order_item')
                    ).slice(0, 30);
                    if (rows.length === 0) {
                        const bodyTxt = normalize(document.body.innerText || '');
                        if (bodyTxt.includes('nenhum dado') || bodyTxt.includes('total 0')) {
                            return true;
                        }
                        return false;
                    }

                    const matchTarget = (lojaLinha) => {
                        if (!lojaLinha) return false;
                        return targets.some((t) => lojaLinha === t || lojaLinha.includes(t) || t.includes(lojaLinha));
                    };

                    let comparadas = 0;
                    let ok = 0;
                    for (const row of rows) {
                        const rowTxt = normalize(row.textContent || '');
                        const lojaEl =
                            row.querySelector('span.d_ib.max_w_160, span[class*="max_w_160"], [class*="shop"], [class*="store"]');
                        const txtLoja = normalize(lojaEl ? lojaEl.textContent : '');
                        let lojaLinha = txtLoja;
                        if (!lojaLinha && rowTxt) {
                            const m = rowTxt.match(/([a-z0-9][a-z0-9 ._-]{1,60})\\s*\\|\\s*(shopee|shein|mercado livre|tiktok|amazon|magalu|kwai)/i);
                            if (m && m[1]) lojaLinha = normalize(m[1]);
                        }
                        if (!lojaLinha) continue;
                        comparadas++;
                        if (matchTarget(lojaLinha)) {
                            ok++;
                            continue;
                        }
                        if (!marketRx.test(rowTxt)) {
                            comparadas--;
                        }
                    }

                    if (comparadas === 0) {
                        if (!txtBox) return false;
                        if (txtBox.includes('todas') || txtBox.includes('tudo')) return false;
                        if (targets.some((t) => txtBox.includes(t))) return true;
                        return false;
                    }
                    const ratio = ok / comparadas;
                    return ratio >= 0.75;
                }
            """, nomes)
            return bool(valid)
        except Exception:
            return False

    async def _filtrar_por_lojas_ant_select(self, nomes_lojas: List[str]) -> bool:
        """
        Fallback para layouts que usam ant-select no filtro de loja
        (sem .select_multiple_box).
        """
        nomes = self._normalizar_lista_lojas_filtro(nomes_lojas)
        if not nomes:
            return True

        # Blindagem contra tutorial/overlay que bloqueia cliques no select.
        try:
            await self._fechar_popups(max_tentativas=2)
        except Exception:
            pass

        try:
            await self._page.wait_for_selector(".ant-select", timeout=6000)
        except Exception:
            return False

        try:
            loja_ref = nomes[0]
            store_idx = await self._page.evaluate("""
                (nomeLoja) => {
                    const normalize = (s) => (s || '')
                        .toLowerCase()
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim();
                    const target = normalize(nomeLoja || '');
                    const nodes = Array.from(document.querySelectorAll('.ant-select'));
                    if (!nodes.length) return -1;

                    let bestIdx = -1;
                    let bestScore = -9999;
                    nodes.forEach((el, idx) => {
                        const r = el.getBoundingClientRect();
                        if (r.width < 100 || r.height < 18 || r.y < 30 || r.y > 300) return;
                        const t = normalize(el.textContent || '');
                        let score = 0;
                        if (t.includes('loja') || t.includes('todas lojas') || t.includes('todas as lojas')) score += 20;
                        if (target && (t.includes(target) || target.includes(t))) score += 8;
                        if (el.querySelector('.ant-select-selection-item, .ant-select-selection-overflow-item')) score += 2;
                        if (r.y >= 40 && r.y <= 240) score += 4;
                        if (score > bestScore) {
                            bestScore = score;
                            bestIdx = idx;
                        }
                    });
                    return bestIdx;
                }
            """, loja_ref)
            try:
                store_idx = int(store_idx)
            except Exception:
                store_idx = -1
            if store_idx < 0:
                return False

            box = self._page.locator(".ant-select").nth(store_idx)

            # Limpar selecao atual via X (sem usar "marcar tudo").
            for _ in range(20):
                removed = False
                for sel in [
                    ".ant-select-selection-item-remove",
                    ".ant-select-selection-overflow-item .anticon-close",
                    ".ant-select-clear",
                    "i[aria-label='icon: close']",
                ]:
                    loc = box.locator(sel).first
                    if await loc.count() <= 0:
                        continue
                    try:
                        await loc.click(timeout=1200)
                        removed = True
                        break
                    except Exception:
                        continue
                if not removed:
                    break
                await self._page.wait_for_timeout(120)

            # Abrir dropdown do ant-select.
            trigger = box.locator(".ant-select-selector").first
            if await trigger.count() <= 0:
                trigger = box
            if await trigger.count() <= 0:
                return False
            try:
                await trigger.scroll_into_view_if_needed(timeout=1200)
            except Exception:
                pass
            try:
                await trigger.click(timeout=2200)
            except Exception:
                # Fallback: clique por JS para contornar overlay residual.
                try:
                    await self._fechar_popups(max_tentativas=2)
                except Exception:
                    pass
                abriu_js = await self._page.evaluate("""
                    (idx) => {
                        const nodes = Array.from(document.querySelectorAll('.ant-select'));
                        const el = (idx >= 0 && idx < nodes.length) ? nodes[idx] : null;
                        if (!el) return false;
                        const trg = el.querySelector('.ant-select-selector') || el;
                        try {
                            trg.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
                            trg.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
                            trg.click();
                            return true;
                        } catch (e) {
                            return false;
                        }
                    }
                """, store_idx)
                if not abriu_js:
                    return False
            await self._page.wait_for_timeout(400)

            faltantes = []
            selecionadas = []
            for nome in nomes:
                # Busca (quando input existir).
                try:
                    dd_input = self._page.locator(
                        ".ant-select-dropdown:not(.ant-select-dropdown-hidden) input[type='search'], "
                        ".ant-select-dropdown:not(.ant-select-dropdown-hidden) input[type='text'], "
                        ".ant-select-dropdown:not(.ant-select-dropdown-hidden) input"
                    ).first
                    if await dd_input.count() > 0:
                        await dd_input.fill(nome)
                        await self._page.wait_for_timeout(280)
                except Exception:
                    pass

                match = await self._page.evaluate("""
                    (nomeLoja) => {
                        const normalize = (s) => (s || '')
                            .toLowerCase()
                            .normalize('NFD')
                            .replace(/[\\u0300-\\u036f]/g, '')
                            .replace(/\\s+/g, ' ')
                            .trim();
                        const target = normalize(nomeLoja || '');
                        const dropdowns = Array.from(document.querySelectorAll('.ant-select-dropdown'))
                            .filter((d) => {
                                const st = window.getComputedStyle(d);
                                const r = d.getBoundingClientRect();
                                return st.display !== 'none' &&
                                    st.visibility !== 'hidden' &&
                                    !d.classList.contains('ant-select-dropdown-hidden') &&
                                    r.width > 120 && r.height > 40;
                            });
                        const dd = dropdowns[dropdowns.length - 1];
                        if (!dd) return { ok: false, options: [] };
                        const opts = Array.from(
                            dd.querySelectorAll('.ant-select-item-option, .ant-select-dropdown-menu-item, [role="option"]')
                        );
                        const labels = opts.map((o) => (o.textContent || '').trim()).filter(Boolean);

                        let best = null;
                        for (const o of opts) {
                            const t = normalize(o.textContent || '');
                            if (!t) continue;
                            if (t === target) { best = o; break; }
                        }
                        if (!best) {
                            for (const o of opts) {
                                const t = normalize(o.textContent || '');
                                if (!t) continue;
                                if (t.includes(target) || target.includes(t)) { best = o; break; }
                            }
                        }
                        if (!best) return { ok: false, options: labels.slice(0, 12) };
                        best.click();
                        return { ok: true, text: (best.textContent || '').trim() };
                    }
                """, nome)

                if not match or not match.get("ok"):
                    faltantes.append({
                        "nome": nome,
                        "opcoes": (match or {}).get("options", [])[:12],
                    })
                    continue

                selecionadas.append((match or {}).get("text") or nome)
                await self._page.wait_for_timeout(180)
            # Fechar dropdown.
            try:
                await self._page.keyboard.press("Escape")
            except Exception:
                pass
            try:
                await self._page.evaluate("document.body.click()")
            except Exception:
                pass
            await self._page.wait_for_timeout(500)

            if faltantes:
                logger.warning(f"[UpSeller] Ant-select: lojas nao encontradas: {faltantes}")
                return False
            if not selecionadas:
                return False

            # Validacao minima no trigger: nao pode ficar em "todas".
            trigger_ok = await self._page.evaluate("""
                (idx) => {
                    const normalize = (s) => (s || '')
                        .toLowerCase()
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim();
                    const nodes = Array.from(document.querySelectorAll('.ant-select'));
                    const el = (idx >= 0 && idx < nodes.length) ? nodes[idx] : null;
                    if (!el) return false;
                    const txt = normalize(el.textContent || '');
                    if (!txt) return false;
                    if (txt.includes('todas') || txt.includes('tudo')) return false;
                    return true;
                }
            """, store_idx)
            if not trigger_ok:
                return False

            logger.info(f"[UpSeller] Filtro aplicado via ant-select ({len(selecionadas)} loja(s))")
            return True
        except Exception as e:
            logger.warning(f"[UpSeller] Falha no fallback ant-select de lojas: {e}")
            return False

    async def _filtrar_por_lojas(self, nomes_lojas: List[str]) -> bool:
        """
        Aplica filtro de MULTIPLAS lojas em um unico salvamento.
        """
        nomes = self._normalizar_lista_lojas_filtro(nomes_lojas)
        if not nomes:
            return True
        if len(nomes) == 1:
            return await self._filtrar_por_loja(nomes[0])

        try:
            await self._fechar_popups(max_tentativas=2)
        except Exception:
            pass

        logger.info(f"[UpSeller] Filtrando por lote de lojas ({len(nomes)}): {nomes[:6]}")

        try:
            try:
                await self._page.wait_for_selector(
                    ".select_multiple_box, .select_multiple_box .inp_box",
                    timeout=8000
                )
            except Exception:
                await self._page.wait_for_timeout(800)

            loja_ref = nomes[0]
            store_box_idx = await self._page.evaluate("""
                (nomeLoja) => {
                    const normalize = (s) => (s || '')
                        .toLowerCase()
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim();
                    const target = normalize(nomeLoja);
                    const boxes = Array.from(document.querySelectorAll('.select_multiple_box'));
                    if (!boxes.length) return -1;

                    let bestIdx = 0;
                    let bestScore = -9999;
                    boxes.forEach((box, idx) => {
                        const inp = box.querySelector('.inp_box');
                        const txt = normalize(inp ? (inp.textContent || '') : (box.textContent || ''));
                        const r = box.getBoundingClientRect();
                        let score = 0;
                        if (txt.includes('loja') || txt.includes('todas lojas') || txt.includes('todas as lojas')) score += 20;
                        if (target && (txt.includes(target) || target.includes(txt))) score += 8;
                        if (txt.includes('+')) score += 3;
                        if (box.querySelector('.tag_item')) score += 2;
                        if (r.y >= 40 && r.y <= 240) score += 4;
                        if (r.width >= 120 && r.width <= 260) score += 2;
                        if (score > bestScore) {
                            bestScore = score;
                            bestIdx = idx;
                        }
                    });
                    if (bestScore < 0) return -1;
                    return bestIdx;
                }
            """, loja_ref)
            try:
                store_box_idx = int(store_box_idx)
            except Exception:
                store_box_idx = -1
            if store_box_idx < 0:
                try:
                    dbg = await self._page.evaluate("""
                        () => ({
                            url: window.location.href,
                            select_multiple_count: document.querySelectorAll('.select_multiple_box').length,
                            ant_select_count: document.querySelectorAll('.ant-select').length,
                            body_head: (document.body?.innerText || '').slice(0, 180),
                        })
                    """)
                    logger.warning(f"[UpSeller][FiltroDebug] multiloja_caixa_nao_encontrada: {dbg}")
                except Exception:
                    pass
                logger.warning("[UpSeller] Caixa de filtro de loja nao encontrada (multiloja). Tentando ant-select...")
                return await self._filtrar_por_lojas_ant_select(nomes)
            store_box = self._page.locator(".select_multiple_box").nth(store_box_idx)

            # Limpa chips atuais somente pelo X.
            removed_total = 0
            for _ in range(15):
                removed = False
                for sel in [
                    ".tag_item .anticon-close",
                    "i[aria-label='icon: close']",
                    ".ant-select-selection-item-remove",
                    ".ant-select-selection-overflow-item .anticon-close",
                    ".icon_clear.icon_item.anticon-close-circle",
                    ".ant-select-clear",
                ]:
                    loc = store_box.locator(sel).first
                    if await loc.count() <= 0:
                        continue
                    try:
                        await loc.click(timeout=1200)
                        removed = True
                        break
                    except Exception:
                        continue
                if not removed:
                    break
                removed_total += 1
                await self._page.wait_for_timeout(120)

            if removed_total > 0:
                logger.info(f"[UpSeller] Filtro anterior removido via X ({removed_total} clique(s))")
                await self._page.wait_for_timeout(220)

            # Abre dropdown.
            abriu = {"found": False}
            trigger = store_box.locator(".inp_box").first
            if await trigger.count() <= 0:
                trigger = store_box
            if await trigger.count() > 0:
                try:
                    await trigger.scroll_into_view_if_needed(timeout=1200)
                except Exception:
                    pass
                try:
                    await trigger.click(timeout=1800)
                    abriu = {"found": True}
                except Exception:
                    abriu = {"found": False}
            if not abriu.get("found"):
                try:
                    abriu_js = await self._page.evaluate("""
                        (idx) => {
                            const boxes = Array.from(document.querySelectorAll('.select_multiple_box'));
                            if (!boxes.length) return false;
                            const box = boxes[Math.max(0, Math.min(idx, boxes.length - 1))];
                            if (!box) return false;
                            const trg = box.querySelector('.inp_box') || box;
                            trg.click();
                            return true;
                        }
                    """, store_box_idx)
                    abriu = {"found": bool(abriu_js)}
                except Exception:
                    abriu = {"found": False}
            if not abriu.get("found"):
                logger.warning("[UpSeller] Trigger do filtro multiloja nao encontrado")
                return False

            await self._page.wait_for_timeout(700)

            wrap_selector = await self._page.evaluate("""
                (() => {
                    const wraps = Array.from(document.querySelectorAll('.my_select_dropdown_wrap'));
                    const wrap = wraps.find((w) => {
                        const st = window.getComputedStyle(w);
                        const r = w.getBoundingClientRect();
                        const visible = st.display !== 'none' && st.visibility !== 'hidden' && r.width > 120 && r.height > 120;
                        const hasLabels = w.querySelectorAll('label.ant-checkbox-wrapper').length > 0;
                        return visible && hasLabels;
                    }) || null;
                    if (!wrap) return null;
                    if (!wrap.id) wrap.id = 'store_filter_wrap_multi_' + Date.now();
                    return '#' + wrap.id;
                })()
            """)
            if not wrap_selector:
                logger.warning("[UpSeller] Dropdown de lojas nao encontrado (multiloja). Tentando ant-select...")
                return await self._filtrar_por_lojas_ant_select(nomes)

            # Desmarca "Tudo".
            await self._page.evaluate("""
                (wrapSelector) => {
                    const wrap = document.querySelector(wrapSelector);
                    if (!wrap) return;
                    const normalize = (s) => (s || '')
                        .toLowerCase()
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .trim();
                    const allLabel = Array.from(wrap.querySelectorAll('label.ant-checkbox-wrapper'))
                        .find((l) => normalize(l.textContent).includes('tudo'));
                    if (allLabel && allLabel.classList.contains('ant-checkbox-wrapper-checked')) {
                        allLabel.click();
                    }
                }
            """, wrap_selector)
            await self._page.wait_for_timeout(220)

            search_input = await self._page.query_selector(
                f'{wrap_selector} .option_search input.ant-input, {wrap_selector} input.ant-input, '
                f'{wrap_selector} input[type="text"], {wrap_selector} input[type="search"]'
            )

            selecionadas = []
            faltantes = []
            for nome in nomes:
                if search_input:
                    try:
                        await search_input.fill(nome)
                    except Exception:
                        pass
                    await self._page.wait_for_timeout(350)

                match = await self._page.evaluate("""
                    (args) => {
                        const nomeLoja = args.nomeLoja;
                        const wrapSelector = args.wrapSelector;
                        const wrap = document.querySelector(wrapSelector);
                        if (!wrap) return { idx: -1, method: '', checked: false };
                        const labels = Array.from(wrap.querySelectorAll('label.ant-checkbox-wrapper'));
                        const normalizar = (s) => (s || '').toLowerCase().normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').trim();
                        const target = normalizar(nomeLoja);
                        let idx = -1;
                        let method = '';
                        for (let i = 0; i < labels.length; i++) {
                            const text = normalizar(labels[i].textContent || '');
                            if (text === target) {
                                idx = i;
                                method = 'exact';
                                break;
                            }
                        }
                        if (idx < 0) {
                            for (let i = 0; i < labels.length; i++) {
                                const text = normalizar(labels[i].textContent || '');
                                if (text.includes(target) || target.includes(text)) {
                                    idx = i;
                                    method = 'partial';
                                    break;
                                }
                            }
                        }
                        if (idx < 0) return { idx: -1, method: '', checked: false };
                        const el = labels[idx];
                        const checked =
                            el.classList.contains('ant-checkbox-wrapper-checked') ||
                            !!el.querySelector('.ant-checkbox-checked, input[type="checkbox"]:checked');
                        return {
                            idx,
                            method,
                            checked,
                            selectedText: (el.textContent || '').trim(),
                        };
                    }
                """, {"nomeLoja": nome, "wrapSelector": wrap_selector})

                idx = int((match or {}).get("idx", -1) or -1)
                if idx < 0:
                    faltantes.append(nome)
                    continue

                if not bool((match or {}).get("checked")):
                    try:
                        opcoes = self._page.locator(f"{wrap_selector} label.ant-checkbox-wrapper")
                        if await opcoes.count() > idx:
                            await opcoes.nth(idx).click(timeout=2000)
                            await self._page.wait_for_timeout(120)
                        else:
                            faltantes.append(nome)
                            continue
                    except Exception:
                        faltantes.append(nome)
                        continue
                selecionadas.append((match or {}).get("selectedText") or nome)

            if search_input:
                try:
                    await search_input.fill("")
                    await self._page.wait_for_timeout(120)
                except Exception:
                    pass

            if not selecionadas:
                logger.warning("[UpSeller] Nenhuma loja foi marcada no filtro multiloja")
                return False
            if faltantes:
                logger.warning(f"[UpSeller] Lojas nao encontradas no filtro multiloja: {faltantes}")
                return False

            # Salvar.
            clicou_salvar = False
            for sel in [
                f"{wrap_selector} .option_action .d_ib:text-is('Salvar')",
                f"{wrap_selector} .option_action button:text-is('Salvar')",
                f"{wrap_selector} .option_action a:text-is('Salvar')",
                f"{wrap_selector} .option_action span:text-is('Salvar')",
                f"{wrap_selector} .option_action div:text-is('Salvar')",
            ]:
                loc = self._page.locator(sel).first
                if await loc.count() <= 0:
                    continue
                try:
                    await loc.click(timeout=2200)
                    clicou_salvar = True
                    break
                except Exception:
                    continue
            if not clicou_salvar:
                logger.warning("[UpSeller] Botao 'Salvar' nao encontrado no multiloja; tentando fechar com Enter")
                try:
                    await self._page.keyboard.press("Enter")
                except Exception:
                    pass

            try:
                await self._page.keyboard.press("Escape")
                await self._page.wait_for_timeout(200)
                await self._page.evaluate("document.body.click()")
            except Exception:
                pass

            try:
                await self._page.wait_for_selector(wrap_selector, state='hidden', timeout=5000)
            except Exception:
                logger.warning("[UpSeller] Dropdown multiloja permaneceu aberto apos salvar")

            await self._page.wait_for_timeout(1700)
            await self.screenshot("filtro_lojas_multi")
            logger.info(f"[UpSeller] Filtro multiloja aplicado e salvo ({len(selecionadas)} loja(s))")
            return True
        except Exception as e:
            logger.error(f"[UpSeller] Erro ao filtrar por lojas: {e}")
            return False

    async def _aplicar_filtro_loja_seguro(self, nome_loja: str, contexto: str = "") -> bool:
        """
        Aplica filtro de loja com retries e confirma na tabela.
        Este helper e o caminho unico para evitar vazar pedidos de outras lojas.
        """
        nome = (nome_loja or "").strip()
        if not nome:
            return True

        ctx = f" ({contexto})" if contexto else ""
        for tentativa in range(1, 4):
            filtrou = await self._filtrar_por_loja(nome)
            if not filtrou:
                logger.warning(
                    f"[UpSeller] Tentativa {tentativa}/3: falha ao aplicar filtro '{nome}'{ctx}"
                )
                await self._page.wait_for_timeout(700)
                continue

            await self._page.wait_for_timeout(1200)
            tabela_ok = await self._tabela_filtrada_para_loja(nome)
            if tabela_ok:
                logger.info(
                    f"[UpSeller] Filtro confirmado para loja '{nome}'{ctx} (tentativa {tentativa}/3)"
                )
                return True

            logger.warning(
                f"[UpSeller] Tentativa {tentativa}/3: filtro '{nome}' nao confirmado na tabela{ctx}"
            )
            await self._page.wait_for_timeout(800)

        logger.error(
            f"[UpSeller] Filtro por loja '{nome}' NAO confirmado apos 3 tentativas{ctx}"
        )
        return False

    async def _aplicar_filtro_lojas_seguro(self, nomes_lojas: List[str], contexto: str = "") -> bool:
        """
        Aplica filtro de MULTIPLAS lojas com retries e confirma na tabela.
        """
        nomes = self._normalizar_lista_lojas_filtro(nomes_lojas)
        if not nomes:
            return True
        if len(nomes) == 1:
            return await self._aplicar_filtro_loja_seguro(nomes[0], contexto=contexto)

        ctx = f" ({contexto})" if contexto else ""
        for tentativa in range(1, 4):
            filtrou = await self._filtrar_por_lojas(nomes)
            if not filtrou:
                logger.warning(
                    f"[UpSeller] Tentativa {tentativa}/3: falha ao aplicar filtro multiloja{ctx}"
                )
                await self._page.wait_for_timeout(700)
                continue

            await self._page.wait_for_timeout(1200)
            tabela_ok = await self._tabela_filtrada_para_lojas(nomes)
            if tabela_ok:
                logger.info(
                    f"[UpSeller] Filtro multiloja confirmado ({len(nomes)} lojas){ctx} "
                    f"(tentativa {tentativa}/3)"
                )
                return True

            logger.warning(
                f"[UpSeller] Tentativa {tentativa}/3: filtro multiloja nao confirmado na tabela{ctx}"
            )
            await self._page.wait_for_timeout(800)

        logger.error(
            f"[UpSeller] Filtro multiloja NAO confirmado apos 3 tentativas{ctx}"
        )
        return False

    async def _abrir_pagina_para_enviar(self) -> None:
        """Navega para /order/to-ship e garante foco no item 'Para Enviar'."""
        try:
            await self._page.goto(UPSELLER_PEDIDOS, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(1800)
            await self._fechar_popups()
            await self._page.wait_for_timeout(300)
            await self._page.evaluate("""
                (() => {
                    const nodes = document.querySelectorAll('li, a, span, div');
                    for (const el of nodes) {
                        const t = (el.textContent || '').trim();
                        if (/^Para Enviar(\\s+\\d+)?$/.test(t)) {
                            const r = el.getBoundingClientRect();
                            if (r.width > 5 && r.height > 5 && r.height < 120) {
                                el.click();
                                return true;
                            }
                        }
                    }
                    return false;
                })()
            """)
            await self._page.wait_for_timeout(1100)
        except Exception:
            pass

    async def _contagem_precisa_por_loja(self, nomes_lojas: List[str], mapa_fallback: Dict[str, dict]) -> List[dict]:
        """
        Recalcula quantidade por loja aplicando filtro loja-a-loja e lendo
        a contagem da aba 'Para Programar' (mais confiavel para uso no sistema).
        """
        if not nomes_lojas:
            return []

        lojas_precisas = []
        # Sempre iniciar em contexto limpo para evitar herdar estado de outra rotina
        # (ex.: pagina/aba diferente apos contar "Para Emitir"/"Para Imprimir").
        try:
            await self._abrir_pagina_para_enviar()
            await self._limpar_filtro_loja()
            await self._page.wait_for_timeout(400)
        except Exception:
            pass

        for idx, nome in enumerate(nomes_lojas, start=1):
            nome_limpo = (nome or '').strip()
            if not nome_limpo:
                continue
            key = nome_limpo.casefold()
            fallback = mapa_fallback.get(key, {})
            pedidos_fallback = int(fallback.get("pedidos", 0) or 0)
            marketplace_fallback = (fallback.get("marketplace") or '').strip()

            try:
                logger.info(f"[UpSeller] Contagem precisa ({idx}/{len(nomes_lojas)}): {nome_limpo}")
                filtrou = await self._aplicar_filtro_loja_seguro(nome_limpo, contexto="contagem_precisa")
                if not filtrou:
                    # Em alguns layouts o filtro some apos navegações/paginacao.
                    # Recarrega a tela base e tenta novamente.
                    await self._abrir_pagina_para_enviar()
                    filtrou = await self._aplicar_filtro_loja_seguro(
                        nome_limpo, contexto="contagem_precisa_retry"
                    )
                if not filtrou:
                    lojas_precisas.append({
                        "nome": nome_limpo,
                        "marketplace": marketplace_fallback,
                        "pedidos": pedidos_fallback,
                        "orders": [],
                        "_src": "fallback_filtro",
                    })
                    continue

                # Confirmacao final do filtro antes de mudar de aba.
                tabela_ok = await self._tabela_filtrada_para_loja(nome_limpo)
                if not filtrou or not tabela_ok:
                    lojas_precisas.append({
                        "nome": nome_limpo,
                        "marketplace": marketplace_fallback,
                        "pedidos": pedidos_fallback,
                        "orders": [],
                        "_src": "fallback_tabela",
                    })
                    continue

                await self._abrir_subaba_para_programar()
                await self._page.wait_for_timeout(450)

                # Garantir que o filtro da loja permaneceu aplicado apos trocar de aba.
                tabela_ok_pos_aba = await self._tabela_filtrada_para_loja(nome_limpo)
                if not tabela_ok_pos_aba:
                    lojas_precisas.append({
                        "nome": nome_limpo,
                        "marketplace": marketplace_fallback,
                        "pedidos": pedidos_fallback,
                        "orders": [],
                        "_src": "fallback_pos_aba",
                    })
                    continue

                # Modo rapido e seguro:
                # - evita paginacao loja-a-loja (lento e sujeito a perder filtro)
                # - usa contadores da aba filtrada + linhas visiveis
                pedidos_programar = await self._ler_contagem_para_programar()
                pedidos_subabas = await self._ler_total_subabas()
                linhas_visiveis = await self._contar_linhas_visiveis_tabela()

                if linhas_visiveis == 0:
                    pedidos = 0
                    src = "preciso_vazio"
                elif pedidos_programar > 0:
                    pedidos = pedidos_programar
                    src = "preciso_prog"
                elif pedidos_subabas > 0:
                    pedidos = pedidos_subabas
                    src = "preciso_tabs"
                elif linhas_visiveis > 0:
                    pedidos = linhas_visiveis
                    src = "preciso_rows"
                elif pedidos_fallback >= 0:
                    pedidos = pedidos_fallback
                    src = "fallback_db"
                else:
                    pedidos = 0
                    src = "fallback"

                marketplace = await self._ler_marketplace_primeira_linha() or marketplace_fallback

                lojas_precisas.append({
                    "nome": nome_limpo,
                    "marketplace": marketplace,
                    "pedidos": max(0, int(pedidos or 0)),
                    "orders": [],
                    "_src": (
                        f"{src}(prog={pedidos_programar},"
                        f"tabs={pedidos_subabas},rows={linhas_visiveis},fb={pedidos_fallback})"
                    ),
                })
            except Exception as e:
                logger.warning(f"[UpSeller] Falha na contagem precisa de '{nome_limpo}': {e}")
                lojas_precisas.append({
                    "nome": nome_limpo,
                    "marketplace": marketplace_fallback,
                    "pedidos": pedidos_fallback,
                    "orders": [],
                    "_src": "fallback_ex",
                })

        return lojas_precisas

    async def contar_pedidos_loja(self, nome_loja: str, pedidos_fallback: int = 0, marketplace_fallback: str = "") -> Dict:
        """
        Atualiza contagem de UMA loja especifica usando filtro dedicado no UpSeller.

        Args:
            nome_loja: Nome da loja no UpSeller.
            pedidos_fallback: Valor de fallback caso o filtro falhe.
            marketplace_fallback: Marketplace de fallback.

        Retorna:
            {
                "sucesso": bool,
                "loja": {"nome","marketplace","pedidos","orders", "_src"?},
                "erro": str (quando houver)
            }
        """
        nome = (nome_loja or "").strip()
        if not nome:
            return {"sucesso": False, "erro": "loja_vazia"}

        if not self._page:
            await self._iniciar_navegador()

        if not await self._esta_logado():
            if not await self.login():
                return {"sucesso": False, "erro": "nao_logado"}

        try:
            mapa_fallback = {
                nome.casefold(): {
                    "pedidos": max(0, int(pedidos_fallback or 0)),
                    "marketplace": (marketplace_fallback or "").strip(),
                }
            }
            lojas = await self._contagem_precisa_por_loja([nome], mapa_fallback)
            if not lojas:
                return {
                    "sucesso": True,
                    "loja": {
                        "nome": nome,
                        "marketplace": (marketplace_fallback or "").strip(),
                        "pedidos": max(0, int(pedidos_fallback or 0)),
                        "orders": [],
                        "_src": "fallback_vazio",
                    },
                }
            return {"sucesso": True, "loja": lojas[0]}
        except Exception as e:
            logger.warning(f"[UpSeller] Falha na contagem individual da loja '{nome}': {e}")
            return {"sucesso": False, "erro": str(e)}

    # ===== HELPERS: Filtro de loja e configuracao de etiqueta =====

    async def _filtrar_por_loja(self, nome_loja: str) -> bool:
        """
        Filtra pedidos por loja no dropdown multi-checkbox do UpSeller.

        Regra principal para trocar de loja:
        - limpar a loja atual clicando no "X" do chip selecionado
        - nao usar estrategia de "marcar/desmarcar tudo"
        """
        if not nome_loja:
            return True

        logger.info(f"[UpSeller] Filtrando por loja: '{nome_loja}'")

        try:
            await self._fechar_popups(max_tentativas=2)
        except Exception:
            pass

        try:
            # Aguarda os filtros do topo estarem visiveis no DOM.
            try:
                await self._page.wait_for_selector(
                    ".select_multiple_box, .select_multiple_box .inp_box",
                    timeout=8000
                )
            except Exception:
                await self._page.wait_for_timeout(800)

            # 0) Antes de trocar de loja, remove selecao atual apenas pelo "X" do chip.
            removed_total = 0
            store_box_idx = await self._page.evaluate("""
                (nomeLoja) => {
                    const normalize = (s) => (s || '')
                        .toLowerCase()
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim();
                    const target = normalize(nomeLoja);
                    const boxes = Array.from(document.querySelectorAll('.select_multiple_box'));
                    if (!boxes.length) return -1;

                    let bestIdx = 0;
                    let bestScore = -9999;
                    boxes.forEach((box, idx) => {
                        const inp = box.querySelector('.inp_box');
                        const txt = normalize(inp ? (inp.textContent || '') : (box.textContent || ''));
                        const r = box.getBoundingClientRect();
                        let score = 0;
                        if (txt.includes('loja') || txt.includes('todas lojas') || txt.includes('todas as lojas')) score += 20;
                        if (target && (txt.includes(target) || target.includes(txt))) score += 8;
                        if (txt.includes('+')) score += 3;  // multi-selecao de lojas
                        if (box.querySelector('.tag_item')) score += 2;
                        if (r.y >= 40 && r.y <= 240) score += 4;
                        if (r.width >= 120 && r.width <= 260) score += 2;
                        if (score > bestScore) {
                            bestScore = score;
                            bestIdx = idx;
                        }
                    });
                    if (bestScore < 0) return -1;
                    return bestIdx;
                }
            """, nome_loja)
            try:
                store_box_idx = int(store_box_idx)
            except Exception:
                store_box_idx = -1
            if store_box_idx < 0:
                try:
                    dbg = await self._page.evaluate("""
                        () => {
                            const pick = (arr, n=6) => Array.from(arr).slice(0, n).map((el) => {
                                const r = el.getBoundingClientRect();
                                return {
                                    cls: (el.className || '').toString().slice(0, 90),
                                    txt: (el.textContent || '').trim().slice(0, 90),
                                    y: Math.round(r.y),
                                    w: Math.round(r.width),
                                };
                            });
                            return {
                                url: window.location.href,
                                select_multiple_count: document.querySelectorAll('.select_multiple_box').length,
                                ant_select_count: document.querySelectorAll('.ant-select').length,
                                top_select_multiple: pick(document.querySelectorAll('.select_multiple_box')),
                                top_ant_select: pick(document.querySelectorAll('.ant-select')),
                                body_head: (document.body?.innerText || '').slice(0, 180),
                            };
                        }
                    """)
                    logger.warning(f"[UpSeller][FiltroDebug] caixa_loja_nao_encontrada: {dbg}")
                except Exception:
                    pass
                logger.warning("[UpSeller] Caixa de filtro de loja nao encontrada. Tentando ant-select...")
                return await self._filtrar_por_lojas_ant_select([nome_loja])
            store_box = self._page.locator(".select_multiple_box").nth(store_box_idx)
            for _ in range(12):
                removed = False
                for sel in [
                    ".tag_item .anticon-close",
                    "i[aria-label='icon: close']",
                    ".ant-select-selection-item-remove",
                    ".ant-select-selection-overflow-item .anticon-close",
                    ".icon_clear.icon_item.anticon-close-circle",
                    ".ant-select-clear",
                ]:
                    loc = store_box.locator(sel).first
                    if await loc.count() <= 0:
                        continue
                    try:
                        await loc.click(timeout=1200)
                        removed = True
                        break
                    except Exception:
                        continue
                if not removed:
                    break
                removed_total += 1
                await self._page.wait_for_timeout(140)

            if removed_total > 0:
                logger.info(f"[UpSeller] Loja anterior removida pelo X ({removed_total} clique(s))")
                await self._page.wait_for_timeout(220)
            logger.info(f"[UpSeller] Filtro de loja usando select_multiple_box idx={store_box_idx}")

            # Helper JS para localizar o dropdown de lojas de forma dinamica.
            find_wrap_js = """
                () => {
                    const normalize = (s) => (s || '').toLowerCase().normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').trim();
                    const nodes = Array.from(document.querySelectorAll('div, section, aside'));
                    let best = null;
                    let bestScore = -1;
                    for (const n of nodes) {
                        const text = normalize(n.textContent || '');
                        if (!text) continue;
                        const hasSalvar = text.includes('salvar');
                        const hasTudo = text.includes('tudo');
                        const hasCancelar = text.includes('cancelar');
                        const hasCheckbox = n.querySelector('label.ant-checkbox-wrapper, input[type="checkbox"]');
                        const hasSearch = n.querySelector('input[type="text"], input.ant-input, input[type="search"]');
                        const r = n.getBoundingClientRect();
                        const isReasonableSize = r.width >= 180 && r.width <= 520 && r.height >= 140 && r.height <= 700;
                        const isTopArea = r.y >= 40 && r.y <= 460;
                        const score = (hasSalvar ? 4 : 0) + (hasTudo ? 4 : 0) + (hasCancelar ? 3 : 0) + (hasCheckbox ? 3 : 0) + (hasSearch ? 2 : 0) + (isReasonableSize ? 3 : 0) + (isTopArea ? 2 : 0);
                        if (!hasSalvar || !hasTudo || !hasCheckbox) continue;
                        if (score > bestScore) { best = n; bestScore = score; }
                    }
                    if (!best || bestScore < 10) return null;
                    if (!best.id) best.id = 'dynamic_store_filter_wrap_' + Date.now();
                    return '#' + best.id;
                }
            """

            # 1) Abrir o popup de lojas.
            abriu = {"found": False}
            trigger = store_box.locator(".inp_box").first
            if await trigger.count() <= 0:
                # Fallback: alguns layouts nao possuem .inp_box interno.
                trigger = store_box
            if await trigger.count() > 0:
                try:
                    txt = (await trigger.text_content() or "").strip()
                    try:
                        await trigger.scroll_into_view_if_needed(timeout=1200)
                    except Exception:
                        pass
                    await trigger.click(timeout=1800)
                    abriu = {"found": True, "text": txt[:80]}
                except Exception:
                    abriu = {"found": False}
            if not abriu.get("found"):
                # Fallback hard: click via JS no box identificado.
                try:
                    abriu_js = await self._page.evaluate("""
                        (idx) => {
                            const boxes = Array.from(document.querySelectorAll('.select_multiple_box'));
                            if (!boxes.length) return false;
                            const box = boxes[Math.max(0, Math.min(idx, boxes.length - 1))];
                            if (!box) return false;
                            const trg = box.querySelector('.inp_box') || box;
                            trg.click();
                            return true;
                        }
                    """, store_box_idx)
                    if abriu_js:
                        abriu = {"found": True, "text": ""}
                except Exception:
                    pass
            if not abriu or not abriu.get("found"):
                logger.warning("[UpSeller] Trigger de loja nao encontrado")
                return False

            await self._page.wait_for_timeout(700)
            wrap_selector = await self._page.evaluate("""
                (() => {
                    const wraps = Array.from(document.querySelectorAll('.my_select_dropdown_wrap'));
                    const wrap = wraps.find((w) => {
                        const st = window.getComputedStyle(w);
                        const r = w.getBoundingClientRect();
                        const visible = st.display !== 'none' && st.visibility !== 'hidden' && r.width > 120 && r.height > 120;
                        const hasLabels = w.querySelectorAll('label.ant-checkbox-wrapper').length > 0;
                        return visible && hasLabels;
                    }) || null;
                    if (!wrap) return null;
                    if (!wrap.id) wrap.id = 'store_filter_wrap_' + Date.now();
                    return '#' + wrap.id;
                })()
            """)
            if not wrap_selector:
                wrap_selector = await self._page.evaluate(find_wrap_js)
            if not wrap_selector:
                # Segunda tentativa: reabrir trigger e procurar wrapper padrao.
                try:
                    trigger_retry = store_box.locator(".inp_box").first
                    if await trigger_retry.count() > 0:
                        await trigger_retry.click(timeout=1500)
                except Exception:
                    pass
                await self._page.wait_for_timeout(500)
                wrap_selector = await self._page.evaluate("""
                    (() => {
                        const wraps = Array.from(document.querySelectorAll('.my_select_dropdown_wrap'));
                        const wrap = wraps.find((w) => {
                            const st = window.getComputedStyle(w);
                            const r = w.getBoundingClientRect();
                            const visible = st.display !== 'none' && st.visibility !== 'hidden' && r.width > 120 && r.height > 120;
                            const hasLabels = w.querySelectorAll('label.ant-checkbox-wrapper').length > 0;
                            return visible && hasLabels;
                        }) || null;
                        if (!wrap) return null;
                        if (!wrap.id) wrap.id = 'store_filter_wrap_retry_' + Date.now();
                        return '#' + wrap.id;
                    })()
                """)
            if not wrap_selector:
                logger.warning("[UpSeller] Nao encontrou dropdown de lojas dinamicamente")
                return await self._filtrar_por_lojas_ant_select([nome_loja])

            # 2) Garantir apenas que "Tudo" esteja desmarcado (sem estrategia de reset global).
            await self._page.evaluate("""
                (wrapSelector) => {
                    const wrap = document.querySelector(wrapSelector);
                    if (!wrap) return;
                    const normalize = (s) => (s || '')
                        .toLowerCase()
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .trim();
                    const allLabel = Array.from(wrap.querySelectorAll('label.ant-checkbox-wrapper'))
                        .find((l) => normalize(l.textContent).includes('tudo'));
                    if (allLabel && allLabel.classList.contains('ant-checkbox-wrapper-checked')) {
                        allLabel.click();
                    }
                }
            """, wrap_selector)
            await self._page.wait_for_timeout(220)

            # 3) Buscar loja pelo nome.
            search_input = await self._page.query_selector(
                f'{wrap_selector} .option_search input.ant-input, {wrap_selector} input.ant-input, '
                f'{wrap_selector} input[type="text"], {wrap_selector} input[type="search"]'
            )
            if search_input:
                await search_input.fill(nome_loja)
                await self._page.wait_for_timeout(450)

            # 4) Selecionar loja alvo.
            match = await self._page.evaluate("""
                (args) => {
                    const nomeLoja = args.nomeLoja;
                    const wrapSelector = args.wrapSelector;
                    const wrap = document.querySelector(wrapSelector);
                    if (!wrap) return { idx: -1, method: '', available: [] };
                    const labels = Array.from(wrap.querySelectorAll('label.ant-checkbox-wrapper'));
                    const normalizar = (s) => (s || '').toLowerCase().normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').trim();
                    const target = normalizar(nomeLoja);
                    let idx = -1;
                    let method = '';
                    for (let i = 0; i < labels.length; i++) {
                        const text = normalizar(labels[i].textContent || '');
                        if (text === target) {
                            idx = i;
                            method = 'exact';
                            break;
                        }
                    }
                    if (idx < 0) {
                        for (let i = 0; i < labels.length; i++) {
                            const text = normalizar(labels[i].textContent || '');
                            if (text.includes(target) || target.includes(text)) {
                                idx = i;
                                method = 'partial';
                                break;
                            }
                        }
                    }
                    return {
                        idx,
                        method,
                        selectedText: idx >= 0 ? (labels[idx].textContent || '').trim() : '',
                        available: labels.map((l) => (l.textContent || '').trim())
                    };
                }
            """, {"nomeLoja": nome_loja, "wrapSelector": wrap_selector})
            selecionou = {"selected": False, "text": "", "method": ""}
            if isinstance(match, dict) and int(match.get("idx", -1)) >= 0:
                idx = int(match.get("idx"))
                opcoes = self._page.locator(f"{wrap_selector} label.ant-checkbox-wrapper")
                if await opcoes.count() > idx:
                    try:
                        await opcoes.nth(idx).click(timeout=2000)
                        selecionou = {
                            "selected": True,
                            "text": match.get("selectedText", ""),
                            "method": match.get("method", ""),
                        }
                    except Exception:
                        # Fallback: click JS direto no label.
                        try:
                            ok_click = await self._page.evaluate("""
                                (args) => {
                                    const wrap = document.querySelector(args.wrapSelector);
                                    if (!wrap) return false;
                                    const labels = Array.from(wrap.querySelectorAll('label.ant-checkbox-wrapper'));
                                    const idx = args.idx;
                                    if (idx < 0 || idx >= labels.length) return false;
                                    labels[idx].click();
                                    return true;
                                }
                            """, {"wrapSelector": wrap_selector, "idx": idx})
                        except Exception:
                            ok_click = False
                        selecionou = {
                            "selected": bool(ok_click),
                            "text": match.get("selectedText", ""),
                            "method": match.get("method", ""),
                        }
                else:
                    selecionou = {"selected": False, "text": "", "method": ""}
            else:
                selecionou = {"selected": False, "text": "", "method": ""}
            if not selecionou or not selecionou.get("selected"):
                lojas_disp = match.get("available", []) if isinstance(match, dict) else []
                logger.warning(f"[UpSeller] Loja '{nome_loja}' nao encontrada. Disponiveis: {lojas_disp}")
                await self._page.evaluate("""
                    (() => {
                        const wrap = document.querySelector('.my_select_dropdown_wrap');
                        if (!wrap) return;
                        const cancel = Array.from(wrap.querySelectorAll('.option_action .d_ib, .option_action button, .option_action a, .option_action div'))
                            .find((el) => ((el.textContent || '').trim().toLowerCase() === 'cancelar'));
                        if (cancel) cancel.click();
                    })()
                """)
                await self._page.wait_for_timeout(350)
                return False

            logger.info(f"[UpSeller] Loja '{selecionou.get('text')}' marcada ({selecionou.get('method')})")

            # 5) Salvar.
            clicou_salvar = False
            for sel in [
                f"{wrap_selector} .option_action .d_ib:text-is('Salvar')",
                f"{wrap_selector} .option_action button:text-is('Salvar')",
                f"{wrap_selector} .option_action a:text-is('Salvar')",
                f"{wrap_selector} .option_action span:text-is('Salvar')",
                f"{wrap_selector} .option_action div:text-is('Salvar')",
            ]:
                loc = self._page.locator(sel).first
                if await loc.count() <= 0:
                    continue
                try:
                    await loc.click(timeout=2200)
                    clicou_salvar = True
                    break
                except Exception:
                    continue
            if not clicou_salvar:
                # Alguns layouts aplicam selecao sem botao "Salvar".
                logger.warning("[UpSeller] Botao 'Salvar' nao encontrado; tentando confirmar sem salvar")
                try:
                    await self._page.keyboard.press("Enter")
                except Exception:
                    pass
                try:
                    await self._page.evaluate("document.body.click()")
                except Exception:
                    pass
                await self._page.wait_for_timeout(350)

            # Tenta fechar o dropdown e aguarda aplicar.
            try:
                await self._page.keyboard.press("Escape")
                await self._page.wait_for_timeout(200)
                # Clicar em area neutra via JS (evita clicar em botoes como Ordenar)
                await self._page.evaluate("document.body.click()")
            except Exception:
                pass

            fechou_dropdown = False
            try:
                await self._page.wait_for_selector(wrap_selector, state='hidden', timeout=5000)
                fechou_dropdown = True
            except Exception:
                logger.warning("[UpSeller] Dropdown de loja permaneceu aberto apos salvar")

            # 6) Validar trigger/chips de forma tolerante.
            applied = await self._page.evaluate("""
                (args) => {
                    const nomeLoja = args.nomeLoja || '';
                    const idxLoja = Number.isInteger(args.idx) ? args.idx : -1;
                    const normalize = (s) => (s || '')
                      .toLowerCase()
                      .normalize('NFD')
                      .replace(/[\\u0300-\\u036f]/g, '')
                      .trim();
                    const target = normalize(nomeLoja);
                    const boxes = Array.from(document.querySelectorAll('.select_multiple_box'));
                    const alvo = (idxLoja >= 0 && idxLoja < boxes.length) ? [boxes[idxLoja]] : boxes;
                    let txt = '';
                    let applied = false;
                    let hasAll = false;
                    let selectedLabels = [];
                    for (const box of alvo) {
                        const trigger = box.querySelector('.inp_box') || box;
                        const txtLocal = trigger ? (trigger.textContent || '').trim() : '';
                        const normLocal = normalize(txtLocal);
                        if (!normLocal) continue;

                        const labels = Array.from(
                            box.querySelectorAll('.tag_item, .ant-select-selection-item, .ant-tag')
                        )
                            .map((el) => normalize(el.textContent || ''))
                            .filter(Boolean);
                        if (labels.length) selectedLabels = labels;

                        if (normLocal.includes('todas') || normLocal.includes('tudo')) {
                            hasAll = true;
                        }
                        if (normLocal.includes(target)) {
                            txt = txtLocal;
                            applied = true;
                            break;
                        }
                        if (labels.some((lb) => lb === target || lb.includes(target) || target.includes(lb))) {
                            txt = txtLocal;
                            applied = true;
                            break;
                        }
                    }
                    return {
                        applied,
                        triggerText: txt,
                        hasAll,
                        selectedLabels,
                    };
                }
            """, {"nomeLoja": nome_loja, "idx": store_box_idx})
            if applied and applied.get("hasAll"):
                logger.warning(
                    f"[UpSeller] Trigger ainda mostra todas lojas para '{nome_loja}'"
                )
                ant_ok = await self._filtrar_por_lojas_ant_select([nome_loja])
                if ant_ok:
                    return True
                return False

            if not applied or not applied.get("applied"):
                # Algumas variacoes do UpSeller nao refletem o nome no trigger/chip.
                # Nesses casos, a confirmacao final fica a cargo de _tabela_filtrada_para_loja.
                logger.warning(
                    f"[UpSeller] Filtro nao confirmado no trigger. trigger='{(applied or {}).get('triggerText', '')}'"
                )
                ant_ok = await self._filtrar_por_lojas_ant_select([nome_loja])
                if ant_ok:
                    return True
                await self._page.wait_for_timeout(400)

            await self._page.wait_for_timeout(2200)
            await self.screenshot(f"filtro_loja_{nome_loja[:20]}")
            logger.info(
                f"[UpSeller] Filtro por loja '{nome_loja}' aplicado e salvo (dropdown_fechado={fechou_dropdown})"
            )
            return True
        except Exception as e:
            logger.error(f"[UpSeller] Erro ao filtrar por loja: {e}")
            return False

    async def _configurar_formato_etiqueta(self, sem_rodape_upseller: bool = False) -> bool:
        """
        Ajusta configuracao de impressao no UpSeller antes de baixar etiquetas.

        URL: /pt/settings/order/print-setting

        Objetivo principal:
        - manter formato de etiqueta em PDF 10x15
        - por padrao, preservar configuracao de lista de separacao/rodape do proprio UpSeller
          (modo antigo que o cliente usa em producao)
        - opcionalmente permitir desativar elementos extras quando sem_rodape_upseller=True.

        Retorna:
            True se conseguiu abrir a tela e salvar/aplicar sem erro fatal.
        """
        try:
            # Cache curto para nao reconfigurar em toda chamada.
            if self._ultima_config_etiqueta_ts:
                delta = (datetime.now() - self._ultima_config_etiqueta_ts).total_seconds()
                if delta < 600:
                    logger.info("[UpSeller] Configuracao de etiqueta reutilizada do cache")
                    return True

            if not self._page:
                await self._iniciar_navegador()

            if not await self._esta_logado():
                if not await self.login():
                    logger.warning("[UpSeller] Nao foi possivel configurar etiqueta (nao logado)")
                    return False

            logger.info("[UpSeller] Abrindo configuracao de impressao de etiqueta...")
            await self._page.goto(UPSELLER_PRINT_SETTING, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(2500)
            await self._fechar_popups()

            resultado = await self._page.evaluate(
                """
                (opts) => {
                    const norm = (s) => (s || '')
                      .toLowerCase()
                      .normalize('NFD')
                      .replace(/[\\u0300-\\u036f]/g, '')
                      .replace(/\\s+/g, ' ')
                      .trim();

                    const clickNode = (el) => {
                        if (!el) return false;
                        try {
                            el.scrollIntoView({ behavior: 'instant', block: 'center', inline: 'center' });
                        } catch (_) {}
                        const fire = (node) => {
                            if (!node) return;
                            ['pointerdown', 'mousedown', 'mouseup', 'click'].forEach((evt) => {
                                try {
                                    node.dispatchEvent(new MouseEvent(evt, { bubbles: true, cancelable: true, view: window }));
                                } catch (_) {}
                            });
                        };
                        fire(el);
                        fire(el.closest('label'));
                        fire(el.parentElement);
                        return true;
                    };

                    const clicarOpcaoPorTexto = (palavras, preferirRadio = false) => {
                        const wants = palavras.map(norm);
                        const nodes = Array.from(document.querySelectorAll('label, span, div, button, a'));
                        for (const node of nodes) {
                            const txt = norm(node.textContent || '');
                            if (!txt) continue;
                            if (!wants.some((w) => txt.includes(w))) continue;
                            if (preferirRadio) {
                                const radio = node.querySelector('input[type="radio"]');
                                if (radio) {
                                    clickNode(radio);
                                    return true;
                                }
                            }
                            if (clickNode(node)) return true;
                        }
                        return false;
                    };

                    const setToggle = (keywords, enabled) => {
                        const wants = keywords.map(norm);
                        const labels = Array.from(document.querySelectorAll('label, div, span, tr, li'));
                        for (const lb of labels) {
                            const txt = norm(lb.textContent || '');
                            if (!txt) continue;
                            if (!wants.some((w) => txt.includes(w))) continue;

                            const checkbox = lb.querySelector('input[type="checkbox"]');
                            if (checkbox) {
                                const checked = !!checkbox.checked;
                                if (checked !== enabled) clickNode(checkbox);
                                return true;
                            }

                            const sw = lb.querySelector('.ant-switch, [role="switch"]');
                            if (sw) {
                                const cls = (sw.className || '').toLowerCase();
                                const isOn = cls.includes('ant-switch-checked') || sw.getAttribute('aria-checked') === 'true';
                                if (isOn !== enabled) clickNode(sw);
                                return true;
                            }

                            // Alguns layouts sao apenas botao/linha clicavel.
                            if (clickNode(lb)) return true;
                        }
                        return false;
                    };

                    const passos = [];

                    // Tentar manter formato padrao de etiqueta.
                    if (clicarOpcaoPorTexto(['etiqueta personalizada', 'etiqueta de envio personalizada'], true)) {
                        passos.push('tipo_personalizada');
                    }
                    if (clicarOpcaoPorTexto(['pdf'], true)) {
                        passos.push('formato_pdf');
                    }
                    if (clicarOpcaoPorTexto(['10x15', '10 x 15', '10*15'], true)) {
                        passos.push('tamanho_10x15');
                    }

                    if (opts.semRodape) {
                        // Desativa itens que costumam adicionar conteudo extra no rodape/lista.
                        const k1 = setToggle(
                            ['lista de separacao', 'lista de separação', 'separacao', 'separação'],
                            false
                        );
                        const k2 = setToggle(
                            ['declaracao de conteudo', 'declaração de conteúdo', 'conteudo adicional', 'conteúdo adicional'],
                            false
                        );
                        const k3 = setToggle(
                            ['informacoes do produto', 'informações do produto', 'sku + variante', 'sku variante', 'show sku'],
                            false
                        );
                        if (k1 || k2 || k3) passos.push('rodape_extras_desativados');
                    }

                    // Salvar configuracao.
                    // IMPORTANTE: aqui estamos dentro de evaluate(), portanto nao pode usar
                    // pseudo-seletores do Playwright como :has-text().
                    let salvou = false;
                    const candidatos = Array.from(
                        document.querySelectorAll('button, a, [role="button"], .ant-btn, .ant-btn-primary')
                    );
                    for (const btn of candidatos) {
                        const txt = norm(btn.textContent || btn.innerText || '');
                        if (!txt) continue;
                        if (
                            txt.includes('salvar') ||
                            txt.includes('save') ||
                            txt.includes('confirmar') ||
                            txt.includes('aplicar')
                        ) {
                            clickNode(btn);
                            salvou = true;
                            break;
                        }
                    }

                    // Fallback: se nao encontrou por texto, tenta botao primario visivel.
                    if (!salvou) {
                        const primarios = Array.from(
                            document.querySelectorAll('.ant-btn-primary, button[type="submit"]')
                        );
                        for (const btn of primarios) {
                            const txt = norm(btn.textContent || btn.innerText || '');
                            if (!txt || txt.includes('cancelar') || txt.includes('close') || txt.includes('fechar')) {
                                continue;
                            }
                            clickNode(btn);
                            salvou = true;
                            break;
                        }
                    }

                    return { ok: true, salvou, passos };
                }
                """,
                {"semRodape": bool(sem_rodape_upseller)},
            )

            await self._page.wait_for_timeout(1400)
            await self._fechar_popups()
            await self.screenshot("print_setting_configurado")

            if resultado and resultado.get("ok"):
                passos = resultado.get("passos", [])
                logger.info(
                    "[UpSeller] Configuracao de etiqueta aplicada (sem_rodape=%s, salvou=%s, passos=%s)",
                    sem_rodape_upseller,
                    bool(resultado.get("salvou")),
                    ",".join(passos) if passos else "nenhum",
                )
                self._ultima_config_etiqueta_ts = datetime.now()
                return True

            logger.warning("[UpSeller] Nao confirmou configuracao de etiqueta na pagina")
            return False

        except Exception as e:
            logger.warning(f"[UpSeller] Falha ao configurar formato de etiqueta: {e}")
            return False

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

    async def emitir_nfe(self, filtro_loja: Union[str, List[str], None] = None) -> dict:
        """
        Emite NF-e dos pedidos pendentes em 'Para Emitir'.

        Fluxo:
          1. Navega para /pt/order/pending-invoice (Para Emitir)
          2. Fecha popups/tutoriais
          3. FILTRA POR LOJA se especificado
          4. Verifica se ha pedidos na aba 'Para Emitir'
          5. Seleciona todos os pedidos
          6. Clica em 'Emitir Nota Fiscal' (botao batch no topo)
          7. Confirma no modal se aparecer
          8. Aguarda processamento

        Args:
            filtro_loja: Nome da loja (str) ou lista de lojas para filtrar (opcional, None = todas)

        Retorna: dict com {total_emitidos, sucesso, mensagem}
        """
        if not self._page:
            await self._iniciar_navegador()

        # Emitir NF-e nao deve iniciar tentativa de login interativo.
        # Se a sessao expirou, retorna erro claro para o usuario reconectar.
        if not await self._esta_logado():
            return {
                "total_emitidos": 0,
                "sucesso": False,
                "mensagem": "Sessao expirada. Clique em Reconectar.",
            }

        logger.info("[UpSeller] Iniciando emissao de NF-e...")
        resultado = {"total_emitidos": 0, "sucesso": False, "mensagem": ""}
        filtro_lojas = self._normalizar_lista_lojas_filtro(filtro_loja)
        filtro_desc = (
            filtro_lojas[0] if len(filtro_lojas) == 1
            else f"{len(filtro_lojas)} lojas selecionadas"
        )

        try:
            # 1. Navegar para pagina "Para Emitir"
            await self._page.goto(UPSELLER_PARA_EMITIR, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(3000)

            # 2. Fechar popups/tutoriais
            await self._fechar_popups()
            await self._page.wait_for_timeout(1000)
            await self._fechar_popups()

            # 3. Clicar na aba "Para Emitir" para garantir que estamos na aba correta
            clicou_tab = await self._page.evaluate("""
                (() => {
                    const candidates = document.querySelectorAll(
                        '[role="tab"], .ant-tabs-tab, [class*="tab"], span, div, a'
                    );
                    for (const el of candidates) {
                        const text = (el.textContent || '').trim();
                        if (/^Para Emitir(\\s+\\d+)?$/.test(text)) {
                            if (text.includes('Emitido') || text.includes('Falha'))
                                continue;
                            const rect = el.getBoundingClientRect();
                            if (rect.width > 5 && rect.width < 500 && rect.height < 100) {
                                el.click();
                                return { clicked: true, text: text };
                            }
                        }
                    }
                    return { clicked: false };
                })()
            """)
            if clicou_tab and clicou_tab.get("clicked"):
                logger.info(f"[UpSeller] Clicou na aba 'Para Emitir': {clicou_tab}")
                await self._page.wait_for_timeout(2000)

            await self._fechar_popups()

            # 4. Ler contadores das tabs para evitar filtro em aba zerada.
            cont_tabs_inicio = await self._ler_contadores_tabs_nfe()
            total_para_emitir_tab = int(cont_tabs_inicio.get("para_emitir", 0) or 0)
            if total_para_emitir_tab <= 0:
                # Pequeno retry para evitar falso zero durante render.
                await self._page.wait_for_timeout(450)
                cont_tabs_retry = await self._ler_contadores_tabs_nfe()
                total_para_emitir_tab = max(
                    total_para_emitir_tab,
                    int(cont_tabs_retry.get("para_emitir", 0) or 0),
                )
            logger.info(
                "[UpSeller] Contadores NF-e (inicio): "
                f"para_emitir={total_para_emitir_tab}, "
                f"falha_emissao={int(cont_tabs_inicio.get('falha_na_emissao', 0) or 0)}, "
                f"falha_subir={int(cont_tabs_inicio.get('falha_ao_subir', 0) or 0)}"
            )

            # 5. FILTRAR POR LOJA(S) apenas se houver itens em "Para Emitir".
            # Em aba zerada, pular filtro poupa muito tempo.
            if filtro_lojas and total_para_emitir_tab > 0:
                if len(filtro_lojas) == 1:
                    filtrou = await self._aplicar_filtro_loja_seguro(
                        filtro_lojas[0], contexto="emitir_nfe"
                    )
                else:
                    filtrou = await self._aplicar_filtro_lojas_seguro(
                        filtro_lojas, contexto="emitir_nfe_lote"
                    )
                if not filtrou:
                    logger.error(f"[UpSeller] Nao conseguiu filtrar por loja(s) '{filtro_desc}' em Para Emitir. Abortando.")
                    resultado["mensagem"] = f"Falha ao aplicar filtro de loja(s): {filtro_desc}"
                    return resultado
                logger.info(f"[UpSeller] NF-e filtrado por loja(s): {filtro_desc}")
            elif filtro_lojas and total_para_emitir_tab <= 0:
                logger.info(
                    "[UpSeller] Aba 'Para Emitir' com contador 0; "
                    f"pulando filtro de loja nesta etapa ({filtro_desc})."
                )

            await self.screenshot("emitir_01_pagina_para_emitir")

            # 6. Verificar se ha pedidos
            page_text = await self._page.evaluate("document.body.innerText")

            # Extrair quantidade
            total_match = re.search(r'Para Emitir\s*(\d+)', page_text)
            total_para_emitir = int(total_match.group(1)) if total_match else 0
            logger.info(f"[UpSeller] Pedidos Para Emitir: {total_para_emitir}")

            selecionados = 0
            if total_para_emitir > 0 and "Nenhum Dado" not in page_text and "Total 0" not in page_text:
                # 6. Selecionar todos os pedidos
                try:
                    select_all = await self._page.wait_for_selector(
                        'thead .ant-checkbox-wrapper, th .ant-checkbox-input, '
                        'thead input[type="checkbox"], '
                        '.ant-table-header .ant-checkbox-wrapper',
                        timeout=5000
                    )
                    await select_all.click()
                    await self._page.wait_for_timeout(1000)
                    logger.info("[UpSeller] Checkbox 'selecionar todos' clicado (NF-e)")

                    sel_text = await self._page.evaluate("document.body.innerText")
                    sel_match = re.search(r'Selecionado\s*(\d+)', sel_text)
                    selecionados = int(sel_match.group(1)) if sel_match else total_para_emitir

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

                logger.info(f"[UpSeller] {selecionados} pedidos selecionados para emissao NF-e")
                await self.screenshot("emitir_02_selecionados")

                if selecionados > 0:
                    # 7. Clicar no botao "Emitir Nota Fiscal" (batch na barra de acoes)
                    clicou_btn = await self._page.evaluate("""
                        (() => {
                            const btns = document.querySelectorAll(
                                'button.ant-btn, a.ant-btn, button.ant-btn-link, a.ant-btn-link'
                            );
                            for (const btn of btns) {
                                const text = (btn.textContent || '').trim();
                                if (text === 'Emitir Nota Fiscal' || text.includes('Emitir Nota Fiscal')) {
                                    const rect = btn.getBoundingClientRect();
                                    if (rect.width > 30 && rect.y < 400) {
                                        btn.click();
                                        return { clicked: true, text: text, y: Math.round(rect.y) };
                                    }
                                }
                            }
                            return { clicked: false };
                        })()
                    """)

                    if not clicou_btn or not clicou_btn.get("clicked"):
                        logger.warning("[UpSeller] JS nao encontrou 'Emitir Nota Fiscal', tentando Playwright")
                        btn_emitir = self._page.locator('button:has-text("Emitir Nota Fiscal"), a:has-text("Emitir Nota Fiscal")').first
                        if await btn_emitir.count() > 0:
                            await btn_emitir.click(timeout=5000)
                            logger.info("[UpSeller] Clicou 'Emitir Nota Fiscal' via locator")
                        else:
                            logger.error("[UpSeller] Botao 'Emitir Nota Fiscal' nao encontrado")
                            await self.screenshot("emitir_erro_sem_botao")
                            resultado["mensagem"] = "Botao 'Emitir Nota Fiscal' nao encontrado"
                            return resultado
                    else:
                        logger.info(f"[UpSeller] Clicou 'Emitir Nota Fiscal': {clicou_btn}")

                    await self._page.wait_for_timeout(2000)
                    await self.screenshot("emitir_03_apos_click")

                    # 8. Modal de confirmacao (se aparecer)
                    try:
                        modal_btn = await self._page.wait_for_selector(
                            '.ant-modal button.ant-btn-primary',
                            timeout=5000
                        )
                        if modal_btn:
                            btn_text = await modal_btn.evaluate("el => el.textContent.trim()")
                            logger.info(f"[UpSeller] Modal NF-e encontrado, botao: '{btn_text}'")
                            await modal_btn.click()
                            logger.info("[UpSeller] Confirmou emissao NF-e no modal")
                            await self._page.wait_for_timeout(3000)
                    except Exception:
                        logger.info("[UpSeller] Sem modal de confirmacao de emissao NF-e")

                    await self._fechar_popups()
                    await self.screenshot("emitir_04_apos_confirmar")

                    # 9. Aguardar processamento e verificar resultado
                    logger.info("[UpSeller] Aguardando processamento NF-e (15s)...")
                    await self._page.wait_for_timeout(15000)

                    await self._page.goto(UPSELLER_PARA_EMITIR, wait_until="domcontentloaded", timeout=30000)
                    await self._page.wait_for_timeout(3000)
                    await self._fechar_popups()

                    if filtro_lojas:
                        if len(filtro_lojas) == 1:
                            await self._aplicar_filtro_loja_seguro(
                                filtro_lojas[0], contexto="emitir_nfe_pos"
                            )
                        else:
                            await self._aplicar_filtro_lojas_seguro(
                                filtro_lojas, contexto="emitir_nfe_lote_pos"
                            )

                    new_text = await self._page.evaluate("document.body.innerText")
                    new_match = re.search(r'Para Emitir\s*(\d+)', new_text)
                    novo_total = int(new_match.group(1)) if new_match else 0
                    emitidos_real = total_para_emitir - novo_total
                    if emitidos_real < 0:
                        emitidos_real = selecionados

                    logger.info(f"[UpSeller] NF-e - Antes: {total_para_emitir}, Agora: {novo_total}, Emitidos: {emitidos_real}")
                    await self.screenshot("emitir_05_finalizado")

                    resultado["total_emitidos"] = emitidos_real if emitidos_real > 0 else selecionados
                    resultado["sucesso"] = True
                    resultado["mensagem"] = f"{resultado['total_emitidos']} NF-e emitidas com sucesso"
                    logger.info(f"[UpSeller] Emissao NF-e concluida: {resultado['total_emitidos']}")
                else:
                    logger.info("[UpSeller] Para Emitir > 0, mas sem itens selecionados para emissao em lote")
                    resultado["sucesso"] = True
                    resultado["mensagem"] = "Nenhum pedido selecionado na aba Para Emitir"
            else:
                # Nao aborta aqui: ainda precisamos tratar abas de falha.
                resultado["sucesso"] = True
                resultado["mensagem"] = "Nenhum pedido na aba Para Emitir"
                logger.info("[UpSeller] Aba 'Para Emitir' com 0 itens. Seguindo para reprocessar abas de falha.")

            # === 10. Tentar reprocessar "Falha na Emissao" ===
            falha_result = {}
            try:
                # Reaproveita a lista normalizada para manter o mesmo escopo
                # de filtro usado na etapa principal de emissao.
                falha_result = await self._retentar_falha_emissao(filtro_loja=filtro_lojas)
                if falha_result.get("total_retentados", 0) > 0:
                    resultado["total_emitidos"] += falha_result.get("sucesso_reemissao", 0)
                    resultado["mensagem"] = (
                        f"{resultado['total_emitidos']} NF-e emitidas "
                        f"({falha_result.get('sucesso_reemissao', 0)} re-emitidas de falhas)"
                    )
                if falha_result.get("falhas_persistentes", 0) > 0:
                    resultado["aviso_falhas"] = (
                        f"Atencao: {falha_result['falhas_persistentes']} NF-e com falha persistente. "
                        f"Verifique no UpSeller (abas 'Falha na Emissao' e 'Falha ao subir')."
                    )
                    resultado["falhas_persistentes"] = falha_result["falhas_persistentes"]
                if falha_result.get("motivos_falhas"):
                    resultado["motivos_falhas"] = falha_result.get("motivos_falhas", [])[:8]
            except Exception as e_falha:
                logger.warning(f"[UpSeller] Erro ao retentar falhas de emissao: {e_falha}")

            # IMPORTANTE:
            # A acao "Emitir Notas Fiscais" NAO deve gerar etiquetas.
            # Aqui mantemos somente o fluxo de NF-e (incluindo retentativa em abas de falha).

        except Exception as e:
            logger.error(f"[UpSeller] Erro ao emitir NF-e: {e}")
            resultado["mensagem"] = str(e)
            await self.screenshot("emitir_erro")

        return resultado

    async def _retentar_falha_emissao(self, filtro_loja: Union[str, List[str], None] = None) -> dict:
        """
        Reprocessa falhas de NF-e nas abas:
        - Falha na Emissao
        - Falha ao subir

        Se houver falha persistente, retorna tambem motivos resumidos para aviso ao usuario.
        """
        resultado = {
            "total_retentados": 0,
            "sucesso_reemissao": 0,
            "falhas_persistentes": 0,
            "mensagem": "",
            "motivos_falhas": [],
            "detalhes_abas": [],
        }
        filtro_lojas = self._normalizar_lista_lojas_filtro(filtro_loja)

        try:
            logger.info("[UpSeller] Reprocessando abas de falha de NF-e...")

            abas_alvo = [
                {
                    "nome": "Falha na Emissao",
                    "alvos": ["falha na emissao", "falha emissao"],
                    "key": "falha_na_emissao",
                },
                {
                    "nome": "Falha ao subir",
                    "alvos": ["falha ao subir"],
                    "key": "falha_ao_subir",
                },
            ]

            async def _clicar_aba_por_alvo(alvos):
                return await self._page.evaluate(
                    """
                    (targets) => {
                        const normalize = (s) => (s || '')
                          .toLowerCase()
                          .normalize('NFD')
                          .replace(/[\\u0300-\\u036f]/g, '')
                          .replace(/\\s+/g, ' ')
                          .trim();
                        const tgts = (targets || []).map(normalize).filter(Boolean);
                        const nodes = document.querySelectorAll('[role="tab"], .ant-tabs-tab');
                        for (const el of nodes) {
                            const tRaw = (el.textContent || '').trim();
                            const t = normalize(tRaw);
                            if (!t) continue;
                            if (t.includes('para emitir')) continue;
                            if (t.includes('emitido')) continue;
                            if (!tgts.some(x => t.includes(x))) continue;
                            const r = el.getBoundingClientRect();
                            if (r.width > 5 && r.width < 500 && r.height > 5 && r.height < 100 && r.y < 420) {
                                el.click();
                                return { clicked: true, text: tRaw };
                            }
                        }
                        return { clicked: false, text: '' };
                    }
                    """,
                    alvos or [],
                )

            async def _ler_contador_tab_por_alvo(alvos):
                try:
                    tgts = [
                        re.sub(r"\s+", " ", (str(x or "")).strip().casefold())
                        for x in (alvos or [])
                    ]
                    cont = await self._ler_contadores_tabs_nfe()
                    if any("falha ao subir" in t for t in tgts):
                        return int(cont.get("falha_ao_subir", 0) or 0)
                    if any(("falha na emissao" in t) or ("falha emissao" in t) for t in tgts):
                        return int(cont.get("falha_na_emissao", 0) or 0)
                    if any("emitindo" in t for t in tgts):
                        return int(cont.get("emitindo", 0) or 0)
                    if any("para emitir" in t for t in tgts):
                        return int(cont.get("para_emitir", 0) or 0)
                    return 0
                except Exception:
                    return 0

            async def _aba_ativa_confere_alvo(alvos):
                try:
                    ok = await self._page.evaluate(
                        """
                        (targets) => {
                            const normalize = (s) => (s || '')
                              .toLowerCase()
                              .normalize('NFD')
                              .replace(/[\\u0300-\\u036f]/g, '')
                              .replace(/\\s+/g, ' ')
                              .trim();
                            const tgts = (targets || []).map(normalize).filter(Boolean);
                            const active = document.querySelector('.ant-tabs-tab-active, [role="tab"][aria-selected="true"]');
                            const txt = normalize(active ? (active.textContent || '') : '');
                            if (!txt || !tgts.length) return false;
                            if (txt.includes('para emitir') || txt.includes('emitido')) return false;
                            return tgts.some((x) => txt.includes(x));
                        }
                        """,
                        alvos or [],
                    )
                    return bool(ok)
                except Exception:
                    return False

            async def _contar_itens_aba_atual():
                return await self._page.evaluate("""
                    (() => {
                        const txt = (document.body.innerText || '').trim();
                        if (txt.includes('Nenhum Dado') || txt.includes('Total 0')) return 0;

                        const active = document.querySelector('.ant-tabs-tab-active, .ant-tabs-tab.ant-tabs-tab-active');
                        if (active) {
                            const at = (active.textContent || '').trim();
                            const mAt = at.match(/(\\d+)\\s*$/);
                            if (mAt) return parseInt(mAt[1], 10) || 0;
                        }

                        const topRows = document.querySelectorAll('tbody tr.top_row, tr.top_row').length;
                        if (topRows > 0) return topRows;
                        const dataRows = document.querySelectorAll('tbody tr.ant-table-row, tbody tr[data-row-key], tbody tr.row.my_table_border').length;
                        return dataRows > 0 ? dataRows : 0;
                    })()
                """)

            async def _coletar_motivos_visiveis():
                motivos = await self._page.evaluate("""
                    (() => {
                        const out = [];
                        const seen = new Set();
                        const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
                        const rows = document.querySelectorAll('tbody tr, tr.top_row, tr.row.my_table_border');
                        const re = /(erro|falha|rejei|inv[aá]l|duplic|cpf|cnpj|ncm|icms|cep|uf|xml|assinatur|schema|timeout)/i;
                        for (const row of rows) {
                            const txt = norm(row.textContent || '');
                            if (!txt) continue;
                            if (!re.test(txt)) continue;
                            const s = txt.slice(0, 180);
                            const key = s.toLowerCase();
                            if (!seen.has(key)) {
                                seen.add(key);
                                out.push(s);
                            }
                            if (out.length >= 8) break;
                        }
                        return out;
                    })()
                """)
                return [str(x).strip() for x in (motivos or []) if str(x or "").strip()]

            async def _executar_batch_aba():
                selecionados = 0
                try:
                    select_all = await self._page.wait_for_selector(
                        'thead .ant-checkbox-wrapper, th .ant-checkbox-input, '
                        'thead input[type="checkbox"], .ant-table-header .ant-checkbox-wrapper',
                        timeout=5000
                    )
                    await select_all.click()
                    await self._page.wait_for_timeout(900)
                    sel_txt = await self._page.evaluate("document.body.innerText")
                    m_sel = re.search(r'Selecionad[oa]s?\s*(\d+)', sel_txt)
                    selecionados = int(m_sel.group(1)) if m_sel else 0
                except Exception:
                    checkboxes = await self._page.query_selector_all(
                        'tbody .ant-checkbox-input, tbody input[type="checkbox"]'
                    )
                    for cb in checkboxes[:200]:
                        try:
                            await cb.click()
                            selecionados += 1
                        except Exception:
                            pass
                    await self._page.wait_for_timeout(500)

                if selecionados <= 0:
                    return {"ok": False, "motivo": "Nenhum pedido selecionado"}

                clicou = await self._page.evaluate("""
                    (() => {
                        const targets = ['Emitir Nota Fiscal', 'Subir', 'Reenviar', 'Reprocessar'];
                        const btns = document.querySelectorAll('button.ant-btn, a.ant-btn, button, a');
                        for (const btn of btns) {
                            const t = (btn.textContent || '').trim();
                            if (!t) continue;
                            if (!targets.some(x => t.includes(x))) continue;
                            const r = btn.getBoundingClientRect();
                            if (r.width > 30 && r.height > 10 && r.y < 420) {
                                btn.click();
                                return { clicked: true, text: t };
                            }
                        }
                        return { clicked: false, text: '' };
                    })()
                """)
                if not clicou or not clicou.get("clicked"):
                    return {"ok": False, "motivo": "Botao de reprocessamento nao encontrado"}

                try:
                    modal_btn = await self._page.wait_for_selector(
                        '.ant-modal button.ant-btn-primary',
                        timeout=5000
                    )
                    if modal_btn:
                        await modal_btn.click()
                        await self._page.wait_for_timeout(2000)
                except Exception:
                    pass

                return {"ok": True}

            # Leitura inicial dos contadores para pular abas zeradas sem gastar tempo.
            contadores_hint = {}
            try:
                await self._page.goto(UPSELLER_PARA_EMITIR, wait_until="domcontentloaded", timeout=30000)
                await self._page.wait_for_timeout(1700)
                await self._fechar_popups()
                contadores_hint = await self._ler_contadores_tabs_nfe()
            except Exception:
                contadores_hint = await self._ler_contadores_tabs_nfe()

            total_falhas_hint = int(contadores_hint.get("falha_na_emissao", 0) or 0) + int(
                contadores_hint.get("falha_ao_subir", 0) or 0
            )
            if total_falhas_hint <= 0:
                logger.info("[UpSeller] Abas de falha com contador 0; pulando reprocessamento.")
                for aba in abas_alvo:
                    resultado["detalhes_abas"].append({
                        "aba": aba["nome"],
                        "antes": 0,
                        "sucesso": 0,
                        "persistentes": 0,
                        "erro": "",
                        "pulado": "contador_zero",
                    })
                resultado["mensagem"] = "Nenhuma falha para reprocessar"
                return resultado

            for aba in abas_alvo:
                nome_aba = aba["nome"]
                slug_aba = re.sub(r'[^a-z0-9]+', '_', (nome_aba or '').lower()).strip('_') or "falha"
                detalhe = {
                    "aba": nome_aba,
                    "antes": 0,
                    "sucesso": 0,
                    "persistentes": 0,
                    "erro": "",
                }
                hint_aba = int(contadores_hint.get(aba.get("key", ""), 0) or 0)
                if hint_aba <= 0:
                    detalhe["pulado"] = "contador_zero"
                    resultado["detalhes_abas"].append(detalhe)
                    logger.info(f"[UpSeller] Aba '{nome_aba}' com 0; pulando filtro/execucao.")
                    continue
                try:
                    await self._page.goto(UPSELLER_PARA_EMITIR, wait_until="domcontentloaded", timeout=30000)
                    await self._page.wait_for_timeout(2500)
                    await self._fechar_popups()

                    c1 = await _clicar_aba_por_alvo(aba["alvos"])
                    if not c1 or not c1.get("clicked"):
                        detalhe["erro"] = "aba_nao_encontrada"
                        resultado["detalhes_abas"].append(detalhe)
                        continue
                    await self._page.wait_for_timeout(1800)
                    await self._fechar_popups()

                    # Blindagem: nunca executar batch se a aba ativa nao for a de falha alvo.
                    # Isso evita clicar "Emitir Nota Fiscal" novamente em "Para Emitir".
                    aba_ok = await _aba_ativa_confere_alvo(aba["alvos"])
                    if not aba_ok:
                        # tentativa extra de clique antes de desistir
                        c1_retry = await _clicar_aba_por_alvo(aba["alvos"])
                        if c1_retry and c1_retry.get("clicked"):
                            await self._page.wait_for_timeout(1200)
                            aba_ok = await _aba_ativa_confere_alvo(aba["alvos"])
                    if not aba_ok:
                        detalhe["erro"] = "aba_alvo_nao_ativa"
                        resultado["detalhes_abas"].append(detalhe)
                        continue

                    # Pula abas zeradas rapidamente para evitar voltas desnecessarias.
                    contador_aba = await _ler_contador_tab_por_alvo(aba["alvos"])
                    if contador_aba <= 0:
                        detalhe["antes"] = 0
                        resultado["detalhes_abas"].append(detalhe)
                        continue

                    if filtro_lojas:
                        if len(filtro_lojas) == 1:
                            filtrou = await self._aplicar_filtro_loja_seguro(
                                filtro_lojas[0], contexto=f"retentar_{slug_aba}"
                            )
                        else:
                            filtrou = await self._aplicar_filtro_lojas_seguro(
                                filtro_lojas, contexto=f"retentar_{slug_aba}_lote"
                            )
                        if not filtrou:
                            detalhe["erro"] = "falha_filtro_lojas"
                            resultado["detalhes_abas"].append(detalhe)
                            continue

                    total_antes = int((await _contar_itens_aba_atual()) or 0)
                    detalhe["antes"] = total_antes
                    if total_antes <= 0:
                        resultado["detalhes_abas"].append(detalhe)
                        continue

                    resultado["total_retentados"] += total_antes
                    await self.screenshot(f"falha_nfe_{slug_aba}_01_antes")

                    exec_batch = await _executar_batch_aba()
                    if not exec_batch.get("ok"):
                        detalhe["erro"] = exec_batch.get("motivo", "erro_batch")
                        detalhe["persistentes"] = total_antes
                        resultado["falhas_persistentes"] += total_antes
                        motivos = await _coletar_motivos_visiveis()
                        for m in motivos:
                            if m not in resultado["motivos_falhas"]:
                                resultado["motivos_falhas"].append(m)
                        resultado["detalhes_abas"].append(detalhe)
                        continue

                    await self._fechar_popups()
                    await self._page.wait_for_timeout(12000)

                    await self._page.goto(UPSELLER_PARA_EMITIR, wait_until="domcontentloaded", timeout=30000)
                    await self._page.wait_for_timeout(2200)
                    await self._fechar_popups()
                    c2 = await _clicar_aba_por_alvo(aba["alvos"])
                    if c2 and c2.get("clicked") and await _aba_ativa_confere_alvo(aba["alvos"]):
                        await self._page.wait_for_timeout(1500)
                        if filtro_lojas:
                            if len(filtro_lojas) == 1:
                                await self._aplicar_filtro_loja_seguro(
                                    filtro_lojas[0], contexto=f"retentar_pos_{slug_aba}"
                                )
                            else:
                                await self._aplicar_filtro_lojas_seguro(
                                    filtro_lojas, contexto=f"retentar_pos_{slug_aba}_lote"
                                )
                        total_depois = int((await _contar_itens_aba_atual()) or 0)
                    else:
                        total_depois = 0

                    sucesso = max(0, total_antes - total_depois)
                    persist = max(0, total_depois)
                    detalhe["sucesso"] = sucesso
                    detalhe["persistentes"] = persist

                    resultado["sucesso_reemissao"] += sucesso
                    resultado["falhas_persistentes"] += persist

                    if persist > 0:
                        motivos = await _coletar_motivos_visiveis()
                        if not motivos:
                            motivos = [f"{nome_aba}: erro persistente sem detalhe visivel"]
                        for m in motivos:
                            if m not in resultado["motivos_falhas"]:
                                resultado["motivos_falhas"].append(m)

                    await self.screenshot(f"falha_nfe_{slug_aba}_02_depois")
                    resultado["detalhes_abas"].append(detalhe)
                except Exception as e_aba:
                    detalhe["erro"] = str(e_aba)
                    resultado["detalhes_abas"].append(detalhe)
                    logger.warning(f"[UpSeller] Falha ao reprocessar aba '{nome_aba}': {e_aba}")

            if resultado["total_retentados"] <= 0:
                resultado["mensagem"] = "Nenhuma falha para reprocessar"
            else:
                resultado["mensagem"] = (
                    f"{resultado['sucesso_reemissao']} reprocessadas, "
                    f"{resultado['falhas_persistentes']} falhas persistentes"
                )
            if resultado["motivos_falhas"]:
                resultado["motivos_falhas"] = resultado["motivos_falhas"][:8]

        except Exception as e:
            logger.error(f"[UpSeller] Erro ao retentar falhas de emissao: {e}")
            resultado["mensagem"] = str(e)
            await self.screenshot("falha_emissao_erro")

        return resultado

    async def programar_envio(self, filtro_loja: Union[str, List[str], None] = None) -> dict:
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
            filtro_loja: Nome da loja (str) ou lista de lojas para filtrar (opcional, None = todas)

        Retorna: dict com {total_programados, sucesso, mensagem}
        """
        if not self._page:
            await self._iniciar_navegador()

        if not await self._esta_logado():
            if not await self.login():
                return {"total_programados": 0, "sucesso": False, "mensagem": "Nao logado"}

        logger.info("[UpSeller] Iniciando programacao de envio...")
        resultado = {"total_programados": 0, "sucesso": False, "mensagem": ""}
        filtro_lojas = self._normalizar_lista_lojas_filtro(filtro_loja)
        filtro_multiplo = len(filtro_lojas) > 1
        filtro_desc = (
            filtro_lojas[0] if len(filtro_lojas) == 1
            else f"{len(filtro_lojas)} lojas selecionadas"
        )

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

            # 3.5. FILTRAR POR LOJA(S) se especificado
            filtrou_loja = False
            if filtro_lojas:
                if len(filtro_lojas) == 1:
                    filtrou_loja = await self._aplicar_filtro_loja_seguro(
                        filtro_lojas[0], contexto="programar_envio"
                    )
                else:
                    filtrou_loja = await self._aplicar_filtro_lojas_seguro(
                        filtro_lojas, contexto="programar_envio_lote"
                    )
                if filtrou_loja:
                    logger.info(f"[UpSeller] Filtrado por loja(s): {filtro_desc}")
                    await self._page.wait_for_timeout(2000)
                else:
                    logger.error(f"[UpSeller] Nao conseguiu filtrar por loja(s) '{filtro_desc}'. Abortando para nao processar tudo.")
                    return {"sucesso": False, "total_programados": 0, "mensagem": f"Falha ao filtrar loja(s): '{filtro_desc}'"}

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

            # 4.1 Validar se filtro de loja realmente entrou na tabela.
            # Se ficar misto, tenta reaplicar. Se falhar, ABORTA.
            if filtro_lojas and filtrou_loja:
                if len(filtro_lojas) == 1:
                    tabela_ok = await self._tabela_filtrada_para_loja(filtro_lojas[0])
                else:
                    tabela_ok = await self._tabela_filtrada_para_lojas(filtro_lojas)
                if not tabela_ok:
                    logger.warning(f"[UpSeller] Tabela ainda mista apos filtro '{filtro_desc}', reaplicando...")
                    if len(filtro_lojas) == 1:
                        filtrou_loja = await self._aplicar_filtro_loja_seguro(
                            filtro_lojas[0], contexto="programar_envio_reaplicar"
                        )
                    else:
                        filtrou_loja = await self._aplicar_filtro_lojas_seguro(
                            filtro_lojas, contexto="programar_envio_reaplicar_lote"
                        )
                    if filtrou_loja:
                        await self._page.wait_for_timeout(1500)
                        await self._abrir_subaba_para_programar()
                        if len(filtro_lojas) == 1:
                            tabela_ok = await self._tabela_filtrada_para_loja(filtro_lojas[0])
                        else:
                            tabela_ok = await self._tabela_filtrada_para_lojas(filtro_lojas)
                if not tabela_ok:
                    logger.error(
                        f"[UpSeller] Filtro por loja(s) '{filtro_desc}' nao confirmou na tabela. "
                        "Abortando para nao processar pedidos de outras lojas."
                    )
                    return {"sucesso": False, "total_programados": 0,
                            "mensagem": f"Filtro por loja(s) '{filtro_desc}' nao confirmado na tabela"}

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

            # 6. Selecionar pedidos
            selecionados = 0
            try:
                # Se filtro foi aplicado com sucesso, usar selecao em massa da tabela.
                # Fallback por linha fica restrito a quando o filtro falhar.
                if (not filtro_lojas) or filtrou_loja:
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

                    # Popup "Selecionar todas as paginas" (quando existir).
                    # IMPORTANTE: NÃO clicar quando filtro de loja esta ativo!
                    # O "Selecionar todas as páginas" do UpSeller ignora o filtro
                    # e seleciona pedidos de TODAS as lojas no backend.
                    if (not filtro_lojas) or filtro_multiplo:
                        try:
                            btn_todas_paginas = await self._page.query_selector(
                                "button:has-text('Selecionar todas as páginas'), "
                                "a:has-text('Selecionar todas as páginas'), "
                                "button:has-text('Selecionar todas as paginas'), "
                                "a:has-text('Selecionar todas as paginas')"
                            )
                            if btn_todas_paginas:
                                await btn_todas_paginas.click()
                                await self._page.wait_for_timeout(900)
                                logger.info("[UpSeller] Clicou 'Selecionar todas as paginas'")
                        except Exception:
                            pass
                    else:
                        logger.info(
                            f"[UpSeller] Filtro por loja unica '{filtro_desc}' ativo - "
                            "nao clicando 'Selecionar todas as paginas' para evitar mistura indevida"
                        )

                    # Extrair quantos foram selecionados
                    sel_text = await self._page.evaluate("document.body.innerText")
                    sel_match = re.search(r'Selecionad[oa]s?\s*(\d+)', sel_text)
                    selecionados = int(sel_match.group(1)) if sel_match else total_para_programar
                else:
                    # Fallback robusto: selecionar apenas linhas que contem a loja alvo.
                    sel_info = await self._page.evaluate("""
                        (nomeLoja) => {
                            const normalize = (s) => (s || '')
                              .toLowerCase()
                              .normalize('NFD')
                              .replace(/[\\u0300-\\u036f]/g, '')
                              .trim();
                            const target = normalize(nomeLoja);
                            const rows = Array.from(document.querySelectorAll(
                                'tr, .table_sub_head_row, .tr_top_content, .order_item, .list_table tbody tr'
                            ));

                            let candidates = 0;
                            let selected = 0;
                            for (const row of rows) {
                                const txt = normalize(row.textContent || '');
                                if (!txt || !txt.includes(target)) continue;
                                // Evita misturar plataformas no batch: programar envio deve ficar em Shopee.
                                if (!txt.includes('shopee') || txt.includes('shein')) continue;
                                candidates += 1;

                                const cb = row.querySelector('input[type="checkbox"], .ant-checkbox-input');
                                if (!cb) continue;
                                try {
                                    const isChecked = cb.checked === true || cb.getAttribute('aria-checked') === 'true';
                                    if (!isChecked) cb.click();
                                    selected += 1;
                                } catch (_) {}
                            }
                            return { candidates, selected };
                        }
                    """, (filtro_lojas[0] if filtro_lojas else ""))
                    selecionados = int((sel_info or {}).get("selected", 0))
                    logger.info(
                        f"[UpSeller] Fallback selecao por loja '{filtro_desc}': "
                        f"candidatos={(sel_info or {}).get('candidates', 0)}, selecionados={selecionados}"
                    )
                    await self._page.wait_for_timeout(1000)

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

            # 7.1 Detectar erro de plataforma mista (não seguir no pipeline inválido)
            try:
                body_text = await self._page.evaluate("document.body.innerText")
                if "Compatível apenas mesmos pedidos de plataforma" in body_text:
                    logger.error("[UpSeller] Erro: pedidos de plataformas diferentes no lote")
                    resultado["mensagem"] = "Erro: lote com plataformas diferentes. Revise o filtro de loja."
                    resultado["sucesso"] = False
                    await self.screenshot("programar_erro_plataforma_mista")
                    return resultado
            except Exception:
                pass

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

            # 9. Aguardar processamento completo (nao apenas sleep fixo)
            # para evitar seguir pipeline antes de finalizar "Programando/Obtendo rastreio".
            logger.info("[UpSeller] Aguardando conclusao de Programando/Obtendo rastreio...")
            await self._page.wait_for_timeout(5000)
            cont_final = await self._aguardar_conclusao_programacao(timeout_segundos=300)

            # Recalcular total para programar apos conclusao
            try:
                await self._page.goto(UPSELLER_PEDIDOS, wait_until="domcontentloaded", timeout=30000)
                await self._page.wait_for_timeout(2500)
                await self._fechar_popups()
                await self._abrir_subaba_para_programar()
            except Exception:
                pass
            novo_total = await self._ler_contagem_para_programar()
            programados_real = total_para_programar - int(novo_total or 0)
            if programados_real < 0:
                programados_real = 0
            if programados_real == 0 and selecionados > 0:
                # Se o contador nao refletiu no DOM, assumir ao menos os selecionados
                # para manter consistencia do pipeline.
                programados_real = selecionados

            logger.info(
                "[UpSeller] Programacao concluida (tabs finais: para_programar=%s, programando=%s, "
                "falha_programacao=%s, obtendo_rastreio=%s, erro_rastreio=%s)",
                cont_final.get("para_programar", 0),
                cont_final.get("programando", 0),
                cont_final.get("falha_na_programacao", 0),
                cont_final.get("obtendo_rastreio", 0),
                cont_final.get("erro_obter_rastreio", 0),
            )
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

    async def _salvar_pdf_de_popup(self, popup_page, save_path: str) -> bool:
        """
        Tenta salvar o PDF original quando o UpSeller abre um novo tab.

        Cenarios suportados:
        1) URL direta .pdf
        2) Preview HTML com iframe/object/embed apontando para PDF
        3) Preview com blob: URL

        Retorna True se o arquivo foi salvo.
        """
        try:
            await popup_page.wait_for_timeout(800)
        except Exception:
            pass

        try:
            popup_url = popup_page.url or ""
            if popup_url:
                logger.info(f"[UpSeller] URL popup: {popup_url}")
        except Exception:
            popup_url = ""

        # Coletar candidatos de URL de PDF a partir da pagina.
        candidates = []
        try:
            candidates = await popup_page.evaluate(
                """
                () => {
                    const out = [];
                    const add = (u) => {
                        if (!u) return;
                        let v = String(u).trim();
                        if (!v) return;
                        try {
                            v = new URL(v, window.location.href).href;
                        } catch (_) {}
                        if (!out.includes(v)) out.push(v);
                    };

                    add(window.location.href);
                    try {
                        const params = new URLSearchParams(window.location.search || '');
                        for (const key of ['file', 'url', 'src', 'pdf', 'download']) {
                            add(params.get(key));
                        }
                    } catch (_) {}

                    document.querySelectorAll('iframe, embed, object, a, source').forEach((el) => {
                        add(el.getAttribute('src'));
                        add(el.getAttribute('data'));
                        add(el.getAttribute('href'));
                        add(el.src);
                        add(el.data);
                        add(el.href);
                    });

                    return out;
                }
                """
            )
        except Exception:
            candidates = []

        if popup_url:
            candidates = [popup_url] + list(candidates or [])

        # Normalizar e manter apenas urls com cara de PDF.
        vistos = set()
        filtrados = []
        for u in candidates or []:
            u = (u or "").strip()
            if not u or u in vistos:
                continue
            vistos.add(u)
            low = u.lower()
            if low.endswith(".pdf") or ".pdf?" in low or "/pdf" in low or low.startswith("blob:"):
                filtrados.append(u)

        # Fallback: tentar todos os candidatos se nenhum passou no filtro.
        if not filtrados:
            filtrados = list(vistos)

        for url_cand in filtrados:
            try:
                low = (url_cand or "").lower()

                if low.startswith("blob:"):
                    # Blob precisa ser lido no contexto da pagina.
                    b64 = await popup_page.evaluate(
                        """
                        async (u) => {
                            const r = await fetch(u);
                            const b = await r.arrayBuffer();
                            const bytes = new Uint8Array(b);
                            let binary = '';
                            const chunk = 0x8000;
                            for (let i = 0; i < bytes.length; i += chunk) {
                                const sub = bytes.subarray(i, i + chunk);
                                binary += String.fromCharCode.apply(null, sub);
                            }
                            return btoa(binary);
                        }
                        """,
                        url_cand,
                    )
                    if b64:
                        import base64
                        pdf_data = base64.b64decode(b64)
                        with open(save_path, "wb") as f:
                            f.write(pdf_data)
                        logger.info(f"[UpSeller] PDF salvo via blob do popup: {save_path}")
                        return True
                    continue

                # URL HTTP(S) com cookies/sessao do contexto atual.
                resp = await popup_page.context.request.get(url_cand, timeout=90000)
                if not resp or not resp.ok:
                    continue

                body = await resp.body()
                if not body or len(body) < 1200:
                    continue

                # Validar assinatura PDF.
                if not body.startswith(b"%PDF"):
                    # Alguns servidores retornam HTML com status 200.
                    ct = (resp.headers.get("content-type") or "").lower()
                    if "pdf" not in ct:
                        continue

                with open(save_path, "wb") as f:
                    f.write(body)
                logger.info(f"[UpSeller] PDF salvo via URL real do popup: {save_path}")
                return True
            except Exception as e:
                logger.debug(f"[UpSeller] Falha ao baixar candidato de popup '{url_cand}': {e}")

        return False

    async def baixar_lista_separacao(self, filtro_loja: str = None) -> List[str]:
        """
        Baixa a "Lista de Separação" do UpSeller para obter dados de produtos
        (SKU, variação, quantidade) que serão usados no rodapé das etiquetas.

        Fluxo:
        1. Navega para "Etiqueta para Impressão"
        2. Filtra por loja (se especificado)
        3. Seleciona todos os pedidos
        4. Hover em "Imprimir Etiquetas" → clica "Imprimir Lista de Separação"
        5. Captura novo tab (print-pick-list) e salva como PDF via CDP

        Retorna lista de caminhos dos PDFs baixados.
        """
        pdfs_baixados = []
        try:
            if not self._page:
                await self._iniciar_navegador()
            if not await self._esta_logado():
                if not await self.login():
                    logger.warning("[UpSeller] Nao logado para baixar lista de separacao")
                    return []

            logger.info(f"[UpSeller] Baixando lista de separacao (loja={filtro_loja or 'todas'})...")

            # 1. Navegar para "Etiqueta para Impressão"
            await self._page.goto(UPSELLER_PARA_IMPRIMIR, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(3000)
            await self._fechar_popups()

            # Clicar na aba "Etiqueta para Impressão"
            async def _clicar_aba_impressao():
                result = await self._page.evaluate("""
                    (() => {
                        const tabs = document.querySelectorAll('[role="tab"], .ant-tabs-tab, div.ant-tabs-tab');
                        for (const tab of tabs) {
                            const text = (tab.textContent || '').trim();
                            if (text.includes('Etiqueta para Impress') ||
                                (text.includes('Impress') && !text.includes('Falhada') && !text.includes('Gerando'))) {
                                tab.click();
                                return { clicked: true, text: text };
                            }
                        }
                        return { clicked: false };
                    })()
                """)
                if result and result.get("clicked"):
                    logger.info(f"[UpSeller] Aba clicada para lista separacao: {result.get('text')}")
                    await self._page.wait_for_timeout(2000)
                    return True
                return False

            await _clicar_aba_impressao()

            # 2. Filtrar por loja
            if filtro_loja:
                filtrou = await self._aplicar_filtro_loja_seguro(
                    filtro_loja, contexto="lista_separacao"
                )
                if not filtrou:
                    logger.error(f"[UpSeller] Nao filtrou loja '{filtro_loja}' para lista separacao. Abortando.")
                    return []
                logger.info(f"[UpSeller] Lista separacao filtrada por loja: {filtro_loja}")

            # 3. Verificar se há pedidos na tabela
            tem_dados = await self._page.evaluate("""
                (() => {
                    const rows = document.querySelectorAll(
                        'tbody tr.ant-table-row, tbody tr.top_row, tbody tr:not(.ant-table-placeholder)'
                    );
                    const dataRows = Array.from(rows).filter(r => {
                        const text = (r.textContent || '').trim();
                        return text.length > 5 && !text.includes('Nenhum Dado');
                    });
                    if (dataRows.length > 0) return { hasData: true, count: dataRows.length };
                    const activeTab = document.querySelector('.ant-tabs-tab-active');
                    if (activeTab) {
                        const m = (activeTab.textContent || '').match(/(\\d+)/);
                        if (m && parseInt(m[1]) > 0) return { hasData: true, count: parseInt(m[1]) };
                    }
                    return { hasData: false, count: 0 };
                })()
            """)

            if not tem_dados or not tem_dados.get("hasData"):
                logger.info("[UpSeller] 0 pedidos para lista de separacao - nada para baixar")
                return []

            logger.info(f"[UpSeller] {tem_dados.get('count', '?')} pedido(s) para lista de separacao")

            # 4. Selecionar todos
            try:
                select_all = await self._page.wait_for_selector(
                    'thead .ant-checkbox-wrapper, th .ant-checkbox-input, '
                    'thead input[type="checkbox"], '
                    '.ant-table-header .ant-checkbox-wrapper',
                    timeout=5000
                )
                await select_all.click()
                await self._page.wait_for_timeout(1000)
                logger.info("[UpSeller] Checkbox 'selecionar todos' clicado (lista separacao)")

                # NÃO clicar "Selecionar todas as páginas" quando filtro_loja ativo
                if not filtro_loja:
                    try:
                        loc_sel_todas = self._page.locator(
                            '.ant-dropdown-menu-item:has-text("Selecionar todas")'
                        ).first
                        if await loc_sel_todas.is_visible(timeout=2000):
                            await loc_sel_todas.click()
                            logger.info("[UpSeller] 'Selecionar todas as paginas' clicado (lista separacao)")
                            await self._page.wait_for_timeout(1500)
                    except Exception:
                        pass
                else:
                    logger.info(f"[UpSeller] Filtro loja '{filtro_loja}' ativo - NÃO clicando 'Selecionar todas'")
            except Exception as e:
                logger.warning(f"[UpSeller] Checkbox selecionar todos nao encontrado: {e}")
                return []

            # 5. Hover no trigger "Imprimir Etiquetas" para abrir dropdown
            try:
                trigger = self._page.locator(
                    'a.ant-dropdown-trigger:has-text("Imprimir Etiquetas")'
                ).first
                if await trigger.count() == 0:
                    trigger = self._page.locator(
                        'a.ant-btn-link:has-text("Imprimir Etiquetas")'
                    ).first
                if await trigger.count() == 0:
                    trigger = self._page.locator(
                        'a:has-text("Imprimir em Massa")'
                    ).first

                if await trigger.count() == 0:
                    logger.error("[UpSeller] Botao 'Imprimir Etiquetas' nao encontrado para lista separacao")
                    return []

                logger.info("[UpSeller] Hover em 'Imprimir Etiquetas' para abrir dropdown (lista separacao)...")
                await trigger.hover()
                await self._page.wait_for_timeout(1500)

                # 6. Clicar opção "Imprimir Lista de Separação"
                opcao_lista = self._page.locator(
                    '.ant-dropdown-menu-item:has-text("Lista de Separação"), '
                    '.ant-dropdown-menu-item:has-text("Lista de Separacao")'
                ).first

                if not await opcao_lista.is_visible(timeout=3000):
                    # Fallback: buscar por texto parcial
                    opcao_lista = self._page.locator(
                        '.ant-dropdown-menu-item:has-text("Separação"), '
                        '.ant-dropdown-menu-item:has-text("Separacao")'
                    ).first

                if not await opcao_lista.is_visible(timeout=2000):
                    logger.error("[UpSeller] Opcao 'Imprimir Lista de Separação' nao encontrada no dropdown")
                    await self.screenshot("lista_sep_sem_opcao")
                    return []

                opcao_text = await opcao_lista.text_content()
                logger.info(f"[UpSeller] Clicando opcao: '{(opcao_text or '').strip()}'")

                # Capturar popup (novo tab)
                _captured_popups = []
                _captured_downloads = []
                self._page.on('download', lambda d: _captured_downloads.append(d))
                self._page.context.on('page', lambda p: _captured_popups.append(p))

                await opcao_lista.click()
                logger.info("[UpSeller] Opcao 'Lista de Separação' clicada, aguardando resposta...")
                await self._page.wait_for_timeout(3000)

                # Se modal de confirmação aparecer
                try:
                    modal_btn = await self._page.wait_for_selector(
                        '.ant-modal button.ant-btn-primary',
                        timeout=5000
                    )
                    if modal_btn:
                        btn_text = await modal_btn.evaluate("el => el.textContent.trim()")
                        logger.info(f"[UpSeller] Modal encontrado, botao: '{btn_text}'")
                        await modal_btn.click()
                        await self._page.wait_for_timeout(3000)
                except Exception:
                    pass

                # Aguardar popup ou download (até 60s)
                for i in range(30):
                    if _captured_downloads or _captured_popups:
                        break
                    await self._page.wait_for_timeout(2000)
                    if i % 5 == 4:
                        logger.info(f"[UpSeller] Aguardando popup/download lista separacao... ({(i+1)*2}s)")

                save_path = os.path.join(
                    self.download_dir,
                    f"lista_separacao_{filtro_loja or 'todas'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                )

                logger.info(f"[UpSeller] Lista separacao: downloads={len(_captured_downloads)}, popups={len(_captured_popups)}")

                if _captured_downloads:
                    # Download direto
                    download = _captured_downloads[0]
                    filename = download.suggested_filename or os.path.basename(save_path)
                    actual_path = os.path.join(self.download_dir, filename)
                    await download.save_as(actual_path)
                    pdfs_baixados.append(actual_path)
                    logger.info(f"[UpSeller] Lista separacao baixada via download: {actual_path}")

                elif _captured_popups:
                    # Novo tab aberto (print-pick-list)
                    new_page = _captured_popups[0]
                    logger.info(f"[UpSeller] Novo tab lista separacao: {new_page.url}")
                    try:
                        await new_page.wait_for_load_state('networkidle', timeout=30000)
                    except Exception:
                        await new_page.wait_for_timeout(5000)

                    salvo = False

                    # Prioridade: CDP Page.printToPDF (funciona em headed mode)
                    try:
                        import base64
                        cdp = await new_page.context.new_cdp_session(new_page)
                        result = await cdp.send('Page.printToPDF', {
                            'printBackground': True,
                            'preferCSSPageSize': True,
                        })
                        pdf_bytes = base64.b64decode(result['data'])
                        with open(save_path, 'wb') as f:
                            f.write(pdf_bytes)
                        await cdp.detach()
                        salvo = True
                        logger.info(f"[UpSeller] Lista separacao salva via CDP: {save_path}")
                    except Exception as cdp_err:
                        logger.warning(f"[UpSeller] CDP printToPDF falhou: {cdp_err}")

                    # Fallback: page.pdf() (funciona em headless)
                    if not salvo:
                        try:
                            await new_page.pdf(path=save_path, format='A4', print_background=True)
                            salvo = True
                            logger.info(f"[UpSeller] Lista separacao salva via page.pdf(): {save_path}")
                        except Exception as pdf_err:
                            logger.warning(f"[UpSeller] page.pdf() falhou: {pdf_err}")

                    # Fallback: salvar conteúdo HTML como PDF
                    if not salvo:
                        try:
                            salvo = await self._salvar_pdf_de_popup(new_page, save_path)
                        except Exception:
                            pass

                    if salvo and os.path.exists(save_path):
                        pdfs_baixados.append(save_path)
                    else:
                        logger.error("[UpSeller] Nao conseguiu salvar lista de separacao")

                    try:
                        await new_page.close()
                    except Exception:
                        pass

                else:
                    logger.warning("[UpSeller] Nem download nem popup para lista separacao")
                    # Fallback: verificar filesystem
                    downloads_novos = self._verificar_downloads_novos("*.pdf")
                    if downloads_novos:
                        pdfs_baixados.extend(downloads_novos)

            except Exception as e:
                logger.error(f"[UpSeller] Erro no processo de lista separacao: {e}")
                import traceback
                traceback.print_exc()

        except Exception as e:
            logger.error(f"[UpSeller] Erro ao baixar lista de separacao: {e}")

        logger.info(f"[UpSeller] Lista(s) de separacao baixada(s): {len(pdfs_baixados)}")
        return pdfs_baixados

    async def baixar_lista_resumo(self, filtro_loja: Union[str, List[str], None] = None) -> List[str]:
        """
        Baixa a opcao "Imprimir Lista de Resumo" (normalmente XLSX/CSV) antes
        de gerar as etiquetas.

        Esse arquivo alimenta os dados de produtos usados no rodape/modelo novo.

        Fluxo:
          1. Navega para "Etiqueta para Impressao"
          2. Filtra por loja (se informado)
          3. Seleciona pedidos da tabela
          4. Hover em "Imprimir Etiquetas"
          5. Clica "Imprimir Lista de Resumo"
          6. Captura arquivo baixado (xlsx/xls/csv)
        """
        arquivos_baixados = []
        try:
            if not self._page:
                await self._iniciar_navegador()
            if not await self._esta_logado():
                if not await self.login():
                    logger.warning("[UpSeller] Nao logado para baixar lista de resumo")
                    return []

            filtro_lojas = self._normalizar_lista_lojas_filtro(filtro_loja)
            filtro_multiplo = len(filtro_lojas) > 1
            filtro_desc = (
                filtro_lojas[0] if len(filtro_lojas) == 1
                else f"{len(filtro_lojas)} lojas"
            )
            logger.info(f"[UpSeller] Baixando lista de resumo (loja={filtro_desc or 'todas'})...")

            await self._page.goto(UPSELLER_PARA_IMPRIMIR, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(3000)
            await self._fechar_popups()

            async def _clicar_aba_impressao():
                result = await self._page.evaluate("""
                    (() => {
                        const tabs = document.querySelectorAll('[role="tab"], .ant-tabs-tab, div.ant-tabs-tab');
                        for (const tab of tabs) {
                            const text = (tab.textContent || '').trim();
                            if (text.includes('Etiqueta para Impress') ||
                                (text.includes('Impress') && !text.includes('Falhada') && !text.includes('Gerando'))) {
                                tab.click();
                                return { clicked: true, text: text };
                            }
                        }
                        return { clicked: false };
                    })()
                """)
                if result and result.get("clicked"):
                    await self._page.wait_for_timeout(2000)
                    return True
                return False

            async def _clicar_subaba_nao_impressa():
                """
                Forca a sub-aba "Etiqueta nao impressa" e valida a aba ativa.
                """
                ok = await self._ativar_subaba_etiquetas(
                    alvo="nao_impressa",
                    tentativas=3,
                    estrito=True
                )
                st = await self._status_subaba_etiquetas("nao_impressa")
                if ok:
                    logger.info("[UpSeller] Sub-aba 'Etiqueta nao impressa' confirmada para Lista de Resumo")
                    return True
                if st.get("exists"):
                    logger.error(
                        "[UpSeller] Nao ativou sub-aba 'Etiqueta nao impressa' na Lista de Resumo (ativa: %s)",
                        st.get("active_text", "")
                    )
                return False

            await _clicar_aba_impressao()
            ok_subaba = await _clicar_subaba_nao_impressa()
            st_subaba = await self._status_subaba_etiquetas("nao_impressa")
            if st_subaba.get("exists") and not ok_subaba:
                logger.error("[UpSeller] Abortando lista de resumo para evitar leitura na sub-aba errada.")
                return []

            if filtro_lojas:
                if len(filtro_lojas) == 1:
                    filtrou = await self._aplicar_filtro_loja_seguro(
                        filtro_lojas[0], contexto="lista_resumo"
                    )
                else:
                    filtrou = await self._aplicar_filtro_lojas_seguro(
                        filtro_lojas, contexto="lista_resumo_lote"
                    )
                if not filtrou:
                    logger.error(f"[UpSeller] Nao filtrou loja(s) '{filtro_desc}' para lista de resumo. Abortando.")
                    return []
                # Alguns filtros voltam para "Todos"; reafirma a sub-aba correta.
                ok_subaba = await _clicar_subaba_nao_impressa()
                st_subaba = await self._status_subaba_etiquetas("nao_impressa")
                if st_subaba.get("exists") and not ok_subaba:
                    logger.error("[UpSeller] Filtro aplicado, mas sem ativar 'Etiqueta nao impressa'. Abortando.")
                    return []

            tem_dados = await self._page.evaluate("""
                (() => {
                    const rows = document.querySelectorAll(
                        'tbody tr.ant-table-row, tbody tr.top_row, tbody tr:not(.ant-table-placeholder)'
                    );
                    const dataRows = Array.from(rows).filter(r => {
                        const text = (r.textContent || '').trim();
                        return text.length > 5 && !text.includes('Nenhum Dado');
                    });
                    if (dataRows.length > 0) return { hasData: true, count: dataRows.length };
                    const activeTab = document.querySelector('.ant-tabs-tab-active');
                    if (activeTab) {
                        const m = (activeTab.textContent || '').match(/(\\d+)/);
                        if (m && parseInt(m[1]) > 0) return { hasData: true, count: parseInt(m[1]) };
                    }
                    return { hasData: false, count: 0 };
                })()
            """)

            if not tem_dados or not tem_dados.get("hasData"):
                logger.info("[UpSeller] 0 pedidos para lista de resumo")
                return []

            try:
                select_all = await self._page.wait_for_selector(
                    'thead .ant-checkbox-wrapper, th .ant-checkbox-input, '
                    'thead input[type="checkbox"], '
                    '.ant-table-header .ant-checkbox-wrapper',
                    timeout=5000
                )
                await select_all.click()
                await self._page.wait_for_timeout(1000)

                if (not filtro_lojas) or filtro_multiplo:
                    try:
                        loc_sel_todas = self._page.locator(
                            '.ant-dropdown-menu-item:has-text("Selecionar todas")'
                        ).first
                        if await loc_sel_todas.is_visible(timeout=2000):
                            await loc_sel_todas.click()
                            await self._page.wait_for_timeout(1200)
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"[UpSeller] Falha ao selecionar pedidos para lista de resumo: {e}")
                return []

            trigger = self._page.locator(
                'a.ant-dropdown-trigger:has-text("Imprimir Etiquetas")'
            ).first
            if await trigger.count() == 0:
                trigger = self._page.locator(
                    'a.ant-btn-link:has-text("Imprimir Etiquetas")'
                ).first
            if await trigger.count() == 0:
                trigger = self._page.locator(
                    'a:has-text("Imprimir em Massa")'
                ).first
            if await trigger.count() == 0:
                logger.error("[UpSeller] Trigger 'Imprimir Etiquetas' nao encontrado para lista de resumo")
                return []

            await trigger.hover()
            await self._page.wait_for_timeout(1300)

            opcao_resumo = self._page.locator(
                '.ant-dropdown-menu-item:has-text("Lista de Resumo")'
            ).first
            if not await opcao_resumo.is_visible(timeout=2500):
                opcao_resumo = self._page.locator(
                    '.ant-dropdown-menu-item:has-text("Resumo")'
                ).first

            if not await opcao_resumo.is_visible(timeout=2000):
                logger.error("[UpSeller] Opcao 'Imprimir Lista de Resumo' nao encontrada no dropdown")
                await self.screenshot("lista_resumo_sem_opcao")
                return []

            _captured_downloads = []
            _captured_popups = []
            self._page.on('download', lambda d: _captured_downloads.append(d))
            self._page.context.on('page', lambda p: _captured_popups.append(p))

            await opcao_resumo.click()
            await self._page.wait_for_timeout(2500)

            try:
                modal_btn = await self._page.wait_for_selector(
                    '.ant-modal button.ant-btn-primary',
                    timeout=5000
                )
                if modal_btn:
                    await modal_btn.click()
                    await self._page.wait_for_timeout(2500)
            except Exception:
                pass

            for _ in range(30):
                if _captured_downloads or _captured_popups:
                    break
                await self._page.wait_for_timeout(2000)

            base_nome = f"lista_resumo_{(filtro_desc or 'todas').replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            for idx, download in enumerate(_captured_downloads):
                try:
                    suggested = (download.suggested_filename or "").strip()
                    ext = os.path.splitext(suggested)[1] if suggested else ""
                    if not ext:
                        ext = ".xlsx"
                    fname = suggested or f"{base_nome}_{idx + 1}{ext}"
                    destino = os.path.join(self.download_dir, fname)
                    await download.save_as(destino)
                    if os.path.exists(destino) and self._arquivo_tabulado_valido(destino):
                        arquivos_baixados.append(destino)
                        logger.info(f"[UpSeller] Lista de resumo baixada: {destino}")
                    else:
                        logger.warning(f"[UpSeller] Arquivo de resumo invalido (provavel HTML), ignorando: {destino}")
                        try:
                            if os.path.exists(destino):
                                os.remove(destino)
                        except Exception:
                            pass
                except Exception as e_dw:
                    logger.warning(f"[UpSeller] Falha ao salvar download da lista de resumo: {e_dw}")

            if not arquivos_baixados and _captured_popups:
                for idx, new_page in enumerate(_captured_popups):
                    try:
                        try:
                            await new_page.wait_for_load_state('networkidle', timeout=20000)
                        except Exception:
                            await new_page.wait_for_timeout(3000)

                        popup_url = (new_page.url or "").strip()
                        if popup_url.startswith("http"):
                            resp = await new_page.context.request.get(popup_url, timeout=60000)
                            if resp and resp.ok:
                                body = await resp.body()
                                if body and len(body) > 256:
                                    ct = (resp.headers.get("content-type") or "").lower()
                                    low_url = popup_url.lower()
                                    low_body = body[:2048].lower()
                                    # Evitar salvar shell HTML do SPA como se fosse XLSX.
                                    if "html" in ct or b"<!doctype html" in low_body or b"<html" in low_body:
                                        logger.warning(
                                            f"[UpSeller] Popup retornou HTML (nao planilha): {popup_url}"
                                        )
                                        continue
                                    ext = ".xlsx"
                                    if "csv" in ct or low_url.endswith(".csv"):
                                        ext = ".csv"
                                    elif low_url.endswith(".xls"):
                                        ext = ".xls"
                                    elif low_url.endswith(".xlsx"):
                                        ext = ".xlsx"
                                    destino = os.path.join(self.download_dir, f"{base_nome}_{idx + 1}{ext}")
                                    with open(destino, "wb") as f:
                                        f.write(body)
                                    if self._arquivo_tabulado_valido(destino):
                                        arquivos_baixados.append(destino)
                                        logger.info(f"[UpSeller] Lista de resumo salva via popup URL: {destino}")
                                    else:
                                        logger.warning(f"[UpSeller] Popup salvou arquivo invalido, ignorando: {destino}")
                                        try:
                                            os.remove(destino)
                                        except Exception:
                                            pass
                    except Exception as e_popup:
                        logger.warning(f"[UpSeller] Falha ao obter lista de resumo via popup: {e_popup}")
                    finally:
                        try:
                            await new_page.close()
                        except Exception:
                            pass

            if not arquivos_baixados:
                # Fallback: arquivos recentes que possam ter sido baixados sem evento.
                for padrao in ("*.xlsx", "*.xls", "*.csv"):
                    for path in self._verificar_downloads_novos(padrao, segundos_atras=180):
                        if path and path not in arquivos_baixados and self._arquivo_tabulado_valido(path):
                            arquivos_baixados.append(path)

        except Exception as e:
            logger.error(f"[UpSeller] Erro ao baixar lista de resumo: {e}")

        logger.info(f"[UpSeller] Lista(s) de resumo baixada(s): {len(arquivos_baixados)}")
        return arquivos_baixados

    async def _auto_configurar_impressao(self, btn_ir_configurar) -> bool:
        """
        Quando o modal 'diferentes logisticas precisa ser configurado' aparece,
        clica 'Ir para Configurar' e tenta auto-configurar cada logistica
        selecionando 'Etiqueta de Envio Padrao' ou a primeira opcao disponivel.

        Retorna True se a configuracao foi salva com sucesso.
        """
        try:
            # 1. Clicar "Ir para Configurar"
            await btn_ir_configurar.click()
            await self._page.wait_for_timeout(3000)
            await self.screenshot("auto_config_01_pagina")

            # 2. Verificar se estamos na pagina de configuracao de impressao
            page_text = await self._page.evaluate("document.body.innerText.substring(0, 500)")
            logger.info(f"[UpSeller] Pagina de config: {page_text[:200]}")

            # 3. Procurar por selects/dropdowns de formato de etiqueta
            # No UpSeller, cada logistica tem um select para escolher o formato
            selects = await self._page.query_selector_all(
                'select, .ant-select, .ant-select-selector'
            )
            logger.info(f"[UpSeller] Encontrados {len(selects)} selects na pagina de config")

            # 4. Para cada select que nao tem valor, selecionar a primeira opcao
            configurou_algo = False
            for sel in selects:
                try:
                    # Verificar se eh um select de formato de etiqueta
                    parent_text = await sel.evaluate(
                        "el => (el.closest('tr, .ant-row, .form-group') || el.parentElement).innerText.substring(0, 100)"
                    )
                    if 'etiqueta' in parent_text.lower() or 'formato' in parent_text.lower() or 'envio' in parent_text.lower():
                        await sel.click()
                        await self._page.wait_for_timeout(500)
                        # Selecionar primeira opcao visivel
                        opcao = self._page.locator('.ant-select-dropdown .ant-select-item').first
                        if await opcao.is_visible(timeout=2000):
                            opcao_text = await opcao.text_content()
                            logger.info(f"[UpSeller] Auto-selecionando formato: '{opcao_text}'")
                            await opcao.click()
                            await self._page.wait_for_timeout(500)
                            configurou_algo = True
                except Exception as e_sel:
                    logger.debug(f"[UpSeller] Erro ao configurar select: {e_sel}")

            await self.screenshot("auto_config_02_apos_selects")

            # 5. Clicar botao "Salvar" ou "Confirmar"
            salvar_btn = None
            for seletor in [
                'button:has-text("Salvar")',
                'button:has-text("Confirmar")',
                'button:has-text("OK")',
                'button.ant-btn-primary',
            ]:
                try:
                    btn = self._page.locator(seletor).first
                    if await btn.is_visible(timeout=1000):
                        salvar_btn = btn
                        break
                except Exception:
                    continue

            if salvar_btn:
                btn_text = await salvar_btn.text_content()
                logger.info(f"[UpSeller] Clicando botao salvar: '{(btn_text or '').strip()}'")
                await salvar_btn.click()
                await self._page.wait_for_timeout(2000)
                await self.screenshot("auto_config_03_salvo")
                logger.info("[UpSeller] Auto-configuracao de impressao salva!")
                return True
            else:
                logger.warning("[UpSeller] Nao encontrou botao de salvar na pagina de config")
                await self.screenshot("auto_config_03_sem_salvar")
                # Mesmo sem clicar salvar, a pagina pode ter salvo automaticamente
                return configurou_algo

        except Exception as e:
            logger.error(f"[UpSeller] Erro na auto-configuracao de impressao: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def baixar_etiquetas(
        self,
        filtro_loja: Union[str, List[str], None] = None,
        aba_alvo: str = "impressao",
        _retry_config: bool = False,
    ) -> List[str]:
        """
        Navega para pagina "Etiqueta para Impressao" e baixa PDFs de etiquetas.

        Fluxo (mapeado da UI do UpSeller 2026-02-26):
          1. Navega para /pt/order/in-process (Etiqueta para Impressao)
          2. FILTRA POR LOJA se especificado
          3. Clica checkbox "selecionar todos" na tabela
          4. Clica "Selecionar todas as paginas" no popup que aparece
          5. Clica "Imprimir Etiquetas" (dropdown trigger na barra de acoes)
          6. Clica "Imprimir Etiquetas" (primeira opcao do dropdown)
          7. Aguarda download do PDF

        Args:
            filtro_loja: Filtrar por nome de loja (str) ou lista de lojas (opcional)
            aba_alvo:
                - "impressao": fluxo normal (Etiqueta para Impressao)
                - "falha": tenta processar aba de etiquetas com falha

        Retorna: Lista de caminhos dos PDFs baixados
        """
        if not self._page:
            await self._iniciar_navegador()

        if not await self._esta_logado():
            if not await self.login():
                return []

        print("[baixar_etiquetas] INICIO - navegando para Etiqueta para Impressao")
        logger.info("[UpSeller] Navegando para Etiqueta para Impressao...")
        pdfs_baixados = []
        filtro_lojas = self._normalizar_lista_lojas_filtro(filtro_loja)
        filtro_multiplo = len(filtro_lojas) > 1
        filtro_desc = (
            filtro_lojas[0] if len(filtro_lojas) == 1
            else f"{len(filtro_lojas)} lojas"
        )
        aba_norm = (aba_alvo or "impressao").strip().lower()

        try:
            # 0. Configurar formato base de impressao (PDF 10x15), preservando
            # rodape/lista nativos do UpSeller (modo antigo).
            try:
                ok_cfg = await self._configurar_formato_etiqueta(sem_rodape_upseller=False)
                if not ok_cfg:
                    logger.warning("[UpSeller] Configuracao de print-setting nao confirmada; seguindo com fluxo")
            except Exception as e_cfg:
                logger.warning(f"[UpSeller] Erro ao configurar print-setting: {e_cfg}")

            # 1. Navegar para pagina e clicar na aba "Etiqueta para Impressao"
            print(f"[baixar_etiquetas] goto {UPSELLER_PARA_IMPRIMIR}")
            await self._page.goto(UPSELLER_PARA_IMPRIMIR, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(3000)
            await self._fechar_popups()
            # Segundo check apos delay — popup "Avisos" pode carregar assincronamente
            await self._page.wait_for_timeout(1500)
            await self._fechar_popups()

            # Funcao helper para clicar na aba alvo (impressao/falha)
            async def _clicar_aba_impressao():
                result = await self._page.evaluate("""
                    (modoAba) => {
                        const normalize = (s) => (s || '')
                            .toLowerCase()
                            .normalize('NFD')
                            .replace(/[\\u0300-\\u036f]/g, '')
                            .replace(/\\s+/g, ' ')
                            .trim();
                        const modo = normalize(modoAba || 'impressao');
                        // Buscar aba "Etiqueta para Impressão" nos tabs do Ant Design
                        const tabs = document.querySelectorAll(
                            '.ant-tabs-tab, .ant-tabs-tab-btn, [role="tab"], ' +
                            '.ant-tabs-nav .ant-tabs-tab > div'
                        );
                        for (const tab of tabs) {
                            const raw = (tab.textContent || '').trim();
                            const text = normalize(raw);
                            let match = false;
                            if (modo.includes('falha')) {
                                match = text.includes('falha') || text.includes('falhada') || text.includes('erro');
                            } else {
                                match = text.includes('etiqueta para impress') ||
                                    (text.includes('impress') && !text.includes('falha') && !text.includes('gerando'));
                            }
                            if (match) {
                                tab.click();
                                return { clicked: true, text: raw, modo: modo };
                            }
                        }
                        // Fallback para aba de impressao (somente no modo normal)
                        if (modo.includes('falha')) {
                            return { clicked: false, modo: modo };
                        }
                        // Fallback: buscar qualquer elemento clicavel com texto de impressao
                        const all = document.querySelectorAll('div, span, a');
                        for (const el of all) {
                            // Apenas elementos com texto direto (sem filhos com texto)
                            const directText = Array.from(el.childNodes)
                                .filter(n => n.nodeType === 3)
                                .map(n => n.textContent.trim())
                                .join('');
                            if (normalize(directText).includes('etiqueta para impress')) {
                                const rect = el.getBoundingClientRect();
                                if (rect.width > 20 && rect.height > 10) {
                                    el.click();
                                    return { clicked: true, text: directText, modo: modo };
                                }
                            }
                        }
                        return { clicked: false, modo: modo };
                    }
                """, aba_norm)
                if result and result.get("clicked"):
                    logger.info(f"[UpSeller] Aba clicada: {result.get('text')}")
                    await self._page.wait_for_timeout(2000)
                    return True
                return False

            async def _clicar_subaba_nao_impressa() -> bool:
                """
                Garante que estamos na sub-aba "Etiqueta nao impressa".
                Isso evita gerar da aba "Todos" e melhora a baixa para "impresso".
                """
                try:
                    ok = await self._ativar_subaba_etiquetas(
                        alvo="nao_impressa",
                        tentativas=3,
                        estrito=True
                    )
                    if ok:
                        logger.info("[UpSeller] Sub-aba 'Etiqueta nao impressa' selecionada")
                        await self._page.wait_for_timeout(1300)
                    else:
                        logger.warning("[UpSeller] Sub-aba 'Etiqueta nao impressa' nao encontrada; seguindo com aba atual")
                    return bool(ok)
                except Exception as e_sub:
                    logger.warning(f"[UpSeller] Erro ao selecionar sub-aba 'Etiqueta nao impressa': {e_sub}")
                    return False

            async def _marcar_como_impresso_pos_download() -> bool:
                """
                Fallback de consistencia: apos baixar etiquetas, tenta marcar o lote como impresso.
                """
                if aba_norm.startswith("falha"):
                    return True
                try:
                    await self._page.goto(UPSELLER_PARA_IMPRIMIR, wait_until="domcontentloaded", timeout=30000)
                    await self._page.wait_for_timeout(2200)
                    await self._fechar_popups()
                    await _clicar_aba_impressao()
                    ok_sub = await _clicar_subaba_nao_impressa()
                    st_sub = await self._status_subaba_etiquetas("nao_impressa")
                    if st_sub.get("exists") and not ok_sub:
                        logger.warning("[UpSeller] Nao confirmou sub-aba 'nao impressa' para marcacao pos-download")
                        return False

                    if filtro_lojas:
                        if len(filtro_lojas) == 1:
                            ok_filtro = await self._aplicar_filtro_loja_seguro(
                                filtro_lojas[0], contexto="marcar_impresso_pos_download"
                            )
                        else:
                            ok_filtro = await self._aplicar_filtro_lojas_seguro(
                                filtro_lojas, contexto="marcar_impresso_pos_download_lote"
                            )
                        if not ok_filtro:
                            logger.warning("[UpSeller] Nao conseguiu aplicar filtro para marcar como impresso")
                            return False
                        await _clicar_subaba_nao_impressa()

                    tem_nao_impressa = await self._page.evaluate("""
                        (() => {
                            const rows = document.querySelectorAll('tbody tr.ant-table-row, tbody tr.top_row, tbody tr:not(.ant-table-placeholder)');
                            const vis = Array.from(rows).filter(r => {
                                const t = (r.textContent || '').trim();
                                return t.length > 5 && !t.includes('Nenhum Dado');
                            });
                            if (vis.length > 0) return true;
                            const txt = (document.body.innerText || '').toLowerCase();
                            return !(txt.includes('nenhum dado') || txt.includes('total 0'));
                        })()
                    """)
                    if not tem_nao_impressa:
                        return True

                    # Selecionar TODOS os pedidos (incluindo outras paginas)
                    try:
                        select_all = await self._page.wait_for_selector(
                            'thead .ant-checkbox-wrapper, th .ant-checkbox-input, '
                            'thead input[type="checkbox"], .ant-table-header .ant-checkbox-wrapper',
                            timeout=4000
                        )
                        await select_all.click()
                        await self._page.wait_for_timeout(1500)
                        logger.info("[UpSeller][mark] Checkbox 'selecionar todos' clicado")
                    except Exception as e_sel:
                        logger.warning(f"[UpSeller][mark] Checkbox selecionar todos nao encontrado: {e_sel}")

                    # Clicar "Selecionar todas as paginas" se aparecer
                    # (Ant Design mostra popup apos clicar select-all em tabela paginada)
                    if (not filtro_lojas) or filtro_multiplo:
                        try:
                            loc_sel_todas = self._page.locator(
                                '.ant-dropdown-menu-item:has-text("Selecionar todas"), '
                                'a:has-text("Selecionar todas"), '
                                'span:has-text("Selecionar todas as")'
                            ).first
                            if await loc_sel_todas.is_visible(timeout=3000):
                                await loc_sel_todas.click()
                                await self._page.wait_for_timeout(1500)
                                logger.info("[UpSeller][mark] 'Selecionar todas as paginas' clicado")
                            else:
                                logger.info("[UpSeller][mark] Sem popup 'Selecionar todas' (pagina unica ou ja selecionado)")
                        except Exception:
                            logger.info("[UpSeller][mark] Sem popup de selecao de paginas")

                    acao = await self._page.evaluate("""
                        (() => {
                            const normalize = (s) => (s || '')
                                .toLowerCase()
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .replace(/\\s+/g, ' ')
                                .trim();
                            const clickNode = (el) => {
                                if (!el) return false;
                                try { el.click(); return true; } catch(_) {}
                                try {
                                    ['mouseover','mouseenter','mousedown','mouseup','click'].forEach(ev =>
                                        el.dispatchEvent(new MouseEvent(ev, { bubbles: true, cancelable: true, view: window }))
                                    );
                                    return true;
                                } catch(_) {}
                                return false;
                            };
                            const direta = [
                                'marcar como impressa',
                                'marcar impressa',
                                'definir como impressa',
                                'mover para impressa',
                                'mark as printed',
                                'set as printed'
                            ];
                            const botoes = document.querySelectorAll('button, a, [role="button"], .ant-btn, .ant-btn-link');
                            for (const b of botoes) {
                                const txt = normalize(b.textContent || b.innerText || '');
                                if (!txt) continue;
                                if (!direta.some(t => txt.includes(t))) continue;
                                const r = b.getBoundingClientRect();
                                if (r.width < 25 || r.height < 10 || r.y > 440) continue;
                                if (clickNode(b)) return { ok: true, modo: 'direto', texto: txt };
                            }
                            for (const b of botoes) {
                                const txt = normalize(b.textContent || b.innerText || '');
                                if (!txt) continue;
                                if (!(txt.includes('mais acoes') || txt.includes('mais ações') || txt.includes('more actions'))) continue;
                                const r = b.getBoundingClientRect();
                                if (r.width < 25 || r.height < 10 || r.y > 460) continue;
                                if (clickNode(b)) return { ok: true, modo: 'abrir_menu', texto: txt };
                            }
                            return { ok: false, modo: '', texto: '' };
                        })()
                    """)

                    if isinstance(acao, dict) and acao.get("ok"):
                        modo = acao.get("modo", "")
                        logger.info(f"[UpSeller][mark] Acao executada: modo={modo}, texto={acao.get('texto','')}")

                        if modo == "abrir_menu":
                            # "Mais Acoes" foi aberto.  O item "Marcar como Impresso" tem
                            # um SUBMENU com "Etiqueta" / "Lista de Separacao".
                            await self._page.wait_for_timeout(600)

                            # 1) Hover no item "Marcar como Impresso" para abrir submenu
                            sub_parent = self._page.locator(
                                '.ant-dropdown-menu-submenu-title:has-text("Marcar como Impresso"), '
                                '.ant-dropdown-menu-submenu-title:has-text("impressa"), '
                                '.ant-dropdown-menu-item:has-text("Marcar como Impresso")'
                            ).first
                            if await sub_parent.count() > 0 and await sub_parent.is_visible(timeout=3000):
                                await sub_parent.hover()
                                await self._page.wait_for_timeout(800)

                                # 2) Clicar em "Etiqueta" no submenu
                                sub_item = self._page.locator(
                                    '.ant-dropdown-menu-item:has-text("Etiqueta")'
                                ).first
                                if await sub_item.count() > 0 and await sub_item.is_visible(timeout=3000):
                                    await sub_item.click()
                                    await self._page.wait_for_timeout(1200)
                                    logger.info("[UpSeller][mark] Clicou 'Marcar como Impresso > Etiqueta'")
                                else:
                                    # Fallback: clicar direto no pai (caso nao tenha submenu)
                                    await sub_parent.click()
                                    await self._page.wait_for_timeout(1200)
                                    logger.info("[UpSeller][mark] Clicou 'Marcar como Impresso' (sem submenu)")
                            else:
                                logger.warning("[UpSeller][mark] Item 'Marcar como Impresso' nao encontrado no dropdown")
                        else:
                            # Modo 'direto' - botao clicado diretamente
                            await self._page.wait_for_timeout(1200)
                    else:
                        logger.warning(f"[UpSeller][mark] Nenhum botao de acao encontrado: {acao}")

                    # Confirmar modal se aparecer
                    try:
                        modal_btn = await self._page.wait_for_selector(
                            '.ant-modal button.ant-btn-primary',
                            timeout=5000
                        )
                        if modal_btn:
                            btn_txt = await modal_btn.evaluate("el => (el.textContent || '').trim()")
                            await modal_btn.click()
                            await self._page.wait_for_timeout(2000)
                            logger.info(f"[UpSeller][mark] Modal de confirmacao clicado: '{btn_txt}'")
                    except Exception:
                        logger.info("[UpSeller][mark] Sem modal de confirmacao pos-acao")

                    logger.info("[UpSeller][mark] Pos-download: marcar como impresso executado")
                    return True
                except Exception as e_mark:
                    logger.warning(f"[UpSeller] Falha ao marcar como impresso no pos-download: {e_mark}")
                    return False

            clicou_aba_alvo = await _clicar_aba_impressao()
            if not clicou_aba_alvo and aba_norm.startswith("falha"):
                logger.info("[UpSeller] Aba de etiquetas em falha nao encontrada nesta tela")
                return []

            if not aba_norm.startswith("falha"):
                ok_subaba = await _clicar_subaba_nao_impressa()
                st_subaba = await self._status_subaba_etiquetas("nao_impressa")
                if st_subaba.get("exists") and not ok_subaba:
                    logger.error(
                        "[UpSeller] Abortando download para evitar imprimir aba incorreta (ativa: %s)",
                        st_subaba.get("active_text", "")
                    )
                    return []

            # Se o fluxo e "todas as lojas", limpar qualquer filtro residual
            # deixado por etapas anteriores para evitar falso-zero.
            if not filtro_lojas:
                try:
                    await self._limpar_filtro_loja()
                    if not aba_norm.startswith("falha"):
                        await _clicar_subaba_nao_impressa()
                except Exception as e_clear:
                    logger.warning(f"[UpSeller] Aviso ao limpar filtro residual em etiquetas: {e_clear}")

            # 2. FILTRAR POR LOJA(S) se especificado
            if filtro_lojas:
                if len(filtro_lojas) == 1:
                    filtrou = await self._aplicar_filtro_loja_seguro(
                        filtro_lojas[0], contexto="baixar_etiquetas"
                    )
                else:
                    filtrou = await self._aplicar_filtro_lojas_seguro(
                        filtro_lojas, contexto="baixar_etiquetas_lote"
                    )
                if not filtrou:
                    logger.error(f"[UpSeller] Nao filtrou por loja(s) '{filtro_desc}'. Abortando para nao baixar de todas.")
                    return []
                logger.info(f"[UpSeller] Etiquetas filtradas por loja(s): {filtro_desc}")
                # Alguns filtros resetam para "Todos"; reafirma a sub-aba correta.
                if not aba_norm.startswith("falha"):
                    ok_subaba = await _clicar_subaba_nao_impressa()
                    st_subaba = await self._status_subaba_etiquetas("nao_impressa")
                    if st_subaba.get("exists") and not ok_subaba:
                        logger.error(
                            "[UpSeller] Filtro aplicado, mas sem ativar 'Etiqueta nao impressa' (ativa: %s).",
                            st_subaba.get("active_text", "")
                        )
                        return []

            # Verificar se ha etiquetas na TABELA (nao no texto geral da pagina)
            # Diferencia "pagina carregada com 0 itens" vs "pagina ainda carregando"
            max_tentativas = 6
            pagina_carregada_vazia = False
            tentou_recuperar_zero = False
            for tentativa in range(max_tentativas):
                tem_dados = await self._page.evaluate("""
                    (() => {
                        // 1. Verificar se a tabela tem linhas de dados (nao placeholder)
                        const rows = document.querySelectorAll(
                            'tbody tr.ant-table-row, tbody tr.top_row, ' +
                            'tbody tr:not(.ant-table-placeholder)'
                        );
                        const dataRows = Array.from(rows).filter(r => {
                            const text = (r.textContent || '').trim();
                            return text.length > 5 && !text.includes('Nenhum Dado');
                        });
                        if (dataRows.length > 0) return { hasData: true, count: dataRows.length, loaded: true };

                        // 2. Verificar se "Selecionado" ou "Total XX" na barra de acoes
                        const actionBar = document.body.innerText;
                        const selMatch = actionBar.match(/Selecionado\\s+(\\d+)/);
                        const totalMatch = actionBar.match(/Total\\s+(\\d+)/);
                        if (totalMatch && parseInt(totalMatch[1]) > 0)
                            return { hasData: true, count: parseInt(totalMatch[1]), loaded: true };
                        if (selMatch && parseInt(selMatch[1]) > 0)
                            return { hasData: true, count: parseInt(selMatch[1]), loaded: true };

                        // 3. Verificar contagem na aba ativa (ex: "Etiqueta para Impressão 50")
                        const activeTab = document.querySelector('.ant-tabs-tab-active, .ant-tabs-tab.ant-tabs-tab-active');
                        if (activeTab) {
                            const tabText = activeTab.textContent || '';
                            const m = tabText.match(/(\\d+)/);
                            if (m) {
                                const n = parseInt(m[1]);
                                if (n > 0) return { hasData: true, count: n, loaded: true };
                                // Aba ativa mostra 0 — pagina carregou mas esta vazia
                                return { hasData: false, count: 0, loaded: true, tabText: tabText.trim() };
                            }
                        }

                        // 4. Verificar se a pagina carregou (tem tabs visiveis, "Nenhum Dado Disponivel", etc.)
                        const tabs = document.querySelectorAll('.ant-tabs-tab');
                        const temPlaceholder = document.body.innerText.includes('Nenhum Dado Dispon');
                        if (tabs.length > 0 || temPlaceholder) {
                            return { hasData: false, count: 0, loaded: true, tabCount: tabs.length };
                        }

                        // Pagina ainda nao carregou
                        return { hasData: false, count: 0, loaded: false };
                    })()
                """)

                if tem_dados and tem_dados.get("hasData"):
                    print(f"[baixar_etiquetas] Encontradas {tem_dados.get('count')} etiqueta(s)")
                    logger.info(f"[UpSeller] Encontradas {tem_dados.get('count')} etiqueta(s) na tabela")
                    break

                # Se a pagina carregou mas nao tem dados, nao precisa ficar retentando
                if tem_dados and tem_dados.get("loaded"):
                    tab_info = tem_dados.get("tabText", "")

                    # Recuperacao automatica: em alguns casos a tela abre com estado/filtro
                    # residual e retorna 0 indevidamente apesar de haver pendencias.
                    if (
                        not tentou_recuperar_zero
                        and not filtro_lojas
                        and not aba_norm.startswith("falha")
                    ):
                        tentou_recuperar_zero = True
                        print("[baixar_etiquetas] 0 detectado, tentando recuperar estado/filtro...")
                        logger.info("[UpSeller] 0 detectado em impressao; tentando recuperar estado/filtro antes de abortar")
                        try:
                            await self._page.reload(wait_until="domcontentloaded")
                            await self._page.wait_for_timeout(2500)
                            await self._fechar_popups()
                            await _clicar_aba_impressao()
                            await _clicar_subaba_nao_impressa()
                            await self._limpar_filtro_loja()
                            await self._page.wait_for_timeout(1200)
                            continue
                        except Exception as e_retry_zero:
                            logger.warning(f"[UpSeller] Falha na recuperacao de falso-zero em etiquetas: {e_retry_zero}")

                    print(f"[baixar_etiquetas] Pagina carregada mas vazia (aba: '{tab_info}', count=0)")
                    logger.info(f"[UpSeller] Pagina carregada, 0 etiquetas pendentes (aba: {tab_info})")
                    pagina_carregada_vazia = True
                    break

                if tentativa < max_tentativas - 1:
                    print(f"[baixar_etiquetas] Pagina ainda carregando... ({tentativa+1}/{max_tentativas})")
                    logger.info(f"[UpSeller] Pagina carregando, aguardando... ({tentativa+1}/{max_tentativas})")
                    await self._page.wait_for_timeout(10000)
                    await self._page.reload(wait_until="domcontentloaded")
                    await self._page.wait_for_timeout(3000)
                    await self._fechar_popups()
                    await _clicar_aba_impressao()  # Re-clicar aba apos reload
                    if not aba_norm.startswith("falha"):
                        await _clicar_subaba_nao_impressa()
                    if filtro_lojas:
                        if len(filtro_lojas) == 1:
                            ok_reload = await self._aplicar_filtro_loja_seguro(
                                filtro_lojas[0], contexto="baixar_etiquetas_reload"
                            )
                        else:
                            ok_reload = await self._aplicar_filtro_lojas_seguro(
                                filtro_lojas, contexto="baixar_etiquetas_reload_lote"
                            )
                        if not ok_reload:
                            logger.error(
                                f"[UpSeller] Filtro '{filtro_desc}' perdeu consistencia apos reload em etiquetas."
                            )
                            return []
            else:
                print("[baixar_etiquetas] ERRO: Pagina nao carregou apos 6 tentativas")
                logger.info("[UpSeller] Pagina nao carregou apos 6 tentativas.")
                await self.screenshot("etiquetas_00_nao_carregou")
                return []

            if pagina_carregada_vazia:
                print("[baixar_etiquetas] 0 etiquetas pendentes - nada para imprimir")
                await self.screenshot("etiquetas_00_sem_dados")
                return []

            await self.screenshot("etiquetas_01_para_imprimir")

            # 3. Clicar checkbox "selecionar todos" no header da tabela
            try:
                select_all = await self._page.wait_for_selector(
                    'thead .ant-checkbox-wrapper, th .ant-checkbox-input, '
                    'thead input[type="checkbox"], '
                    '.ant-table-header .ant-checkbox-wrapper',
                    timeout=5000
                )
                await select_all.click()
                await self._page.wait_for_timeout(1500)
                logger.info("[UpSeller] Checkbox 'selecionar todos' clicado")
            except Exception:
                logger.warning("[UpSeller] Checkbox 'selecionar todos' nao encontrado, tentando individuais")
                has_data = await self._page.query_selector('tr.row, tr.top_row, tbody tr')
                if not has_data:
                    logger.info("[UpSeller] Nenhuma linha encontrada.")
                    return []
                checkboxes = await self._page.query_selector_all(
                    'tbody .ant-checkbox-input, tr.top_row .ant-checkbox-input'
                )
                for cb in checkboxes[:50]:
                    try:
                        await cb.click()
                    except:
                        pass

            # 4. Clicar "Selecionar todas as paginas" se o popup aparecer
            # IMPORTANTE: NÃO clicar quando filtro de loja esta ativo!
            # O "Selecionar todas as páginas" do UpSeller ignora o filtro
            # e seleciona pedidos de TODAS as lojas no backend.
            if (not filtro_lojas) or filtro_multiplo:
                try:
                    loc_sel_todas = self._page.locator(
                        '.ant-dropdown-menu-item:has-text("Selecionar todas")'
                    ).first
                    if await loc_sel_todas.is_visible(timeout=2000):
                        await loc_sel_todas.click()
                        logger.info("[UpSeller] 'Selecionar todas as paginas' clicado")
                        await self._page.wait_for_timeout(1500)
                    else:
                        logger.info("[UpSeller] Sem popup 'Selecionar todas' (pagina unica)")
                except Exception:
                    logger.info("[UpSeller] Sem popup de selecao de paginas (pagina unica)")
            else:
                logger.info(
                    f"[UpSeller] Filtro por loja unica '{filtro_desc}' ativo - "
                    "nao clicando 'Selecionar todas as paginas'"
                )

            await self.screenshot("etiquetas_02_selecionados")

            # 5-6. HOVER no "Imprimir Etiquetas" para abrir dropdown, depois clicar opcao
            # IMPORTANTE: O dropdown do UpSeller abre com HOVER, nao com click!
            # Usar Playwright .hover() que dispara mouseenter real
            try:
                # Encontrar o botao "Imprimir Etiquetas" na barra de acoes (dropdown trigger)
                trigger = self._page.locator(
                    'a.ant-dropdown-trigger:has-text("Imprimir Etiquetas")'
                ).first

                if await trigger.count() == 0:
                    trigger = self._page.locator(
                        'a.ant-btn-link:has-text("Imprimir Etiquetas")'
                    ).first

                if await trigger.count() == 0:
                    trigger = self._page.locator(
                        'a:has-text("Imprimir em Massa")'
                    ).first

                if await trigger.count() == 0:
                    logger.error("[UpSeller] Botao 'Imprimir Etiquetas' nao encontrado na barra")
                    await self.screenshot("etiquetas_sem_botao_imprimir")
                    return []

                # HOVER para abrir o dropdown (Ant Design usa hover, nao click)
                print("[baixar_etiquetas] Fazendo HOVER no trigger do dropdown...")
                logger.info("[UpSeller] Fazendo hover em 'Imprimir Etiquetas' para abrir dropdown...")
                await trigger.hover()
                await self._page.wait_for_timeout(1500)

                await self.screenshot("etiquetas_03_dropdown_aberto")

                # Clicar na opcao "Imprimir Etiquetas" dentro do dropdown visivel
                opcao = self._page.locator(
                    '.ant-dropdown-menu-item:has-text("Imprimir Etiquetas")'
                ).first

                if await opcao.is_visible(timeout=3000):
                    opcao_text = await opcao.text_content()
                    logger.info(f"[UpSeller] Clicando opcao do dropdown: '{(opcao_text or '').strip()}'")
                else:
                    # Fallback: primeira opcao visivel no dropdown
                    opcao = self._page.locator(
                        '.ant-dropdown:not([style*="display: none"]) .ant-dropdown-menu-item'
                    ).first
                    logger.warning("[UpSeller] Usando primeira opcao do dropdown como fallback")

                # ---- Capturar DOWNLOAD e POPUP (novo tab) ----
                # UpSeller pode: (a) baixar PDF, (b) abrir novo tab com PDF,
                # (c) abrir novo tab com preview HTML
                _captured_downloads = []
                _captured_popups = []

                self._page.on('download', lambda d: _captured_downloads.append(d))
                self._page.context.on('page', lambda p: _captured_popups.append(p))

                # Clicar a opcao do dropdown
                await opcao.click()
                print("[baixar_etiquetas] Opcao do dropdown CLICADA, aguardando resposta...")
                logger.info("[UpSeller] Opcao clicada, aguardando resposta...")
                await self._page.wait_for_timeout(3000)

                # Se aparece modal de confirmacao, clicar no botao adequado
                try:
                    modal_btn = await self._page.wait_for_selector(
                        '.ant-modal button.ant-btn-primary',
                        timeout=5000
                    )
                    if modal_btn:
                        btn_text = await modal_btn.evaluate("el => el.textContent.trim()")
                        logger.info(f"[UpSeller] Modal encontrado, botao primario: '{btn_text}'")

                        # Se o botao primario eh "Ir para Configurar" (modal multi-logistica),
                        # clicar para configurar, tentar auto-configurar, e voltar a tentar
                        if "configurar" in btn_text.lower():
                            if _retry_config:
                                # Ja tentou auto-configurar e falhou. Abortar.
                                logger.error("[UpSeller] Modal multi-logistica persiste apos auto-config!")
                                print("[baixar_etiquetas] Modal persiste apos auto-config - retornando []")
                                # Fechar modal
                                try:
                                    await self._page.locator('.ant-modal button:not(.ant-btn-primary)').first.click()
                                except Exception:
                                    pass
                                return []

                            logger.warning(
                                "[UpSeller] Modal 'precisa ser configurado' detectado. "
                                "Tentando auto-configurar..."
                            )
                            print("[baixar_etiquetas] Modal multi-logistica - tentando auto-configurar")
                            configurou = await self._auto_configurar_impressao(modal_btn)
                            if not configurou:
                                print("[baixar_etiquetas] Auto-config falhou - retornando []")
                                return []
                            # Auto-config OK: voltar a pagina de etiquetas e re-selecionar
                            print("[baixar_etiquetas] Auto-config OK! Voltando para re-selecionar...")
                            await self._page.goto(
                                "https://app.upseller.com/pt/order/to-ship",
                                wait_until="domcontentloaded", timeout=30000
                            )
                            await self._page.wait_for_timeout(3000)
                            await self._fechar_popups()
                            # Re-executar baixar_etiquetas (1 retry)
                            return await self.baixar_etiquetas(
                                filtro_loja=filtro_loja, _retry_config=True
                            )
                        else:
                            await modal_btn.click()
                            await self._page.wait_for_timeout(3000)
                except Exception:
                    logger.info("[UpSeller] Sem modal de confirmacao")

                await self.screenshot("etiquetas_04_apos_click")

                # Aguardar ate 90s por download ou popup
                for i in range(45):
                    if _captured_downloads or _captured_popups:
                        break
                    await self._page.wait_for_timeout(2000)
                    if i % 5 == 4:
                        logger.info(f"[UpSeller] Aguardando download/popup... ({(i+1)*2}s)")

                save_path = os.path.join(
                    self.download_dir,
                    f"etiquetas_{(filtro_desc or 'todas').replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                )

                print(f"[baixar_etiquetas] Resultado: downloads={len(_captured_downloads)}, popups={len(_captured_popups)}")

                if _captured_downloads:
                    # === Download direto ===
                    download = _captured_downloads[0]
                    filename = download.suggested_filename or os.path.basename(save_path)
                    actual_path = os.path.join(self.download_dir, filename)
                    await download.save_as(actual_path)
                    pdfs_baixados.append(actual_path)
                    logger.info(f"[UpSeller] PDF baixado via download: {actual_path}")

                elif _captured_popups:
                    # === Novo tab aberto (PDF ou preview) ===
                    new_page = _captured_popups[0]
                    popup_url = "(desconhecido)"
                    try:
                        popup_url = new_page.url or "(vazio)"
                    except Exception:
                        pass
                    logger.info(f"[UpSeller] Novo tab aberto: {popup_url}")

                    # Aguardar carregamento do popup com tratamento robusto
                    # (o popup pode fechar sozinho apos disparar download)
                    popup_vivo = True
                    try:
                        await new_page.wait_for_load_state('networkidle', timeout=60000)
                    except Exception as e_load:
                        err_str = str(e_load).lower()
                        if 'closed' in err_str or 'target' in err_str:
                            logger.warning(f"[UpSeller] Popup fechou durante carregamento: {e_load}")
                            popup_vivo = False
                        else:
                            try:
                                await new_page.wait_for_timeout(5000)
                            except Exception:
                                logger.warning("[UpSeller] Popup morreu no fallback wait_for_timeout")
                                popup_vivo = False

                    salvo_popup = False

                    if popup_vivo:
                        try:
                            page_url = new_page.url
                            logger.info(f"[UpSeller] URL do novo tab: {page_url}")
                        except Exception:
                            popup_vivo = False

                    if popup_vivo:
                        # Prioridade: salvar PDF ORIGINAL (evita capturar sidebar/miniaturas do preview).
                        try:
                            salvo_popup = await self._salvar_pdf_de_popup(new_page, save_path)
                        except Exception as popup_err:
                            logger.warning(f"[UpSeller] Falha ao extrair PDF real do popup: {popup_err}")

                        # Fallback final: print da pagina (pode vir com UI do preview).
                        if not salvo_popup:
                            try:
                                await new_page.pdf(path=save_path, format='A4', print_background=True)
                                logger.warning(f"[UpSeller] Fallback page.pdf() usado (preview HTML): {save_path}")
                                salvo_popup = True
                            except Exception as pdf_err:
                                logger.error(f"[UpSeller] Erro ao renderizar PDF do popup: {pdf_err}")

                    if salvo_popup and os.path.exists(save_path):
                        pdfs_baixados.append(save_path)

                    # Se popup morreu, verificar se um download aconteceu em paralelo
                    if not salvo_popup:
                        # Popup pode ter disparado download antes de fechar
                        await self._page.wait_for_timeout(3000)
                        if _captured_downloads:
                            download = _captured_downloads[0]
                            filename = download.suggested_filename or os.path.basename(save_path)
                            actual_path = os.path.join(self.download_dir, filename)
                            try:
                                await download.save_as(actual_path)
                                pdfs_baixados.append(actual_path)
                                logger.info(f"[UpSeller] PDF recuperado de download tardio: {actual_path}")
                            except Exception as e_dl:
                                logger.warning(f"[UpSeller] Falha ao salvar download tardio: {e_dl}")

                        # Fallback: verificar filesystem por PDFs novos
                        if not pdfs_baixados:
                            downloads_novos = self._verificar_downloads_novos("*.pdf")
                            if downloads_novos:
                                pdfs_baixados.extend(downloads_novos)
                                logger.info(f"[UpSeller] PDFs encontrados no filesystem apos popup morto: {len(downloads_novos)}")

                    try:
                        await new_page.close()
                    except Exception:
                        pass

                else:
                    # === Nem download nem popup ===
                    logger.warning("[UpSeller] Nem download nem popup detectado")
                    await self.screenshot("etiquetas_sem_download")

                    # Fallback: verificar filesystem
                    downloads_novos = self._verificar_downloads_novos("*.pdf")
                    if downloads_novos:
                        pdfs_baixados.extend(downloads_novos)
                        logger.info(f"[UpSeller] PDFs encontrados no filesystem: {len(downloads_novos)}")
                    else:
                        logger.error("[UpSeller] Nenhum PDF obtido por nenhum metodo")

                if pdfs_baixados and not aba_norm.startswith("falha"):
                    # ---- PRIORIDADE: Usar modal nativo do UpSeller ----
                    # Apos gerar etiquetas, UpSeller mostra um dialog com
                    # "Marcar como Impresso" que marca TODOS os pedidos do lote.
                    # Isso e MUITO mais confiavel que selecionar manualmente.
                    marcou_via_modal = False
                    try:
                        # Aguardar o modal nativo do UpSeller (barra de progresso 100%)
                        for _attempt in range(10):
                            modal_marcar = self._page.locator(
                                '.ant-modal button.ant-btn-primary:has-text("Marcar como Impresso"), '
                                '.ant-modal button.ant-btn-primary:has-text("Marcar como impresso"), '
                                '.ant-modal button.ant-btn-primary:has-text("Mark as Printed")'
                            ).first
                            if await modal_marcar.count() > 0 and await modal_marcar.is_visible(timeout=1500):
                                break
                            await self._page.wait_for_timeout(2000)

                        if await modal_marcar.count() > 0 and await modal_marcar.is_visible(timeout=2000):
                            logger.info("[UpSeller] Modal nativo 'Marcar como Impresso' encontrado!")
                            await self.screenshot("etiquetas_modal_marcar_impresso")
                            await modal_marcar.click()
                            await self._page.wait_for_timeout(2500)
                            marcou_via_modal = True
                            logger.info("[UpSeller] Modal nativo: clicou 'Marcar como Impresso' com sucesso!")

                            # Confirmar modal de confirmacao se aparecer
                            try:
                                confirm_btn = await self._page.wait_for_selector(
                                    '.ant-modal button.ant-btn-primary', timeout=3000
                                )
                                if confirm_btn:
                                    confirm_text = await confirm_btn.evaluate("el => (el.textContent || '').trim()")
                                    if 'marcar' not in confirm_text.lower():
                                        await confirm_btn.click()
                                        await self._page.wait_for_timeout(1500)
                            except Exception:
                                pass
                        else:
                            logger.info("[UpSeller] Modal nativo nao apareceu, usando fallback manual")
                    except Exception as e_modal:
                        logger.warning(f"[UpSeller] Erro ao buscar modal nativo: {e_modal}")

                    # Fallback: se modal nativo nao funcionou, usar metodo manual
                    if not marcou_via_modal:
                        await _marcar_como_impresso_pos_download()

            except Exception as e:
                print(f"[baixar_etiquetas] ERRO no processo de impressao: {e}")
                logger.error(f"[UpSeller] Erro no processo de impressao: {e}")
                import traceback
                traceback.print_exc()

            await self.screenshot("etiquetas_04_finalizado")

        except Exception as e:
            logger.error(f"[UpSeller] Erro ao baixar etiquetas: {e}")

        print(f"[baixar_etiquetas] FIM - Total de PDFs: {len(pdfs_baixados)}")
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

    async def extrair_dados_pedidos(
        self,
        status_filter: str = "para_imprimir",
        filtro_loja: Union[str, List[str], None] = None
    ) -> str:
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
            filtro_loja: nome da loja (str) ou lista de lojas para aplicar filtro antes da extracao

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

            # Aplicar filtro de loja quando informado (evita misturar produtos de outras lojas).
            filtro_lojas = self._normalizar_lista_lojas_filtro(filtro_loja)
            if filtro_lojas:
                try:
                    if len(filtro_lojas) == 1:
                        filtrou = await self._aplicar_filtro_loja_seguro(
                            filtro_lojas[0], contexto=f"extrair_{status_filter}"
                        )
                        filtro_desc = filtro_lojas[0]
                    else:
                        filtrou = await self._aplicar_filtro_lojas_seguro(
                            filtro_lojas, contexto=f"extrair_{status_filter}_lote"
                        )
                        filtro_desc = f"{len(filtro_lojas)} lojas"
                    if filtrou:
                        logger.info(f"[UpSeller] Extracao de pedidos filtrada por loja(s): {filtro_desc}")
                        await self._page.wait_for_timeout(1200)
                    else:
                        logger.error(f"[UpSeller] Nao filtrou loja(s) '{filtro_desc}' para extracao. Abortando.")
                        return ""
                except Exception as e_f:
                    logger.error(f"[UpSeller] Erro ao filtrar loja(s) na extracao: {e_f}. Abortando.")
                    return ""

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

            # Selecionar 300/pagina para reduzir paginacao
            await self._selecionar_300_por_pagina()

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

    async def extrair_dados_pedidos_em_impressao(self, filtro_loja: Union[str, List[str], None] = None) -> str:
        """
        Extrai pedidos diretamente da pagina 'Etiqueta para Impressao'
        (/pt/order/in-process) e gera XLSX compativel.

        Este metodo e usado como fallback no botao 'Gerar Etiquetas',
        quando a extracao via /order/to-ship nao retorna pedidos.
        """
        if not self._page:
            await self._iniciar_navegador()

        if not await self._esta_logado():
            if not await self.login():
                return ""

        logger.info("[UpSeller] Extraindo dados de pedidos em /pt/order/in-process...")
        pedidos = []

        try:
            await self._page.goto(UPSELLER_PARA_IMPRIMIR, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(2500)
            await self._fechar_popups()
            await self._fechar_popups()

            filtro_lojas = self._normalizar_lista_lojas_filtro(filtro_loja)
            if filtro_lojas:
                try:
                    if len(filtro_lojas) == 1:
                        filtrou = await self._aplicar_filtro_loja_seguro(
                            filtro_lojas[0], contexto="extrair_inprocess"
                        )
                        filtro_desc = filtro_lojas[0]
                    else:
                        filtrou = await self._aplicar_filtro_lojas_seguro(
                            filtro_lojas, contexto="extrair_inprocess_lote"
                        )
                        filtro_desc = f"{len(filtro_lojas)} lojas"
                    if filtrou:
                        logger.info(f"[UpSeller] Extracao (in-process) filtrada por loja(s): {filtro_desc}")
                        await self._page.wait_for_timeout(1200)
                    else:
                        logger.error(f"[UpSeller] Nao filtrou loja(s) '{filtro_desc}' em in-process. Abortando.")
                        return ""
                except Exception as e_f:
                    logger.error(f"[UpSeller] Erro ao filtrar loja(s) em in-process: {e_f}. Abortando.")
                    return ""

            try:
                await self._page.wait_for_selector(
                    'table.my_custom_table, tr.top_row, tr.row.my_table_border',
                    timeout=12000
                )
            except Exception:
                logger.warning("[UpSeller] Tabela de in-process nao carregou, tentando extrair mesmo assim...")

            await self.screenshot("pedidos_inprocess_lista")

            # Selecionar 300/pagina para reduzir paginacao
            await self._selecionar_300_por_pagina()

            pagina_num = 1
            while True:
                logger.info(f"[UpSeller] In-process: processando pagina {pagina_num}...")
                pedidos_pagina = await self._extrair_pedidos_pagina()
                if pedidos_pagina:
                    pedidos.extend(pedidos_pagina)
                    logger.info(f"[UpSeller] In-process pagina {pagina_num}: {len(pedidos_pagina)} pedidos extraidos")
                else:
                    logger.info(f"[UpSeller] In-process pagina {pagina_num}: nenhum pedido encontrado")
                    if pagina_num == 1:
                        pedidos_alt = await self._extrair_pedidos_alternativo()
                        if pedidos_alt:
                            pedidos.extend(pedidos_alt)
                            logger.info(f"[UpSeller] In-process metodo alternativo: {len(pedidos_alt)} pedidos")
                    break

                proximo = await self._ir_proxima_pagina()
                if not proximo:
                    break
                pagina_num += 1
                await self._page.wait_for_timeout(1200)

        except Exception as e:
            logger.error(f"[UpSeller] Erro ao extrair pedidos em in-process: {e}")
            await self.screenshot("pedidos_inprocess_erro")

        if not pedidos:
            logger.warning("[UpSeller] Nenhum pedido encontrado em /pt/order/in-process")
            return ""

        xlsx_path = self._gerar_xlsx_pedidos(pedidos)
        logger.info(f"[UpSeller] XLSX (in-process) gerado com {len(pedidos)} pedidos: {xlsx_path}")
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

                # Fallback de identificador unico por linha (UP_ID no top_row)
                up_id = ''
                m_up = re.search(r'UP[A-Z0-9]{4,}', top_text, flags=re.IGNORECASE)
                if m_up:
                    up_id = (m_up.group(0) or '').upper()

                if not loja:
                    loja = 'Desconhecida'

                if loja not in lojas_dict:
                    lojas_dict[loja] = {'marketplace': marketplace, 'orders': set()}
                elif marketplace and not lojas_dict[loja]['marketplace']:
                    lojas_dict[loja]['marketplace'] = marketplace

                # A chave principal deve ser UP_ID da top_row (quando disponivel),
                # pois e a referencia mais estavel da linha e evita erro de pareamento
                # entre top_rows e data_rows.
                order_key = (up_id or order_sn or '').strip()
                if not order_key:
                    # Assinatura estavel para evitar inflar contagem quando order_sn nao aparece.
                    assinatura = re.sub(r'\s+', ' ', (top_text or '')).strip().encode('utf-8', 'ignore')
                    digest = hashlib.md5(assinatura).hexdigest()[:12]
                    order_key = f'_row_{digest}_{i}'
                lojas_dict[loja]['orders'].add(order_key)

            except Exception as e:
                logger.debug(f"[UpSeller] Erro ao ler linha {i}: {e}")

    async def _contar_lojas_via_pedidos(
        self,
        url: Optional[str] = None,
        nome_aba: Optional[str] = None,
        clicar_para_enviar: bool = True,
        exigir_aba: bool = False
    ) -> List[dict]:
        """
        Recalcula contagem por loja de forma manual e agregada:
        - garante 300/pagina
        - percorre todas as paginas (1/N, 2/N, ...)
        - em cada pagina, conta pedidos pelo nome da loja em tr.top_row
          usando UP_ID/order_sn como chave de deduplicacao.

        Nao usa filtro loja-a-loja (mais rapido e mais estavel para sincronizacao).
        """
        lojas = {}  # nome -> {marketplace, orders:set()}
        url_alvo = (url or UPSELLER_PEDIDOS)

        # Garantir contexto base na pagina alvo.
        try:
            await self._page.goto(url_alvo, wait_until="domcontentloaded", timeout=30000)
            await self._page.wait_for_timeout(1800)
            await self._fechar_popups()
            await self._page.wait_for_timeout(300)
        except Exception:
            pass

        # Para /order/to-ship, focar explicitamente no item "Para Enviar" do sidebar.
        if clicar_para_enviar:
            try:
                await self._page.evaluate("""
                    (() => {
                        const nodes = document.querySelectorAll('li, a, span, div');
                        for (const el of nodes) {
                            const t = (el.textContent || '').trim();
                            if (/^Para Enviar(\\s+\\d+)?$/.test(t)) {
                                const r = el.getBoundingClientRect();
                                if (r.width > 5 && r.height > 5 && r.height < 120) {
                                    el.click();
                                    return true;
                                }
                            }
                        }
                        return false;
                    })()
                """)
                await self._page.wait_for_timeout(900)
            except Exception:
                pass

        # Em paginas com sub-abas, focar na aba desejada (ex.: "Para Emitir", "Para Imprimir").
        if nome_aba:
            try:
                clicou_aba = await self._page.evaluate("""
                    (nomeAba) => {
                        const normalize = (s) => (s || '')
                            .toLowerCase()
                            .normalize('NFD')
                            .replace(/[\\u0300-\\u036f]/g, '')
                            .replace(/\\s+/g, ' ')
                            .trim();
                        const target = normalize(nomeAba);
                        const nodes = Array.from(
                            document.querySelectorAll(
                                '[role="tab"], .ant-tabs-tab, .ant-menu-item, li.ant-menu-item, a, button, span'
                            )
                        );
                        let best = null;
                        let bestScore = -1;
                        for (const el of nodes) {
                            const raw = (el.textContent || '').trim();
                            if (!raw) continue;
                            const txt = normalize(raw);
                            let score = -1;
                            if (txt === target || txt.startsWith(target + ' ')) score = 100;
                            else if (txt.includes(target)) score = 80;
                            else continue;

                            const r = el.getBoundingClientRect();
                            if (r.width <= 8 || r.width >= 720 || r.height <= 8 || r.height >= 120) continue;
                            if (r.y > 460) score -= 15;
                            const cls = ((el.className || '') + '').toLowerCase();
                            const hint = /(tab|menu|item|dropdown|nav)/.test(cls);
                            if (txt.includes(target) && !(txt === target || txt.startsWith(target + ' ')) && !hint) {
                                score -= 25;
                            }

                            if (score > bestScore) {
                                best = el;
                                bestScore = score;
                            }
                        }
                        if (!best) return false;
                        best.click();
                        return true;
                    }
                """, nome_aba)
                await self._page.wait_for_timeout(900)
                if exigir_aba and not clicou_aba:
                    logger.info(
                        f"[UpSeller] Aba '{nome_aba}' nao encontrada em {url_alvo}; retornando vazio."
                    )
                    return []
            except Exception:
                if exigir_aba:
                    logger.info(
                        f"[UpSeller] Erro ao focar aba '{nome_aba}' em {url_alvo}; retornando vazio."
                    )
                    return []

        # Garantir que nao ficou filtro de loja preso da execucao anterior.
        try:
            await self._limpar_filtro_loja()
            await self._page.wait_for_timeout(400)
        except Exception:
            pass

        async def _ler_info_paginacao() -> Dict:
            try:
                info = await self._page.evaluate("""
                    (() => {
                        const out = { current: 1, total_pages: 1, total_itens: 0, page_size: 0 };
                        const ui = document.querySelector('.my_page_ui');
                        const txt = (ui ? ui.textContent : document.body.textContent || '') || '';

                        const mTotal = txt.match(/Total\\s*(\\d+)/i);
                        if (mTotal) out.total_itens = parseInt(mTotal[1], 10) || 0;

                        const curTxt = (document.querySelector('.my_page_ui .hover_cl_link')?.textContent || '').trim();
                        const mCur = curTxt.match(/(\\d+)\\s*\\/\\s*(\\d+)/);
                        if (mCur) {
                            out.current = parseInt(mCur[1], 10) || 1;
                            out.total_pages = parseInt(mCur[2], 10) || 1;
                        } else {
                            const mAny = txt.match(/(\\d+)\\s*\\/\\s*(\\d+)/);
                            if (mAny) {
                                out.current = parseInt(mAny[1], 10) || 1;
                                out.total_pages = parseInt(mAny[2], 10) || 1;
                            }
                        }

                        const sizeTxt = (
                            document.querySelector('.my_page_ui .ant-select-selection-selected-value')?.textContent ||
                            document.querySelector('.my_page_ui .ant-select-selection__rendered')?.textContent ||
                            ''
                        ).trim();
                        const mSize = sizeTxt.match(/(\\d+)\\s*\\/\\s*p[áa]g/i);
                        if (mSize) out.page_size = parseInt(mSize[1], 10) || 0;
                        return out;
                    })()
                """)
                return info if isinstance(info, dict) else {"current": 1, "total_pages": 1, "total_itens": 0, "page_size": 0}
            except Exception:
                return {"current": 1, "total_pages": 1, "total_itens": 0, "page_size": 0}

        async def _garantir_300_por_pagina() -> bool:
            for _ in range(3):
                try:
                    await self._selecionar_300_por_pagina()
                except Exception:
                    pass
                await self._page.wait_for_timeout(700)
                info = await _ler_info_paginacao()
                if int(info.get("page_size", 0) or 0) >= 300:
                    return True
            return False

        async def _coletar_lojas_pagina_atual(pagina_atual: int, esperado_na_pagina: int) -> int:
            """
            Coleta pedidos da pagina atual rolando verticalmente para carregar
            todos os blocos lazy-load e deduplicando por UP_ID/order_sn.
            """
            vistos_pagina = set()
            sem_novos = 0
            y_anterior = -1
            sy_anterior = -1

            try:
                await self._page.evaluate("""
                    (() => {
                        window.scrollTo(0, 0);
                        const sels = [
                            '.ant-table-body',
                            '.list_table .ant-table-body',
                            '.my_table_body',
                            '.table_body',
                            '.my_custom_table_wrap',
                        ];
                        for (const s of sels) {
                            document.querySelectorAll(s).forEach((el) => {
                                try { el.scrollTop = 0; } catch (_) {}
                            });
                        }
                    })()
                """)
            except Exception:
                pass
            await self._page.wait_for_timeout(250)

            max_passos = 420
            passo_px = 1200

            for _ in range(max_passos):
                leitura = await self._page.evaluate("""
                    (() => {
                        const out = { itens: [], y: 0, h: 0, sy: 0, sh: 0, sch: 0 };
                        const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
                        const rows = document.querySelectorAll('tr.top_row, tr[class*="top_row"], .order_item');

                        const detectarMarketplace = (txt) => {
                            const t = (txt || '').toLowerCase();
                            if (t.includes('shopee')) return 'Shopee';
                            if (t.includes('shein')) return 'Shein';
                            if (t.includes('mercado livre') || t.includes('mercado')) return 'Mercado Livre';
                            if (t.includes('tiktok')) return 'TikTok';
                            if (t.includes('amazon')) return 'Amazon';
                            if (t.includes('magalu')) return 'Magalu';
                            if (t.includes('kwai')) return 'Kwai';
                            return '';
                        };
                        const hashTxt = (s) => {
                            let h = 0;
                            const str = s || '';
                            for (let k = 0; k < str.length; k++) {
                                h = ((h << 5) - h) + str.charCodeAt(k);
                                h |= 0;
                            }
                            return Math.abs(h).toString(36);
                        };

                        for (let i = 0; i < rows.length; i++) {
                            const row = rows[i];
                            const txt = norm(row.textContent || '');
                            if (!txt) continue;

                            let loja = '';
                            const lojaEl = row.querySelector(
                                'span.d_ib.max_w_160, span[class*="max_w_160"], [class*="shop_name"], [class*="store_name"]'
                            );
                            if (lojaEl) loja = norm(lojaEl.textContent || '');

                            let marketplace = detectarMarketplace(txt);
                            if (!loja) {
                                const mLojaMp = txt.match(/([^|\\n]{2,})\\|\\s*(Shopee|Shein|Mercado Livre|TikTok|Amazon|Magalu|Kwai)\\s*$/i);
                                if (mLojaMp) {
                                    loja = norm(mLojaMp[1] || '');
                                    marketplace = marketplace || norm(mLojaMp[2] || '');
                                }
                            }

                            let upId = '';
                            const mUp = txt.match(/\\b(UP[A-Z0-9]{4,})\\b/i);
                            if (mUp) upId = (mUp[1] || '').toUpperCase();

                            let orderSn = '';
                            if (!upId) {
                                const dataRow = row.nextElementSibling;
                                if (dataRow) {
                                    const tds = dataRow.querySelectorAll('td');
                                    if (tds && tds.length >= 4) {
                                        const c3 = norm((tds[3].textContent || '').split('\\n')[0] || '');
                                        const mOrder = c3.match(/\\b([A-Z0-9]{8,})\\b/i);
                                        if (mOrder) orderSn = (mOrder[1] || '').toUpperCase();
                                    }
                                }
                            }

                            const key = upId || orderSn || (`_row_${hashTxt(txt)}_${i}`);
                            if (!loja) loja = 'Desconhecida';
                            out.itens.push({
                                key,
                                loja,
                                marketplace: marketplace || ''
                            });
                        }

                        out.y = window.scrollY || document.documentElement.scrollTop || 0;
                        out.h = Math.max(
                            document.body.scrollHeight || 0,
                            document.documentElement.scrollHeight || 0
                        );
                        const cands = Array.from(document.querySelectorAll(
                            '.ant-table-body, .list_table .ant-table-body, .my_table_body, .table_body, .my_custom_table_wrap'
                        ));
                        let best = null;
                        let bestSpan = 0;
                        for (const c of cands) {
                            if (!c) continue;
                            const span = (c.scrollHeight || 0) - (c.clientHeight || 0);
                            if (span <= 40) continue;
                            const r = c.getBoundingClientRect();
                            if (r.width < 100 || r.height < 40) continue;
                            if (span > bestSpan) {
                                best = c;
                                bestSpan = span;
                            }
                        }
                        if (best) {
                            out.sy = best.scrollTop || 0;
                            out.sh = best.scrollHeight || 0;
                            out.sch = best.clientHeight || 0;
                        }
                        return out;
                    })()
                """)

                itens = (leitura or {}).get("itens", []) if isinstance(leitura, dict) else []
                novos = 0
                for it in itens:
                    key = (it.get("key") or "").strip()
                    if not key:
                        continue
                    if key in vistos_pagina:
                        continue
                    vistos_pagina.add(key)
                    novos += 1

                    nome = (it.get("loja") or "Desconhecida").strip() or "Desconhecida"
                    mp = (it.get("marketplace") or "").strip()
                    if nome not in lojas:
                        lojas[nome] = {"marketplace": mp, "orders": set()}
                    elif mp and not lojas[nome].get("marketplace"):
                        lojas[nome]["marketplace"] = mp
                    lojas[nome]["orders"].add(key)

                if novos == 0:
                    sem_novos += 1
                else:
                    sem_novos = 0

                if esperado_na_pagina > 0 and len(vistos_pagina) >= esperado_na_pagina:
                    break

                y = int((leitura or {}).get("y", 0) or 0) if isinstance(leitura, dict) else 0
                h = int((leitura or {}).get("h", 0) or 0) if isinstance(leitura, dict) else 0
                sy = int((leitura or {}).get("sy", 0) or 0) if isinstance(leitura, dict) else 0
                sh = int((leitura or {}).get("sh", 0) or 0) if isinstance(leitura, dict) else 0
                sch = int((leitura or {}).get("sch", 0) or 0) if isinstance(leitura, dict) else 0
                fim_tabela = sh > 0 and sy >= max(0, sh - sch - 30)

                # Se nao esta encontrando novos itens por um tempo e ja chegou perto do fim, para.
                if sem_novos >= 8 and ((h > 0 and y >= (h - 1800)) or fim_tabela):
                    break

                if y == y_anterior and sy == sy_anterior and sem_novos >= 5:
                    break
                y_anterior = y
                sy_anterior = sy

                try:
                    await self._page.evaluate("""
                        (delta) => {
                            window.scrollBy(0, delta);
                            const sels = [
                                '.ant-table-body',
                                '.list_table .ant-table-body',
                                '.my_table_body',
                                '.table_body',
                                '.my_custom_table_wrap',
                            ];
                            for (const s of sels) {
                                document.querySelectorAll(s).forEach((el) => {
                                    try {
                                        const maxTop = Math.max(0, (el.scrollHeight || 0) - (el.clientHeight || 0));
                                        el.scrollTop = Math.min(maxTop, (el.scrollTop || 0) + delta);
                                    } catch (_) {}
                                });
                            }
                        }
                    """, passo_px)
                except Exception:
                    pass
                await self._page.wait_for_timeout(220)

            try:
                await self._page.evaluate("""
                    (() => {
                        window.scrollTo(0, 0);
                        const sels = [
                            '.ant-table-body',
                            '.list_table .ant-table-body',
                            '.my_table_body',
                            '.table_body',
                            '.my_custom_table_wrap',
                        ];
                        for (const s of sels) {
                            document.querySelectorAll(s).forEach((el) => {
                                try { el.scrollTop = 0; } catch (_) {}
                            });
                        }
                    })()
                """)
            except Exception:
                pass
            await self._page.wait_for_timeout(220)
            logger.info(
                f"[UpSeller] Pagina {pagina_atual}: coletados {len(vistos_pagina)} pedidos "
                f"(esperado~{esperado_na_pagina})"
            )
            return len(vistos_pagina)

        # 1) Garantir 300/pagina antes da contagem manual.
        garantiu_300 = await _garantir_300_por_pagina()
        info_pag = await _ler_info_paginacao()
        logger.info(
            f"[UpSeller] Contagem manual agregada ({url_alvo}, aba={nome_aba or 'default'}): "
            f"300/pagina={'ok' if garantiu_300 else 'nao_confirmado'}, paginacao={info_pag}"
        )

        # 2) Percorrer paginas e agregar por loja sem refiltrar loja-a-loja.
        pagina = 1
        max_paginas = 60
        while pagina <= max_paginas:
            info_atual = await _ler_info_paginacao()
            try:
                cur = int(info_atual.get("current", pagina) or pagina)
                total_pag = int(info_atual.get("total_pages", 1) or 1)
                total_itens = int(info_atual.get("total_itens", 0) or 0)
                page_size = int(info_atual.get("page_size", 300) or 300)
            except Exception:
                cur, total_pag, total_itens, page_size = pagina, 1, 0, 300

            if total_pag > 1:
                pagina = cur

            esperado = 0
            if total_itens > 0 and page_size > 0:
                base = (pagina - 1) * page_size
                esperado = max(0, min(page_size, total_itens - base))

            try:
                await _coletar_lojas_pagina_atual(pagina, esperado)
            except Exception as e_pag:
                logger.debug(f"[UpSeller] Falha na coleta manual da pagina {pagina}: {e_pag}")

            proxima = await self._ir_proxima_pagina()
            if not proxima:
                break
            pagina += 1
            await self._page.wait_for_timeout(900)

        resultado = []
        for nome, info in lojas.items():
            resultado.append({
                "nome": nome,
                "marketplace": info.get('marketplace', ''),
                "pedidos": len(info.get('orders', set())),
                "orders": [],
            })
        resultado.sort(key=lambda x: (-(int(x.get("pedidos", 0) or 0)), (x.get("nome") or "").lower()))
        return resultado

    async def _contar_linhas_visiveis_tabela(self) -> int:
        """Conta linhas de pedido visiveis na tabela atual."""
        try:
            n = await self._page.evaluate("""
                (() => {
                    const rows = document.querySelectorAll('tr.top_row, tr[class*="top_row"], .order_item');
                    return rows ? rows.length : 0;
                })()
            """)
            return int(n or 0)
        except Exception:
            return 0

    async def _contar_pedidos_loja_filtrada(self, nome_loja: str, max_paginas: int = 20) -> int:
        """
        Conta pedidos da loja ja filtrada percorrendo paginacao da tabela.
        Usa chaves de linha (UP_ID/order_sn) direto do DOM para evitar contaminar
        com contador global e evitar dependencia do parser de loja.
        """
        ids = set()
        total_comparadas = 0
        total_ok = 0
        pagina = 1

        while pagina <= max_paginas:
            leitura = {}
            try:
                leitura = await self._page.evaluate("""
                    (nomeLoja) => {
                        const normalize = (s) => (s || '')
                          .toLowerCase()
                          .normalize('NFD')
                          .replace(/[\\u0300-\\u036f]/g, '')
                          .replace(/[^a-z0-9 ]+/g, ' ')
                          .replace(/\\s+/g, ' ')
                          .trim();
                        const target = normalize(nomeLoja);

                        const out = [];
                        let comparadas = 0;
                        let ok = 0;
                        const topRows = Array.from(document.querySelectorAll('tr.top_row, tr[class*="top_row"], .order_item'));
                        const normalizeRaw = (s) => (s || '').replace(/\\s+/g, ' ').trim();

                        for (let i = 0; i < topRows.length; i++) {
                            const row = topRows[i];
                            const txt = normalizeRaw(row.textContent || '');
                            let key = '';

                            const lojaEl =
                              row.querySelector('span.d_ib.max_w_160, span[class*="max_w_160"], [class*="shop"], [class*="store"]');
                            const lojaTxt = normalize(lojaEl ? lojaEl.textContent : '');
                            // Sem nome de loja legivel na linha, nao conta (evita total global).
                            if (!lojaTxt) {
                                continue;
                            }
                            comparadas++;
                            const matchLoja = lojaTxt === target || lojaTxt.includes(target) || target.includes(lojaTxt);
                            if (!matchLoja) {
                                continue;
                            }
                            ok++;

                            // 1) UP_ID (mais estavel)
                            let m = txt.match(/\\b(UP[A-Z0-9]{4,})\\b/i);
                            if (m) key = (m[1] || '').toUpperCase();

                            // 2) order_sn na linha de dados seguinte
                            if (!key) {
                                const dataRow = row.nextElementSibling;
                                if (dataRow) {
                                    const tds = dataRow.querySelectorAll('td');
                                    if (tds && tds.length >= 4) {
                                        const c3 = normalize((tds[3].textContent || '').split('\\n')[0]);
                                        m = c3.match(/\\b([A-Z0-9]{8,})\\b/);
                                        if (m) key = (m[1] || '').toUpperCase();
                                    }
                                }
                            }

                            if (!key) key = `_row_${i}`;
                            out.push(key);
                        }
                        return { keys: out, comparadas, ok };
                    }
                """, nome_loja)
            except Exception:
                leitura = {"keys": [], "comparadas": 0, "ok": 0}

            chaves_pagina = (leitura.get("keys") or []) if isinstance(leitura, dict) else []
            try:
                total_comparadas += int((leitura or {}).get("comparadas", 0) or 0)
                total_ok += int((leitura or {}).get("ok", 0) or 0)
            except Exception:
                pass

            for idx, k in enumerate(chaves_pagina or []):
                key = (k or '').strip()
                if not key:
                    key = f"_p{pagina}_i{idx}"
                # Evita colisao de fallback "_row_i" entre paginas.
                if key.startswith("_row_"):
                    key = f"{key}_p{pagina}"
                ids.add(key)

            proxima = await self._ir_proxima_pagina()
            if not proxima:
                break

            pagina += 1
            await self._page.wait_for_timeout(800)

        # Se conseguimos ler lojas da tabela e a maioria nao bate com a loja alvo,
        # considera contagem invalida (provavel filtro nao aplicado).
        if total_comparadas == 0:
            return 0
        if total_comparadas > 0:
            ratio = (total_ok / max(total_comparadas, 1))
            if ratio < 0.85:
                return 0

        return len(ids)

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
                txt_low = (top_text or '').lower()
                if 'shopee' in txt_low:
                    marketplace = 'Shopee'
                elif 'shein' in txt_low:
                    marketplace = 'Shein'
                elif 'mercado livre' in txt_low or 'mercado' in txt_low:
                    marketplace = 'Mercado Livre'
                elif 'tiktok' in txt_low:
                    marketplace = 'TikTok'
                elif 'amazon' in txt_low:
                    marketplace = 'Amazon'
                elif 'magalu' in txt_low:
                    marketplace = 'Magalu'
                else:
                    mp_el = await top_row.query_selector('span.f_cl_59:last-of-type')
                    if mp_el:
                        mp_txt = (await mp_el.inner_text()).strip()
                        if mp_txt.lower() in ('shopee', 'shein', 'mercado livre', 'tiktok', 'amazon', 'magalu'):
                            marketplace = mp_txt
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

    async def _selecionar_300_por_pagina(self):
        """Seleciona '300/página' no dropdown de paginacao para minimizar navegacao entre paginas."""
        try:
            # Procurar o dropdown de tamanho de pagina (ex: "50/página", "100/página")
            selecionou = await self._page.evaluate("""
                (() => {
                    // Tentar select nativo primeiro
                    const selects = document.querySelectorAll('select');
                    for (const sel of selects) {
                        const opts = sel.querySelectorAll('option');
                        for (const opt of opts) {
                            if (opt.value === '300' || opt.textContent.includes('300')) {
                                sel.value = opt.value;
                                sel.dispatchEvent(new Event('change', {bubbles: true}));
                                return 'select';
                            }
                        }
                    }
                    // Procurar dropdown ant-design (.ant-select / .ant-pagination-options-size-changer)
                    const sizeChanger = document.querySelector('.ant-pagination-options-size-changer, .ant-select[class*="page"]');
                    if (sizeChanger) {
                        sizeChanger.click();
                        return 'ant-clicked';
                    }
                    // Procurar dropdown customizado do UpSeller (my_page_ui)
                    const pageUi = document.querySelector('.my_page_ui');
                    if (pageUi) {
                        const btns = pageUi.querySelectorAll('button, div[class*="select"], span[class*="select"]');
                        for (const btn of btns) {
                            const t = (btn.textContent || '').trim();
                            if (/\\d+\\s*\\/\\s*p[áa]g/i.test(t)) {
                                btn.click();
                                return 'dropdown-clicked';
                            }
                        }
                    }
                    return null;
                })()
            """)

            if selecionou == 'select':
                await self._page.wait_for_timeout(2000)
                logger.info("[UpSeller] Selecionou 300/pagina via <select>")
                return True

            if selecionou in ('ant-clicked', 'dropdown-clicked'):
                await self._page.wait_for_timeout(800)
                # Agora procurar a opcao "300" no dropdown aberto
                clicou_300 = await self._page.evaluate("""
                    (() => {
                        // Procurar em dropdowns abertos (ant-design overlay, ou lista customizada)
                        const candidates = document.querySelectorAll(
                            '.ant-select-dropdown .ant-select-item, ' +
                            '.ant-dropdown li, .ant-dropdown-menu-item, ' +
                            '[class*="dropdown"] li, [class*="popup"] li, ' +
                            '[class*="option"], [class*="menu-item"]'
                        );
                        for (const el of candidates) {
                            const t = (el.textContent || '').trim();
                            if (t.includes('300')) {
                                el.click();
                                return true;
                            }
                        }
                        // Fallback: qualquer elemento visivel com "300" que parece opcao de paginacao
                        const all = document.querySelectorAll('li, div, span, a');
                        for (const el of all) {
                            const t = (el.textContent || '').trim();
                            const rect = el.getBoundingClientRect();
                            if (t.match(/^300\\s*(\\/\\s*p[áa]g)?/i) && rect.width > 20 && rect.height > 10 && rect.height < 60) {
                                el.click();
                                return true;
                            }
                        }
                        return false;
                    })()
                """)
                if clicou_300:
                    await self._page.wait_for_timeout(2500)
                    logger.info("[UpSeller] Selecionou 300/pagina via dropdown")
                    return True
                else:
                    logger.warning("[UpSeller] Dropdown abriu mas nao encontrou opcao 300")
                    # Fechar dropdown (clicar fora)
                    await self._page.evaluate("document.body.click()")
                    await self._page.wait_for_timeout(500)

            logger.debug("[UpSeller] Dropdown de paginacao nao encontrado")
            return False

        except Exception as e:
            logger.debug(f"[UpSeller] Erro ao selecionar 300/pagina: {e}")
            return False

    async def _ir_proxima_pagina(self) -> bool:
        """Tenta navegar para a proxima pagina da lista de pedidos."""
        try:
            info_before = await self._page.evaluate("""
                () => {
                    const out = { raw: null, cur: 1, tot: 1 };
                    const txtCur = (document.querySelector('.my_page_ui .hover_cl_link')?.textContent || '').trim();
                    const txtUi = (document.querySelector('.my_page_ui')?.textContent || '').trim();
                    const txt = txtCur || txtUi || '';
                    const m = txt.match(/(\\d+)\\s*\\/\\s*(\\d+)/);
                    if (m) {
                        out.raw = `${m[1]}/${m[2]}`;
                        out.cur = parseInt(m[1], 10) || 1;
                        out.tot = parseInt(m[2], 10) || 1;
                    }
                    return out;
                }
            """)
            cur_before = int((info_before or {}).get("cur", 1) or 1)
            tot_before = int((info_before or {}).get("tot", 1) or 1)
            if cur_before >= tot_before:
                return False

            selectors = [
                ".my_page_ui button[title*='Página Seguinte']",
                ".my_page_ui button[title*='Pagina Seguinte']",
                ".my_page_ui button[title*='Próxima']",
                ".my_page_ui button[title*='Proxima']",
                ".my_page_ui button[title*='Próximo']",
                ".my_page_ui button[title*='Proximo']",
                ".ant-pagination-next button",
                ".ant-pagination-next",
                "button[aria-label='next']",
                "a[aria-label='next']",
            ]

            async def _pagina_atual():
                try:
                    info = await self._page.evaluate("""
                        () => {
                            const txtCur = (document.querySelector('.my_page_ui .hover_cl_link')?.textContent || '').trim();
                            const txtUi = (document.querySelector('.my_page_ui')?.textContent || '').trim();
                            const txt = txtCur || txtUi || '';
                            const m = txt.match(/(\\d+)\\s*\\/\\s*(\\d+)/);
                            if (!m) return {cur: null, raw: null};
                            return {cur: parseInt(m[1], 10) || null, raw: `${m[1]}/${m[2]}`};
                        }
                    """)
                    return info or {"cur": None, "raw": None}
                except Exception:
                    return {"cur": None, "raw": None}

            # 1) Tentativa principal: botao "proxima".
            clicou = False
            for sel in selectors:
                loc = self._page.locator(sel).first
                if await loc.count() == 0:
                    continue
                try:
                    disabled = await loc.get_attribute("disabled")
                    aria = (await loc.get_attribute("aria-disabled") or "").lower()
                    cls = (await loc.get_attribute("class") or "").lower()
                    if disabled is not None or aria == "true" or "disabled" in cls:
                        continue
                    await loc.click(timeout=2500)
                    clicou = True
                    break
                except Exception:
                    continue

            if clicou:
                await self._page.wait_for_timeout(1100)
                after = await _pagina_atual()
                if int((after or {}).get("cur") or 0) > cur_before:
                    return True

            # 2) Fallback: selecionar pagina alvo no combobox "1/2".
            alvo_raw = f"{cur_before + 1}/{tot_before}"
            tentou_combo = await self._page.evaluate("""
                (alvoRaw) => {
                    const trigger =
                        document.querySelector('.my_page_ui .my_combobox_box .input_box') ||
                        document.querySelector('.my_page_ui .my_combobox_box .hover_cl_link') ||
                        document.querySelector('.my_page_ui .my_combobox_box');
                    if (!trigger) return false;
                    trigger.click();
                    const itens = Array.from(document.querySelectorAll('.my_page_ui .combobox_item, .combobox_item'));
                    for (const it of itens) {
                        const t = (it.textContent || '').replace(/\\s+/g, ' ').trim();
                        if (t === alvoRaw) {
                            it.click();
                            return true;
                        }
                    }
                    return false;
                }
            """, alvo_raw)
            if tentou_combo:
                await self._page.wait_for_timeout(1200)
                after = await _pagina_atual()
                if int((after or {}).get("cur") or 0) > cur_before:
                    return True

            return False

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

        # Copiar XLSX principal + extras (quando extracao for por loja)
        xlsx_paths = []
        xlsx_main = resultado.get("xlsx", "")
        if xlsx_main:
            xlsx_paths.append(xlsx_main)
        for xp in resultado.get("xlsx_extra", []) or []:
            if xp:
                xlsx_paths.append(xp)

        for xlsx_path in xlsx_paths:
            if not xlsx_path or not os.path.exists(xlsx_path):
                continue
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

        # 1. Baixar Lista de Resumo (XLSX) antes das etiquetas.
        if incluir_xlsx:
            try:
                xlsx_lista = await scraper.baixar_lista_resumo()
                if isinstance(xlsx_lista, list) and xlsx_lista:
                    resultado["xlsx"] = xlsx_lista[0]
                elif isinstance(xlsx_lista, str):
                    resultado["xlsx"] = xlsx_lista

                # Fallback legado caso Lista de Resumo nao retorne arquivo.
                if not resultado["xlsx"]:
                    xlsx_path = await scraper.extrair_dados_pedidos()
                    resultado["xlsx"] = xlsx_path or ""

                logger.info(f"[UpSeller] XLSX para processamento: {resultado['xlsx']}")
            except Exception as e:
                logger.warning(f"[UpSeller] Erro ao obter XLSX (continuando): {e}")

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

