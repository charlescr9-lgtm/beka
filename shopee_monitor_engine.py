# -*- coding: utf-8 -*-
"""
Shopee Monitor Engine — Motor CDP para orquestrar Claude via extensao Chrome.

Conecta ao Chrome via DevTools Protocol, envia comandos ao chat da extensao
Claude no sidebar e aguarda respostas com sentinelas.

ARQUITETURA TESTADA:
1. Chrome abre com perfil CDP dedicado (--user-data-dir separado)
2. Abre aba do Shopee (ativa) + aba do sidepanel Claude (inativa)
3. Override chrome.tabs.query no sidepanel para retornar aba Shopee
4. Digita via CDP Input.dispatchKeyEvent (char a char) no ProseMirror
5. Envia via React props onClick do send-button
6. Poll DOM para detectar sentinelas nas respostas
"""

import asyncio
import json
import logging
import os
import re
import subprocess
import threading
import time
from datetime import datetime

import requests

logger = logging.getLogger("shopee_monitor")

# =============================================
# CONFIGURACOES
# =============================================

EXTENSION_ID = "fcoeoabgfenejglbffodgkkbkcdhcgfn"
DEFAULT_CDP_PORT = 9222
POLL_INTERVAL = 2       # segundos entre polls
TIMEOUT_ETAPA = 600     # 10 min timeout por etapa

SENTINELAS = {
    1: "FINALIZADO_ETAPA_1",
    2: "FINALIZADO_ETAPA_2",
    3: "FINALIZADO_ETAPA_3",
    4: "FINALIZADO_ETAPA_4",
}

COMANDO_ETAPA_1 = """Acesse https://seller.shopee.com.br/portal/shop e colete a lista completa de todas as lojas.
Pagine se necessario ate ter todas.
Retorne APENAS um JSON no formato:
{"lojas": [{"nome": "...", "username": "..."}]}
Ao final escreva exatamente: FINALIZADO_ETAPA_1"""

COMANDO_ETAPA_2 = """Para CADA loja da lista abaixo, faca:
1. Acesse o dashboard de envios da loja
2. Colete o numero de envios processados
3. Na lista de pedidos, execute JS para detectar alertas (atrasado, risco de atraso, cancelamento)

Lojas: {lista_json}

Retorne JSON no formato:
{{"resultado": [{{"nome":"...","username":"...","envios_processados":0,"atrasado":0,"risco":0,"cancelamento":0,"tem_alerta":false}}]}}
Ao final escreva exatamente: FINALIZADO_ETAPA_2"""

COMANDO_ETAPA_3 = """Para CADA loja da lista abaixo, acesse a pagina de reembolsos e devolucoes.
Colete:
- Quantidade de reembolsos aprovados pela Shopee hoje
- Lista de erros do vendedor (pedido_id, motivo, valor, acao, status, data)

Lojas: {lista_json}

Retorne JSON no formato:
{{"resultado": [{{"nome":"...","username":"...","reembolsos_shopee_hoje":0,"erros_vendedor":[]}}]}}
Ao final escreva exatamente: FINALIZADO_ETAPA_3"""

COMANDO_ETAPA_4 = """Para CADA loja com alertas abaixo, abra uma nova aba com a pagina de etiqueta de cada pedido atrasado/em risco.
Deixe as abas abertas no ponto de "Salvar como PDF" - NAO clique em nada apos abrir.

Lojas com alertas: {alertas_json}

Retorne JSON no formato:
{{"resultado": [{{"nome":"...","username":"...","etiquetas_abertas":0,"pedidos":["..."]}}],"total_abas_etiqueta":0}}
Ao final escreva exatamente: FINALIZADO_ETAPA_4"""


# =============================================
# CHROME MANAGER
# =============================================

def find_chrome_path():
    """Detecta o caminho do Chrome no Windows."""
    candidates = [
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
    ]
    for p in candidates:
        if os.path.isfile(p):
            return p
    return None


def is_cdp_available(port=DEFAULT_CDP_PORT):
    """Verifica se o Chrome esta acessivel via CDP."""
    try:
        r = requests.get(f"http://localhost:{port}/json/version", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


CDP_USER_DATA = os.path.join(
    os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome", "User Data CDP"
)


def ensure_chrome_cdp(port=DEFAULT_CDP_PORT):
    """Garante que o Chrome esta rodando com CDP ativo.

    Usa um perfil Chrome dedicado ('User Data CDP') para que:
    1. O Chrome do usuario continue rodando normalmente
    2. CDP funcione (Chrome exige --user-data-dir nao-padrao para CDP)
    3. Cookies/sessoes da Shopee ficam salvos nesse perfil dedicado

    Returns:
        (True, msg) se OK
        (False, msg) se falhou
    """
    if is_cdp_available(port):
        return True, "Chrome CDP ja ativo"

    chrome_path = find_chrome_path()
    if not chrome_path:
        return False, "Chrome nao encontrado no sistema"

    # Copiar extensoes do perfil principal (primeira vez ou atualizacao)
    _sync_extensions_to_cdp_profile()

    # Lancar Chrome com CDP + perfil dedicado
    try:
        os.makedirs(CDP_USER_DATA, exist_ok=True)
        subprocess.Popen(
            [
                chrome_path,
                f"--remote-debugging-port={port}",
                f"--user-data-dir={CDP_USER_DATA}",
                "--no-first-run",
                "--disable-default-apps",
            ],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        # Aguardar Chrome iniciar
        for _ in range(20):
            time.sleep(1)
            if is_cdp_available(port):
                return True, "Chrome CDP iniciado (perfil dedicado)"
        return False, "Chrome iniciou mas CDP nao respondeu em 20s"
    except Exception as e:
        return False, f"Erro ao iniciar Chrome: {e}"


def _sync_extensions_to_cdp_profile():
    """Copia extensoes e configs essenciais do perfil Chrome principal para o perfil CDP."""
    import shutil
    src_profile = os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome", "User Data")
    dst_profile = CDP_USER_DATA

    items = [
        os.path.join("Default", "Extensions"),
        os.path.join("Default", "Extension State"),
        os.path.join("Default", "Preferences"),
        os.path.join("Default", "Secure Preferences"),
        os.path.join("Default", "Login Data"),
        os.path.join("Default", "Web Data"),
        "Local State",
    ]

    os.makedirs(os.path.join(dst_profile, "Default"), exist_ok=True)

    for item in items:
        src = os.path.join(src_profile, item)
        dst = os.path.join(dst_profile, item)
        if not os.path.exists(src):
            continue
        try:
            if os.path.isdir(src):
                if not os.path.exists(dst):
                    shutil.copytree(src, dst)
                else:
                    # Atualizar apenas extensao Claude se ja existe
                    claude_src = os.path.join(src, EXTENSION_ID)
                    claude_dst = os.path.join(dst, EXTENSION_ID)
                    if os.path.exists(claude_src) and not os.path.exists(claude_dst):
                        shutil.copytree(claude_src, claude_dst)
            else:
                if not os.path.exists(dst):
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    shutil.copy2(src, dst)
        except Exception:
            pass  # Ignorar erros de permissao


def get_cdp_targets(port=DEFAULT_CDP_PORT):
    """Lista todos os targets CDP."""
    try:
        r = requests.get(f"http://localhost:{port}/json", timeout=5)
        return r.json()
    except Exception:
        return []


# =============================================
# SIDEPANEL MANAGER (testado e funcionando)
# =============================================

async def _sw_eval(ws, expr, mid_counter):
    """Executa JS no service worker via websocket."""
    mid_counter[0] += 1
    await ws.send(json.dumps({
        "id": mid_counter[0], "method": "Runtime.evaluate",
        "params": {"expression": expr, "awaitPromise": True, "returnByValue": True}
    }))
    while True:
        raw = await asyncio.wait_for(ws.recv(), timeout=15)
        d = json.loads(raw)
        if d.get("id") == mid_counter[0]:
            r = d.get("result", {}).get("result", {})
            err = d.get("result", {}).get("exceptionDetails", {})
            if err:
                raise Exception(err.get("exception", {}).get("description", "Unknown error")[:300])
            return r.get("value", r.get("description", ""))


def find_extension_sidebar(port=DEFAULT_CDP_PORT):
    """Encontra ou cria o sidepanel da extensao Claude.

    Estrategia testada:
    1. Se sidepanel ja existe como tab, retorna
    2. Senao, abre aba Shopee (ativa) + sidepanel (inativo) via service worker
    3. Aplica override de chrome.tabs.query no sidepanel

    Returns:
        dict target ou None
    """
    import websockets as _ws_mod

    targets = get_cdp_targets(port)
    ext_prefix = f"chrome-extension://{EXTENSION_ID}/"

    # Verificar se sidepanel ja existe
    for t in targets:
        url = t.get("url", "")
        if url.startswith(ext_prefix) and "sidepanel" in url.lower() and t.get("type") == "page":
            return t

    # Precisa criar: encontrar service worker e abrir tabs
    sw_target = None
    for t in targets:
        if t.get("type") == "service_worker" and EXTENSION_ID in t.get("url", ""):
            sw_target = t
            break

    if not sw_target or not sw_target.get("webSocketDebuggerUrl"):
        logger.error("Service worker da extensao Claude nao encontrado")
        return None

    try:
        async def _setup_tabs():
            ws = await _ws_mod.connect(sw_target["webSocketDebuggerUrl"], max_size=5*1024*1024)
            mid = [0]

            async def sw_ev(expr):
                return await _sw_eval(ws, expr, mid)

            # 1. Encontrar ou criar aba Shopee (ativa)
            r = await sw_ev(
                'chrome.tabs.query({url: "*://seller.shopee.com.br/*"})'
                '.then(tabs => JSON.stringify(tabs.map(t => ({id:t.id}))))'
            )
            tabs = json.loads(r)
            if tabs:
                shopee_tab_id = tabs[0]["id"]
            else:
                r = await sw_ev(
                    'chrome.tabs.create({url: "https://seller.shopee.com.br/", active: true})'
                    '.then(t => t.id)'
                )
                shopee_tab_id = int(r)
                await asyncio.sleep(4)

            # Ativar aba Shopee
            await sw_ev(f"chrome.tabs.update({shopee_tab_id}, {{active: true}})")

            # 2. Abrir sidepanel como tab inativa com tabId
            url = f"chrome-extension://{EXTENSION_ID}/sidepanel.html?tabId={shopee_tab_id}"
            await sw_ev(f'chrome.tabs.create({{url: "{url}", active: false}}).then(t => t.id)')

            await ws.close()

        loop = asyncio.new_event_loop()
        loop.run_until_complete(_setup_tabs())
        loop.close()

    except Exception as e:
        logger.warning(f"Falha ao abrir sidepanel: {e}")

    # Aguardar pagina carregar e re-verificar targets
    time.sleep(6)
    targets = get_cdp_targets(port)
    for t in targets:
        url = t.get("url", "")
        if url.startswith(ext_prefix) and "sidepanel" in url.lower() and t.get("type") == "page":
            return t

    return None


# =============================================
# CLAUDE CHAT (via CDP websocket) — TESTADO
# =============================================

class ClaudeChat:
    """Interface para ler/escrever no chat da extensao Claude via CDP.

    Usa:
    - Input.dispatchKeyEvent (char a char) para digitar no ProseMirror
    - React props onClick do send-button para enviar
    - Runtime.evaluate para ler respostas do DOM
    """

    def __init__(self, ws_url):
        self.ws_url = ws_url
        self._msg_id = 0
        self._ws = None
        self._tabs_overridden = False

    async def connect(self):
        """Conecta ao websocket CDP."""
        import websockets
        self._ws = await websockets.connect(
            self.ws_url,
            max_size=10 * 1024 * 1024,  # 10MB
            ping_interval=30,
        )
        # Habilitar Runtime e Console
        await self._send_cdp("Runtime.enable", {})
        await self._send_cdp("Console.enable", {})

        # Override chrome.tabs.query para retornar aba Shopee
        if not self._tabs_overridden:
            await self._override_tabs_query()
            self._tabs_overridden = True

        return True

    async def _override_tabs_query(self):
        """Override chrome.tabs.query para retornar aba Shopee como aba ativa."""
        override_js = (
            "(function() {"
            "  const origQuery = chrome.tabs.query.bind(chrome.tabs);"
            "  chrome.tabs.query = function(qi) {"
            "    if (qi && qi.active === true) {"
            '      return origQuery({url: "*://seller.shopee.com.br/*"});'
            "    }"
            "    return origQuery(qi);"
            "  };"
            '  return "overridden";'
            "})()"
        )
        await self.eval_js(override_js)

    async def close(self):
        if self._ws:
            await self._ws.close()

    async def _send_cdp(self, method, params=None):
        """Envia comando CDP e retorna resposta."""
        self._msg_id += 1
        msg = {"id": self._msg_id, "method": method, "params": params or {}}
        await self._ws.send(json.dumps(msg))

        # Ler respostas ate encontrar a que tem nosso id
        while True:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=30)
            data = json.loads(raw)
            if data.get("id") == self._msg_id:
                return data
            # Ignorar eventos/notificacoes do CDP

    async def eval_js(self, expression):
        """Executa JS no contexto da extensao e retorna resultado."""
        result = await self._send_cdp("Runtime.evaluate", {
            "expression": expression,
            "returnByValue": True,
            "awaitPromise": True,
        })
        err = result.get("result", {}).get("exceptionDetails", {})
        if err:
            desc = err.get("exception", {}).get("description", "")
            logger.warning(f"JS error: {desc[:200]}")
            return None

        r = result.get("result", {}).get("result", {})
        if r.get("type") == "undefined":
            return None
        return r.get("value", r.get("description", ""))

    async def send_message(self, text):
        """Envia mensagem no chat da extensao Claude.

        Estrategia TESTADA:
        1. Foca o ProseMirror input [data-test-id="message-input"]
        2. Limpa conteudo existente (Ctrl+A, Backspace)
        3. Digita char a char via Input.dispatchKeyEvent (ProseMirror reconhece)
        4. Clica send via React props onClick
        5. Registra body_length_at_send para filtrar sentinela
        """
        # Registrar snapshot do body ANTES de enviar (para filtrar sentinela)
        body_before = await self.get_last_response()
        self._body_snapshot_before_send = body_before or ""

        # Focar input
        await self.eval_js('document.querySelector("[data-test-id=message-input]")?.focus()')
        await asyncio.sleep(0.3)

        # Limpar conteudo existente
        await self._send_cdp("Input.dispatchKeyEvent", {
            "type": "keyDown", "key": "a", "code": "KeyA",
            "windowsVirtualKeyCode": 65, "modifiers": 2  # Ctrl+A
        })
        await self._send_cdp("Input.dispatchKeyEvent", {"type": "keyUp", "key": "a"})
        await self._send_cdp("Input.dispatchKeyEvent", {
            "type": "keyDown", "key": "Backspace", "code": "Backspace",
            "windowsVirtualKeyCode": 8
        })
        await self._send_cdp("Input.dispatchKeyEvent", {"type": "keyUp", "key": "Backspace"})
        await asyncio.sleep(0.2)

        # Digitar char a char (ProseMirror reconhece Input.dispatchKeyEvent)
        for ch in text:
            vk = ord(ch.upper()) if ch.isalpha() else ord(ch)
            await self._send_cdp("Input.dispatchKeyEvent", {
                "type": "keyDown", "text": ch, "key": ch,
                "windowsVirtualKeyCode": vk
            })
            await self._send_cdp("Input.dispatchKeyEvent", {"type": "keyUp", "key": ch})

        await asyncio.sleep(0.5)

        # Verificar texto inserido
        content = await self.eval_js(
            'document.querySelector("[data-test-id=message-input]")?.textContent'
        )
        if not content or not content.strip():
            return False, "Texto nao foi inserido no input"

        # Clicar send via React props onClick
        result = await self.eval_js(
            'var btn = document.querySelector("[data-test-id=send-button]");'
            'var pk = Object.keys(btn).find(function(k){return k.startsWith("__reactProps")});'
            'btn[pk].onClick();'
            '"clicked"'
        )
        if result == "clicked":
            return True, "ENVIADO"
        return False, f"Falha ao clicar send: {result}"

    async def get_last_response(self):
        """Extrai texto completo do body (inclui mensagens do chat)."""
        return await self.eval_js("document.body.innerText") or ""

    async def is_generating(self):
        """Verifica se o Claude ainda esta gerando resposta.

        Checa se o send-button esta disabled (indica geracao em andamento).
        """
        result = await self.eval_js(
            'document.querySelector("[data-test-id=send-button]")?.disabled'
        )
        return result is True

    async def wait_for_sentinel(self, sentinel, timeout=TIMEOUT_ETAPA, poll=POLL_INTERVAL):
        """Aguarda sentinela aparecer na resposta do Claude (texto NOVO apos envio).

        Conta ocorrencias do sentinela ANTES do envio vs AGORA.
        Quando a contagem aumenta, significa que o Claude respondeu com o sentinela.

        Returns:
            (True, texto_resposta) ou (False, erro)
        """
        start = time.time()
        last_text = ""
        stable_count = 0

        # Contar quantas vezes o sentinela ja aparecia ANTES de enviar
        snapshot = getattr(self, "_body_snapshot_before_send", "")
        count_before = snapshot.count(sentinel)

        # Esperar body crescer um pouco (mensagem do usuario + inicio de resposta)
        await asyncio.sleep(3)

        while (time.time() - start) < timeout:
            await asyncio.sleep(poll)

            full_text = await self.get_last_response()

            # Contar ocorrencias atuais do sentinela
            count_now = full_text.count(sentinel)
            # O comando do usuario adiciona +1 ocorrencia
            # A resposta do Claude adiciona mais +1
            # Entao: count_now > count_before + 1 = Claude respondeu
            if count_now > count_before + 1:
                return True, full_text

            # Se count_now == count_before + 1, pode ser so o comando do usuario
            # Mas se o body cresceu muito, provavelmente Claude ja respondeu
            if count_now == count_before + 1 and len(full_text) > len(snapshot) + 300:
                # Verificar se nao esta mais gerando
                generating = await self.is_generating()
                if not generating:
                    return True, full_text

            # Verificar se parou de gerar e sentinela nao apareceu
            if full_text and full_text == last_text:
                stable_count += 1
                if stable_count >= 3:  # 3 polls sem mudanca
                    generating = await self.is_generating()
                    if not generating:
                        # Claude parou — checar uma ultima vez
                        await asyncio.sleep(3)
                        full_text = await self.get_last_response()
                        count_now = full_text.count(sentinel)
                        if count_now > count_before + 1:
                            return True, full_text
                        # Se tem pelo menos o sentinela do comando + body cresceu
                        if count_now >= count_before + 1 and len(full_text) > len(snapshot) + 200:
                            return True, full_text
                        return False, f"Claude parou sem sentinela. Ultima resposta: {full_text[-500:]}"
            else:
                stable_count = 0

            last_text = full_text

        return False, f"Timeout de {timeout}s atingido"

    async def clear_conversation(self):
        """Limpa conversa atual clicando no botao 'Limpar conversa'."""
        result = await self.eval_js(
            'var btns = document.querySelectorAll("button");'
            'var cleared = false;'
            'for (var i = 0; i < btns.length; i++) {'
            '  var aria = btns[i].getAttribute("aria-label") || "";'
            '  if (aria.indexOf("Limpar") > -1 || aria.indexOf("Clear") > -1 || aria.indexOf("Nova") > -1) {'
            '    btns[i].click(); cleared = true; break;'
            '  }'
            '}'
            'cleared ? "cleared" : "no clear button"'
        )
        if result == "cleared":
            await asyncio.sleep(2)
            # Re-apply tabs override (new page context)
            await self._override_tabs_query()
        return result


# =============================================
# EXTRATOR DE JSON
# =============================================

def extract_json_from_text(text):
    """Extrai o primeiro objeto JSON valido de um texto."""
    # Tentar encontrar blocos ```json ... ```
    match = re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', text)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Tentar encontrar { ... } no texto
    depth = 0
    start = None
    for i, c in enumerate(text):
        if c == '{':
            if depth == 0:
                start = i
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    start = None

    return None


# =============================================
# MONITOR FLOW (orquestrador)
# =============================================

class MonitorFlow:
    """Orquestra as 4 etapas de monitoramento."""

    def __init__(self, cdp_port=DEFAULT_CDP_PORT, log_callback=None, db_log=None):
        self.cdp_port = cdp_port
        self._log_cb = log_callback
        self._db_log = db_log  # ShopeeMonitorLog instance
        self._running = False
        self._chat = None

    def _log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        logger.info(line)
        if self._log_cb:
            self._log_cb(line)
        if self._db_log:
            self._db_log.log_text = (self._db_log.log_text or '') + line + '\n'

    async def run(self):
        """Executa o monitoramento completo."""
        self._running = True

        # 1. Conectar ao Chrome CDP
        self._log("Conectando ao Chrome CDP...")
        ok, msg = ensure_chrome_cdp(self.cdp_port)
        if not ok:
            self._log(f"ERRO: {msg}")
            return {"status": "erro", "erro": msg}
        self._log(f"Chrome: {msg}")

        # 2. Encontrar sidebar da extensao
        self._log("Procurando sidebar da extensao Claude...")
        target = find_extension_sidebar(self.cdp_port)
        if not target:
            self._log("ERRO: Sidebar da extensao Claude nao encontrada.")
            return {"status": "erro", "erro": "Sidebar nao encontrada"}

        ws_url = target.get("webSocketDebuggerUrl")
        if not ws_url:
            self._log("ERRO: webSocketDebuggerUrl nao disponivel no target")
            return {"status": "erro", "erro": "ws_url indisponivel"}

        self._log(f"Sidebar encontrada: {target.get('title', 'Claude')}")

        # 3. Conectar ao chat
        self._chat = ClaudeChat(ws_url)
        try:
            await self._chat.connect()
            self._log("Conectado ao chat da extensao (tabs override aplicado)")
        except Exception as e:
            self._log(f"ERRO ao conectar websocket: {e}")
            return {"status": "erro", "erro": str(e)}

        resultado_final = {}

        try:
            # ===== ETAPA 1: Lista de lojas =====
            if self._db_log:
                self._db_log.etapa_atual = 1
            self._log("ETAPA 1: Coletando lista de lojas...")
            ok, resp = await self._executar_etapa(COMANDO_ETAPA_1, SENTINELAS[1])
            if not ok:
                self._log(f"ERRO Etapa 1: {resp}")
                return {"status": "erro", "erro": f"Etapa 1: {resp}"}

            data1 = extract_json_from_text(resp)
            if not data1 or "lojas" not in data1:
                self._log("ERRO: JSON da Etapa 1 nao contem 'lojas'")
                return {"status": "erro", "erro": "Resposta invalida Etapa 1"}

            lojas = data1["lojas"]
            self._log(f"Etapa 1 OK: {len(lojas)} lojas encontradas")
            if self._db_log:
                self._db_log.total_lojas = len(lojas)
            resultado_final["lojas"] = lojas

            if not self._running:
                return {"status": "cancelado"}

            # ===== ETAPA 2: Alertas =====
            if self._db_log:
                self._db_log.etapa_atual = 2
            self._log("ETAPA 2: Verificando alertas por loja...")
            cmd2 = COMANDO_ETAPA_2.replace("{lista_json}", json.dumps(lojas, ensure_ascii=False))
            ok, resp = await self._executar_etapa(cmd2, SENTINELAS[2])
            if not ok:
                self._log(f"ERRO Etapa 2: {resp}")
                return {"status": "erro", "erro": f"Etapa 2: {resp}"}

            data2 = extract_json_from_text(resp)
            if not data2 or "resultado" not in data2:
                self._log("ERRO: JSON da Etapa 2 invalido")
                return {"status": "erro", "erro": "Resposta invalida Etapa 2"}

            resultado2 = data2["resultado"]
            com_alerta = [l for l in resultado2 if l.get("tem_alerta")]
            self._log(f"Etapa 2 OK: {len(com_alerta)} lojas com alertas de {len(resultado2)} verificadas")
            if self._db_log:
                self._db_log.total_alertas = len(com_alerta)
            resultado_final["alertas"] = resultado2

            if not self._running:
                return {"status": "cancelado"}

            # ===== ETAPA 3: Reembolsos =====
            if self._db_log:
                self._db_log.etapa_atual = 3
            self._log("ETAPA 3: Verificando reembolsos e erros...")
            cmd3 = COMANDO_ETAPA_3.replace("{lista_json}", json.dumps(lojas, ensure_ascii=False))
            ok, resp = await self._executar_etapa(cmd3, SENTINELAS[3])
            if not ok:
                self._log(f"ERRO Etapa 3: {resp}")
                return {"status": "erro", "erro": f"Etapa 3: {resp}"}

            data3 = extract_json_from_text(resp)
            if data3 and "resultado" in data3:
                resultado_final["reembolsos"] = data3["resultado"]
                total_erros = sum(len(l.get("erros_vendedor", [])) for l in data3["resultado"])
                self._log(f"Etapa 3 OK: {total_erros} erros de vendedor encontrados")
            else:
                self._log("AVISO: JSON da Etapa 3 nao parseado, continuando...")
                resultado_final["reembolsos"] = []

            if not self._running:
                return {"status": "cancelado"}

            # ===== ETAPA 4: Etiquetas (so se houver alertas) =====
            if com_alerta:
                if self._db_log:
                    self._db_log.etapa_atual = 4
                self._log(f"ETAPA 4: Abrindo etiquetas para {len(com_alerta)} lojas com alertas...")
                cmd4 = COMANDO_ETAPA_4.replace("{alertas_json}", json.dumps(com_alerta, ensure_ascii=False))
                ok, resp = await self._executar_etapa(cmd4, SENTINELAS[4])
                if not ok:
                    self._log(f"ERRO Etapa 4: {resp}")
                    return {"status": "erro", "erro": f"Etapa 4: {resp}"}

                data4 = extract_json_from_text(resp)
                if data4:
                    total_etiq = data4.get("total_abas_etiqueta", 0)
                    resultado_final["etiquetas"] = data4
                    if self._db_log:
                        self._db_log.total_etiquetas = total_etiq
                    self._log(f"Etapa 4 OK: {total_etiq} etiquetas abertas")
                else:
                    self._log("AVISO: JSON da Etapa 4 nao parseado")
            else:
                self._log("Etapa 4: PULADA (sem alertas)")
                resultado_final["etiquetas"] = {"total_abas_etiqueta": 0}

            self._log("Monitoramento concluido com sucesso!")
            resultado_final["status"] = "concluido"
            return resultado_final

        except Exception as e:
            self._log(f"ERRO inesperado: {e}")
            return {"status": "erro", "erro": str(e)}
        finally:
            await self._chat.close()
            self._running = False

    async def _executar_etapa(self, comando, sentinela):
        """Envia comando e aguarda sentinela."""
        ok, msg = await self._chat.send_message(comando)
        if not ok:
            return False, f"Falha ao enviar: {msg}"

        return await self._chat.wait_for_sentinel(sentinela)

    def cancelar(self):
        """Cancela execucao em andamento."""
        self._running = False


# =============================================
# RUNNER (thread wrapper)
# =============================================

def run_monitor_async(cdp_port, log_callback, db_log=None):
    """Roda o monitoramento em thread separada com event loop proprio.

    Returns:
        (MonitorFlow, threading.Thread)
    """
    flow = MonitorFlow(cdp_port=cdp_port, log_callback=log_callback, db_log=db_log)

    def _thread_target():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(flow.run())
            if log_callback:
                log_callback(json.dumps(result, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Erro no monitor thread: {e}")
            if log_callback:
                log_callback(f"[ERRO] {e}")
        finally:
            loop.close()

    t = threading.Thread(target=_thread_target, daemon=True)
    t.start()
    return flow, t
