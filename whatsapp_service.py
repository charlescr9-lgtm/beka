# -*- coding: utf-8 -*-
"""
Servico de envio de mensagens via WhatsApp.
Suporta dois providers:
  - 'baileys' : Baileys REST API local (gratuito, self-hosted)
  - 'uazapi'  : UAZAPI v2 (pago, cloud)

O provider ativo e configurado via variavel de ambiente WHATSAPP_PROVIDER
ou passando provider= no construtor.
"""

import os
import time
import random
import base64
import logging
import mimetypes
import requests
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

# =============================================
# CONFIGURACAO
# =============================================
# Provider: 'baileys' ou 'uazapi'
DEFAULT_PROVIDER = os.environ.get("WHATSAPP_PROVIDER", "uazapi")

# Baileys
BAILEYS_API_URL = os.environ.get("WHATSAPP_API_URL", "http://localhost:3005")
BAILEYS_SESSION = os.environ.get("WHATSAPP_SESSION", "beka-mkt")

# UAZAPI
UAZAPI_BASE_URL = os.environ.get("UAZAPI_BASE_URL", "https://bekamkt.uazapi.com")
UAZAPI_TOKEN = os.environ.get("UAZAPI_TOKEN", "39ebd492-fb20-458c-8bbf-8b0d3a8f7b62")


class WhatsAppService:
    """
    Wrapper unificado para envio WhatsApp.
    Detecta o provider configurado e delega para a implementacao correta.
    """

    def __init__(self, provider: str = None, **kwargs):
        self.provider = (provider or DEFAULT_PROVIDER).lower().strip()

        if self.provider == "uazapi":
            self._impl = _UazapiProvider(
                base_url=kwargs.get("base_url") or UAZAPI_BASE_URL,
                token=kwargs.get("token") or UAZAPI_TOKEN,
            )
        else:
            self._impl = _BaileysProvider(
                api_url=kwargs.get("api_url") or BAILEYS_API_URL,
                session_name=kwargs.get("session_name") or BAILEYS_SESSION,
            )

    # --- Interface publica (igual para qualquer provider) ---

    def verificar_conexao(self) -> dict:
        return self._impl.verificar_conexao()

    def iniciar_sessao(self) -> dict:
        return self._impl.iniciar_sessao()

    def get_qr_code(self) -> dict:
        return self._impl.get_qr_code()

    def enviar_mensagem(self, telefone: str, texto: str) -> dict:
        return self._impl.enviar_mensagem(telefone, texto)

    def enviar_pdf(self, telefone: str, pdf_path: str, caption: str = "") -> dict:
        return self.enviar_arquivo(telefone, pdf_path, caption)

    def enviar_arquivo(self, telefone: str, file_path: str, caption: str = "") -> dict:
        return self._impl.enviar_arquivo(telefone, file_path, caption)

    def enviar_imagem(self, telefone: str, image_path: str, caption: str = "") -> dict:
        return self._impl.enviar_imagem(telefone, image_path, caption)

    def enviar_lote(
        self,
        entregas: List[dict],
        delay_min: int = 8,
        delay_max: int = 15,
        progress_cb=None,
    ) -> List[dict]:
        resultados = []
        total = len(entregas)

        for i, entrega in enumerate(entregas):
            telefone = entrega.get("telefone", "")
            file_path = entrega.get("file_path", entrega.get("pdf_path", ""))
            loja = entrega.get("loja", "")
            caption = entrega.get("caption", f"Etiquetas {loja}")

            logger.info(f"[WhatsApp] Enviando {i+1}/{total}: {loja} -> {telefone}")

            resultado = self.enviar_arquivo(telefone, file_path, caption)
            resultado["telefone"] = telefone
            resultado["loja"] = loja
            resultados.append(resultado)

            if callable(progress_cb):
                try:
                    progress_cb(i + 1, total, resultado, entrega)
                except Exception as cb_err:
                    logger.debug(f"[WhatsApp] progress_cb falhou: {cb_err}")

            if i < total - 1:
                delay = random.uniform(delay_min, delay_max)
                logger.debug(f"[WhatsApp] Aguardando {delay:.1f}s...")
                time.sleep(delay)

        enviados = sum(1 for r in resultados if r.get("success"))
        erros = total - enviados
        logger.info(f"[WhatsApp] Lote concluido: {enviados}/{total} enviados, {erros} erros")
        return resultados

    def verificar_numero(self, telefone: str) -> dict:
        return self._impl.verificar_numero(telefone)

    def desconectar(self) -> dict:
        return self._impl.desconectar()


# =============================================================================
# PROVIDER: UAZAPI v2
# =============================================================================

class _UazapiProvider:
    """Provider UAZAPI v2 — API paga, cloud-hosted."""

    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._timeout = 60

        if not self.base_url or not self.token:
            logger.warning(
                "[UAZAPI] base_url ou token nao configurados. "
                "Defina UAZAPI_BASE_URL e UAZAPI_TOKEN."
            )

    def _headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "token": self.token,
        }

    @staticmethod
    def _formatar_numero(telefone: str) -> str:
        """Formata para digitos puros (ex: 5511999999999)."""
        numero = "".join(c for c in telefone if c.isdigit())
        if len(numero) in (10, 11):
            numero = "55" + numero
        return numero

    # --- Conexao ---

    def verificar_conexao(self) -> dict:
        try:
            resp = requests.get(
                f"{self.base_url}/instance/status",
                headers=self._headers(),
                timeout=self._timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                inst = data.get("instance", data)
                status = inst.get("status", "")
                connected = status == "connected"
                return {
                    "connected": connected,
                    "phone": inst.get("phone", inst.get("owner", "")),
                    "name": inst.get("profileName", ""),
                    "status_raw": data,
                }
            return {"connected": False, "error": f"HTTP {resp.status_code}"}
        except requests.ConnectionError:
            return {"connected": False, "error": "UAZAPI nao acessivel"}
        except Exception as e:
            return {"connected": False, "error": str(e)}

    def iniciar_sessao(self) -> dict:
        return {"info": "UAZAPI gerencia sessoes automaticamente via painel"}

    def get_qr_code(self) -> dict:
        try:
            resp = requests.get(
                f"{self.base_url}/qrcode",
                headers=self._headers(),
                timeout=self._timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                return {"qr": data.get("qr", data.get("qrcode", ""))}
            return {"error": f"HTTP {resp.status_code}: {resp.text}"}
        except Exception as e:
            return {"error": str(e)}

    # --- Envio ---

    def enviar_mensagem(self, telefone: str, texto: str) -> dict:
        try:
            numero = self._formatar_numero(telefone)
            resp = requests.post(
                f"{self.base_url}/send/text",
                headers=self._headers(),
                json={
                    "number": numero,
                    "text": texto,
                },
                timeout=self._timeout,
            )
            if resp.status_code in (200, 201):
                data = resp.json()
                msg_id = data.get("messageid", data.get("messageId", data.get("key", {}).get("id", "")))
                logger.info(f"[UAZAPI] Texto enviado para {telefone}")
                return {"success": True, "messageId": msg_id}
            logger.warning(f"[UAZAPI] Falha texto para {telefone}: HTTP {resp.status_code}")
            return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}
        except Exception as e:
            logger.error(f"[UAZAPI] Erro texto para {telefone}: {e}")
            return {"success": False, "error": str(e)}

    def enviar_arquivo(self, telefone: str, file_path: str, caption: str = "") -> dict:
        if not os.path.exists(file_path):
            return {"success": False, "error": f"Arquivo nao encontrado: {file_path}"}
        try:
            numero = self._formatar_numero(telefone)
            filename = os.path.basename(file_path)

            with open(file_path, "rb") as f:
                file_b64 = base64.b64encode(f.read()).decode()

            mime, _ = mimetypes.guess_type(filename)
            mime = mime or "application/octet-stream"
            data_uri = f"data:{mime};base64,{file_b64}"

            resp = requests.post(
                f"{self.base_url}/send/media",
                headers=self._headers(),
                json={
                    "number": numero,
                    "type": "document",
                    "file": data_uri,
                    "docName": filename,
                    "text": caption or "",
                },
                timeout=120,
            )
            if resp.status_code in (200, 201):
                data = resp.json()
                msg_id = data.get("messageid", data.get("messageId", data.get("key", {}).get("id", "")))
                logger.info(f"[UAZAPI] Arquivo enviado para {telefone}: {filename}")
                return {"success": True, "messageId": msg_id}

            logger.warning(f"[UAZAPI] Falha arquivo para {telefone}: HTTP {resp.status_code}")
            return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}
        except Exception as e:
            logger.error(f"[UAZAPI] Erro arquivo para {telefone}: {e}")
            return {"success": False, "error": str(e)}

    def enviar_imagem(self, telefone: str, image_path: str, caption: str = "") -> dict:
        if not os.path.exists(image_path):
            return {"success": False, "error": f"Arquivo nao encontrado: {image_path}"}
        try:
            numero = self._formatar_numero(telefone)
            filename = os.path.basename(image_path)

            with open(image_path, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode()

            mime, _ = mimetypes.guess_type(filename)
            mime = mime or "image/jpeg"
            data_uri = f"data:{mime};base64,{img_b64}"

            resp = requests.post(
                f"{self.base_url}/send/media",
                headers=self._headers(),
                json={
                    "number": numero,
                    "type": "image",
                    "file": data_uri,
                    "text": caption or "",
                },
                timeout=120,
            )
            if resp.status_code in (200, 201):
                data = resp.json()
                msg_id = data.get("messageid", data.get("messageId", data.get("key", {}).get("id", "")))
                logger.info(f"[UAZAPI] Imagem enviada para {telefone}: {filename}")
                return {"success": True, "messageId": msg_id}

            logger.warning(f"[UAZAPI] Falha imagem para {telefone}: HTTP {resp.status_code}")
            return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}
        except Exception as e:
            logger.error(f"[UAZAPI] Erro imagem para {telefone}: {e}")
            return {"success": False, "error": str(e)}

    # --- Utilidades ---

    def verificar_numero(self, telefone: str) -> dict:
        try:
            numero = self._formatar_numero(telefone)
            resp = requests.get(
                f"{self.base_url}/contact/check/{numero}",
                headers=self._headers(),
                timeout=self._timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                return {"exists": data.get("exists", False), "jid": f"{numero}@s.whatsapp.net"}
            return {"exists": False, "error": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"exists": False, "error": str(e)}

    def desconectar(self) -> dict:
        try:
            resp = requests.post(
                f"{self.base_url}/session/disconnect",
                headers=self._headers(),
                timeout=self._timeout,
            )
            return {"success": resp.status_code in (200, 204)}
        except Exception as e:
            return {"success": False, "error": str(e)}


# =============================================================================
# PROVIDER: BAILEYS (original)
# =============================================================================

class _BaileysProvider:
    """Provider Baileys — API gratuita, self-hosted."""

    def __init__(self, api_url: str, session_name: str):
        self.api_url = api_url.rstrip("/")
        self.session_name = session_name
        self._timeout = 30

    def _formatar_telefone(self, telefone: str) -> str:
        numero = "".join(c for c in telefone if c.isdigit())
        if len(numero) == 11:
            numero = "55" + numero
        elif len(numero) == 10:
            numero = "55" + numero
        return f"{numero}@s.whatsapp.net"

    def _resolver_jid(self, telefone: str) -> str:
        numero = "".join(c for c in telefone if c.isdigit())
        if len(numero) == 11:
            numero = "55" + numero
        elif len(numero) == 10:
            numero = "55" + numero
        try:
            resp = requests.get(
                f"{self.api_url}/api/sessions/{self.session_name}/contacts/{numero}/check",
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("exists") and data.get("jid"):
                    logger.info(f"[Baileys] JID resolvido: {numero} -> {data['jid']}")
                    return data["jid"]
        except Exception as e:
            logger.warning(f"[Baileys] Falha ao resolver JID para {numero}: {e}")
        return f"{numero}@s.whatsapp.net"

    # --- Conexao ---

    def verificar_conexao(self) -> dict:
        try:
            resp = requests.get(
                f"{self.api_url}/api/sessions/{self.session_name}/status",
                timeout=self._timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                return {
                    "connected": data.get("status") == "CONNECTED" or data.get("connected", False),
                    "phone": data.get("phone", data.get("me", {}).get("id", "").split("@")[0] if isinstance(data.get("me"), dict) else ""),
                    "name": data.get("name", data.get("me", {}).get("name", "") if isinstance(data.get("me"), dict) else ""),
                    "status_raw": data,
                }
            return {"connected": False, "error": f"HTTP {resp.status_code}"}
        except requests.ConnectionError:
            return {"connected": False, "error": "Baileys API nao esta rodando"}
        except Exception as e:
            return {"connected": False, "error": str(e)}

    def iniciar_sessao(self) -> dict:
        try:
            resp = requests.post(
                f"{self.api_url}/api/sessions",
                json={"sessionName": self.session_name},
                timeout=self._timeout,
            )
            return resp.json() if resp.status_code in (200, 201) else {"error": resp.text}
        except Exception as e:
            return {"error": str(e)}

    def get_qr_code(self) -> dict:
        try:
            resp = requests.get(
                f"{self.api_url}/api/sessions/{self.session_name}/qr",
                timeout=self._timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                return {"qr": data.get("qr", data.get("qrCode", ""))}
            return {"error": f"HTTP {resp.status_code}: {resp.text}"}
        except requests.ConnectionError:
            return {"error": "Baileys API nao esta rodando em " + self.api_url}
        except Exception as e:
            return {"error": str(e)}

    # --- Envio ---

    def enviar_mensagem(self, telefone: str, texto: str) -> dict:
        try:
            jid = self._resolver_jid(telefone)
            resp = requests.post(
                f"{self.api_url}/api/sessions/{self.session_name}/messages/send",
                json={"jid": jid, "type": "text", "message": texto},
                timeout=self._timeout,
            )
            if resp.status_code in (200, 201):
                data = resp.json()
                return {"success": True, "messageId": data.get("messageId", data.get("key", {}).get("id", ""))}
            return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}
        except Exception as e:
            logger.error(f"[Baileys] Erro mensagem para {telefone}: {e}")
            return {"success": False, "error": str(e)}

    def enviar_arquivo(self, telefone: str, file_path: str, caption: str = "") -> dict:
        if not os.path.exists(file_path):
            return {"success": False, "error": f"Arquivo nao encontrado: {file_path}"}
        try:
            jid = self._resolver_jid(telefone)
            filename = os.path.basename(file_path)
            mime, _ = mimetypes.guess_type(filename)
            mime = mime or "application/octet-stream"
            with open(file_path, "rb") as f:
                file_b64 = base64.b64encode(f.read()).decode()
            resp = requests.post(
                f"{self.api_url}/api/sessions/{self.session_name}/messages/send",
                json={
                    "jid": jid,
                    "type": "document",
                    "message": {
                        "document": f"data:{mime};base64,{file_b64}",
                        "mimetype": mime,
                        "fileName": filename,
                        "caption": caption or f"Arquivo - {filename}",
                    },
                },
                timeout=60,
            )
            if resp.status_code in (200, 201):
                data = resp.json()
                logger.info(f"[Baileys] Arquivo enviado para {telefone}: {filename}")
                return {"success": True, "messageId": data.get("messageId", data.get("key", {}).get("id", ""))}
            logger.warning(f"[Baileys] Falha arquivo para {telefone}: HTTP {resp.status_code}")
            return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}
        except Exception as e:
            logger.error(f"[Baileys] Erro arquivo para {telefone}: {e}")
            return {"success": False, "error": str(e)}

    def enviar_imagem(self, telefone: str, image_path: str, caption: str = "") -> dict:
        if not os.path.exists(image_path):
            return {"success": False, "error": f"Arquivo nao encontrado: {image_path}"}
        try:
            jid = self._resolver_jid(telefone)
            with open(image_path, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode()
            resp = requests.post(
                f"{self.api_url}/api/sessions/{self.session_name}/messages/send",
                json={
                    "jid": jid,
                    "type": "image",
                    "message": {"image": img_b64, "caption": caption or ""},
                },
                timeout=60,
            )
            if resp.status_code in (200, 201):
                data = resp.json()
                logger.info(f"[Baileys] Imagem enviada para {telefone}: {os.path.basename(image_path)}")
                return {"success": True, "messageId": data.get("messageId", data.get("key", {}).get("id", ""))}
            logger.warning(f"[Baileys] Falha imagem para {telefone}: HTTP {resp.status_code}")
            return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}
        except Exception as e:
            logger.error(f"[Baileys] Erro imagem para {telefone}: {e}")
            return {"success": False, "error": str(e)}

    # --- Utilidades ---

    def verificar_numero(self, telefone: str) -> dict:
        try:
            jid = self._formatar_telefone(telefone)
            numero = jid.split("@")[0]
            resp = requests.get(
                f"{self.api_url}/api/sessions/{self.session_name}/contacts/{numero}/check",
                timeout=self._timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                return {"exists": data.get("exists", False), "jid": jid}
            return {"exists": False, "error": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"exists": False, "error": str(e)}

    def desconectar(self) -> dict:
        try:
            resp = requests.delete(
                f"{self.api_url}/api/sessions/{self.session_name}",
                timeout=self._timeout,
            )
            return {"success": resp.status_code in (200, 204)}
        except Exception as e:
            return {"success": False, "error": str(e)}
