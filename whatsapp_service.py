# -*- coding: utf-8 -*-
"""
Servico de envio de mensagens via WhatsApp - Wrapper para Baileys REST API.
Envia PDFs de etiquetas para contatos cadastrados por loja.
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

# Configuracao padrao
DEFAULT_API_URL = os.environ.get("WHATSAPP_API_URL", "http://localhost:3005")
DEFAULT_SESSION = os.environ.get("WHATSAPP_SESSION", "beka-mkt")


class WhatsAppService:
    """
    Wrapper Python para a Baileys REST API.

    Arquitetura:
        [Flask App] --HTTP--> [Baileys REST API :3005] --WebSocket--> [WhatsApp]

    A Baileys REST API deve estar rodando como servico Node.js separado.
    Escanear QR code uma vez via dashboard web da API.
    """

    def __init__(self, api_url: str = None, session_name: str = None):
        self.api_url = (api_url or DEFAULT_API_URL).rstrip("/")
        self.session_name = session_name or DEFAULT_SESSION
        self._timeout = 30  # segundos

    # =============================================
    # CONEXAO E STATUS
    # =============================================

    def verificar_conexao(self) -> dict:
        """
        Verifica se o WhatsApp esta conectado e autenticado.

        Retorna:
            {"connected": True/False, "phone": "5511...", "name": "...", "error": "..."}
        """
        try:
            resp = requests.get(
                f"{self.api_url}/api/sessions/{self.session_name}/status",
                timeout=self._timeout
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
        """Inicia uma nova sessao no Baileys (gera QR code)."""
        try:
            resp = requests.post(
                f"{self.api_url}/api/sessions",
                json={"sessionName": self.session_name},
                timeout=self._timeout
            )
            return resp.json() if resp.status_code in (200, 201) else {"error": resp.text}
        except Exception as e:
            return {"error": str(e)}

    def get_qr_code(self) -> dict:
        """
        Retorna QR code para escanear com WhatsApp.

        Retorna:
            {"qr": "data:image/png;base64,...", "error": "..."}
        """
        try:
            resp = requests.get(
                f"{self.api_url}/api/sessions/{self.session_name}/qr",
                timeout=self._timeout
            )
            if resp.status_code == 200:
                data = resp.json()
                return {"qr": data.get("qr", data.get("qrCode", ""))}
            return {"error": f"HTTP {resp.status_code}: {resp.text}"}
        except requests.ConnectionError:
            return {"error": "Baileys API nao esta rodando em " + self.api_url}
        except Exception as e:
            return {"error": str(e)}

    # =============================================
    # ENVIO DE MENSAGENS
    # =============================================

    def _formatar_telefone(self, telefone: str) -> str:
        """
        Formata numero para padrao WhatsApp: 5511999999999@s.whatsapp.net
        Aceita: +55 11 99999-9999, 5511999999999, 11999999999
        """
        # Remover tudo que nao eh digito
        numero = "".join(c for c in telefone if c.isdigit())

        # Adicionar codigo do Brasil se nao tiver
        if len(numero) == 11:  # DDD + 9 digitos
            numero = "55" + numero
        elif len(numero) == 10:  # DDD + 8 digitos (fixo)
            numero = "55" + numero

        return f"{numero}@s.whatsapp.net"

    def enviar_mensagem(self, telefone: str, texto: str) -> dict:
        """
        Envia mensagem de texto simples.

        Args:
            telefone: Numero no formato 5511999999999 ou similar
            texto: Texto da mensagem

        Retorna:
            {"success": True, "messageId": "..."} ou {"success": False, "error": "..."}
        """
        try:
            jid = self._formatar_telefone(telefone)
            resp = requests.post(
                f"{self.api_url}/api/sessions/{self.session_name}/messages/send",
                json={
                    "jid": jid,
                    "type": "text",
                    "message": texto,
                },
                timeout=self._timeout
            )
            if resp.status_code in (200, 201):
                data = resp.json()
                return {"success": True, "messageId": data.get("messageId", data.get("key", {}).get("id", ""))}
            return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}
        except Exception as e:
            logger.error(f"[WhatsApp] Erro ao enviar mensagem para {telefone}: {e}")
            return {"success": False, "error": str(e)}

    def enviar_pdf(self, telefone: str, pdf_path: str, caption: str = "") -> dict:
        """
        Envia arquivo PDF via WhatsApp.

        Args:
            telefone: Numero destino
            pdf_path: Caminho absoluto do PDF
            caption: Legenda opcional da mensagem

        Retorna:
            {"success": True, "messageId": "..."} ou {"success": False, "error": "..."}
        """
        return self.enviar_arquivo(telefone, pdf_path, caption)

    def enviar_arquivo(self, telefone: str, file_path: str, caption: str = "") -> dict:
        """
        Envia arquivo generico via WhatsApp como documento.

        Suporta PDF, XLS/XLSX e imagens (enviadas como documento para preservar arquivo).
        """
        if not os.path.exists(file_path):
            return {"success": False, "error": f"Arquivo nao encontrado: {file_path}"}

        try:
            jid = self._formatar_telefone(telefone)
            filename = os.path.basename(file_path)
            mime, _ = mimetypes.guess_type(filename)
            mime = mime or "application/octet-stream"

            # Ler e codificar arquivo em base64
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
                timeout=60  # Arquivos grandes podem demorar mais
            )

            if resp.status_code in (200, 201):
                data = resp.json()
                logger.info(f"[WhatsApp] Arquivo enviado para {telefone}: {filename}")
                return {"success": True, "messageId": data.get("messageId", data.get("key", {}).get("id", ""))}

            logger.warning(f"[WhatsApp] Falha ao enviar arquivo para {telefone}: HTTP {resp.status_code}")
            return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}

        except Exception as e:
            logger.error(f"[WhatsApp] Erro ao enviar arquivo para {telefone}: {e}")
            return {"success": False, "error": str(e)}

    def enviar_lote(
        self,
        entregas: List[dict],
        delay_min: int = 8,
        delay_max: int = 15,
        progress_cb=None,
    ) -> List[dict]:
        """
        Envia arquivos em lote com delay aleatorio entre mensagens (anti-ban).

        Args:
            entregas: Lista de dicts com:
                - telefone: str (numero destino)
                - file_path|pdf_path: str (caminho do arquivo)
                - loja: str (nome da loja, para caption)
                - caption: str (opcional, legenda customizada)
            delay_min: Delay minimo entre mensagens (segundos)
            delay_max: Delay maximo entre mensagens (segundos)

        Retorna:
            Lista de resultados por entrega, cada um com:
                {"telefone", "loja", "success", "messageId"/"error"}
        """
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

            # Delay anti-ban (nao aplicar apos a ultima mensagem)
            if i < total - 1:
                delay = random.uniform(delay_min, delay_max)
                logger.debug(f"[WhatsApp] Aguardando {delay:.1f}s antes da proxima mensagem...")
                time.sleep(delay)

        # Resumo
        enviados = sum(1 for r in resultados if r.get("success"))
        erros = total - enviados
        logger.info(f"[WhatsApp] Lote concluido: {enviados}/{total} enviados, {erros} erros")

        return resultados

    # =============================================
    # UTILIDADES
    # =============================================

    def verificar_numero(self, telefone: str) -> dict:
        """Verifica se um numero esta registrado no WhatsApp."""
        try:
            jid = self._formatar_telefone(telefone)
            numero = jid.split("@")[0]
            resp = requests.get(
                f"{self.api_url}/api/sessions/{self.session_name}/contacts/{numero}/check",
                timeout=self._timeout
            )
            if resp.status_code == 200:
                data = resp.json()
                return {"exists": data.get("exists", False), "jid": jid}
            return {"exists": False, "error": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"exists": False, "error": str(e)}

    def desconectar(self) -> dict:
        """Desconecta a sessao WhatsApp."""
        try:
            resp = requests.delete(
                f"{self.api_url}/api/sessions/{self.session_name}",
                timeout=self._timeout
            )
            return {"success": resp.status_code in (200, 204)}
        except Exception as e:
            return {"success": False, "error": str(e)}
