# -*- coding: utf-8 -*-
"""
Utilitarios de envio de email para verificacao de conta e recuperacao de senha.
Configurado via variaveis de ambiente:
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM
Se nao configurado, os emails nao sao enviados (modo silencioso).
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


def _normalize_smtp_config(cfg):
    """Normaliza config SMTP e valida campos minimos obrigatorios."""
    if not cfg:
        return None
    host = str(cfg.get('host', '') or '').strip()
    user = str(cfg.get('user', '') or '').strip()
    password = str(cfg.get('password', '') or '').strip()
    if not host or not user or not password:
        return None
    try:
        port = int(cfg.get('port', 587) or 587)
    except Exception:
        port = 587
    from_addr = str(cfg.get('from_addr', '') or '').strip() or user
    return {
        'host': host,
        'port': port,
        'user': user,
        'password': password,
        'from_addr': from_addr,
    }


def _get_smtp_config():
    """Retorna config SMTP ou None se nao configurado."""
    return _normalize_smtp_config({
        'host': os.environ.get('SMTP_HOST', ''),
        'port': os.environ.get('SMTP_PORT', '587'),
        'user': os.environ.get('SMTP_USER', ''),
        'password': os.environ.get('SMTP_PASS', ''),
        'from_addr': os.environ.get('SMTP_FROM', os.environ.get('SMTP_USER', '')),
    })


def get_smtp_config():
    """Retorna config SMTP global atual (ou None)."""
    return _get_smtp_config()


def smtp_configurado(cfg_override=None):
    """Retorna True se SMTP esta configurado."""
    if cfg_override is None:
        return _get_smtp_config() is not None
    return _normalize_smtp_config(cfg_override) is not None


def enviar_codigo_verificacao(email_destino, codigo):
    """Envia email com codigo de verificacao. Retorna True se enviou, False se SMTP nao configurado."""
    cfg = _get_smtp_config()
    if not cfg:
        return False

    msg = MIMEMultipart()
    msg['From'] = cfg['from_addr']
    msg['To'] = email_destino
    msg['Subject'] = f'Beka MultiPlace - Codigo de verificacao: {codigo}'

    corpo = f"""
    <html>
    <body style="font-family: Arial, sans-serif; background: #0a0e1a; color: #e0e0e0; padding: 30px;">
        <div style="max-width: 400px; margin: 0 auto; background: #151929; border-radius: 16px; padding: 32px; border: 1px solid #1e2540;">
            <h2 style="color: #ff6b35; text-align: center;">Beka MultiPlace</h2>
            <p style="text-align: center; font-size: 16px;">Seu codigo de verificacao:</p>
            <div style="text-align: center; font-size: 36px; font-weight: 700; color: #ff6b35; letter-spacing: 8px; margin: 20px 0;">{codigo}</div>
            <p style="text-align: center; font-size: 13px; color: #888;">Este codigo expira em 10 minutos.</p>
        </div>
    </body>
    </html>
    """

    msg.attach(MIMEText(corpo, 'html'))

    try:
        server = smtplib.SMTP(cfg['host'], cfg['port'])
        server.starttls()
        server.login(cfg['user'], cfg['password'])
        server.sendmail(cfg['from_addr'], email_destino, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"Erro ao enviar email: {e}")
        return False


def enviar_codigo_reset_senha(email_destino, codigo):
    """Envia email com codigo para recuperacao de senha."""
    cfg = _get_smtp_config()
    if not cfg:
        return False

    msg = MIMEMultipart()
    msg['From'] = cfg['from_addr']
    msg['To'] = email_destino
    msg['Subject'] = f'Beka MultiPlace - Recuperacao de senha'

    corpo = f"""
    <html>
    <body style="font-family: Arial, sans-serif; background: #0a0e1a; color: #e0e0e0; padding: 30px;">
        <div style="max-width: 400px; margin: 0 auto; background: #151929; border-radius: 16px; padding: 32px; border: 1px solid #1e2540;">
            <h2 style="color: #ff6b35; text-align: center;">Beka MultiPlace</h2>
            <p style="text-align: center; font-size: 16px;">Recuperacao de senha</p>
            <p style="text-align: center; font-size: 14px; color: #aaa;">Use o codigo abaixo para redefinir sua senha:</p>
            <div style="text-align: center; font-size: 36px; font-weight: 700; color: #ff6b35; letter-spacing: 8px; margin: 20px 0;">{codigo}</div>
            <p style="text-align: center; font-size: 13px; color: #888;">Este codigo expira em 15 minutos.</p>
            <p style="text-align: center; font-size: 12px; color: #666; margin-top: 16px;">Se voce nao solicitou a recuperacao de senha, ignore este email.</p>
        </div>
    </body>
    </html>
    """

    msg.attach(MIMEText(corpo, 'html'))

    try:
        server = smtplib.SMTP(cfg['host'], cfg['port'])
        server.starttls()
        server.login(cfg['user'], cfg['password'])
        server.sendmail(cfg['from_addr'], email_destino, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"Erro ao enviar email de reset: {e}")
        return False


def enviar_email_com_anexo(
    email_destino,
    assunto,
    loja_nome,
    timestamp,
    anexo_path,
    nome_anexo=None,
    from_addr_override=None,
    from_name_override=None,
    smtp_override=None,
):
    """
    Envia email com PDF de etiquetas em anexo.
    Retorna dict {"success": True/False, "error": str ou None}.
    """
    cfg = _normalize_smtp_config(smtp_override) if smtp_override else _get_smtp_config()
    if not cfg:
        return {"success": False, "error": "SMTP nao configurado"}

    if not os.path.exists(anexo_path):
        return {"success": False, "error": f"Arquivo nao encontrado: {anexo_path}"}

    if not nome_anexo:
        nome_anexo = os.path.basename(anexo_path)

    msg = MIMEMultipart()
    from_addr = (from_addr_override or '').strip() or cfg['from_addr']
    from_name = (from_name_override or '').strip()
    msg['From'] = f"{from_name} <{from_addr}>" if from_name else from_addr
    msg['To'] = email_destino
    msg['Subject'] = assunto

    corpo = f"""
    <html>
    <body style="font-family: Arial, sans-serif; background: #0a0e1a; color: #e0e0e0; padding: 30px;">
        <div style="max-width: 500px; margin: 0 auto; background: #151929; border-radius: 16px; padding: 32px; border: 1px solid #1e2540;">
            <h2 style="color: #ff6b35; text-align: center;">Beka MultiPlace</h2>
            <p style="text-align: center; font-size: 16px; color: #e0e0e0;">Etiquetas prontas para impressao</p>
            <div style="background: #1a2035; border-radius: 8px; padding: 16px; margin: 16px 0;">
                <p style="margin: 4px 0; font-size: 14px;"><strong style="color: #ff6b35;">Loja:</strong> {loja_nome}</p>
                <p style="margin: 4px 0; font-size: 14px;"><strong style="color: #ff6b35;">Gerado em:</strong> {timestamp}</p>
                <p style="margin: 4px 0; font-size: 14px;"><strong style="color: #ff6b35;">Anexo:</strong> {nome_anexo}</p>
            </div>
            <p style="text-align: center; font-size: 13px; color: #888;">Este email foi enviado automaticamente pelo Beka MultiPlace.</p>
        </div>
    </body>
    </html>
    """

    msg.attach(MIMEText(corpo, 'html'))

    # Anexar PDF
    try:
        with open(anexo_path, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{nome_anexo}"')
        msg.attach(part)
    except Exception as e:
        return {"success": False, "error": f"Erro ao ler anexo: {e}"}

    try:
        server = smtplib.SMTP(cfg['host'], cfg['port'])
        server.starttls()
        server.login(cfg['user'], cfg['password'])
        server.sendmail(from_addr, email_destino, msg.as_string())
        server.quit()
        return {"success": True, "error": None}
    except Exception as e:
        print(f"Erro ao enviar email com anexo: {e}")
        return {"success": False, "error": str(e)}


def enviar_email_com_anexos(
    email_destino,
    assunto,
    loja_nome,
    timestamp,
    anexos_paths,
    from_addr_override=None,
    from_name_override=None,
    smtp_override=None,
):
    """
    Envia email com multiplos anexos (PDF/IMG/XLSX).
    Retorna dict {"success": True/False, "error": str ou None}.
    """
    cfg = _normalize_smtp_config(smtp_override) if smtp_override else _get_smtp_config()
    if not cfg:
        return {"success": False, "error": "SMTP nao configurado"}

    paths = [str(p).strip() for p in (anexos_paths or []) if str(p).strip()]
    if not paths:
        return {"success": False, "error": "Nenhum anexo informado"}

    faltando = [p for p in paths if not os.path.exists(p)]
    if faltando:
        return {"success": False, "error": f"Arquivo(s) nao encontrado(s): {faltando[0]}"}

    msg = MIMEMultipart()
    from_addr = (from_addr_override or '').strip() or cfg['from_addr']
    from_name = (from_name_override or '').strip()
    msg['From'] = f"{from_name} <{from_addr}>" if from_name else from_addr
    msg['To'] = email_destino
    msg['Subject'] = assunto

    corpo = f"""
    <html>
    <body style="font-family: Arial, sans-serif; background: #0a0e1a; color: #e0e0e0; padding: 30px;">
        <div style="max-width: 560px; margin: 0 auto; background: #151929; border-radius: 16px; padding: 32px; border: 1px solid #1e2540;">
            <h2 style="color: #ff6b35; text-align: center;">Beka MultiPlace</h2>
            <p style="text-align: center; font-size: 16px; color: #e0e0e0;">Arquivos prontos para envio</p>
            <div style="background: #1a2035; border-radius: 8px; padding: 16px; margin: 16px 0;">
                <p style="margin: 4px 0; font-size: 14px;"><strong style="color: #ff6b35;">Loja:</strong> {loja_nome}</p>
                <p style="margin: 4px 0; font-size: 14px;"><strong style="color: #ff6b35;">Gerado em:</strong> {timestamp}</p>
                <p style="margin: 4px 0; font-size: 14px;"><strong style="color: #ff6b35;">Arquivos:</strong> {len(paths)}</p>
            </div>
            <p style="text-align: center; font-size: 13px; color: #888;">Este email foi enviado automaticamente pelo Beka MultiPlace.</p>
        </div>
    </body>
    </html>
    """
    msg.attach(MIMEText(corpo, 'html'))

    try:
        for anexo_path in paths:
            nome_anexo = os.path.basename(anexo_path)
            with open(anexo_path, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{nome_anexo}"')
            msg.attach(part)
    except Exception as e:
        return {"success": False, "error": f"Erro ao ler anexos: {e}"}

    try:
        server = smtplib.SMTP(cfg['host'], cfg['port'])
        server.starttls()
        server.login(cfg['user'], cfg['password'])
        server.sendmail(from_addr, email_destino, msg.as_string())
        server.quit()
        return {"success": True, "error": None}
    except Exception as e:
        print(f"Erro ao enviar email com anexos: {e}")
        return {"success": False, "error": str(e)}
