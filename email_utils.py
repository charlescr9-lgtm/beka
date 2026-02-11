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


def _get_smtp_config():
    """Retorna config SMTP ou None se nao configurado."""
    host = os.environ.get('SMTP_HOST', '').strip()
    if not host:
        return None
    return {
        'host': host,
        'port': int(os.environ.get('SMTP_PORT', '587')),
        'user': os.environ.get('SMTP_USER', ''),
        'password': os.environ.get('SMTP_PASS', ''),
        'from_addr': os.environ.get('SMTP_FROM', os.environ.get('SMTP_USER', '')),
    }


def smtp_configurado():
    """Retorna True se SMTP esta configurado."""
    return _get_smtp_config() is not None


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
