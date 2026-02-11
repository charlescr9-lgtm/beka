# -*- coding: utf-8 -*-
"""
Rotas de autenticacao - registro, login, perfil
Limite de IPs por plano para evitar compartilhamento de senha
"""

import os
import re
import random
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity, get_jwt
from models import db, User, Session, PLANOS
from email_utils import smtp_configurado, enviar_codigo_verificacao, enviar_codigo_reset_senha

auth_bp = Blueprint('auth', __name__)

# Emails com acesso vitalicio (plano empresarial permanente)
EMAILS_VITALICIO = [
    "charlescr9@gmail.com",
]


def _garantir_vitalicio(user):
    """Se o email esta na lista VIP, garante plano empresarial."""
    if user.email in EMAILS_VITALICIO and user.plano != "empresarial":
        user.plano = "empresarial"
        db.session.commit()


def _get_ip():
    """Pega o IP real do cliente (mesmo atras de proxy/Railway)."""
    return request.headers.get('X-Forwarded-For', request.remote_addr or '').split(',')[0].strip()


def _validar_email(email):
    return re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email)


@auth_bp.route('/api/auth/register', methods=['POST'])
def register():
    """Cadastro de novo usuario."""
    dados = request.get_json()
    if not dados:
        return jsonify({"erro": "Dados nao enviados"}), 400

    email = (dados.get('email') or '').strip().lower()
    senha = dados.get('senha') or ''
    senha2 = dados.get('senha2') or ''

    if not email or not _validar_email(email):
        return jsonify({"erro": "Email invalido"}), 400

    if len(senha) < 6:
        return jsonify({"erro": "Senha deve ter pelo menos 6 caracteres"}), 400

    if senha != senha2:
        return jsonify({"erro": "As senhas nao coincidem"}), 400

    if User.query.filter_by(email=email).first():
        return jsonify({"erro": "Email ja cadastrado"}), 409

    user = User(email=email)
    user.set_password(senha)

    # Verificacao de email
    if smtp_configurado():
        codigo = str(random.randint(100000, 999999))
        user.email_code = codigo
        user.email_code_expires = datetime.utcnow() + timedelta(minutes=10)
        user.email_verified = False
    else:
        # Sem SMTP: auto-verificar
        user.email_verified = True

    db.session.add(user)
    db.session.commit()

    _garantir_vitalicio(user)

    # Enviar codigo por email (se SMTP configurado)
    email_enviado = False
    if smtp_configurado() and user.email_code:
        email_enviado = enviar_codigo_verificacao(email, user.email_code)

    ip = _get_ip()
    token_id = user.criar_sessao(ip)
    token = create_access_token(identity=str(user.id), additional_claims={"sid": token_id})

    resp = {
        "mensagem": "Conta criada com sucesso",
        "token": token,
        "user": user.to_dict(),
    }
    if smtp_configurado() and not user.email_verified:
        resp["verificacao_pendente"] = True
        resp["email_enviado"] = email_enviado

    return jsonify(resp), 201


@auth_bp.route('/api/auth/login', methods=['POST'])
def login():
    """Login do usuario. Bloqueia se limite de IPs do plano for atingido."""
    dados = request.get_json()
    if not dados:
        return jsonify({"erro": "Dados nao enviados"}), 400

    email = (dados.get('email') or '').strip().lower()
    senha = dados.get('senha') or ''

    user = User.query.filter_by(email=email).first()

    if not user or not user.check_password(senha):
        return jsonify({"erro": "Email ou senha incorretos"}), 401

    if not user.is_active:
        return jsonify({"erro": "Conta desativada"}), 403

    _garantir_vitalicio(user)

    ip = _get_ip()
    token_id = user.criar_sessao(ip)

    if token_id is None:
        info = user.get_plano_info()
        return jsonify({
            "erro": f"Limite de {info['max_ips']} dispositivo(s) atingido no plano {info['nome']}. Faca upgrade para mais dispositivos."
        }), 403

    token = create_access_token(identity=str(user.id), additional_claims={"sid": token_id})

    return jsonify({
        "mensagem": "Login realizado",
        "token": token,
        "user": user.to_dict(),
    })


@auth_bp.route('/api/auth/logout', methods=['POST'])
@jwt_required()
def logout():
    """Remove a sessao atual (logout) - libera o IP."""
    user_id = get_jwt_identity()
    claims = get_jwt()
    token_id = claims.get("sid", "")

    user = User.query.get(int(user_id))
    if user and token_id:
        user.remover_sessao(token_id)

    return jsonify({"mensagem": "Logout realizado"})


@auth_bp.route('/api/auth/me')
@jwt_required()
def me():
    """Retorna dados do usuario logado."""
    user_id = get_jwt_identity()
    user = User.query.get(int(user_id))
    if not user:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    return jsonify({"user": user.to_dict()})


@auth_bp.route('/api/planos')
def listar_planos():
    """Retorna os planos disponiveis (rota publica)."""
    lista = []
    for key, info in PLANOS.items():
        if key == 'free':
            continue
        lista.append({
            "id": key,
            "nome": info["nome"],
            "max_ips": info["max_ips"],
            "valor": info["valor"],
            "limite_proc": info["limite_proc"],
        })
    return jsonify({"planos": lista})


# ================================================================
# VERIFICACAO DE EMAIL
# ================================================================

@auth_bp.route('/api/auth/verify-email', methods=['POST'])
@jwt_required()
def verify_email():
    """Verifica codigo enviado por email."""
    user_id = get_jwt_identity()
    user = User.query.get(int(user_id))
    if not user:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    if user.email_verified:
        return jsonify({"mensagem": "Email ja verificado"})

    dados = request.get_json()
    codigo = (dados.get('codigo') or '').strip()

    if not codigo:
        return jsonify({"erro": "Codigo nao informado"}), 400

    if user.email_code != codigo:
        return jsonify({"erro": "Codigo incorreto"}), 400

    if user.email_code_expires and datetime.utcnow() > user.email_code_expires:
        return jsonify({"erro": "Codigo expirado. Solicite um novo."}), 400

    user.email_verified = True
    user.email_code = ''
    user.email_code_expires = None
    db.session.commit()

    return jsonify({"mensagem": "Email verificado com sucesso", "user": user.to_dict()})


@auth_bp.route('/api/auth/resend-code', methods=['POST'])
@jwt_required()
def resend_code():
    """Reenvia codigo de verificacao por email."""
    user_id = get_jwt_identity()
    user = User.query.get(int(user_id))
    if not user:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    if user.email_verified:
        return jsonify({"mensagem": "Email ja verificado"})

    if not smtp_configurado():
        # Auto-verificar se SMTP nao disponivel
        user.email_verified = True
        db.session.commit()
        return jsonify({"mensagem": "Email verificado automaticamente", "user": user.to_dict()})

    codigo = str(random.randint(100000, 999999))
    user.email_code = codigo
    user.email_code_expires = datetime.utcnow() + timedelta(minutes=10)
    db.session.commit()

    enviado = enviar_codigo_verificacao(user.email, codigo)
    if enviado:
        return jsonify({"mensagem": "Codigo reenviado para " + user.email})
    return jsonify({"erro": "Erro ao enviar email. Tente novamente."}), 500


# ================================================================
# RECUPERACAO DE SENHA
# ================================================================

@auth_bp.route('/api/auth/forgot-password', methods=['POST'])
def forgot_password():
    """Envia codigo de recuperacao de senha por email."""
    dados = request.get_json()
    email = (dados.get('email') or '').strip().lower() if dados else ''

    if not email or not _validar_email(email):
        return jsonify({"erro": "Email invalido"}), 400

    if not smtp_configurado():
        return jsonify({"erro": "Servico de email nao configurado. Contate o suporte."}), 503

    user = User.query.filter_by(email=email).first()
    if not user:
        # Nao revelar se email existe ou nao (seguranca)
        return jsonify({"mensagem": "Se o email estiver cadastrado, voce recebera um codigo de recuperacao."})

    codigo = str(random.randint(100000, 999999))
    user.reset_code = codigo
    user.reset_code_expires = datetime.utcnow() + timedelta(minutes=15)
    db.session.commit()

    enviado = enviar_codigo_reset_senha(email, codigo)
    if not enviado:
        return jsonify({"erro": "Erro ao enviar email. Tente novamente."}), 500

    return jsonify({"mensagem": "Se o email estiver cadastrado, voce recebera um codigo de recuperacao."})


@auth_bp.route('/api/auth/reset-password', methods=['POST'])
def reset_password():
    """Valida codigo e redefine a senha."""
    dados = request.get_json()
    if not dados:
        return jsonify({"erro": "Dados nao enviados"}), 400

    email = (dados.get('email') or '').strip().lower()
    codigo = (dados.get('codigo') or '').strip()
    nova_senha = dados.get('nova_senha') or ''

    if not email or not codigo or not nova_senha:
        return jsonify({"erro": "Preencha todos os campos"}), 400

    if len(nova_senha) < 6:
        return jsonify({"erro": "Senha deve ter pelo menos 6 caracteres"}), 400

    user = User.query.filter_by(email=email).first()
    if not user:
        return jsonify({"erro": "Codigo invalido ou expirado"}), 400

    if not user.reset_code or user.reset_code != codigo:
        return jsonify({"erro": "Codigo invalido ou expirado"}), 400

    if user.reset_code_expires and datetime.utcnow() > user.reset_code_expires:
        return jsonify({"erro": "Codigo expirado. Solicite um novo."}), 400

    user.set_password(nova_senha)
    user.reset_code = ''
    user.reset_code_expires = None
    db.session.commit()

    return jsonify({"mensagem": "Senha redefinida com sucesso! Faca login com a nova senha."})


# ================================================================
# ADMIN - GERENCIAR ACESSOS
# ================================================================

def _is_admin(user_id):
    """Verifica se o usuario e admin (email na lista VIP)."""
    user = User.query.get(int(user_id))
    return user and user.email in EMAILS_VITALICIO


@auth_bp.route('/api/admin/usuarios')
@jwt_required()
def admin_listar_usuarios():
    """Lista todos os usuarios (somente admin)."""
    user_id = get_jwt_identity()
    if not _is_admin(user_id):
        return jsonify({"erro": "Acesso negado"}), 403

    usuarios = User.query.order_by(User.created_at.desc()).all()
    lista = []
    for u in usuarios:
        vitalicio = u.email in EMAILS_VITALICIO
        if vitalicio:
            expira_str = "Vitalicio"
        elif u.plano_expira:
            expira_str = u.plano_expira.strftime("%d/%m/%Y")
        elif u.plano != 'free':
            expira_str = "Sem expiracao"
        else:
            expira_str = ""
        lista.append({
            "id": u.id,
            "email": u.email,
            "plano": u.plano,
            "plano_nome": u.get_plano_info()["nome"],
            "is_active": u.is_active,
            "vitalicio": vitalicio,
            "created_at": u.created_at.strftime("%d/%m/%Y"),
            "plano_expira": expira_str,
            "meses_gratis": u.meses_gratis or 0,
            "email_verified": u.email_verified,
        })
    return jsonify({"usuarios": lista})


@auth_bp.route('/api/admin/liberar-acesso', methods=['POST'])
@jwt_required()
def admin_liberar_acesso():
    """Libera acesso gratuito a um email (somente admin)."""
    user_id = get_jwt_identity()
    if not _is_admin(user_id):
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.get_json() or {}
    email = (dados.get('email') or '').strip().lower()
    plano = dados.get('plano', 'empresarial')
    meses = int(dados.get('meses', 1))

    if not email:
        return jsonify({"erro": "Email nao informado"}), 400
    if plano not in PLANOS or plano == 'free':
        return jsonify({"erro": "Plano invalido"}), 400
    if meses < 1 or meses > 120:
        return jsonify({"erro": "Meses deve ser entre 1 e 120"}), 400

    user = User.query.filter_by(email=email).first()
    if not user:
        return jsonify({"erro": f"Usuario '{email}' nao encontrado"}), 404

    user.plano = plano
    # Calcular expiracao
    base = user.plano_expira if user.plano_expira and user.plano_expira > datetime.utcnow() else datetime.utcnow()
    user.plano_expira = base + timedelta(days=meses * 30)
    db.session.commit()

    return jsonify({
        "mensagem": f"Acesso {PLANOS[plano]['nome']} liberado para {email} por {meses} mes(es)",
        "usuario": {
            "email": user.email,
            "plano": user.plano,
            "plano_expira": user.plano_expira.strftime("%d/%m/%Y") if user.plano_expira else '',
        },
    })


@auth_bp.route('/api/admin/revogar-acesso', methods=['POST'])
@jwt_required()
def admin_revogar_acesso():
    """Revoga acesso de um usuario, voltando para Free (somente admin)."""
    user_id = get_jwt_identity()
    if not _is_admin(user_id):
        return jsonify({"erro": "Acesso negado"}), 403

    dados = request.get_json() or {}
    email = (dados.get('email') or '').strip().lower()

    if not email:
        return jsonify({"erro": "Email nao informado"}), 400

    user = User.query.filter_by(email=email).first()
    if not user:
        return jsonify({"erro": f"Usuario '{email}' nao encontrado"}), 404

    if user.email in EMAILS_VITALICIO:
        return jsonify({"erro": "Nao e possivel revogar acesso de um administrador"}), 400

    user.plano = 'free'
    user.plano_expira = None
    db.session.commit()

    return jsonify({"mensagem": f"Acesso de {email} revogado. Plano: Free"})


@auth_bp.route('/api/admin/check')
@jwt_required()
def admin_check():
    """Verifica se o usuario logado e admin."""
    user_id = get_jwt_identity()
    return jsonify({"is_admin": _is_admin(user_id)})


# ================================================================
# GOOGLE OAUTH
# ================================================================

@auth_bp.route('/api/auth/google', methods=['POST'])
def google_login():
    """Login/registro via Google OAuth (ID Token)."""
    GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID', '').strip()
    if not GOOGLE_CLIENT_ID:
        return jsonify({"erro": "Login com Google nao configurado"}), 503

    dados = request.get_json()
    id_token_str = dados.get('credential') or dados.get('id_token') or ''
    if not id_token_str:
        return jsonify({"erro": "Token Google nao informado"}), 400

    try:
        from google.oauth2 import id_token
        from google.auth.transport import requests as google_requests
        idinfo = id_token.verify_oauth2_token(id_token_str, google_requests.Request(), GOOGLE_CLIENT_ID)
    except Exception as e:
        return jsonify({"erro": f"Token Google invalido: {str(e)}"}), 401

    google_id = idinfo.get('sub')
    email = idinfo.get('email', '').lower()

    if not google_id or not email:
        return jsonify({"erro": "Dados do Google incompletos"}), 400

    # Buscar usuario existente por google_id ou email
    user = User.query.filter_by(google_id=google_id).first()
    if not user:
        user = User.query.filter_by(email=email).first()
        if user:
            # Vincular Google ao usuario existente
            user.google_id = google_id
        else:
            # Criar novo usuario
            user = User(email=email, google_id=google_id, email_verified=True)
            user.set_password(os.urandom(32).hex())  # senha aleatoria (login via Google)
            db.session.add(user)

    user.email_verified = True  # Google ja verificou o email
    db.session.commit()

    _garantir_vitalicio(user)

    ip = _get_ip()
    token_id = user.criar_sessao(ip)
    if token_id is None:
        info = user.get_plano_info()
        return jsonify({
            "erro": f"Limite de {info['max_ips']} dispositivo(s) atingido no plano {info['nome']}."
        }), 403

    token = create_access_token(identity=str(user.id), additional_claims={"sid": token_id})

    return jsonify({
        "mensagem": "Login Google realizado",
        "token": token,
        "user": user.to_dict(),
    })


@auth_bp.route('/api/auth/google-client-id')
def google_client_id():
    """Retorna o Google Client ID (para o frontend carregar o botao)."""
    cid = os.environ.get('GOOGLE_CLIENT_ID', '').strip()
    return jsonify({"client_id": cid})
