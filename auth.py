# -*- coding: utf-8 -*-
"""
Rotas de autenticacao - registro, login, perfil
Limite de IPs por plano para evitar compartilhamento de senha
"""

import re
from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity, get_jwt
from models import db, User, Session, PLANOS

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

    if not email or not _validar_email(email):
        return jsonify({"erro": "Email invalido"}), 400

    if len(senha) < 6:
        return jsonify({"erro": "Senha deve ter pelo menos 6 caracteres"}), 400

    if User.query.filter_by(email=email).first():
        return jsonify({"erro": "Email ja cadastrado"}), 409

    user = User(email=email)
    user.set_password(senha)
    db.session.add(user)
    db.session.commit()

    _garantir_vitalicio(user)

    ip = _get_ip()
    token_id = user.criar_sessao(ip)
    token = create_access_token(identity=str(user.id), additional_claims={"sid": token_id})

    return jsonify({
        "mensagem": "Conta criada com sucesso",
        "token": token,
        "user": user.to_dict(),
    }), 201


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
