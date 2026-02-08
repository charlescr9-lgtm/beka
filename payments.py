# -*- coding: utf-8 -*-
"""
Integracao com Mercado Pago - planos mensais
"""

import os
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from models import db, User, Payment, PLANOS

payments_bp = Blueprint('payments', __name__)


def _get_mp_sdk():
    """Inicializa SDK do Mercado Pago."""
    import mercadopago
    token = os.environ.get('MERCADOPAGO_ACCESS_TOKEN', '')
    if not token:
        return None
    return mercadopago.SDK(token)


@payments_bp.route('/api/payment/create', methods=['POST'])
@jwt_required()
def create_payment():
    """Cria link de pagamento no Mercado Pago para o plano escolhido."""
    user_id = get_jwt_identity()
    user = User.query.get(int(user_id))
    if not user:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    dados = request.get_json() or {}
    plano_id = dados.get('plano', 'basico')

    if plano_id not in PLANOS or plano_id == 'free':
        return jsonify({"erro": "Plano invalido"}), 400

    plano = PLANOS[plano_id]

    sdk = _get_mp_sdk()
    if not sdk:
        return jsonify({"erro": "Mercado Pago nao configurado"}), 500

    base_url = os.environ.get('APP_URL', 'https://web-production-274ef.up.railway.app')

    preference_data = {
        "items": [
            {
                "title": f"Etiquetas Shopee - Plano {plano['nome']} Mensal",
                "description": f"Ate {plano['max_ips']} dispositivo(s), processamentos ilimitados",
                "quantity": 1,
                "unit_price": plano["valor"],
                "currency_id": "BRL",
            }
        ],
        "payer": {
            "email": user.email,
        },
        "back_urls": {
            "success": f"{base_url}/",
            "failure": f"{base_url}/",
            "pending": f"{base_url}/",
        },
        "auto_return": "approved",
        "notification_url": f"{base_url}/api/payment/webhook",
        "external_reference": f"{user.id}:{plano_id}",
    }

    result = sdk.preference().create(preference_data)
    preference = result.get("response", {})

    if not preference.get("id"):
        return jsonify({"erro": "Erro ao criar pagamento"}), 500

    payment = Payment(
        user_id=user.id,
        status='pending',
        mercadopago_id=preference.get("id", ""),
        plano_contratado=plano_id,
        valor=plano["valor"],
    )
    db.session.add(payment)
    db.session.commit()

    return jsonify({
        "url": preference.get("init_point", ""),
        "preference_id": preference.get("id", ""),
        "plano": plano_id,
        "valor": plano["valor"],
    })


@payments_bp.route('/api/payment/webhook', methods=['POST'])
def webhook():
    """Recebe notificacoes do Mercado Pago."""
    dados = request.get_json(silent=True)
    if not dados:
        return jsonify({"ok": True}), 200

    tipo = dados.get("type") or dados.get("topic", "")

    if tipo == "payment":
        payment_id = dados.get("data", {}).get("id")
        if payment_id:
            _processar_pagamento(payment_id)

    return jsonify({"ok": True}), 200


def _processar_pagamento(payment_id):
    """Busca detalhes do pagamento e atualiza o plano do usuario."""
    try:
        sdk = _get_mp_sdk()
        if not sdk:
            return

        result = sdk.payment().get(payment_id)
        payment_data = result.get("response", {})

        status = payment_data.get("status", "")
        ref = payment_data.get("external_reference", "")

        if not ref or ":" not in ref:
            return

        user_id_str, plano_id = ref.split(":", 1)
        user = User.query.get(int(user_id_str))
        if not user:
            return

        if status == "approved" and plano_id in PLANOS:
            user.plano = plano_id
            db.session.commit()

        payment = Payment.query.filter_by(user_id=user.id).order_by(Payment.id.desc()).first()
        if payment:
            payment.status = status
            payment.mercadopago_id = str(payment_id)
            db.session.commit()

    except Exception as e:
        print(f"Erro ao processar webhook: {e}")


@payments_bp.route('/api/payment/status')
@jwt_required()
def payment_status():
    """Retorna status do plano do usuario."""
    user_id = get_jwt_identity()
    user = User.query.get(int(user_id))
    if not user:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    info = user.get_plano_info()
    return jsonify({
        "plano": user.plano,
        "plano_nome": info["nome"],
        "valor": info["valor"],
        "max_ips": info["max_ips"],
        "planos_disponiveis": [
            {"id": k, "nome": v["nome"], "max_ips": v["max_ips"], "valor": v["valor"]}
            for k, v in PLANOS.items() if k != "free"
        ],
    })
