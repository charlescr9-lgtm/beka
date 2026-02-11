# -*- coding: utf-8 -*-
"""
Integracao com Mercado Pago - planos mensais, semestrais e anuais
Sistema de cupom de indicacao com meses gratis
"""

import os
import uuid
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from models import db, User, Payment, PLANOS

payments_bp = Blueprint('payments', __name__)

# Periodos de assinatura com desconto
PERIODOS = {
    "mensal":    {"meses": 1,  "desconto": 0,    "label": "Mensal"},
    "semestral": {"meses": 6,  "desconto": 0.20, "label": "Semestral (-20%)"},
    "anual":     {"meses": 12, "desconto": 0.40, "label": "Anual (-40%)"},
}


def _get_mp_sdk():
    """Inicializa SDK do Mercado Pago."""
    import mercadopago
    token = os.environ.get('MERCADOPAGO_ACCESS_TOKEN', '')
    if not token:
        return None
    return mercadopago.SDK(token)


def _calcular_valor(plano_id, periodo_id):
    """Calcula valor total com desconto do periodo."""
    plano = PLANOS.get(plano_id)
    periodo = PERIODOS.get(periodo_id)
    if not plano or not periodo:
        return 0, 0, 0
    valor_mensal = plano["valor"]
    meses = periodo["meses"]
    desconto = periodo["desconto"]
    valor_total = round(valor_mensal * meses * (1 - desconto), 2)
    return valor_total, meses, desconto


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
    periodo_id = dados.get('periodo', 'mensal')
    cupom = (dados.get('cupom') or '').strip().upper()

    if plano_id not in PLANOS or plano_id == 'free':
        return jsonify({"erro": "Plano invalido"}), 400

    if periodo_id not in PERIODOS:
        return jsonify({"erro": "Periodo invalido"}), 400

    plano = PLANOS[plano_id]
    periodo = PERIODOS[periodo_id]
    valor_total, meses, desconto = _calcular_valor(plano_id, periodo_id)

    # Validar cupom de indicacao (se informado)
    indicador_id = None
    if cupom:
        indicador = User.query.filter_by(cupom_indicacao=cupom).first()
        if not indicador:
            return jsonify({"erro": "Cupom de indicacao invalido"}), 400
        if indicador.id == user.id:
            return jsonify({"erro": "Voce nao pode usar seu proprio cupom"}), 400
        if user.indicado_por:
            return jsonify({"erro": "Voce ja utilizou um cupom de indicacao anteriormente"}), 400
        indicador_id = indicador.id

    sdk = _get_mp_sdk()
    if not sdk:
        return jsonify({"erro": "Mercado Pago nao configurado"}), 500

    base_url = os.environ.get('APP_URL', 'https://web-production-274ef.up.railway.app')

    desc_periodo = periodo["label"]
    preference_data = {
        "items": [
            {
                "title": f"Beka MultiPlace - {plano['nome']} {desc_periodo}",
                "description": f"Ate {plano['max_ips']} dispositivo(s), processamentos ilimitados, {meses} mes(es)",
                "quantity": 1,
                "unit_price": valor_total,
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
        "external_reference": f"{user.id}:{plano_id}:{periodo_id}:{indicador_id or 0}",
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
        valor=valor_total,
    )
    db.session.add(payment)
    db.session.commit()

    return jsonify({
        "url": preference.get("init_point", ""),
        "preference_id": preference.get("id", ""),
        "plano": plano_id,
        "periodo": periodo_id,
        "valor": valor_total,
        "meses": meses,
        "desconto_pct": int(desconto * 100),
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

        if not ref:
            return

        partes = ref.split(":")
        if len(partes) < 2:
            return

        user_id_str = partes[0]
        plano_id = partes[1]
        periodo_id = partes[2] if len(partes) > 2 else "mensal"
        indicador_id = int(partes[3]) if len(partes) > 3 and partes[3] != "0" else None

        user = User.query.get(int(user_id_str))
        if not user:
            return

        if status == "approved" and plano_id in PLANOS:
            user.plano = plano_id

            # Calcular expiracao do plano
            periodo = PERIODOS.get(periodo_id, PERIODOS["mensal"])
            meses = periodo["meses"]
            # Se ja tem plano ativo, estender; senao, a partir de agora
            base = user.plano_expira if user.plano_expira and user.plano_expira > datetime.utcnow() else datetime.utcnow()
            # Adicionar meses gratis acumulados
            meses_bonus = user.meses_gratis or 0
            user.plano_expira = base + timedelta(days=(meses + meses_bonus) * 30)
            user.meses_gratis = 0  # Zerar apos aplicar
            db.session.commit()

            # Aplicar bonus de indicacao
            if indicador_id and not user.indicado_por:
                user.indicado_por = indicador_id
                db.session.commit()
                # +1 mes gratis para quem indicou
                indicador = User.query.get(indicador_id)
                if indicador:
                    indicador.meses_gratis = (indicador.meses_gratis or 0) + 1
                    db.session.commit()
                # +1 mes gratis para quem usou o cupom (adicionar ao plano)
                user.plano_expira = user.plano_expira + timedelta(days=30)
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
        "periodos": [
            {"id": k, "label": v["label"], "meses": v["meses"], "desconto": int(v["desconto"] * 100)}
            for k, v in PERIODOS.items()
        ],
    })


@payments_bp.route('/api/payment/simular', methods=['POST'])
@jwt_required()
def simular_pagamento():
    """Simula o valor de um plano + periodo para exibir no frontend."""
    dados = request.get_json() or {}
    plano_id = dados.get('plano', 'basico')
    periodo_id = dados.get('periodo', 'mensal')

    if plano_id not in PLANOS or plano_id == 'free':
        return jsonify({"erro": "Plano invalido"}), 400
    if periodo_id not in PERIODOS:
        return jsonify({"erro": "Periodo invalido"}), 400

    plano = PLANOS[plano_id]
    valor_total, meses, desconto = _calcular_valor(plano_id, periodo_id)
    valor_mensal_original = plano["valor"]
    valor_mensal_com_desc = round(valor_total / meses, 2) if meses > 0 else 0

    return jsonify({
        "plano": plano_id,
        "periodo": periodo_id,
        "meses": meses,
        "valor_mensal_original": valor_mensal_original,
        "valor_mensal_com_desconto": valor_mensal_com_desc,
        "valor_total": valor_total,
        "desconto_pct": int(desconto * 100),
        "economia": round(valor_mensal_original * meses - valor_total, 2),
    })


# ================================================================
# SISTEMA DE INDICACAO / CUPOM
# ================================================================

@payments_bp.route('/api/indicacao/meu-cupom')
@jwt_required()
def meu_cupom():
    """Retorna ou gera o cupom de indicacao do usuario."""
    user_id = get_jwt_identity()
    user = User.query.get(int(user_id))
    if not user:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    # Gerar cupom se nao tiver
    if not user.cupom_indicacao:
        # Gerar cupom unico baseado no email
        base = user.email.split('@')[0].upper().replace('.', '')[:6]
        sufixo = uuid.uuid4().hex[:4].upper()
        cupom = f"{base}{sufixo}"
        # Garantir unicidade
        while User.query.filter_by(cupom_indicacao=cupom).first():
            sufixo = uuid.uuid4().hex[:4].upper()
            cupom = f"{base}{sufixo}"
        user.cupom_indicacao = cupom
        db.session.commit()

    # Contar quantos usaram este cupom
    total_indicados = User.query.filter_by(indicado_por=user.id).count()

    return jsonify({
        "cupom": user.cupom_indicacao,
        "total_indicados": total_indicados,
        "meses_gratis": user.meses_gratis or 0,
    })


@payments_bp.route('/api/indicacao/validar-cupom', methods=['POST'])
@jwt_required()
def validar_cupom():
    """Valida um cupom de indicacao."""
    user_id = get_jwt_identity()
    user = User.query.get(int(user_id))
    if not user:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    dados = request.get_json() or {}
    cupom = (dados.get('cupom') or '').strip().upper()

    if not cupom:
        return jsonify({"erro": "Cupom nao informado"}), 400

    indicador = User.query.filter_by(cupom_indicacao=cupom).first()
    if not indicador:
        return jsonify({"valido": False, "erro": "Cupom invalido"})

    if indicador.id == user.id:
        return jsonify({"valido": False, "erro": "Voce nao pode usar seu proprio cupom"})

    if user.indicado_por:
        return jsonify({"valido": False, "erro": "Voce ja utilizou um cupom anteriormente"})

    return jsonify({
        "valido": True,
        "mensagem": "Cupom valido! Voce e quem indicou ganham 1 mes gratis apos o pagamento.",
    })
