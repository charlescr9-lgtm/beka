# -*- coding: utf-8 -*-
"""
Shopee Monitor Routes — Blueprint para monitoramento de lojas Shopee via CDP.
"""

import json
import threading
from datetime import datetime, timezone, timedelta
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from models import db, ShopeeMonitorConfig, ShopeeMonitorLog

shopee_monitor_bp = Blueprint('shopee_monitor', __name__)

_FUSO_BRASILIA = timezone(timedelta(hours=-3))

def _agora():
    return datetime.now(_FUSO_BRASILIA).replace(tzinfo=None)

# Estado global da execucao em andamento
_current_flow = None
_current_thread = None
_current_log_lines = []
_current_status = "idle"  # idle, running, done, error


# =============================================
# CONFIG
# =============================================

@shopee_monitor_bp.route('/api/shopee-monitor/config', methods=['GET'])
@jwt_required()
def get_config():
    uid = get_jwt_identity()
    cfg = ShopeeMonitorConfig.query.filter_by(user_id=uid).first()
    if not cfg:
        cfg = ShopeeMonitorConfig(user_id=uid)
        db.session.add(cfg)
        db.session.commit()
    return jsonify(cfg.to_dict())


@shopee_monitor_bp.route('/api/shopee-monitor/config', methods=['POST'])
@jwt_required()
def save_config():
    uid = get_jwt_identity()
    data = request.get_json(force=True)
    cfg = ShopeeMonitorConfig.query.filter_by(user_id=uid).first()
    if not cfg:
        cfg = ShopeeMonitorConfig(user_id=uid)
        db.session.add(cfg)

    if "horario" in data:
        cfg.horario = data["horario"]
    if "dias" in data:
        cfg.dias_json = json.dumps(data["dias"])
    if "cdp_port" in data:
        cfg.cdp_port = int(data["cdp_port"])
    if "ativo" in data:
        cfg.ativo = bool(data["ativo"])

    db.session.commit()
    return jsonify(cfg.to_dict())


# =============================================
# EXECUCAO
# =============================================

@shopee_monitor_bp.route('/api/shopee-monitor/run', methods=['POST'])
@jwt_required()
def run_monitor():
    global _current_flow, _current_thread, _current_log_lines, _current_status

    uid = get_jwt_identity()

    if _current_status == "running":
        return jsonify({"error": "Ja existe uma execucao em andamento"}), 409

    cfg = ShopeeMonitorConfig.query.filter_by(user_id=uid).first()
    cdp_port = cfg.cdp_port if cfg else 9222

    # Criar log no banco
    log_entry = ShopeeMonitorLog(user_id=uid, status='rodando')
    db.session.add(log_entry)
    db.session.commit()
    log_id = log_entry.id

    # Resetar estado
    _current_log_lines = []
    _current_status = "running"

    def log_callback(msg):
        global _current_status
        _current_log_lines.append(msg)
        # Detectar fim
        if '"status":' in msg and ('"concluido"' in msg or '"erro"' in msg or '"cancelado"' in msg):
            try:
                result = json.loads(msg)
                _finalize_log(uid, log_id, result)
            except Exception:
                pass

    from shopee_monitor_engine import run_monitor_async
    _current_flow, _current_thread = run_monitor_async(
        cdp_port=cdp_port,
        log_callback=log_callback,
    )

    return jsonify({"ok": True, "log_id": log_id})


def _finalize_log(uid, log_id, result):
    """Atualiza o log no banco com resultado final."""
    global _current_status
    try:
        from models import db as _db
        # Importar app para contexto
        log = ShopeeMonitorLog.query.get(log_id)
        if log:
            log.fim = _agora()
            log.status = result.get("status", "erro")
            log.total_lojas = len(result.get("lojas", []))
            log.total_alertas = result.get("alertas", 0) if isinstance(result.get("alertas"), int) else len([a for a in result.get("alertas", []) if a.get("tem_alerta")])
            log.total_etiquetas = result.get("etiquetas", {}).get("total_abas_etiqueta", 0)
            log.log_text = '\n'.join(_current_log_lines)
            log.resultado_json = json.dumps(result, ensure_ascii=False)
            _db.session.commit()
    except Exception:
        pass
    _current_status = result.get("status", "erro")


@shopee_monitor_bp.route('/api/shopee-monitor/stop', methods=['POST'])
@jwt_required()
def stop_monitor():
    global _current_flow, _current_status
    if _current_flow:
        _current_flow.cancelar()
        _current_status = "idle"
    return jsonify({"ok": True})


# =============================================
# STATUS / LOGS
# =============================================

@shopee_monitor_bp.route('/api/shopee-monitor/status', methods=['GET'])
@jwt_required()
def get_status():
    return jsonify({
        "status": _current_status,
        "log_lines": _current_log_lines[-50:],  # ultimas 50 linhas
        "total_lines": len(_current_log_lines),
    })


@shopee_monitor_bp.route('/api/shopee-monitor/logs', methods=['GET'])
@jwt_required()
def get_logs():
    uid = get_jwt_identity()
    logs = ShopeeMonitorLog.query.filter_by(user_id=uid).order_by(
        ShopeeMonitorLog.id.desc()
    ).limit(20).all()
    return jsonify([l.to_dict() for l in logs])


@shopee_monitor_bp.route('/api/shopee-monitor/results', methods=['GET'])
@jwt_required()
def get_results():
    uid = get_jwt_identity()
    last = ShopeeMonitorLog.query.filter_by(
        user_id=uid, status='concluido'
    ).order_by(ShopeeMonitorLog.id.desc()).first()

    if not last:
        return jsonify({"resultado": None})

    return jsonify({
        "resultado": last.to_dict(),
    })
