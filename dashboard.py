# -*- coding: utf-8 -*-
"""
Dashboard Interativo - Beka MultiPlace
Backend Flask com API REST + Autenticacao JWT + Multi-usuario
"""

import os
import sys
import json
import time
import threading
import subprocess
import shutil
import hmac
import hashlib
import smtplib
import secrets
import re as _re
import unicodedata
from urllib.parse import urlparse, quote_plus, urlencode
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from flask import Flask, request, jsonify, send_from_directory, send_file, redirect
from flask_cors import CORS
from flask_jwt_extended import JWTManager, jwt_required, get_jwt_identity, get_jwt
import xmltodict
import pandas as pd
import openpyxl
import requests
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from etiquetas_shopee import ProcessadorEtiquetasShopee
from models import (db, bcrypt, User, Session, WhatsAppContact, Schedule,
                    UpSellerConfig, ExecutionLog, Loja, EmailContact,
                    WhatsAppQueueItem, MarketplaceApiConfig, MarketplaceLoja,
                    encrypt_value, decrypt_value)
from auth import auth_bp
from email_utils import enviar_email_com_anexo, enviar_email_com_anexos, smtp_configurado, get_smtp_config
from payments import payments_bp
from scheduler import beka_scheduler
from whatsapp_service import WhatsAppService
from whatsapp_delivery import montar_entregas_por_resultado, montar_destinos_por_resultado

# PyInstaller frozen path support
if getattr(sys, 'frozen', False):
    _BASE_DIR = sys._MEIPASS
else:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__, static_folder=os.path.join(_BASE_DIR, 'static'))
CORS(app)

# ----------------------------------------------------------------
# CONFIGURACAO DO APP
# ----------------------------------------------------------------
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'chave-secreta-trocar-em-producao')
app.config['JWT_SECRET_KEY'] = os.environ.get('JWT_SECRET_KEY', 'jwt-secret-trocar-em-producao')
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(days=30)

# Banco de dados SQLite — usar volume persistente do Railway
# Railway monta o volume no path definido em RAILWAY_VOLUME_MOUNT_PATH
_VOLUME_PATH = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', os.environ.get('DB_DIR', os.path.join(_BASE_DIR, 'data')))
os.makedirs(_VOLUME_PATH, exist_ok=True)
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{os.path.join(_VOLUME_PATH, 'app.db')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


def _normalizar_base_url(raw: str) -> str:
    txt = str(raw or "").strip().rstrip("/")
    if not txt:
        return ""
    if "://" not in txt:
        txt = "https://" + txt
    return txt


def _detectar_ngrok_auto() -> str:
    """Detecta tunnel ngrok ativo via API local (localhost:4040)."""
    try:
        import requests as _req
        r = _req.get("http://localhost:4040/api/tunnels", timeout=2)
        if r.status_code == 200:
            tunnels = r.json().get("tunnels", [])
            for t in tunnels:
                url = str(t.get("public_url") or "").strip()
                if url.startswith("https://"):
                    return url
    except Exception:
        pass
    return ""


def _detectar_base_publica() -> str:
    """
    Detecta base publica para callback OAuth/testes:
    - desenvolvimento: NGROK_URL / NGROK_PUBLIC_URL / auto-detect ngrok
    - producao: SHOPEE_REDIRECT_BASE_URL / PUBLIC_BASE_URL / RAILWAY_PUBLIC_DOMAIN
    """
    candidatos = [
        os.environ.get("SHOPEE_REDIRECT_BASE_URL"),
        os.environ.get("NGROK_URL"),
        os.environ.get("NGROK_PUBLIC_URL"),
        os.environ.get("PUBLIC_BASE_URL"),
        os.environ.get("RAILWAY_STATIC_URL"),
        os.environ.get("RAILWAY_PUBLIC_DOMAIN"),
    ]
    for c in candidatos:
        base = _normalizar_base_url(c)
        if base:
            return base

    # Auto-detectar ngrok ativo na maquina local
    ngrok_url = _detectar_ngrok_auto()
    if ngrok_url:
        return _normalizar_base_url(ngrok_url) or ngrok_url

    # Fallback em runtime (quando requisicao chega com host publico).
    try:
        host_url = _normalizar_base_url((request.host_url if request else ""))
        if host_url:
            return host_url
    except Exception:
        pass
    return ""


def _get_shopee_redirect_url() -> str:
    base = _detectar_base_publica()
    return (base + "/api/marketplace/shopee/callback") if base else ""


def _get_shopee_redirect_domain() -> str:
    ru = _get_shopee_redirect_url()
    if not ru:
        return ""
    try:
        return (urlparse(ru).netloc or "").strip()
    except Exception:
        return ""


def _shopee_oauth_state_secret() -> bytes:
    """Segredo para assinar state do OAuth Shopee."""
    secret_txt = str(
        app.config.get("JWT_SECRET_KEY")
        or app.config.get("SECRET_KEY")
        or "beka-shopee-state-secret"
    )
    return secret_txt.encode("utf-8")


def _build_shopee_oauth_state(user_id: int) -> str:
    """Cria state assinado contendo user_id e timestamp (anti-tamper)."""
    uid = int(user_id)
    ts = int(time.time())
    nonce = secrets.token_hex(8)
    payload = f"{uid}:{ts}:{nonce}"
    sig = hmac.new(
        _shopee_oauth_state_secret(),
        payload.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()[:32]
    return f"{payload}:{sig}"


def _parse_shopee_oauth_state(state: str, max_age_sec: int = 1800):
    """Valida state assinado e retorna user_id."""
    txt = str(state or "").strip()
    parts = txt.split(":")
    if len(parts) != 4:
        return None, "state_invalido"
    uid_s, ts_s, nonce, sig = parts
    payload = f"{uid_s}:{ts_s}:{nonce}"
    expected = hmac.new(
        _shopee_oauth_state_secret(),
        payload.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()[:32]
    if not hmac.compare_digest(sig, expected):
        return None, "state_assinatura_invalida"
    try:
        uid = int(uid_s)
        ts = int(ts_s)
    except Exception:
        return None, "state_formato_invalido"
    now = int(time.time())
    if ts > now + 300:
        return None, "state_tempo_invalido"
    if now - ts > max(60, int(max_age_sec or 1800)):
        return None, "state_expirado"
    return uid, ""


# Cache de OAuth pendente: quando geramos a login-url, guardamos user_id + timestamp
# E TAMBEM as credenciais (partner_id, partner_key, base_url) ja decriptadas.
# Isso permite que o callback faca o token exchange IMEDIATAMENTE sem query DB/Fernet.
# O code sandbox da Shopee expira em ~30s — cada ms conta.
_pending_shopee_oauth = {}  # {user_id: {ts, partner_id, partner_key, base_url}}
_PENDING_OAUTH_MAX_AGE = 1800  # 30 minutos


def _register_pending_oauth(user_id: int, partner_id: str = "", partner_key: str = "", base_url: str = ""):
    """Registra que user_id iniciou OAuth, com credenciais pre-carregadas."""
    _pending_shopee_oauth[int(user_id)] = {
        "ts": int(time.time()),
        "partner_id": str(partner_id or "").strip(),
        "partner_key": str(partner_key or "").strip(),
        "base_url": str(base_url or "").strip(),
    }


def _find_pending_oauth_user():
    """Encontra user_id com OAuth pendente mais recente (fallback).
    Retorna (user_id, info_dict) ou (None, {})."""
    now = int(time.time())
    best_uid, best_ts, best_info = None, 0, {}
    expired = []
    for uid, info in _pending_shopee_oauth.items():
        ts = info.get("ts", 0)
        if now - ts > _PENDING_OAUTH_MAX_AGE:
            expired.append(uid)
            continue
        if ts > best_ts:
            best_uid, best_ts, best_info = uid, ts, info
    for uid in expired:
        _pending_shopee_oauth.pop(uid, None)
    return best_uid, best_info


def _consume_pending_oauth(user_id: int):
    """Remove OAuth pendente apos uso."""
    _pending_shopee_oauth.pop(int(user_id), None)


# Inicializar extensoes
db.init_app(app)
bcrypt.init_app(app)
jwt = JWTManager(app)

# Registrar blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(payments_bp)

# Migrar banco existente (adicionar colunas novas em tabelas existentes)
def _migrate_db():
    """SQLite nao adiciona colunas automaticamente em tabelas existentes.
    Esta funcao verifica e adiciona colunas faltantes."""
    import sqlalchemy
    inspector = sqlalchemy.inspect(db.engine)

    if 'users' in inspector.get_table_names():
        colunas = [c['name'] for c in inspector.get_columns('users')]
        with db.engine.begin() as conn:
            if 'email_verified' not in colunas:
                conn.execute(sqlalchemy.text('ALTER TABLE users ADD COLUMN email_verified BOOLEAN DEFAULT 0'))
            if 'email_code' not in colunas:
                conn.execute(sqlalchemy.text("ALTER TABLE users ADD COLUMN email_code VARCHAR(6) DEFAULT ''"))
            if 'email_code_expires' not in colunas:
                conn.execute(sqlalchemy.text('ALTER TABLE users ADD COLUMN email_code_expires DATETIME'))
            if 'google_id' not in colunas:
                conn.execute(sqlalchemy.text('ALTER TABLE users ADD COLUMN google_id VARCHAR(255)'))
            if 'reset_code' not in colunas:
                conn.execute(sqlalchemy.text("ALTER TABLE users ADD COLUMN reset_code VARCHAR(6) DEFAULT ''"))
            if 'reset_code_expires' not in colunas:
                conn.execute(sqlalchemy.text('ALTER TABLE users ADD COLUMN reset_code_expires DATETIME'))
            if 'cupom_indicacao' not in colunas:
                conn.execute(sqlalchemy.text('ALTER TABLE users ADD COLUMN cupom_indicacao VARCHAR(20)'))
            if 'indicado_por' not in colunas:
                conn.execute(sqlalchemy.text('ALTER TABLE users ADD COLUMN indicado_por INTEGER'))
            if 'meses_gratis' not in colunas:
                conn.execute(sqlalchemy.text('ALTER TABLE users ADD COLUMN meses_gratis INTEGER DEFAULT 0'))
            if 'plano_expira' not in colunas:
                conn.execute(sqlalchemy.text('ALTER TABLE users ADD COLUMN plano_expira DATETIME'))
            if 'auto_send_whatsapp' not in colunas:
                conn.execute(sqlalchemy.text('ALTER TABLE users ADD COLUMN auto_send_whatsapp BOOLEAN DEFAULT 0'))
            if 'email_remetente' not in colunas:
                conn.execute(sqlalchemy.text("ALTER TABLE users ADD COLUMN email_remetente VARCHAR(200) DEFAULT ''"))
            if 'nome_remetente' not in colunas:
                conn.execute(sqlalchemy.text("ALTER TABLE users ADD COLUMN nome_remetente VARCHAR(200) DEFAULT ''"))
            if 'smtp_host' not in colunas:
                conn.execute(sqlalchemy.text("ALTER TABLE users ADD COLUMN smtp_host VARCHAR(200) DEFAULT ''"))
            if 'smtp_port' not in colunas:
                conn.execute(sqlalchemy.text("ALTER TABLE users ADD COLUMN smtp_port INTEGER DEFAULT 587"))
            if 'smtp_user' not in colunas:
                conn.execute(sqlalchemy.text("ALTER TABLE users ADD COLUMN smtp_user VARCHAR(200) DEFAULT ''"))
            if 'smtp_pass_enc' not in colunas:
                conn.execute(sqlalchemy.text("ALTER TABLE users ADD COLUMN smtp_pass_enc TEXT DEFAULT ''"))
            if 'smtp_from' not in colunas:
                conn.execute(sqlalchemy.text("ALTER TABLE users ADD COLUMN smtp_from VARCHAR(200) DEFAULT ''"))

    # Migrar tabela lojas
    if 'lojas' in inspector.get_table_names():
        colunas_lojas = [c['name'] for c in inspector.get_columns('lojas')]
        with db.engine.begin() as conn:
            if 'notas_pendentes' not in colunas_lojas:
                conn.execute(sqlalchemy.text('ALTER TABLE lojas ADD COLUMN notas_pendentes INTEGER DEFAULT 0'))
            if 'etiquetas_pendentes' not in colunas_lojas:
                conn.execute(sqlalchemy.text('ALTER TABLE lojas ADD COLUMN etiquetas_pendentes INTEGER DEFAULT 0'))

    # Migrar contatos WhatsApp
    if 'whatsapp_contacts' in inspector.get_table_names():
        cols = [c['name'] for c in inspector.get_columns('whatsapp_contacts')]
        with db.engine.begin() as conn:
            if 'lojas_json' not in cols:
                conn.execute(sqlalchemy.text("ALTER TABLE whatsapp_contacts ADD COLUMN lojas_json TEXT DEFAULT '[]'"))
            if 'grupos_json' not in cols:
                conn.execute(sqlalchemy.text("ALTER TABLE whatsapp_contacts ADD COLUMN grupos_json TEXT DEFAULT '[]'"))

    # Migrar contatos de email
    if 'email_contacts' in inspector.get_table_names():
        cols = [c['name'] for c in inspector.get_columns('email_contacts')]
        with db.engine.begin() as conn:
            if 'lojas_json' not in cols:
                conn.execute(sqlalchemy.text("ALTER TABLE email_contacts ADD COLUMN lojas_json TEXT DEFAULT '[]'"))
            if 'grupos_json' not in cols:
                conn.execute(sqlalchemy.text("ALTER TABLE email_contacts ADD COLUMN grupos_json TEXT DEFAULT '[]'"))

    # Migrar agendamentos
    if 'schedules' in inspector.get_table_names():
        cols = [c['name'] for c in inspector.get_columns('schedules')]
        with db.engine.begin() as conn:
            if 'lojas_json' not in cols:
                conn.execute(sqlalchemy.text("ALTER TABLE schedules ADD COLUMN lojas_json TEXT DEFAULT '[]'"))
            if 'grupos_json' not in cols:
                conn.execute(sqlalchemy.text("ALTER TABLE schedules ADD COLUMN grupos_json TEXT DEFAULT '[]'"))

# Criar tabelas
with app.app_context():
    _migrate_db()
    db.create_all()

# Inicializar scheduler de automacao
beka_scheduler.init_app(app)

# Emails com acesso vitalicio (plano empresarial permanente)
EMAILS_VITALICIO = [
    "charlescr9@gmail.com",
]

with app.app_context():
    for email_vip in EMAILS_VITALICIO:
        user_vip = User.query.filter_by(email=email_vip).first()
        if user_vip and user_vip.plano != "empresarial":
            user_vip.plano = "empresarial"
            db.session.commit()
            print(f"Acesso vitalicio ativado para {email_vip}")


# Verificar sessao valida em TODA request protegida
@jwt.additional_claims_loader
def add_claims(identity):
    return {}


@jwt.token_in_blocklist_loader
def check_session_valid(jwt_header, jwt_payload):
    """Retorna True se o token NAO e mais valido (sessao removida)."""
    token_id = jwt_payload.get("sid", "")
    if not token_id:
        return False  # tokens antigos sem sid passam
    user_id = jwt_payload.get("sub", "")
    if not user_id:
        return False
    try:
        sessao = Session.query.filter_by(user_id=int(user_id), token_id=token_id).first()
        return sessao is None  # True = bloqueado (sessao nao existe mais)
    except Exception:
        # Se o banco estiver com lock (ex: callback Shopee escrevendo),
        # nao bloquear o token — permitir a request.
        return False

# ----------------------------------------------------------------
# ESTADO POR USUARIO (em memoria)
# ----------------------------------------------------------------
estados = {}  # {user_id: {processando, logs, ultimo_resultado, ...}}


def _get_estado(user_id):
    """Retorna o estado do usuario, criando se nao existir."""
    uid = int(user_id)
    if uid not in estados:
        user = User.query.get(uid)
        if not user:
            return None
        estados[uid] = {
            "processando": False,
            "logs": [],
            "ultimo_resultado": None,
            "historico": [],
            "agrupamentos": [],
            "configuracoes": {
                "pasta_entrada": user.get_pasta_entrada(),
                "pasta_saida": user.get_pasta_saida(),
                "pasta_lucro": user.get_pasta_lucro(),
                "largura_mm": 150,
                "altura_mm": 230,
                "margem_esq": 8,
                "margem_dir": 8,
                "margem_topo": 5,
                "margem_inf": 5,
                "fonte_produto": 7,
                "exibicao_produto": "sku",
                "perc_declarado": 100,
                "taxa_shopee": 18,
                "imposto_simples": 4,
                "custo_fixo": 3.0,
                "planilha_custos": "",
                "lucro_por_loja": {},
            }
        }
        # Tentar carregar config salva
        _carregar_config_usuario(uid)
    return estados[uid]


def _config_path(user_id):
    """Caminho do arquivo de config do usuario."""
    user = User.query.get(int(user_id))
    if not user:
        return None
    pasta = user.get_pasta_entrada()
    return os.path.join(pasta, "_config.json")


def _agrupamentos_path(user_id):
    """Caminho do arquivo de agrupamentos persistidos do usuario."""
    user = User.query.get(int(user_id))
    if not user:
        return None
    pasta = user.get_pasta_entrada()
    return os.path.join(pasta, "_agrupamentos.json")


def _salvar_config_usuario(user_id):
    """Salva config do usuario em JSON."""
    try:
        estado = estados.get(int(user_id))
        if not estado:
            return
        path = _config_path(user_id)
        if path:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(estado["configuracoes"], f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Aviso: nao salvou config user {user_id}: {e}")


def _salvar_agrupamentos_usuario(user_id):
    """Salva agrupamentos do usuario em JSON separado (persistente)."""
    try:
        estado = estados.get(int(user_id))
        if not estado:
            return
        path = _agrupamentos_path(user_id)
        if path:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(estado.get("agrupamentos", []) or [], f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Aviso: nao salvou agrupamentos user {user_id}: {e}")


def _carregar_agrupamentos_usuario(user_id):
    """Carrega agrupamentos persistidos do usuario (se existir)."""
    try:
        path = _agrupamentos_path(user_id)
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                grupos = json.load(f)
            estado = estados.get(int(user_id))
            if estado and isinstance(grupos, list):
                estado["agrupamentos"] = grupos
    except Exception as e:
        print(f"Aviso: nao carregou agrupamentos user {user_id}: {e}")


def _carregar_config_usuario(user_id):
    """Carrega config do usuario se existir."""
    try:
        path = _config_path(user_id)
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                config_salva = json.load(f)
            estado = estados.get(int(user_id))
            if estado:
                for chave, valor in config_salva.items():
                    if chave in estado["configuracoes"]:
                        estado["configuracoes"][chave] = valor
    except Exception as e:
        print(f"Aviso: nao carregou config user {user_id}: {e}")
    # Carregar agrupamentos persistidos
    _carregar_agrupamentos_usuario(user_id)
    # Carregar ultimo_resultado salvo em disco
    _carregar_resultado_usuario(user_id)


def _resultado_path(user_id):
    """Caminho do arquivo de resultado do usuario."""
    user = User.query.get(int(user_id))
    if not user:
        return None
    pasta = user.get_pasta_saida()
    return os.path.join(pasta, "_ultimo_resultado.json")


def _user_data_root(user_id):
    """Diretorio raiz de dados do usuario (irmao de entrada/saida)."""
    user = User.query.get(int(user_id))
    if not user:
        return None
    pasta_entrada = user.get_pasta_entrada()
    return os.path.dirname(pasta_entrada)


def _historico_geradas_dir(user_id):
    """Pasta persistente para snapshots de geracoes (ultimas 24h)."""
    root = _user_data_root(user_id)
    if not root:
        return None
    pasta = os.path.join(root, "historico_geradas")
    os.makedirs(pasta, exist_ok=True)
    return pasta


def _historico_geradas_index_path(user_id):
    pasta = _historico_geradas_dir(user_id)
    if not pasta:
        return None
    return os.path.join(pasta, "_index.json")


def _carregar_historico_geradas_raw(user_id):
    """Carrega indice bruto do historico de geracoes."""
    path = _historico_geradas_index_path(user_id)
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []


def _salvar_historico_geradas_raw(user_id, itens):
    """Salva indice bruto do historico de geracoes."""
    path = _historico_geradas_index_path(user_id)
    if not path:
        return
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(itens or [], f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Aviso: nao salvou historico de geradas user {user_id}: {e}")


def _parse_iso_dt(value):
    """Parse ISO date flexivel, com fallback seguro."""
    if not value:
        return None
    try:
        txt = str(value).strip()
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        dt = datetime.fromisoformat(txt)
        if getattr(dt, "tzinfo", None) is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _parse_stamp_dt(value):
    """Extrai datetime a partir de padroes YYYYMMDD_HHMMSS em nomes."""
    if not value:
        return None
    try:
        m = _re.search(r"(\d{8})_(\d{6})", str(value))
        if not m:
            return None
        return datetime.strptime(f"{m.group(1)}_{m.group(2)}", "%Y%m%d_%H%M%S")
    except Exception:
        return None


def _zipar_pasta(origem, destino_zip, ignorar_underscore=True):
    """Compacta pasta inteira em ZIP; retorna quantidade de arquivos adicionados."""
    import zipfile

    arquivos = 0
    with zipfile.ZipFile(destino_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(origem):
            for f in files:
                if ignorar_underscore and f.startswith("_"):
                    continue
                fp = os.path.join(root, f)
                if not os.path.isfile(fp):
                    continue
                arc = os.path.relpath(fp, origem)
                zf.write(fp, arc)
                arquivos += 1
    if arquivos <= 0:
        try:
            if os.path.exists(destino_zip):
                os.remove(destino_zip)
        except Exception:
            pass
    return arquivos


def _reconstruir_historico_geradas(user_id, horas=24):
    """
    Reconstroi entradas faltantes do historico com base em arquivos reais.

    Fontes:
    - ZIPs ja existentes em historico_geradas
    - Lotes em entrada/_upseller_lotes/*
    - Snapshot da pasta atual "Etiquetas prontas"
    """
    pasta_hist = _historico_geradas_dir(user_id)
    user = User.query.get(int(user_id))
    if not pasta_hist or not user:
        return 0

    horas = max(1, int(horas or 24))
    cutoff = datetime.utcnow() - timedelta(hours=horas)
    itens = _carregar_historico_geradas_raw(user_id)
    by_arquivo = {}
    for it in itens:
        arq = os.path.basename(str((it or {}).get("arquivo", "")).strip())
        if arq:
            by_arquivo[arq] = it

    novos = 0

    def _registrar_zip_existente(caminho_zip, dt_hint=None, origem_hint="recuperado"):
        nonlocal novos, itens, by_arquivo

        if not caminho_zip or not os.path.exists(caminho_zip):
            return
        arquivo = os.path.basename(caminho_zip)
        if not arquivo.lower().endswith(".zip"):
            return
        if arquivo in by_arquivo:
            return

        dt = dt_hint or _parse_stamp_dt(arquivo)
        if dt is None:
            try:
                dt = datetime.utcfromtimestamp(os.path.getmtime(caminho_zip))
            except Exception:
                dt = None
        if dt is None or dt < cutoff:
            return

        try:
            size = int(os.path.getsize(caminho_zip) or 0)
        except Exception:
            size = 0
        if size <= 0:
            return

        stamp_token = dt.strftime("%Y%m%d_%H%M%S_%f")
        item = {
            "id": f"r{stamp_token}",
            "created_at": dt.isoformat() + "Z",
            "arquivo": arquivo,
            "origem": origem_hint,
            "total_lojas": 0,
            "total_etiquetas": 0,
            "size": size,
            "timestamp_resultado": "",
        }
        itens.append(item)
        by_arquivo[arquivo] = item
        novos += 1

    # 1) Reindexar ZIPs ja presentes na pasta de historico
    try:
        for nome in os.listdir(pasta_hist):
            if nome.lower().endswith(".zip"):
                _registrar_zip_existente(os.path.join(pasta_hist, nome), origem_hint="historico_orfao")
    except Exception:
        pass

    # 2) Recriar snapshots por lote (entrada/_upseller_lotes)
    try:
        pasta_lotes = os.path.join(user.get_pasta_entrada(), "_upseller_lotes")
        if os.path.isdir(pasta_lotes):
            for nome_dir in sorted(os.listdir(pasta_lotes), reverse=True):
                dir_lote = os.path.join(pasta_lotes, nome_dir)
                if not os.path.isdir(dir_lote):
                    continue
                dt_lote = _parse_stamp_dt(nome_dir)
                if dt_lote is None:
                    try:
                        dt_lote = datetime.utcfromtimestamp(os.path.getmtime(dir_lote))
                    except Exception:
                        dt_lote = None
                if dt_lote is None or dt_lote < cutoff:
                    continue

                zip_nome = f"recuperado_{nome_dir}.zip"
                zip_path = os.path.join(pasta_hist, zip_nome)
                if not os.path.exists(zip_path):
                    arquivos = _zipar_pasta(dir_lote, zip_path, ignorar_underscore=True)
                    if arquivos <= 0:
                        continue
                _registrar_zip_existente(zip_path, dt_hint=dt_lote, origem_hint="recuperado_lote")
    except Exception as e:
        print(f"Aviso: erro ao reconstruir historico por lotes user {user_id}: {e}")

    # 3) Snapshot da pasta de saida atual (caso tenha arquivos recentes ainda nao indexados)
    try:
        pasta_saida = user.get_pasta_saida()
        arquivos_saida = []
        if os.path.isdir(pasta_saida):
            for root, _dirs, files in os.walk(pasta_saida):
                for f in files:
                    if f.startswith("_"):
                        continue
                    fp = os.path.join(root, f)
                    if not os.path.isfile(fp):
                        continue
                    mt = datetime.utcfromtimestamp(os.path.getmtime(fp))
                    if mt >= cutoff:
                        arquivos_saida.append((fp, mt))
        if arquivos_saida:
            dt_saida = max(mt for _fp, mt in arquivos_saida)
            zip_nome = f"recuperado_saida_{dt_saida.strftime('%Y%m%d_%H%M%S')}.zip"
            zip_path = os.path.join(pasta_hist, zip_nome)
            if not os.path.exists(zip_path):
                arquivos = _zipar_pasta(pasta_saida, zip_path, ignorar_underscore=True)
                if arquivos > 0:
                    _registrar_zip_existente(zip_path, dt_hint=dt_saida, origem_hint="recuperado_saida")
            else:
                _registrar_zip_existente(zip_path, dt_hint=dt_saida, origem_hint="recuperado_saida")
    except Exception as e:
        print(f"Aviso: erro ao reconstruir historico da pasta de saida user {user_id}: {e}")

    if novos > 0:
        itens.sort(key=lambda x: str((x or {}).get("created_at", "")), reverse=True)
        _salvar_historico_geradas_raw(user_id, itens)
    return novos


def _limpar_historico_geradas_expirado(user_id, horas=24):
    """
    Remove registros/arquivos expirados do historico.

    Regra: manter somente ultimas `horas` (padrao 24h) e arquivos existentes.
    """
    pasta = _historico_geradas_dir(user_id)
    if not pasta:
        return []

    cutoff = datetime.utcnow() - timedelta(hours=max(1, int(horas or 24)))
    itens = _carregar_historico_geradas_raw(user_id)
    kept = []

    for it in itens:
        arquivo = (it or {}).get("arquivo", "")
        if not arquivo:
            continue
        caminho = os.path.join(pasta, os.path.basename(arquivo))
        dt = _parse_iso_dt((it or {}).get("created_at"))

        # Expirado ou sem data => remover registro e arquivo.
        expired = (dt is None) or (dt < cutoff)
        missing = not os.path.exists(caminho)
        if expired or missing:
            if os.path.exists(caminho):
                try:
                    os.remove(caminho)
                except Exception:
                    pass
            continue

        kept.append(it)

    _salvar_historico_geradas_raw(user_id, kept)
    return kept


def _registrar_historico_gerada(user_id, resultado, pasta_saida, origem="processamento"):
    """
    Cria snapshot ZIP da geracao e registra no historico persistente (24h).
    """
    import zipfile

    if not resultado or not pasta_saida or not os.path.exists(pasta_saida):
        return None

    pasta_hist = _historico_geradas_dir(user_id)
    if not pasta_hist:
        return None

    _limpar_historico_geradas_expirado(user_id, horas=24)

    stamp = datetime.utcnow()
    stamp_token = stamp.strftime("%Y%m%d_%H%M%S_%f")
    nome_zip = f"geradas_{stamp.strftime('%Y%m%d_%H%M%S')}.zip"
    caminho_zip = os.path.join(pasta_hist, nome_zip)

    arquivos = 0
    try:
        with zipfile.ZipFile(caminho_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _dirs, files in os.walk(pasta_saida):
                for f in files:
                    if f.startswith("_"):
                        continue
                    fp = os.path.join(root, f)
                    if not os.path.isfile(fp):
                        continue
                    arc = os.path.relpath(fp, pasta_saida)
                    zf.write(fp, arc)
                    arquivos += 1
    except Exception as e:
        print(f"Aviso: nao criou ZIP de historico user {user_id}: {e}")
        return None

    if arquivos == 0:
        try:
            if os.path.exists(caminho_zip):
                os.remove(caminho_zip)
        except Exception:
            pass
        return None

    size = os.path.getsize(caminho_zip) if os.path.exists(caminho_zip) else 0
    entry_id = f"h{stamp_token}"
    item = {
        "id": entry_id,
        "created_at": stamp.isoformat() + "Z",
        "arquivo": nome_zip,
        "origem": origem,
        "total_lojas": int((resultado or {}).get("total_lojas", 0) or 0),
        "total_etiquetas": int((resultado or {}).get("total_etiquetas", 0) or 0),
        "size": int(size or 0),
        "timestamp_resultado": (resultado or {}).get("timestamp", ""),
    }

    itens = _carregar_historico_geradas_raw(user_id)
    itens.insert(0, item)
    _salvar_historico_geradas_raw(user_id, itens)
    _limpar_historico_geradas_expirado(user_id, horas=24)
    return item


def _listar_historico_geradas(user_id, horas=24):
    """Retorna historico pronto para UI (somente janela desejada)."""
    pasta_hist = _historico_geradas_dir(user_id)
    if not pasta_hist:
        return []

    horas = max(1, int(horas or 24))
    _reconstruir_historico_geradas(user_id, horas=horas)
    _limpar_historico_geradas_expirado(user_id, horas=horas)
    itens = _carregar_historico_geradas_raw(user_id)
    cutoff = datetime.utcnow() - timedelta(hours=horas)

    out = []
    for it in itens:
        arquivo = (it or {}).get("arquivo", "")
        if not arquivo:
            continue
        caminho = os.path.join(pasta_hist, os.path.basename(arquivo))
        if not os.path.exists(caminho):
            continue

        dt = _parse_iso_dt((it or {}).get("created_at"))
        if dt is None or dt < cutoff:
            continue

        size = int((it or {}).get("size", 0) or 0)
        if size <= 0:
            try:
                size = os.path.getsize(caminho)
            except Exception:
                size = 0

        out.append({
            "id": (it or {}).get("id", ""),
            "created_at": (it or {}).get("created_at", ""),
            "created_at_fmt": dt.strftime("%d/%m/%Y %H:%M") if dt else "",
            "arquivo": os.path.basename(arquivo),
            "origem": (it or {}).get("origem", ""),
            "total_lojas": int((it or {}).get("total_lojas", 0) or 0),
            "total_etiquetas": int((it or {}).get("total_etiquetas", 0) or 0),
            "size": size,
            "size_fmt": _formatar_tamanho(size),
            "download_url": f"/api/historico-geradas/download/{(it or {}).get('id', '')}",
        })

    # Mais recente primeiro
    out.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return out


def _salvar_resultado_usuario(user_id):
    """Salva ultimo_resultado do usuario em JSON no volume persistente."""
    try:
        estado = estados.get(int(user_id))
        if not estado or not estado.get("ultimo_resultado"):
            return
        path = _resultado_path(user_id)
        if path:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(estado["ultimo_resultado"], f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Aviso: nao salvou resultado user {user_id}: {e}")


def _carregar_resultado_usuario(user_id):
    """Carrega ultimo_resultado do usuario se existir no disco."""
    try:
        path = _resultado_path(user_id)
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                resultado_salvo = json.load(f)
            estado = estados.get(int(user_id))
            if estado and not estado["ultimo_resultado"]:
                estado["ultimo_resultado"] = resultado_salvo
    except Exception as e:
        print(f"Aviso: nao carregou resultado user {user_id}: {e}")
    # Carregar ultimo_lucro salvo em disco
    _carregar_lucro_usuario(user_id)


def _lucro_path(user_id):
    """Caminho do arquivo de lucro do usuario."""
    user = User.query.get(int(user_id))
    if not user:
        return None
    pasta = user.get_pasta_saida()
    return os.path.join(pasta, "_ultimo_lucro.json")


def _salvar_lucro_usuario(user_id):
    """Salva ultimo_lucro do usuario em JSON no volume persistente."""
    try:
        estado = estados.get(int(user_id))
        if not estado or not estado.get("ultimo_lucro"):
            return
        path = _lucro_path(user_id)
        if path:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(estado["ultimo_lucro"], f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Aviso: nao salvou lucro user {user_id}: {e}")


def _carregar_lucro_usuario(user_id):
    """Carrega ultimo_lucro do usuario se existir no disco."""
    try:
        path = _lucro_path(user_id)
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                lucro_salvo = json.load(f)
            estado = estados.get(int(user_id))
            if estado and not estado.get("ultimo_lucro"):
                estado["ultimo_lucro"] = lucro_salvo
    except Exception as e:
        print(f"Aviso: nao carregou lucro user {user_id}: {e}")


def adicionar_log(estado, msg, tipo="info"):
    """Adiciona mensagem ao log do usuario."""
    estado["logs"].append({
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "mensagem": msg,
        "tipo": tipo
    })
    if len(estado["logs"]) > 500:
        estado["logs"] = estado["logs"][-500:]


# ----------------------------------------------------------------
# ROTAS PUBLICAS
# ----------------------------------------------------------------

@app.route('/version')
def version():
    """Retorna versão do código em execução."""
    version_info = {
        'version': '2026-02-17-12:15',
        'build': 'avisos-visíveis-web-ui',
        'features': [
            'Avisos aparecem na interface web',
            'Etiquetas de RETIRADA separadas em PDF próprio',
            'Avisos de etiquetas sem XML',
            'Declaração de conteúdo desabilitada',
            'PyMuPDF 1.24.14 fixado'
        ]
    }
    # Tentar ler arquivo VERSION se existir
    try:
        with open('VERSION', 'r') as f:
            version_info['file'] = f.read().strip()
    except:
        pass
    return jsonify(version_info)

@app.route('/')
def index():
    """Serve o dashboard (verifica login no frontend)."""
    import sys
    print(f"[ROTA /] method={request.method} args={dict(request.args)} url={request.url} remote={request.remote_addr} host={request.host}", flush=True, file=sys.stderr)

    # Fallback: se a Shopee redirecionar para / com code+shop_id (sem o path do callback),
    # encaminhar para o callback real.
    code = request.args.get("code", "")
    shop_id_arg = request.args.get("shop_id", "")
    if code and shop_id_arg:
        from urllib.parse import urlencode
        qs = urlencode(request.args)
        print(f"[ROTA /] FALLBACK SHOPEE -> redirecionando para /api/marketplace/shopee/callback?{qs}", flush=True, file=sys.stderr)
        return redirect(f"/api/marketplace/shopee/callback?{qs}")

    resp = send_from_directory('static', 'index.html')
    resp.cache_control.no_store = True
    resp.cache_control.no_cache = True
    resp.cache_control.must_revalidate = True
    resp.cache_control.max_age = 0
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    resp.headers['X-Beka-Ui-Version'] = 'assinante-v2'
    return resp


@app.route('/login')
def login_page():
    resp = send_from_directory('static', 'login.html')
    resp.cache_control.no_store = True
    resp.cache_control.no_cache = True
    resp.cache_control.must_revalidate = True
    resp.cache_control.max_age = 0
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    resp.headers['X-Beka-Ui-Version'] = 'assinante-v2'
    return resp


# ----------------------------------------------------------------
# ROTAS DA API (PROTEGIDAS COM JWT)
# ----------------------------------------------------------------

@app.route('/api/status')
@jwt_required()
def api_status():
    """Retorna o status atual do usuario."""
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    # Arquivos de etiquetas (pasta_entrada)
    pasta = estado["configuracoes"]["pasta_entrada"]
    arquivos = []
    if os.path.exists(pasta):
        for f in os.listdir(pasta):
            if f.startswith('_'):
                continue
            fp = os.path.join(pasta, f)
            if os.path.isfile(fp):
                ext = os.path.splitext(f)[1].lower()
                if ext in ('.pdf', '.xlsx', '.xls'):
                    tipo_arq = "PDF" if ext == '.pdf' else "XLSX"
                    tamanho = os.path.getsize(fp)
                    arquivos.append({
                        "nome": f,
                        "tipo": tipo_arq,
                        "tamanho": tamanho,
                        "tamanho_fmt": _formatar_tamanho(tamanho),
                    })

    # Arquivos de lucro (pasta_lucro) - separados
    pasta_lucro = estado["configuracoes"].get("pasta_lucro", "")
    arquivos_lucro = []
    if pasta_lucro and os.path.exists(pasta_lucro):
        for f in os.listdir(pasta_lucro):
            if f.startswith('_'):
                continue
            fp = os.path.join(pasta_lucro, f)
            if os.path.isfile(fp):
                ext = os.path.splitext(f)[1].lower()
                if ext in ('.zip', '.xml', '.xlsx', '.xls'):
                    if f == 'planilha_custos.xlsx':
                        tipo_arq = "CUSTOS"
                    elif ext == '.zip':
                        tipo_arq = "ZIP"
                    elif ext == '.xml':
                        tipo_arq = "XML"
                    else:
                        tipo_arq = "XLSX"
                    tamanho = os.path.getsize(fp)
                    arquivos_lucro.append({
                        "nome": f,
                        "tipo": tipo_arq,
                        "tamanho": tamanho,
                        "tamanho_fmt": _formatar_tamanho(tamanho),
                    })

    saidas = []
    pasta_saida = estado["configuracoes"]["pasta_saida"]
    if os.path.exists(pasta_saida):
        for loja in os.listdir(pasta_saida):
            pasta_loja = os.path.join(pasta_saida, loja)
            if os.path.isdir(pasta_loja):
                arquivos_loja = os.listdir(pasta_loja)
                saidas.append({
                    "loja": loja,
                    "arquivos": len(arquivos_loja),
                    "nomes": arquivos_loja,
                })

    # Info do usuario
    user = User.query.get(int(user_id))

    return jsonify({
        "processando": estado["processando"],
        "arquivos_entrada": arquivos,
        "arquivos_lucro": arquivos_lucro,
        "saidas": saidas,
        "ultimo_resultado": estado["ultimo_resultado"],
        "configuracoes": estado["configuracoes"],
        "agrupamentos": estado["agrupamentos"],
        "ultimo_lucro": estado.get("ultimo_lucro"),
        "user": user.to_dict() if user else None,
    })


@app.route('/api/logs')
@jwt_required()
def api_logs():
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    desde = request.args.get('desde', 0, type=int)
    return jsonify({
        "logs": estado["logs"][desde:],
        "total": len(estado["logs"]),
    })


@app.route('/api/escanear-lojas', methods=['POST'])
@jwt_required()
def api_escanear_lojas():
    """Escaneia PDFs enviados para identificar lojas sem processar tudo.

    Usa o processador real sem recorte de forma lightweight para
    extrair CNPJs e nomes de loja, sem gerar PDFs/XLSX de saida.
    """
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    pasta_entrada = estado["configuracoes"]["pasta_entrada"]
    if not os.path.exists(pasta_entrada):
        return jsonify({"erro": "Pasta de entrada nao encontrada"}), 404

    # Verificar se ha PDFs na pasta
    tem_pdf = any(f.lower().endswith('.pdf') and not f.startswith('_')
                  for f in os.listdir(pasta_entrada) if os.path.isfile(os.path.join(pasta_entrada, f)))
    if not tem_pdf:
        return jsonify({"erro": "Nenhum PDF encontrado na pasta de entrada"}), 400

    try:
        proc = ProcessadorEtiquetasShopee()
        proc.carregar_todos_xlsx(pasta_entrada)
        todas_etiquetas, cpf_auto, pdfs_shein = proc.carregar_todos_pdfs_sem_recorte(pasta_entrada)

        # Juntar CPF auto-detectadas
        etiquetas_cpf = proc.processar_cpf(pasta_entrada)
        etiquetas_cpf.extend(cpf_auto)
        if etiquetas_cpf:
            todas_etiquetas.extend(etiquetas_cpf)

        # Separar por loja
        lojas_dict = proc.separar_por_loja(todas_etiquetas)

        lojas = []
        for cnpj, etqs in lojas_dict.items():
            nome = proc.get_nome_loja(cnpj)
            lojas.append({"cnpj": cnpj, "nome": nome})

        # Adicionar Shein se houver
        if pdfs_shein:
            shein_etqs = proc.processar_shein(pasta_entrada, pdfs_extras=pdfs_shein)
            if shein_etqs:
                # Agrupar por cnpj das etiquetas shein
                for etq in shein_etqs:
                    cnpj_s = etq.get('cnpj', 'SHEIN')
                    if not any(l['cnpj'] == cnpj_s for l in lojas):
                        nome_s = proc.get_nome_loja(cnpj_s)
                        lojas.append({"cnpj": cnpj_s, "nome": nome_s})

        lojas.sort(key=lambda x: x["nome"])
        return jsonify({"lojas": lojas})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"erro": f"Erro ao escanear: {str(e)}"}), 500


@app.route('/api/processar', methods=['POST'])
@jwt_required()
def api_processar():
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    if estado["processando"]:
        return jsonify({"erro": "Processamento ja em andamento"}), 409

    # Verificar limite do plano
    user = User.query.get(int(user_id))
    if not user.pode_processar():
        info = user.get_plano_info()
        return jsonify({
            "erro": f"Limite de {info['limite_proc']} processamentos/mes atingido. Faca upgrade do plano!"
        }), 403

    thread = threading.Thread(
        target=_executar_processamento,
        args=(int(user_id),),
        kwargs={"sem_recorte": True, "resumo_sku_somente": False},
    )
    thread.daemon = True
    thread.start()

    return jsonify({"mensagem": "Processamento iniciado"})


@app.route('/api/configuracoes', methods=['POST'])
@jwt_required()
def api_configuracoes():
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    dados = request.get_json()
    if dados:
        for chave, valor in dados.items():
            if chave in estado["configuracoes"] and chave not in ('pasta_entrada', 'pasta_saida'):
                estado["configuracoes"][chave] = valor
        _salvar_config_usuario(user_id)
        adicionar_log(estado, "Configuracoes atualizadas e salvas", "success")
    return jsonify(estado["configuracoes"])


@app.route('/api/configuracoes-lucro-lojas', methods=['GET', 'POST'])
@jwt_required()
def api_config_lucro_lojas():
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    if request.method == 'GET':
        return jsonify({
            "defaults": {
                "perc_declarado": estado["configuracoes"].get("perc_declarado", 100),
                "taxa_shopee": estado["configuracoes"].get("taxa_shopee", 18),
                "imposto_simples": estado["configuracoes"].get("imposto_simples", 4),
                "custo_fixo": estado["configuracoes"].get("custo_fixo", 3),
            },
            "por_loja": estado["configuracoes"].get("lucro_por_loja", {}),
        })
    dados = request.get_json()
    if dados:
        estado["configuracoes"]["lucro_por_loja"] = dados.get("por_loja", {})
        _salvar_config_usuario(user_id)
        adicionar_log(estado, "Config lucro por loja atualizada", "success")
    return jsonify({"ok": True})


@app.route('/api/historico')
@jwt_required()
def api_historico():
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    return jsonify({"historico": estado["historico"]})


@app.route('/api/historico-geradas', methods=['GET'])
@jwt_required()
def api_historico_geradas():
    """
    Lista snapshots de geracoes disponiveis para download (janela padrao: 24h).
    """
    user_id = int(get_jwt_identity())
    horas = request.args.get("horas", 24, type=int)
    horas = 24 if not horas else max(1, min(int(horas), 168))
    itens = _listar_historico_geradas(user_id, horas=horas)
    return jsonify({
        "historico": itens,
        "janela_horas": horas,
        "total": len(itens),
    })


@app.route('/api/historico-geradas/download/<item_id>', methods=['GET'])
@jwt_required()
def api_historico_geradas_download(item_id):
    """
    Download de um snapshot de geracao do historico (ultimas 24h).
    """
    user_id = int(get_jwt_identity())
    _limpar_historico_geradas_expirado(user_id, horas=24)
    itens = _carregar_historico_geradas_raw(user_id)
    match = next((it for it in itens if str((it or {}).get("id", "")) == str(item_id)), None)
    if not match:
        return jsonify({"erro": "Arquivo de historico nao encontrado ou expirado"}), 404

    dt = _parse_iso_dt((match or {}).get("created_at"))
    if dt is None or dt < (datetime.utcnow() - timedelta(hours=24)):
        return jsonify({"erro": "Arquivo expirado (janela maxima: 24h)"}), 410

    pasta = _historico_geradas_dir(user_id)
    if not pasta:
        return jsonify({"erro": "Pasta de historico indisponivel"}), 404

    arquivo = os.path.basename((match or {}).get("arquivo", ""))
    if not arquivo:
        return jsonify({"erro": "Arquivo invalido no historico"}), 404

    caminho = os.path.realpath(os.path.join(pasta, arquivo))
    base = os.path.realpath(pasta)
    if not caminho.startswith(base):
        return jsonify({"erro": "Caminho invalido"}), 400
    if not os.path.exists(caminho):
        return jsonify({"erro": "Arquivo nao encontrado"}), 404

    return send_file(
        caminho,
        as_attachment=True,
        download_name=arquivo,
        mimetype="application/zip",
    )


@app.route('/api/abrir-pasta', methods=['POST'])
@jwt_required()
def api_abrir_pasta():
    dados = request.get_json()
    pasta = dados.get('pasta', '')
    if pasta and os.path.exists(pasta):
        try:
            os.startfile(pasta)
        except AttributeError:
            pass
        return jsonify({"ok": True})
    return jsonify({"erro": "Pasta nao encontrada"}), 404


@app.route('/api/upload', methods=['POST'])
@jwt_required()
def api_upload():
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    if 'arquivo' not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado"}), 400

    arquivo = request.files['arquivo']
    if arquivo.filename == '':
        return jsonify({"erro": "Nome de arquivo vazio"}), 400

    ext = os.path.splitext(arquivo.filename)[1].lower()
    if ext not in ('.pdf', '.xlsx', '.xls'):
        return jsonify({"erro": "Tipo de arquivo nao suportado. Use PDF, XLSX ou XLS."}), 400

    pasta = estado["configuracoes"]["pasta_entrada"]
    caminho = os.path.join(pasta, arquivo.filename)
    arquivo.save(caminho)
    adicionar_log(estado, f"Arquivo recebido: {arquivo.filename}", "success")
    return jsonify({"mensagem": f"Arquivo {arquivo.filename} salvo com sucesso"})


@app.route('/api/remover-arquivo', methods=['POST'])
@jwt_required()
def api_remover_arquivo():
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    dados = request.get_json()
    nome = dados.get('nome', '')
    if not nome:
        return jsonify({"erro": "Nome nao informado"}), 400
    caminho = os.path.join(estado["configuracoes"]["pasta_entrada"], nome)
    if os.path.exists(caminho):
        os.remove(caminho)
        adicionar_log(estado, f"Arquivo removido: {nome}", "warning")
        return jsonify({"ok": True})
    return jsonify({"erro": "Arquivo nao encontrado"}), 404


@app.route('/api/upload-lucro', methods=['POST'])
@jwt_required()
def api_upload_lucro():
    """Upload de arquivos para calculadora de lucro (ZIP/XML)."""
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    if 'arquivo' not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado"}), 400

    arquivo = request.files['arquivo']
    if arquivo.filename == '':
        return jsonify({"erro": "Nome de arquivo vazio"}), 400

    ext = os.path.splitext(arquivo.filename)[1].lower()
    if ext not in ('.zip', '.xml'):
        return jsonify({"erro": "Tipo nao suportado. Use ZIP ou XML."}), 400

    pasta_lucro = estado["configuracoes"].get("pasta_lucro", "")
    if not pasta_lucro:
        return jsonify({"erro": "Pasta de lucro nao configurada"}), 500
    os.makedirs(pasta_lucro, exist_ok=True)

    caminho = os.path.join(pasta_lucro, arquivo.filename)
    arquivo.save(caminho)
    adicionar_log(estado, f"Arquivo lucro recebido: {arquivo.filename}", "success")
    return jsonify({"mensagem": f"Arquivo {arquivo.filename} salvo com sucesso"})


@app.route('/api/remover-arquivo-lucro', methods=['POST'])
@jwt_required()
def api_remover_arquivo_lucro():
    """Remove arquivo da pasta de lucro."""
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    dados = request.get_json()
    nome = dados.get('nome', '')
    if not nome:
        return jsonify({"erro": "Nome nao informado"}), 400
    pasta_lucro = estado["configuracoes"].get("pasta_lucro", "")
    if not pasta_lucro:
        return jsonify({"erro": "Pasta de lucro nao configurada"}), 500
    caminho = os.path.join(pasta_lucro, nome)
    if os.path.exists(caminho):
        os.remove(caminho)
        adicionar_log(estado, f"Arquivo lucro removido: {nome}", "warning")
        return jsonify({"ok": True})
    return jsonify({"erro": "Arquivo nao encontrado"}), 404


@app.route('/api/limpar-lucro', methods=['POST'])
@jwt_required()
def api_limpar_lucro():
    """Remove todos os arquivos da pasta de lucro."""
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    pasta_lucro = estado["configuracoes"].get("pasta_lucro", "")
    if not pasta_lucro or not os.path.exists(pasta_lucro):
        return jsonify({"ok": True})
    removidos = 0
    for f in os.listdir(pasta_lucro):
        if f.startswith('_') or f == 'planilha_custos.xlsx':
            continue
        fp = os.path.join(pasta_lucro, f)
        if os.path.isfile(fp):
            os.remove(fp)
            removidos += 1
    estado["ultimo_lucro"] = None
    adicionar_log(estado, f"Pasta de lucro limpa ({removidos} arquivos removidos)", "warning")
    return jsonify({"ok": True, "removidos": removidos})


@app.route('/api/limpar-saida', methods=['POST'])
@jwt_required()
def api_limpar_saida():
    import shutil
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    pasta = estado["configuracoes"]["pasta_saida"]
    if os.path.exists(pasta):
        shutil.rmtree(pasta)
        os.makedirs(pasta, exist_ok=True)
        adicionar_log(estado, "Pasta de saida limpa", "warning")
    return jsonify({"ok": True})


@app.route('/api/novo-lote', methods=['POST'])
@jwt_required()
def api_novo_lote():
    """Limpa todos os arquivos de entrada, saida e reseta o estado para novo processamento."""
    import shutil
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    if estado["processando"]:
        return jsonify({"erro": "Aguarde o processamento atual terminar"}), 409

    pasta_entrada = estado["configuracoes"]["pasta_entrada"]
    pasta_saida = estado["configuracoes"]["pasta_saida"]

    # Limpar pasta de entrada de etiquetas (exceto _config.json)
    # Pasta de lucro NAO e limpa (independente)
    if os.path.exists(pasta_entrada):
        for f in os.listdir(pasta_entrada):
            if f.startswith('_'):
                continue  # Preservar _config.json etc
            fp = os.path.join(pasta_entrada, f)
            if os.path.isfile(fp):
                os.remove(fp)

    # Limpar pasta de saida
    if os.path.exists(pasta_saida):
        shutil.rmtree(pasta_saida)
        os.makedirs(pasta_saida, exist_ok=True)

    # Resetar estado
    estado["ultimo_resultado"] = None
    estado["ultimo_lucro"] = None
    estado["logs"] = []
    estado["_etiquetas_por_cnpj"] = {}
    estado["_proc_config"] = {}

    # Limpar arquivos de resultado persistidos
    rp = _resultado_path(user_id)
    if rp and os.path.exists(rp):
        os.remove(rp)
    lp = _lucro_path(user_id)
    if lp and os.path.exists(lp):
        os.remove(lp)

    adicionar_log(estado, "Novo lote iniciado - arquivos limpos", "success")
    return jsonify({"ok": True, "mensagem": "Pronto para novo lote"})


@app.route('/api/download-todos')
@jwt_required()
def api_download_todos():
    """Gera um ZIP com todos os arquivos de saida (PDFs + XLSXs de todas as lojas)."""
    import zipfile
    import io
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    pasta_saida = estado["configuracoes"]["pasta_saida"]
    if not os.path.exists(pasta_saida):
        return jsonify({"erro": "Nenhum resultado disponivel"}), 404

    buf = io.BytesIO()
    arquivos_adicionados = 0
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(pasta_saida):
            for f in files:
                if f.startswith('_'):
                    continue
                filepath = os.path.join(root, f)
                arcname = os.path.relpath(filepath, pasta_saida)
                zf.write(filepath, arcname)
                arquivos_adicionados += 1

    if arquivos_adicionados == 0:
        return jsonify({"erro": "Nenhum arquivo de resultado encontrado"}), 404

    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name="Etiquetas_prontas.zip", mimetype="application/zip")


@app.route('/api/download/<loja>/<arquivo>')
@jwt_required()
def api_download(loja, arquivo):
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    pasta = os.path.join(estado["configuracoes"]["pasta_saida"], loja)
    caminho = os.path.join(pasta, arquivo)
    if os.path.exists(caminho):
        return send_file(caminho, as_attachment=True)
    return jsonify({"erro": "Arquivo nao encontrado"}), 404


@app.route('/api/download-resumo-geral')
@jwt_required()
def api_download_resumo_geral():
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    if not estado["ultimo_resultado"] or "resumo_geral" not in estado["ultimo_resultado"]:
        return jsonify({"erro": "Nenhum resumo geral disponivel"}), 404
    arquivo = estado["ultimo_resultado"]["resumo_geral"]["arquivo"]
    pasta = estado["configuracoes"]["pasta_saida"]
    caminho = os.path.join(pasta, arquivo)
    if os.path.exists(caminho):
        return send_file(caminho, as_attachment=True)
    return jsonify({"erro": "Arquivo nao encontrado"}), 404


@app.route('/api/exemplo-custos')
def api_exemplo_custos():
    """Gera e retorna um XLSX de exemplo para a planilha de custos."""
    import io
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Custos"

    header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True, size=11)
    thin_border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin")
    )

    ws.append(["SKU", "Custo Unitario (R$)"])
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")
        cell.border = thin_border

    exemplos = [
        ("PROD001", 12.50),
        ("PROD002", 8.90),
        ("ABC123", 15.00),
        ("KIT-A", 25.00),
        ("CAMISETA-P", 18.50),
        ("CAMISETA-M", 18.50),
        ("CAMISETA-G", 19.00),
        ("CANECA01", 7.80),
    ]
    for sku, custo in exemplos:
        ws.append([sku, custo])

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=2):
        for cell in row:
            cell.border = thin_border
            if cell.col_idx == 2:
                cell.number_format = 'R$ #,##0.00'

    ws.column_dimensions['A'].width = 20
    ws.column_dimensions['B'].width = 22

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name="exemplo_planilha_custos.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route('/api/upload-custos', methods=['POST'])
@jwt_required()
def api_upload_custos():
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    if 'arquivo' not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado"}), 400
    arquivo = request.files['arquivo']
    if arquivo.filename == '':
        return jsonify({"erro": "Nome de arquivo vazio"}), 400
    ext = os.path.splitext(arquivo.filename)[1].lower()
    if ext != '.xlsx':
        return jsonify({"erro": "Envie um arquivo .xlsx"}), 400

    pasta_lucro = estado["configuracoes"].get("pasta_lucro", "")
    if not pasta_lucro:
        return jsonify({"erro": "Pasta de lucro nao configurada"}), 500
    os.makedirs(pasta_lucro, exist_ok=True)
    caminho = os.path.join(pasta_lucro, "planilha_custos.xlsx")
    arquivo.save(caminho)
    estado["configuracoes"]["planilha_custos"] = caminho
    _salvar_config_usuario(user_id)
    adicionar_log(estado, f"Planilha de custos recebida: {arquivo.filename}", "success")
    return jsonify({"mensagem": "Planilha de custos salva", "caminho": caminho})


def _limpar_nome_loja(nome_raw):
    nome = _re.sub(r'^\d[\d.]+\s+', '', nome_raw)
    nome = _re.sub(r'\s+\d{11}$', '', nome)
    nome = _re.sub(r'\s+(LTDA|ME|MEI|EPP|EIRELI)\s*$', '', nome, flags=_re.IGNORECASE)
    nome = nome.strip().title()
    nome = _re.sub(r'[<>:"/\\|?*]', '', nome)
    return nome.strip() or 'Loja_Desconhecida'


def _formatar_cnpj_curto(cnpj):
    """Retorna a raiz do CNPJ formatada (8 primeiros digitos: XX.XXX.XXX)."""
    cnpj = str(cnpj).strip()
    if len(cnpj) >= 8:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}"
    return cnpj


def _extrair_loja_nfe(nfe, cnpj_loja_map=None):
    emit = nfe.get("emit", {})
    if isinstance(emit, str):
        return "Desconhecida"
    # Tentar usar nome da loja Shopee (mesmo mapeamento do processamento principal)
    cnpj = str(emit.get("CNPJ", "")).strip()
    if cnpj_loja_map and cnpj in cnpj_loja_map:
        return cnpj_loja_map[cnpj]
    nome_raw = str(emit.get("xNome", "")).strip()
    nome = _limpar_nome_loja(nome_raw) if nome_raw else "Desconhecida"
    # Incluir CNPJ curto para diferenciar lojas com mesmo nome empresarial
    if cnpj:
        return f"{_formatar_cnpj_curto(cnpj)} {nome}"
    return nome


def _extrair_sku_principal(sku_completo):
    """Extrai o SKU principal (base) de um SKU completo.
    Estrategia: remover do FINAL APENAS a ultima parte se for claramente variacao.
    Variacao = tamanho roupa (P/M/G/GG/XG/PP/XL) OU numero de 2 digitos (34-50 = tamanho).
    Ex: 'TEN-BO-BR-38' -> 'TEN-BO-BR' (38 e tamanho roupa)
        'PROD-AZUL-M'  -> 'PROD-AZUL' (M e tamanho)
        'SKU-01-42'    -> 'SKU-01'    (42 e tamanho)
        'TEN-BO-BR'    -> 'TEN-BO-BR' (BR nao e tamanho, mantem)
    """
    if not sku_completo:
        return sku_completo
    partes = sku_completo.split('-')
    if len(partes) <= 1:
        return sku_completo

    # Tamanhos de roupa conhecidos
    tamanhos_letra = {'P', 'M', 'G', 'PP', 'GG', 'XG', 'XS', 'XL', 'XXL', 'XXG', 'EG', 'EGG'}

    # Remover do final apenas partes que sao claramente variacao
    i = len(partes) - 1
    while i > 0:
        p = partes[i].strip().upper()
        # Tamanho letra (P, M, G, GG, etc.)
        if p in tamanhos_letra:
            i -= 1
            continue
        # Numero 2 digitos no range tipico de tamanho (24-56)
        if _re.match(r'^\d{2}$', p) and 24 <= int(p) <= 56:
            i -= 1
            continue
        # Numero 1 digito solto (1-9) - pode ser variacao de quantidade
        if _re.match(r'^\d$', p):
            i -= 1
            continue
        break
    base = partes[:i + 1]
    return '-'.join(base) if base else sku_completo


def _buscar_custo_inteligente(sku_xml, dict_custos, chaves_ordenadas):
    """Busca inteligente de custo: SKU principal, match exato, prefixo."""
    sku_upper = sku_xml.upper().strip()
    if not sku_upper:
        return 0.0, False

    # 1. Match exato do SKU completo
    if sku_upper in dict_custos:
        return dict_custos[sku_upper], True

    # 2. Extrair SKU principal (sem sufixos de variacao) e tentar match exato
    sku_base = _extrair_sku_principal(sku_upper)
    if sku_base != sku_upper and sku_base in dict_custos:
        return dict_custos[sku_base], True

    # 3. SKU da planilha comeca com o SKU base (planilha tem chave mais longa)
    for chave in chaves_ordenadas:
        if chave.startswith(sku_base):
            return dict_custos[chave], True

    # 4. SKU do XML comeca com chave da planilha (planilha tem chave mais curta)
    for chave in chaves_ordenadas:
        if sku_upper.startswith(chave):
            return dict_custos[chave], True

    # 5. SKU base comeca com chave da planilha
    if sku_base != sku_upper:
        for chave in chaves_ordenadas:
            if sku_base.startswith(chave):
                return dict_custos[chave], True

    return 0.0, False


def _processar_nfe_lucro(nfe, dict_custos, cfg, cfg_por_loja, chaves_ordenadas=None, cnpj_loja_map=None):
    nome_loja = _extrair_loja_nfe(nfe, cnpj_loja_map)
    cfg_loja = cfg_por_loja.get(nome_loja, {})
    perc_declarado = float(cfg_loja.get("perc_declarado", cfg.get("perc_declarado", 100))) / 100
    taxa_shopee = float(cfg_loja.get("taxa_shopee", cfg.get("taxa_shopee", 18))) / 100
    taxa_imposto = float(cfg_loja.get("imposto_simples", cfg.get("imposto_simples", 4))) / 100
    custo_fixo = float(cfg_loja.get("custo_fixo", cfg.get("custo_fixo", 3)))

    if chaves_ordenadas is None:
        chaves_ordenadas = sorted(dict_custos.keys(), key=len, reverse=True)

    dets = nfe.get("det", [])
    if not isinstance(dets, list):
        dets = [dets]

    # Extrair SKU principal (primeiro item da NF) para busca de custo
    sku_principal = ""
    if dets:
        prod_primeiro = dets[0].get("prod", {})
        sku_principal = str(prod_primeiro.get("cProd", "")).strip()

    # Buscar custo usando apenas o SKU principal
    c_principal_unit, encontrou_principal = _buscar_custo_inteligente(sku_principal, dict_custos, chaves_ordenadas)

    # Somar todos os itens da NF como uma unica linha usando o SKU principal
    qtd_total = 0
    v_declarado_total = 0
    for item in dets:
        prod = item.get("prod", {})
        qtd_total += float(prod.get("qCom", 1))
        v_declarado_total += float(prod.get("vProd", 0))

    c_produto_total = c_principal_unit * qtd_total

    v_real = v_declarado_total / perc_declarado if perc_declarado > 0 else v_declarado_total
    c_imposto = v_declarado_total * taxa_imposto
    c_shopee = v_real * taxa_shopee
    c_fixo_total = custo_fixo * qtd_total
    lucro = v_real - c_imposto - c_shopee - c_fixo_total - c_produto_total

    itens = [{
        "SKU": sku_principal,
        "Qtd": qtd_total,
        "V. Real": round(v_real, 2),
        "V. Decl.": round(v_declarado_total, 2),
        "Custo": round(c_produto_total, 2),
        "Shopee": round(c_shopee, 2),
        "Imposto": round(c_imposto, 2),
        "Custo Fixo": round(c_fixo_total, 2),
        "LUCRO": round(lucro, 2),
    }]
    sem_custo = [0] if not encontrou_principal else []

    return nome_loja, itens, sem_custo


def _construir_mapa_cnpj_lojas(pasta_lucro):
    """Varre XMLs da pasta_lucro e retorna {cnpj_completo: 'XX.XXX.XXX nome_limpo'}.

    Usado por /api/lojas-lucro (config) e api_gerar_lucro (processamento)
    para garantir nomes de loja consistentes.
    """
    import zipfile
    lojas_dict = {}  # {cnpj: nome_limpo}

    def _extrair_emit(conteudo):
        try:
            doc = xmltodict.parse(conteudo)
            if "nfeProc" in doc:
                nfe = doc["nfeProc"]["NFe"]["infNFe"]
            elif "NFe" in doc:
                nfe = doc["NFe"]["infNFe"]
            else:
                return
            emit = nfe.get("emit", {})
            if isinstance(emit, str):
                return
            cnpj = str(emit.get("CNPJ", "")).strip()
            nome_raw = str(emit.get("xNome", "")).strip()
            nome = _limpar_nome_loja(nome_raw) if nome_raw else "Desconhecida"
            if cnpj:
                lojas_dict[cnpj] = nome
        except Exception:
            pass

    if not pasta_lucro or not os.path.exists(pasta_lucro):
        return {}

    for f in os.listdir(pasta_lucro):
        fp = os.path.join(pasta_lucro, f)
        if f.lower().endswith('.zip') and zipfile.is_zipfile(fp):
            try:
                with zipfile.ZipFile(fp) as zf:
                    for nome_arq in zf.namelist():
                        if nome_arq.lower().endswith('.xml'):
                            conteudo = zf.read(nome_arq).decode('utf-8', errors='ignore')
                            _extrair_emit(conteudo)
            except Exception:
                pass
        elif f.lower().endswith('.xml'):
            try:
                with open(fp, 'r', encoding='utf-8', errors='ignore') as xf:
                    _extrair_emit(xf.read())
            except Exception:
                pass

    return {c: f"{_formatar_cnpj_curto(c)} {n}" for c, n in lojas_dict.items()}


@app.route('/api/lojas-lucro')
@jwt_required()
def api_lojas_lucro():
    """Retorna lista de lojas encontradas nos XMLs da pasta_lucro, separadas por CNPJ."""
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    cfg = estado["configuracoes"]
    pasta_lucro = cfg.get("pasta_lucro", "")

    mapa = _construir_mapa_cnpj_lojas(pasta_lucro)
    lojas = [{"cnpj": c, "nome": n} for c, n in sorted(mapa.items(), key=lambda x: x[1])]

    return jsonify({"lojas": lojas})


@app.route('/api/gerar-lucro', methods=['POST'])
@jwt_required()
def api_gerar_lucro():
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    cfg = estado["configuracoes"]
    pasta_lucro = cfg.get("pasta_lucro", "")
    pasta_saida = cfg["pasta_saida"]
    caminho_custos = cfg.get("planilha_custos", "")

    if not caminho_custos or not os.path.exists(caminho_custos):
        return jsonify({"erro": "Planilha de custos nao encontrada. Faca upload primeiro."}), 400

    try:
        adicionar_log(estado, "Gerando relatorio de lucro...", "info")

        df_custos = pd.read_excel(caminho_custos)
        dict_custos = {}
        for _, row in df_custos.iterrows():
            sku_original = str(row.iloc[0]).strip().upper()
            custo = float(row.iloc[1]) if pd.notnull(row.iloc[1]) else 0.0
            if sku_original:
                dict_custos[sku_original] = custo
        # Pre-ordenar chaves por tamanho (maior primeiro) para busca inteligente
        _chaves_custos_ordenadas = sorted(dict_custos.keys(), key=len, reverse=True)

        cfg_por_loja = cfg.get("lucro_por_loja", {})

        import zipfile
        loja_dados = defaultdict(lambda: {"itens": [], "linhas_sem_custo": []})
        chaves_processadas = set()
        total_xmls_lidos = 0
        # Construir mapa CNPJ->nome inline durante processamento (evita ler XMLs 2x)
        _cnpj_loja_map = {}

        def _processar_doc(doc):
            nonlocal total_xmls_lidos
            if "nfeProc" in doc:
                nfe = doc["nfeProc"]["NFe"]["infNFe"]
            elif "NFe" in doc:
                nfe = doc["NFe"]["infNFe"]
            else:
                return
            total_xmls_lidos += 1

            # Construir mapa CNPJ->nome enquanto processa (mesma logica de _construir_mapa_cnpj_lojas)
            emit = nfe.get("emit", {})
            if isinstance(emit, dict):
                cnpj_emit = str(emit.get("CNPJ", "")).strip()
                if cnpj_emit and cnpj_emit not in _cnpj_loja_map:
                    nome_raw = str(emit.get("xNome", "")).strip()
                    nome = _limpar_nome_loja(nome_raw) if nome_raw else "Desconhecida"
                    _cnpj_loja_map[cnpj_emit] = f"{_formatar_cnpj_curto(cnpj_emit)} {nome}"

            # Deduplicar por chave NFe (evita contar mesma NF 2x)
            chave_nfe = str(nfe.get("@Id", "")).strip()
            if not chave_nfe:
                # Fallback: CNPJ + nNF
                cnpj_e = str(emit.get("CNPJ", "")).strip() if isinstance(emit, dict) else ""
                nf_num = str(nfe.get("ide", {}).get("nNF", "")).strip()
                chave_nfe = f"{cnpj_e}_{nf_num}" if cnpj_e and nf_num else ""
            if chave_nfe:
                if chave_nfe in chaves_processadas:
                    return  # Duplicada — ignorar
                chaves_processadas.add(chave_nfe)

            nome_loja, itens, sem_custo = _processar_nfe_lucro(nfe, dict_custos, cfg, cfg_por_loja, _chaves_custos_ordenadas, _cnpj_loja_map)
            offset = len(loja_dados[nome_loja]["itens"])
            loja_dados[nome_loja]["itens"].extend(itens)
            loja_dados[nome_loja]["linhas_sem_custo"].extend([i + offset for i in sem_custo])

        zips = [f for f in os.listdir(pasta_lucro) if f.lower().endswith('.zip')] if pasta_lucro and os.path.exists(pasta_lucro) else []
        for z in zips:
            caminho_zip = os.path.join(pasta_lucro, z)
            try:
                with zipfile.ZipFile(caminho_zip, 'r') as zf:
                    for nome_xml in zf.namelist():
                        if not nome_xml.lower().endswith('.xml'):
                            continue
                        try:
                            conteudo = zf.read(nome_xml).decode('utf-8')
                            doc = xmltodict.parse(conteudo)
                            _processar_doc(doc)
                        except Exception:
                            continue
            except Exception as e:
                adicionar_log(estado, f"Erro ao ler ZIP {z}: {e}", "warning")

        xmls_avulsos = [f for f in os.listdir(pasta_lucro) if f.lower().endswith('.xml')] if pasta_lucro and os.path.exists(pasta_lucro) else []
        for arq in xmls_avulsos:
            caminho_xml = os.path.join(pasta_lucro, arq)
            try:
                with open(caminho_xml, "r", encoding="utf-8") as f:
                    doc = xmltodict.parse(f.read())
                _processar_doc(doc)
            except Exception:
                continue

        # Log de duplicatas ignoradas
        duplicados = total_xmls_lidos - len(chaves_processadas)
        if duplicados > 0:
            adicionar_log(estado, f"{duplicados} NF(s) duplicada(s) ignorada(s) de {total_xmls_lidos} XMLs", "warning")
        adicionar_log(estado, f"{len(chaves_processadas)} NFs unicas processadas", "info")

        if not loja_dados:
            return jsonify({"erro": "Nenhum produto encontrado nos XMLs"}), 400

        # LOG DE SKUs PRINCIPAIS para conferencia do usuario
        adicionar_log(estado, "--- SKUs Principais extraidos (conferencia) ---", "info")
        skus_log = {}  # sku_principal -> set(sku_completos_originais)
        for nome_loja_l, dados_l in loja_dados.items():
            for item in dados_l["itens"]:
                sku_usado = item.get("SKU", "")
                if sku_usado:
                    sku_base = _extrair_sku_principal(sku_usado)
                    if sku_base not in skus_log:
                        skus_log[sku_base] = set()
                    skus_log[sku_base].add(sku_usado)
        for sku_base in sorted(skus_log.keys()):
            originais = sorted(skus_log[sku_base])
            if len(originais) == 1 and originais[0] == sku_base:
                adicionar_log(estado, f"  SKU: {sku_base}", "info")
            else:
                adicionar_log(estado, f"  SKU base: {sku_base} (de: {', '.join(originais)})", "info")
        # Mostrar chaves da planilha de custos para comparacao
        adicionar_log(estado, f"--- Planilha de custos: {len(dict_custos)} SKUs ---", "info")
        for sku_planilha in sorted(dict_custos.keys())[:20]:
            custo = dict_custos[sku_planilha]
            adicionar_log(estado, f"  Planilha: {sku_planilha} = R${custo:.2f}", "info")
        if len(dict_custos) > 20:
            adicionar_log(estado, f"  ... e mais {len(dict_custos) - 20} SKUs", "info")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(pasta_saida, exist_ok=True)

        lojas_lucro = []
        lista_global = []
        linhas_sem_custo_global = []

        for nome_loja in sorted(loja_dados.keys()):
            dados_l = loja_dados[nome_loja]
            itens_l = dados_l["itens"]
            sem_custo_l = dados_l["linhas_sem_custo"]

            if not itens_l:
                continue

            pasta_loja = os.path.join(pasta_saida, nome_loja)
            os.makedirs(pasta_loja, exist_ok=True)
            caminho_loja_xlsx = os.path.join(pasta_loja, f"lucro_{nome_loja}_{timestamp}.xlsx")

            df_loja = pd.DataFrame(itens_l)
            df_loja = df_loja.sort_values('SKU', na_position='last').reset_index(drop=False)
            sem_custo_l_sorted = [i for i, orig in enumerate(df_loja['index']) if orig in sem_custo_l]
            df_loja = df_loja.drop(columns=['index'])
            totais_l = df_loja.sum(numeric_only=True)
            totais_l["SKU"] = "TOTAIS"
            df_loja = pd.concat([df_loja, pd.DataFrame([totais_l])], ignore_index=True)
            df_loja.to_excel(caminho_loja_xlsx, index=False)
            _formatar_excel_lucro(caminho_loja_xlsx, sem_custo_l_sorted)

            lucro_l = round(float(totais_l.get("LUCRO", 0)), 2)
            receita_l = round(float(totais_l.get("V. Real", 0)), 2)
            custo_l = round(float(totais_l.get("Custo", 0)), 2)

            lojas_lucro.append({
                "nome": nome_loja,
                "lucro": lucro_l,
                "receita": receita_l,
                "custo": custo_l,
                "total_itens": len(itens_l),
                "itens_sem_custo": len(sem_custo_l),
                "arquivo": f"lucro_{nome_loja}_{timestamp}.xlsx",
            })

            adicionar_log(estado, f"  Lucro {nome_loja}: {len(itens_l)} itens, R$ {lucro_l:.2f}", "info")

            offset_g = len(lista_global)
            for item in itens_l:
                lista_global.append(item)
            linhas_sem_custo_global.extend([i + offset_g for i in sem_custo_l])

        df_global = pd.DataFrame(lista_global)
        df_global = df_global.sort_values('SKU', na_position='last').reset_index(drop=False)
        sem_custo_global_sorted = [i for i, orig in enumerate(df_global['index']) if orig in linhas_sem_custo_global]
        df_global = df_global.drop(columns=['index'])
        totais_g = df_global.sum(numeric_only=True)
        totais_g["SKU"] = "TOTAIS"
        df_global = pd.concat([df_global, pd.DataFrame([totais_g])], ignore_index=True)

        caminho_xlsx = os.path.join(pasta_saida, f"relatorio_lucro_{timestamp}.xlsx")
        df_global.to_excel(caminho_xlsx, index=False)
        _formatar_excel_lucro(caminho_xlsx, sem_custo_global_sorted)

        lucro_total = round(float(totais_g.get("LUCRO", 0)), 2)
        receita_total = round(float(totais_g.get("V. Real", 0)), 2)
        custo_total = round(float(totais_g.get("Custo", 0)), 2)
        total_itens = len(lista_global)
        itens_sem_custo = len(linhas_sem_custo_global)

        estado["ultimo_lucro"] = {
            "arquivo": f"relatorio_lucro_{timestamp}.xlsx",
            "lucro_total": lucro_total,
            "receita_total": receita_total,
            "custo_total": custo_total,
            "total_itens": total_itens,
            "itens_sem_custo": itens_sem_custo,
            "timestamp": timestamp,
            "lojas": lojas_lucro,
        }

        # Salvar lucro em disco para persistir entre deploys
        _salvar_lucro_usuario(user_id)

        adicionar_log(estado, f"Relatorio de lucro gerado: {total_itens} itens, {len(lojas_lucro)} lojas, Lucro: R$ {lucro_total:.2f}", "success")

        return jsonify({
            "mensagem": "Relatorio gerado com sucesso",
            "lucro_total": lucro_total,
            "receita_total": receita_total,
            "custo_total": custo_total,
            "total_itens": total_itens,
            "itens_sem_custo": itens_sem_custo,
            "arquivo": f"relatorio_lucro_{timestamp}.xlsx",
            "lojas": lojas_lucro,
        })

    except Exception as e:
        import traceback
        adicionar_log(estado, f"Erro ao gerar lucro: {str(e)}", "error")
        adicionar_log(estado, traceback.format_exc(), "error")
        return jsonify({"erro": str(e)}), 500


@app.route('/api/download-lucro')
@jwt_required()
def api_download_lucro():
    """Gera ZIP com XLSX de lucro separado por loja + consolidado na raiz."""
    import zipfile as zf_mod
    import io as io_mod
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    lucro = estado.get("ultimo_lucro")
    if not lucro:
        return jsonify({"erro": "Nenhum relatorio de lucro disponivel"}), 404

    pasta_saida = estado["configuracoes"]["pasta_saida"]
    lojas = lucro.get("lojas", [])

    buf = io_mod.BytesIO()
    arquivos_adicionados = 0
    with zf_mod.ZipFile(buf, 'w', zf_mod.ZIP_DEFLATED) as zf:
        # Consolidado na raiz
        caminho_consolidado = os.path.join(pasta_saida, lucro["arquivo"])
        if os.path.exists(caminho_consolidado):
            zf.write(caminho_consolidado, lucro["arquivo"])
            arquivos_adicionados += 1

        # XLSX separado por loja em pastas
        for loja_info in lojas:
            nome_loja = loja_info.get("nome", "")
            arquivo_loja = loja_info.get("arquivo", "")
            if nome_loja and arquivo_loja:
                caminho_loja = os.path.join(pasta_saida, nome_loja, arquivo_loja)
                if os.path.exists(caminho_loja):
                    zf.write(caminho_loja, os.path.join(nome_loja, arquivo_loja))
                    arquivos_adicionados += 1

    if arquivos_adicionados == 0:
        return jsonify({"erro": "Nenhum arquivo de lucro encontrado"}), 404

    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name="Relatorio_Lucro.zip", mimetype="application/zip")


@app.route('/api/download-lucro/<loja>')
@jwt_required()
def api_download_lucro_loja(loja):
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    pasta = os.path.join(estado["configuracoes"]["pasta_saida"], loja)
    if not os.path.exists(pasta):
        return jsonify({"erro": "Pasta da loja nao encontrada"}), 404
    arquivos = [f for f in os.listdir(pasta) if f.startswith("lucro_") and f.endswith(".xlsx")]
    if not arquivos:
        return jsonify({"erro": "Arquivo de lucro nao encontrado"}), 404
    arquivo = sorted(arquivos)[-1]
    return send_file(os.path.join(pasta, arquivo), as_attachment=True)


def _formatar_excel_lucro(caminho_arquivo, linhas_sem_custo):
    wb = openpyxl.load_workbook(caminho_arquivo)
    ws = wb.active

    header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    total_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
    alert_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

    lucro_positivo = Font(color="006100", bold=True)
    lucro_negativo = Font(color="9C0006", bold=True)
    header_font = Font(color="FFFFFF", bold=True)
    thin_border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin")
    )

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    max_row = ws.max_row
    max_col = ws.max_column
    sem_custo_set = set(linhas_sem_custo)  # Converter para set para O(1) lookup
    for row in ws.iter_rows(min_row=2, max_row=max_row, min_col=1, max_col=max_col):
        for cell in row:
            cell.border = thin_border
            if cell.col_idx >= 3:
                cell.number_format = 'R$ #,##0.00'
            idx_dados = cell.row - 2
            if idx_dados in sem_custo_set:
                cell.fill = alert_fill
            if cell.col_idx == max_col:
                if isinstance(cell.value, (int, float)) and cell.value >= 0:
                    cell.font = lucro_positivo
                elif isinstance(cell.value, (int, float)):
                    cell.font = lucro_negativo

    for cell in ws[max_row]:
        cell.fill = total_fill
        cell.font = Font(bold=True)

    for column in ws.columns:
        col_cells = [cell for cell in column]
        max_length = 0
        for cell in col_cells:
            try:
                val = cell.value
                if isinstance(val, (int, float)) and cell.col_idx >= 3:
                    display_len = len(f"R$ {val:,.2f}")
                else:
                    display_len = len(str(val)) if val is not None else 0
                if display_len > max_length:
                    max_length = display_len
            except Exception:
                pass
        ws.column_dimensions[get_column_letter(col_cells[0].column)].width = max(max_length + 3, 12)

    wb.save(caminho_arquivo)


@app.route('/api/agrupar', methods=['POST'])
@jwt_required()
def api_agrupar():
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404

    dados = request.get_json()
    if not dados:
        return jsonify({"erro": "Dados nao enviados"}), 400

    cnpjs = dados.get('cnpjs', [])
    nome_grupo = dados.get('nome', 'Agrupado').strip() or 'Agrupado'

    if len(cnpjs) < 2:
        return jsonify({"erro": "Selecione pelo menos 2 lojas"}), 400

    if not estado["ultimo_resultado"]:
        return jsonify({"erro": "Nenhum processamento realizado ainda"}), 400

    etiquetas_por_cnpj = estado.get("_etiquetas_por_cnpj", {})
    if not etiquetas_por_cnpj:
        return jsonify({"erro": "Dados de etiquetas nao disponiveis. Reprocesse."}), 400

    pasta_saida = estado["configuracoes"]["pasta_saida"]

    try:
        etiquetas_combinadas = []
        nomes_lojas = []
        for cnpj in cnpjs:
            etqs = etiquetas_por_cnpj.get(cnpj, [])
            if etqs:
                etiquetas_combinadas.extend(etqs)
                cfg = estado.get("_proc_config", {})
                nome = cfg.get("cnpj_loja", {}).get(cnpj) or cfg.get("cnpj_nome", {}).get(cnpj, cnpj)
                nomes_lojas.append(nome)

        if len(nomes_lojas) < 2:
            return jsonify({"erro": "Lojas selecionadas nao encontradas"}), 400

        proc = ProcessadorEtiquetasShopee()
        cfg = estado.get("_proc_config", {})
        proc.LARGURA_PT = cfg.get("largura_pt", proc.LARGURA_PT)
        proc.ALTURA_PT = cfg.get("altura_pt", proc.ALTURA_PT)
        proc.MARGEM_ESQUERDA = cfg.get("margem_esquerda", proc.MARGEM_ESQUERDA)
        proc.MARGEM_DIREITA = cfg.get("margem_direita", proc.MARGEM_DIREITA)
        proc.MARGEM_TOPO = cfg.get("margem_topo", proc.MARGEM_TOPO)
        proc.MARGEM_INFERIOR = cfg.get("margem_inferior", proc.MARGEM_INFERIOR)
        proc.fonte_produto = cfg.get("fonte_produto", proc.fonte_produto)
        proc.exibicao_produto = cfg.get("exibicao_produto", getattr(proc, 'exibicao_produto', 'sku'))
        proc.cnpj_loja = cfg.get("cnpj_loja", {})
        proc.cnpj_nome = cfg.get("cnpj_nome", {})

        etiquetas_combinadas, duplicadas = proc.remover_duplicatas(etiquetas_combinadas)
        if duplicadas:
            adicionar_log(estado, f"  Agrupamento: {len(duplicadas)} duplicatas removidas", "warning")

        etiq_regular = [e for e in etiquetas_combinadas if e.get('tipo_especial') != 'cpf']
        etiq_cpf = [e for e in etiquetas_combinadas if e.get('tipo_especial') == 'cpf']

        pasta_grupo = os.path.join(pasta_saida, nome_grupo)
        if not os.path.exists(pasta_grupo):
            os.makedirs(pasta_grupo)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        total_pags = 0

        if etiq_regular:
            caminho_pdf = os.path.join(pasta_grupo, f"agrupado_{nome_grupo}_{timestamp}.pdf")
            t, _, _, _, _ = proc.gerar_pdf_loja(etiq_regular, caminho_pdf)
            total_pags += t

        if etiq_cpf:
            caminho_cpf = os.path.join(pasta_grupo, f"cpf_{nome_grupo}_{timestamp}.pdf")
            total_pags += proc.gerar_pdf_cpf(etiq_cpf, caminho_cpf)

        caminho_xlsx = os.path.join(pasta_grupo, f"resumo_{nome_grupo}_{timestamp}.xlsx")
        n_skus, total_qtd = proc.gerar_resumo_xlsx(etiquetas_combinadas, caminho_xlsx, nome_grupo)

        adicionar_log(estado, f"Agrupamento '{nome_grupo}': {', '.join(nomes_lojas)}", "success")
        adicionar_log(estado, f"  {total_pags} pags, {n_skus} SKUs, {total_qtd} un.", "info")

        return jsonify({
            "mensagem": f"Agrupamento '{nome_grupo}' gerado com sucesso",
            "lojas": nomes_lojas,
            "total_etiquetas": len(etiquetas_combinadas),
            "arquivo": f"agrupado_{nome_grupo}_{timestamp}.pdf",
            "pasta": nome_grupo,
        })

    except Exception as e:
        adicionar_log(estado, f"ERRO ao agrupar: {str(e)}", "error")
        import traceback
        adicionar_log(estado, traceback.format_exc(), "error")
        return jsonify({"erro": str(e)}), 500


@app.route('/api/agrupamentos', methods=['GET', 'POST'])
@jwt_required()
def api_agrupamentos():
    user_id = get_jwt_identity()
    estado = _get_estado(user_id)
    if not estado:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    if request.method == 'GET':
        return jsonify({"agrupamentos": estado["agrupamentos"]})
    dados = request.get_json() or {}
    estado["agrupamentos"] = dados.get("agrupamentos", [])
    _salvar_agrupamentos_usuario(user_id)
    adicionar_log(estado, f"Agrupamentos salvos: {len(estado['agrupamentos'])} grupo(s)", "success")
    return jsonify({"ok": True})


# ----------------------------------------------------------------
# PROCESSAMENTO EM BACKGROUND
# ----------------------------------------------------------------
def _executar_processamento(user_id, sem_recorte=True, resumo_sku_somente=False, pasta_entrada_override=None):
    """Executa o processamento completo em thread separada.

    Args:
        user_id: ID do usuario.
        sem_recorte: quando True, usa PDF pagina inteira (fluxo UpSeller novo).
        resumo_sku_somente: quando True, gera XLSX em SKU + quantidade.
            Padrão atual: False (SKU + Variação + Soma Quant., igual exemplo validado).
        pasta_entrada_override: quando informado, processa apenas essa pasta de lote.
    """
    with app.app_context():
        estado = _get_estado(user_id)
        if not estado:
            return {"ok": False, "erro": "Estado do usuario nao encontrado", "total_etiquetas": 0, "total_lojas": 0}

        # Regra global atual: etiqueta inteira, sem recorte.
        sem_recorte = True

        estado["processando"] = True
        estado["logs"] = []
        inicio = time.time()
        exec_info = {"ok": False, "erro": "", "total_etiquetas": 0, "total_lojas": 0}

        try:
            pasta_entrada_padrao = estado["configuracoes"]["pasta_entrada"]
            pasta_entrada = pasta_entrada_override or pasta_entrada_padrao
            pasta_saida = estado["configuracoes"]["pasta_saida"]
            os.makedirs(pasta_entrada, exist_ok=True)

            # Valida se o lote/pasta de entrada tem pelo menos 1 PDF real.
            # Isso evita processar "vazio" e sobrescrever o ultimo resultado.
            pdfs_entrada = []
            for raiz, _dirs, arquivos in os.walk(pasta_entrada):
                for nome_arq in arquivos:
                    low = str(nome_arq).lower()
                    if low.endswith(".pdf") and not low.startswith("._"):
                        pdfs_entrada.append(os.path.join(raiz, nome_arq))
            if not pdfs_entrada:
                raise RuntimeError(
                    "Nenhum PDF encontrado no lote atual para processar. "
                    "A saida anterior foi preservada."
                )

            adicionar_log(estado, "Iniciando processamento...", "info")
            if pasta_entrada_override:
                adicionar_log(estado, f"Lote isolado de entrada: {pasta_entrada}", "info")
            adicionar_log(estado, f"PDFs detectados no lote: {len(pdfs_entrada)}", "info")

            proc = ProcessadorEtiquetasShopee()

            proc.LARGURA_PT = estado["configuracoes"]["largura_mm"] * 2.835
            proc.ALTURA_PT = estado["configuracoes"]["altura_mm"] * 2.835
            proc.MARGEM_ESQUERDA = estado["configuracoes"]["margem_esq"]
            proc.MARGEM_DIREITA = estado["configuracoes"]["margem_dir"]
            proc.MARGEM_TOPO = estado["configuracoes"]["margem_topo"]
            proc.MARGEM_INFERIOR = estado["configuracoes"]["margem_inf"]
            proc.fonte_produto = estado["configuracoes"].get("fonte_produto", 7)
            proc.exibicao_produto = estado["configuracoes"].get("exibicao_produto", "sku")

            # Carregar dados dos XLSX de empacotamento (produtos, tracking, order_sn)
            adicionar_log(estado, "Carregando dados dos XLSX...", "info")
            proc.carregar_todos_xlsx(pasta_entrada)

            # Fallback para lotes isolados (ex.: /api/upseller/imprimir):
            # quando o lote atual tiver apenas PDF, tentar reaproveitar XLSX
            # de lotes gerar_* recentes do mesmo usuario.
            if not proc.dados_xlsx_global and pasta_entrada_override:
                try:
                    pasta_lotes_base = os.path.dirname(os.path.realpath(pasta_entrada))
                    if os.path.isdir(pasta_lotes_base):
                        candidatos = []
                        for nome in os.listdir(pasta_lotes_base):
                            if not str(nome).lower().startswith("gerar_"):
                                continue
                            d = os.path.join(pasta_lotes_base, nome)
                            if not os.path.isdir(d):
                                continue
                            try:
                                mt = os.path.getmtime(d)
                            except Exception:
                                mt = 0
                            candidatos.append((mt, d))

                        candidatos.sort(reverse=True)
                        pastas_testadas = 0
                        for _mt, d in candidatos[:10]:
                            pastas_testadas += 1
                            proc.carregar_todos_xlsx(d)
                            # Se ja encontrou bastante pedidos, nao precisa varrer tudo.
                            if len(proc.dados_xlsx_global) >= 300:
                                break

                        if proc.dados_xlsx_global:
                            adicionar_log(
                                estado,
                                f"XLSX fallback: {len(proc.dados_xlsx_global)} pedidos de lotes gerar_* recentes",
                                "success"
                            )
                        else:
                            adicionar_log(
                                estado,
                                f"XLSX fallback: nenhum pedido encontrado em {pastas_testadas} lote(s) gerar_*",
                                "warning"
                            )
                except Exception as e_xlsx_fb:
                    adicionar_log(estado, f"Falha no fallback de XLSX: {e_xlsx_fb}", "warning")

            if proc.dados_xlsx_global:
                adicionar_log(estado, f"XLSX: {len(proc.dados_xlsx_global)} pedidos, {len(proc.dados_xlsx_tracking)} trackings", "success")
            else:
                adicionar_log(estado, "Nenhum XLSX de empacotamento encontrado", "warning")

            if sem_recorte:
                adicionar_log(estado, "Modo: etiqueta inteira (sem recorte)", "info")
                adicionar_log(estado, "Carregando etiquetas dos PDFs (sem recorte)...", "info")
                todas_etiquetas, cpf_auto_detectadas, pdfs_shein_auto = proc.carregar_todos_pdfs_sem_recorte(pasta_entrada)
            else:
                adicionar_log(estado, "Carregando etiquetas dos PDFs...", "info")
                todas_etiquetas, cpf_auto_detectadas, pdfs_shein_auto = proc.carregar_todos_pdfs(pasta_entrada)
            adicionar_log(estado, f"Total: {len(todas_etiquetas)} etiquetas extraídas", "success")

            # Avisar sobre PDFs que podem ter quadrantes/páginas ignorados
            pdfs_na_pasta = [f for f in os.listdir(pasta_entrada) if f.lower().endswith('.pdf')]
            if len(pdfs_na_pasta) > len(todas_etiquetas) / 2:
                adicionar_log(estado, f"INFO: {len(pdfs_na_pasta)} PDFs encontrados, verifique se todos foram processados", "info")
            if cpf_auto_detectadas:
                adicionar_log(estado, f"CPF auto-detectadas: {len(cpf_auto_detectadas)} etiquetas", "info")
            if pdfs_shein_auto:
                adicionar_log(estado, f"Shein auto-detectados: {len(pdfs_shein_auto)} PDF(s)", "info")

            # Verificar quais etiquetas tem/nao tem dados de produto
            n_com_dados = sum(1 for e in todas_etiquetas if e.get('dados_xml', {}).get('produtos'))
            n_sem_dados = len(todas_etiquetas) - n_com_dados
            if n_sem_dados > 0:
                adicionar_log(estado, f"AVISO: {n_sem_dados} etiquetas sem dados de produto (de {len(todas_etiquetas)} total)", "warning")

            adicionar_log(estado, "Verificando etiquetas especiais...", "info")

            etiquetas_cpf_especial = proc.processar_cpf(pasta_entrada)
            # Juntar CPF do lanim*.pdf com CPF auto-detectadas de PDFs genericos
            etiquetas_cpf_especial.extend(cpf_auto_detectadas)
            if etiquetas_cpf_especial:
                todas_etiquetas.extend(etiquetas_cpf_especial)
                adicionar_log(estado, f"CPF: {len(etiquetas_cpf_especial)} etiquetas ({len(cpf_auto_detectadas)} auto-detectadas)", "success")

            etiquetas_shein = proc.processar_shein(pasta_entrada, pdfs_extras=pdfs_shein_auto)
            if etiquetas_shein:
                adicionar_log(estado, f"Shein: {len(etiquetas_shein)} etiquetas", "success")

            if not etiquetas_cpf_especial and not etiquetas_shein:
                adicionar_log(estado, "Nenhuma etiqueta especial encontrada", "info")

            todas_etiquetas, duplicadas = proc.remover_duplicatas(todas_etiquetas)
            if duplicadas:
                adicionar_log(estado, f"AVISO: {len(duplicadas)} etiquetas duplicadas removidas:", "warning")
                for d in duplicadas:
                    adicionar_log(estado, f"  NF duplicada: {d.get('nf', '?')}", "warning")
            else:
                adicionar_log(estado, "Nenhuma duplicata encontrada", "info")

            adicionar_log(estado, "Separando etiquetas por loja...", "info")
            lojas = proc.separar_por_loja(todas_etiquetas)
            adicionar_log(estado, f"{len(lojas)} lojas para processar", "info")
            # Avisos sobre tipos especiais de etiquetas
            n_retirada = sum(1 for e in todas_etiquetas if e.get('tipo_especial') == 'retirada')
            if n_retirada > 0:
                adicionar_log(estado, f"AVISO: {n_retirada} etiqueta(s) de RETIRADA (cliente retira na loja - sem endereço)", "warning")

            total_etiquetas_lojas = sum(len(v) for v in lojas.values())
            if total_etiquetas_lojas <= 0:
                raise RuntimeError(
                    "Nenhuma etiqueta foi extraida dos PDFs do lote atual. "
                    "A saida anterior foi preservada."
                )

            # Limpar pasta de saida somente apos confirmar que o lote e valido.
            import shutil
            if os.path.exists(pasta_saida):
                shutil.rmtree(pasta_saida)
            os.makedirs(pasta_saida, exist_ok=True)

            estado["_etiquetas_por_cnpj"] = dict(lojas)
            estado["_proc_config"] = {
                "largura_pt": proc.LARGURA_PT,
                "altura_pt": proc.ALTURA_PT,
                "margem_esquerda": proc.MARGEM_ESQUERDA,
                "margem_direita": proc.MARGEM_DIREITA,
                "margem_topo": proc.MARGEM_TOPO,
                "margem_inferior": proc.MARGEM_INFERIOR,
                "fonte_produto": proc.fonte_produto,
                "cnpj_loja": dict(proc.cnpj_loja),
                "cnpj_nome": dict(proc.cnpj_nome),
            }

            # Contar etiquetas sem XML/declaracao para aviso
            etiquetas_sem_nf = []

            resultado_lojas = []
            for cnpj, etiquetas_loja in lojas.items():
                nome_loja = proc.get_nome_loja(cnpj)
                n_etiquetas = len(etiquetas_loja)

                # Pular lojas sem etiquetas ou "Loja_Desconhecida" vazia
                if n_etiquetas == 0:
                    continue

                # Verificar etiquetas sem dados XML/declaracao
                for etq in etiquetas_loja:
                    dados = etq.get('dados_xml', {})
                    if not dados.get('chave') and not dados.get('produtos'):
                        etiquetas_sem_nf.append({
                            'nf': etq.get('nf', '?'),
                            'loja': nome_loja,
                        })

                try:
                    adicionar_log(estado, f"Processando: {nome_loja} ({n_etiquetas} etiquetas)...", "info")

                    pasta_loja = os.path.join(pasta_saida, nome_loja)
                    if not os.path.exists(pasta_loja):
                        os.makedirs(pasta_loja)

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                    etiq_regular = [e for e in etiquetas_loja if e.get('tipo_especial') not in ('cpf', 'retirada')]
                    etiq_cpf = [e for e in etiquetas_loja if e.get('tipo_especial') == 'cpf']
                    etiq_retirada = [e for e in etiquetas_loja if e.get('tipo_especial') == 'retirada']

                    total_pags = 0
                    n_simples = n_multi = com_xml = sem_xml = 0
                    pdf_nome = ''

                    if etiq_regular:
                        caminho_pdf = os.path.join(pasta_loja, f"etiquetas_{nome_loja}_{timestamp}.pdf")
                        t, ns, nm, cx, sx = proc.gerar_pdf_loja(etiq_regular, caminho_pdf)
                        total_pags += t
                        n_simples, n_multi, com_xml, sem_xml = ns, nm, cx, sx
                        pdf_nome = os.path.basename(caminho_pdf)

                    if etiq_cpf:
                        caminho_cpf_pdf = os.path.join(pasta_loja, f"cpf_{nome_loja}_{timestamp}.pdf")
                        total_cpf = proc.gerar_pdf_cpf(etiq_cpf, caminho_cpf_pdf)
                        total_pags += total_cpf
                        if not pdf_nome:
                            pdf_nome = os.path.basename(caminho_cpf_pdf)
                        adicionar_log(estado, f"  {nome_loja}: {total_cpf} etiquetas CPF", "info")
                    
                    if etiq_retirada:
                        caminho_retirada_pdf = os.path.join(pasta_loja, f"retirada_{nome_loja}_{timestamp}.pdf")
                        total_retirada = proc.gerar_pdf_cpf(etiq_retirada, caminho_retirada_pdf)  # Usa mesmo formato do CPF
                        total_pags += total_retirada
                        adicionar_log(estado, f"  {nome_loja}: {total_retirada} etiquetas RETIRADA (cliente retira na loja - sem endereço)", "warning")

                    caminho_xlsx = os.path.join(pasta_loja, f"resumo_{nome_loja}_{timestamp}.xlsx")
                    n_skus, total_qtd = proc.gerar_resumo_xlsx(
                        etiquetas_loja,
                        caminho_xlsx,
                        nome_loja,
                        sku_somente=resumo_sku_somente
                    )

                    info_loja = {
                        "nome": nome_loja,
                        "cnpj": cnpj,
                        "etiquetas": n_etiquetas,
                        "paginas": total_pags,
                        "simples": n_simples,
                        "multi_produto": n_multi,
                        "com_xml": com_xml,
                        "sem_xml": sem_xml,
                        "skus": n_skus,
                        "total_qtd": total_qtd,
                        "pdf": pdf_nome,
                        "xlsx": os.path.basename(caminho_xlsx),
                    }
                    resultado_lojas.append(info_loja)

                    adicionar_log(estado, f"  {nome_loja}: {total_pags} pags, {n_skus} SKUs, {total_qtd} un.", "success")
                    if sem_xml > 0:
                        adicionar_log(estado, f"  AVISO: {sem_xml} etiquetas sem XML", "warning")

                except Exception as e_loja:
                    adicionar_log(estado, f"ERRO ao processar loja {nome_loja}: {str(e_loja)}", "error")
                    import traceback
                    adicionar_log(estado, traceback.format_exc(), "error")
                    continue

            adicionar_log(estado, "Gerando resumo geral...", "info")
            timestamp_geral = datetime.now().strftime("%Y%m%d_%H%M%S")
            caminho_resumo_geral = os.path.join(pasta_saida, f"resumo_geral_{timestamp_geral}.xlsx")
            n_lojas_rg, total_un_rg = proc.gerar_resumo_geral_xlsx(
                resultado_lojas, dict(lojas), caminho_resumo_geral
            )
            adicionar_log(estado, f"Resumo geral: {n_lojas_rg} lojas, {total_un_rg} unidades total", "success")

            # Aviso de etiquetas sem NF/declaracao
            if etiquetas_sem_nf:
                adicionar_log(estado, f"AVISO: {len(etiquetas_sem_nf)} etiquetas sem nota fiscal ou declaracao de conteudo!", "warning")
                lojas_afetadas = set()
                for e in etiquetas_sem_nf:
                    lojas_afetadas.add(e['loja'])
                for loja_a in sorted(lojas_afetadas):
                    n_sem = sum(1 for e in etiquetas_sem_nf if e['loja'] == loja_a)
                    adicionar_log(estado, f"  {loja_a}: {n_sem} etiquetas sem NF/declaracao", "warning")

            if etiquetas_shein:
                adicionar_log(estado, "Gerando PDF Shein...", "info")
                from collections import defaultdict as dd
                shein_por_cnpj = dd(list)
                for etq in etiquetas_shein:
                    shein_por_cnpj[etq.get('cnpj', '')].append(etq)

                for cnpj_s, etqs_s in shein_por_cnpj.items():
                    nome_loja_s = proc.get_nome_loja(cnpj_s)
                    pasta_loja_s = os.path.join(pasta_saida, nome_loja_s)
                    if not os.path.exists(pasta_loja_s):
                        os.makedirs(pasta_loja_s)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    caminho_shein = os.path.join(pasta_loja_s, f"shein_{nome_loja_s}_{timestamp}.pdf")
                    total_shein = proc.gerar_pdf_shein(etqs_s, caminho_shein)

                    # Gerar resumo XLSX Shein
                    caminho_xlsx_shein = os.path.join(pasta_loja_s, f"resumo_shein_{nome_loja_s}_{timestamp}.xlsx")
                    n_skus_s, total_qtd_s = proc.gerar_resumo_xlsx_shein(etqs_s, caminho_xlsx_shein, nome_loja_s)

                    adicionar_log(estado, f"  Shein {nome_loja_s}: {total_shein} paginas, {n_skus_s} itens, {total_qtd_s} unidades", "success")

            if estado["agrupamentos"] and resultado_lojas:
                adicionar_log(estado, "Gerando agrupamentos pre-configurados...", "info")

                for grupo in estado["agrupamentos"]:
                    nome_grupo = grupo.get("nome", "Agrupado")
                    cnpjs_grupo = grupo.get("cnpjs", [])
                    if len(cnpjs_grupo) < 2:
                        continue

                    etiquetas_grupo = []
                    nomes_g = []
                    for c in cnpjs_grupo:
                        if c in lojas:
                            etiquetas_grupo.extend(lojas[c])
                            nomes_g.append(proc.get_nome_loja(c))

                    if len(nomes_g) < 2:
                        adicionar_log(estado, f"  Grupo '{nome_grupo}': lojas insuficientes, pulando", "warning")
                        continue

                    try:
                        etiquetas_grupo, _ = proc.remover_duplicatas(etiquetas_grupo)

                        etiq_reg_g = [e for e in etiquetas_grupo if e.get('tipo_especial') != 'cpf']
                        etiq_cpf_g = [e for e in etiquetas_grupo if e.get('tipo_especial') == 'cpf']

                        pasta_grupo = os.path.join(pasta_saida, nome_grupo)
                        if not os.path.exists(pasta_grupo):
                            os.makedirs(pasta_grupo)
                        timestamp_g = datetime.now().strftime("%Y%m%d_%H%M%S")

                        total_pags_g = 0
                        if etiq_reg_g:
                            caminho_agrup = os.path.join(pasta_grupo, f"agrupado_{nome_grupo}_{timestamp_g}.pdf")
                            t_g, _, _, _, _ = proc.gerar_pdf_loja(etiq_reg_g, caminho_agrup)
                            total_pags_g += t_g

                        if etiq_cpf_g:
                            caminho_cpf_g = os.path.join(pasta_grupo, f"cpf_{nome_grupo}_{timestamp_g}.pdf")
                            total_pags_g += proc.gerar_pdf_cpf(etiq_cpf_g, caminho_cpf_g)

                        caminho_xlsx_g = os.path.join(pasta_grupo, f"resumo_{nome_grupo}_{timestamp_g}.xlsx")
                        proc.gerar_resumo_xlsx(
                            etiquetas_grupo,
                            caminho_xlsx_g,
                            nome_grupo,
                            sku_somente=resumo_sku_somente
                        )

                        adicionar_log(estado, f"  Grupo '{nome_grupo}': {', '.join(nomes_g)} ({total_pags_g} pags)", "success")
                    except Exception as e_g:
                        adicionar_log(estado, f"  ERRO grupo '{nome_grupo}': {str(e_g)}", "error")

            # Remover pastas vazias (ex: Loja_Desconhecida sem arquivos)
            if os.path.exists(pasta_saida):
                for d in os.listdir(pasta_saida):
                    dp = os.path.join(pasta_saida, d)
                    if os.path.isdir(dp) and not os.listdir(dp):
                        os.rmdir(dp)
                        adicionar_log(estado, f"  Pasta vazia removida: {d}", "info")

            duracao = round(time.time() - inicio, 1)

            resultado = {
                "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                "duracao_s": duracao,
                "total_xlsx": len(proc.dados_xlsx_global),
                "total_etiquetas": len(todas_etiquetas),
                "total_lojas": len(resultado_lojas),
                "lojas": resultado_lojas,
                "etiquetas_sem_nf": len(etiquetas_sem_nf) if etiquetas_sem_nf else 0,
                "resumo_geral": {
                    "arquivo": os.path.basename(caminho_resumo_geral),
                    "total_lojas": n_lojas_rg,
                    "total_unidades": total_un_rg,
                },
            }

            estado["ultimo_resultado"] = resultado
            estado["historico"].insert(0, resultado)
            estado["historico"] = estado["historico"][:20]

            # Salvar resultado em disco para persistir entre deploys
            _salvar_resultado_usuario(user_id)

            # Registrar snapshot ZIP no historico persistente (ultimas 24h)
            try:
                hist_item = _registrar_historico_gerada(
                    user_id=user_id,
                    resultado=resultado,
                    pasta_saida=pasta_saida,
                    origem="processamento",
                )
                if hist_item:
                    resultado["historico_gerada_id"] = hist_item.get("id", "")
                    resultado["historico_gerada_arquivo"] = hist_item.get("arquivo", "")
                    adicionar_log(estado, "Historico 24h atualizado (download disponivel).", "info")
            except Exception as e_hist:
                print(f"Aviso: falha ao registrar historico 24h user {user_id}: {e_hist}")

            # Registrar processamento no contador do usuario
            user = User.query.get(user_id)
            if user:
                user.registrar_processamento()

            adicionar_log(estado, f"Processamento concluido em {duracao}s!", "success")
            exec_info = {
                "ok": True,
                "erro": "",
                "total_etiquetas": resultado.get("total_etiquetas", 0),
                "total_lojas": resultado.get("total_lojas", 0),
            }

        except Exception as e:
            adicionar_log(estado, f"ERRO: {str(e)}", "error")
            import traceback
            adicionar_log(estado, traceback.format_exc(), "error")
            exec_info = {
                "ok": False,
                "erro": str(e),
                "total_etiquetas": 0,
                "total_lojas": 0,
            }

        finally:
            estado["processando"] = False
        return exec_info


def _formatar_tamanho(bytes_val):
    if bytes_val < 1024:
        return f"{bytes_val} B"
    elif bytes_val < 1024 * 1024:
        return f"{bytes_val / 1024:.1f} KB"
    else:
        return f"{bytes_val / (1024 * 1024):.1f} MB"


def _normalizar_nome_loja_match(nome: str) -> str:
    """Normaliza nome de loja para comparacoes robustas (fallback de envio)."""
    if not nome:
        return ""
    try:
        nome = _re.sub(r"\s+", " ", str(nome)).strip().lower()
        # Remove acentos
        import unicodedata
        nome = ''.join(ch for ch in unicodedata.normalize('NFD', nome) if unicodedata.category(ch) != 'Mn')
        return nome
    except Exception:
        return (str(nome) or "").strip().lower()


# ----------------------------------------------------------------
# WHATSAPP - FILA PERSISTENTE + SUPERVISOR BAILEYS
# ----------------------------------------------------------------

_BAILEYS_PROC = None
_BAILEYS_PROC_LOCK = threading.Lock()
_BAILEYS_LAST_START_TS = 0.0
_BAILEYS_LOG_HANDLES = []
_BACKGROUND_WORKERS_STARTED = False
_BACKGROUND_WORKERS_LOCK = threading.Lock()


def _whatsapp_api_base_url() -> str:
    return os.environ.get("WHATSAPP_API_URL", "http://localhost:3005").rstrip("/")


def _agora_utc() -> datetime:
    return datetime.utcnow()


def _to_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    txt = str(value).strip().lower()
    if txt in ("1", "true", "yes", "sim", "on"):
        return True
    if txt in ("0", "false", "no", "nao", "off"):
        return False
    return default


def _baileys_healthy(timeout=2.0) -> bool:
    try:
        resp = requests.get(f"{_whatsapp_api_base_url()}/health", timeout=timeout)
        return resp.status_code == 200
    except Exception:
        return False


def _garantir_baileys_rodando(motivo: str = "") -> bool:
    """Tenta manter o baileys-api ativo (sem derrubar fluxo principal)."""
    global _BAILEYS_PROC, _BAILEYS_LAST_START_TS
    if _baileys_healthy():
        return True

    with _BAILEYS_PROC_LOCK:
        if _baileys_healthy():
            return True

        now_ts = time.time()
        if now_ts - _BAILEYS_LAST_START_TS < 15:
            return False

        api_base = _whatsapp_api_base_url().lower()
        if "localhost" not in api_base and "127.0.0.1" not in api_base:
            # Em ambiente com API remota, nao tenta spawn local.
            return False

        baileys_dir = os.path.join(_BASE_DIR, "baileys-api")
        server_js = os.path.join(baileys_dir, "server.js")
        node_bin = os.environ.get("NODE_BIN", "node")
        if not os.path.exists(server_js):
            print(f"[BaileysSupervisor] server.js nao encontrado em {server_js}")
            return False
        if shutil.which(node_bin) is None:
            print(f"[BaileysSupervisor] executavel Node.js nao encontrado: {node_bin}")
            return False

        # Evitar spawn duplicado se processo conhecido ainda estiver vivo.
        if _BAILEYS_PROC is not None and _BAILEYS_PROC.poll() is None:
            _BAILEYS_LAST_START_TS = now_ts
            return False

        try:
            stdout_path = os.path.join(baileys_dir, "baileys_stdout.log")
            stderr_path = os.path.join(baileys_dir, "baileys_stderr.log")
            os.makedirs(baileys_dir, exist_ok=True)
            stdout_f = open(stdout_path, "a", encoding="utf-8", buffering=1)
            stderr_f = open(stderr_path, "a", encoding="utf-8", buffering=1)
            _BAILEYS_LOG_HANDLES.extend([stdout_f, stderr_f])

            kwargs = {}
            if os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW"):
                kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
            elif os.name != "nt":
                kwargs["start_new_session"] = True

            _BAILEYS_PROC = subprocess.Popen(
                [node_bin, "server.js"],
                cwd=baileys_dir,
                stdout=stdout_f,
                stderr=stderr_f,
                stdin=subprocess.DEVNULL,
                **kwargs,
            )
            _BAILEYS_LAST_START_TS = now_ts
            print(f"[BaileysSupervisor] iniciado (PID={_BAILEYS_PROC.pid}) motivo={motivo or 'n/a'}")
        except Exception as e:
            print(f"[BaileysSupervisor] erro ao iniciar: {e}")
            return False

    # Aguarda subir (fora do lock).
    for _ in range(20):
        if _baileys_healthy(timeout=1.5):
            return True
        time.sleep(0.5)
    return False


def _calc_backoff_seconds(tentativas: int) -> int:
    # 8s, 16s, 32s... ate 15 min
    base = 8
    return min(900, base * (2 ** max((tentativas or 1) - 1, 0)))


def _enfileirar_envio_whatsapp_resultado(
    user_id: int,
    resultado: dict = None,
    origem: str = "manual",
    respeitar_toggle_auto: bool = False,
) -> dict:
    """Enfileira envios WhatsApp a partir do ultimo resultado processado."""
    uid = int(user_id)
    user = User.query.get(uid)
    if not user:
        return {"ok": False, "erro": "Usuario nao encontrado"}

    if respeitar_toggle_auto and not _to_bool(getattr(user, "auto_send_whatsapp", False), False):
        return {"ok": False, "erro": "Auto-envio desabilitado", "ignorado": True}

    estado = _get_estado(uid)
    resultado_ref = resultado or (estado.get("ultimo_resultado", {}) if estado else {})
    if not resultado_ref or not resultado_ref.get("lojas"):
        return {"ok": False, "erro": "Nenhum resultado para enviar"}

    contatos = WhatsAppContact.query.filter_by(user_id=uid, ativo=True).all()
    if not contatos:
        return {"ok": False, "erro": "Nenhum contato WhatsApp cadastrado"}

    pasta_saida = user.get_pasta_saida()
    agrupamentos_usuario = (estado or {}).get("agrupamentos", []) if estado else []
    entregas, diagnostico = montar_entregas_por_resultado(
        resultado=resultado_ref,
        pasta_saida=pasta_saida,
        contatos=contatos,
        agrupamentos_usuario=agrupamentos_usuario,
    )
    if not entregas:
        return {"ok": False, "erro": "Nenhuma entrega valida para enviar", "diagnostico": diagnostico}

    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    agora = _agora_utc()
    enfileirados = 0
    for ent in entregas:
        file_path = ent.get("file_path", ent.get("pdf_path", ""))
        if not file_path:
            continue
        db.session.add(WhatsAppQueueItem(
            user_id=uid,
            batch_id=batch_id,
            origem=origem,
            loja_nome=ent.get("loja", ""),
            telefone=ent.get("telefone", ""),
            pdf_path=file_path,
            caption=ent.get("caption", ""),
            status="pending",
            tentativas=0,
            max_tentativas=5,
            next_attempt_at=agora,
        ))
        enfileirados += 1
    if enfileirados <= 0:
        db.session.rollback()
        return {"ok": False, "erro": "Nenhum arquivo valido para enfileirar", "diagnostico": diagnostico}
    db.session.commit()

    _garantir_baileys_rodando(motivo=f"queue:{origem}")
    return {
        "ok": True,
        "batch_id": batch_id,
        "total_entregas": enfileirados,
        "diagnostico": diagnostico,
    }


def _status_batch_fila_whatsapp(user_id: int, batch_id: str) -> dict:
    itens = WhatsAppQueueItem.query.filter_by(user_id=int(user_id), batch_id=batch_id).all()
    total = len(itens)
    counts = defaultdict(int)
    for it in itens:
        counts[it.status or "pending"] += 1
    sent = counts.get("sent", 0)
    dead = counts.get("dead", 0)
    pending = counts.get("pending", 0) + counts.get("retry", 0) + counts.get("sending", 0)
    done = sent + dead
    em_andamento = total > 0 and done < total
    if total == 0:
        etapa = "idle"
        detalhes = "Fila vazia"
    elif em_andamento:
        etapa = "enviando"
        detalhes = f"Fila em andamento: {sent}/{total} enviado(s), {counts.get('retry', 0)} em retry"
    else:
        etapa = "concluido" if dead == 0 else "parcial"
        detalhes = f"Fila finalizada: {sent}/{total} enviado(s), {dead} falha(s)"
    progresso = int((done / total) * 100) if total else 0
    return {
        "batch_id": batch_id,
        "etapa": etapa,
        "em_andamento": em_andamento,
        "progresso": progresso,
        "detalhes": detalhes,
        "total": total,
        "enviados": sent,
        "erros": dead,
        "retry": counts.get("retry", 0),
        "pendentes": pending,
    }


def _processar_fila_whatsapp_once() -> int:
    """Processa ate 8 itens da fila. Retorna quantos itens foram pegos."""
    agora = _agora_utc()

    # Rearm de itens presos em "sending".
    limite_stale = agora - timedelta(minutes=10)
    stale = WhatsAppQueueItem.query.filter(
        WhatsAppQueueItem.status == "sending",
        WhatsAppQueueItem.updated_at < limite_stale
    ).all()
    for item in stale:
        item.status = "retry"
        item.next_attempt_at = agora
        item.last_error = "Timeout interno: item rearmado automaticamente."
    if stale:
        db.session.commit()

    itens = WhatsAppQueueItem.query.filter(
        WhatsAppQueueItem.status.in_(["pending", "retry"]),
        WhatsAppQueueItem.next_attempt_at <= agora
    ).order_by(
        WhatsAppQueueItem.next_attempt_at.asc(),
        WhatsAppQueueItem.id.asc()
    ).limit(8).all()
    if not itens:
        return 0

    if not _baileys_healthy():
        _garantir_baileys_rodando(motivo="worker")
        return len(itens)

    wa = WhatsAppService()
    for item in itens:
        try:
            item.status = "sending"
            item.tentativas = (item.tentativas or 0) + 1
            item.updated_at = _agora_utc()
            db.session.commit()

            if not os.path.exists(item.pdf_path or ""):
                raise FileNotFoundError(f"Arquivo nao encontrado: {item.pdf_path}")

            res = wa.enviar_arquivo(item.telefone, item.pdf_path, item.caption or "")
            if res.get("success"):
                item.status = "sent"
                item.sent_at = _agora_utc()
                item.message_id = (res.get("messageId") or "")[:190]
                item.last_error = ""
            else:
                msg = (res.get("error") or "Falha desconhecida")[:500]
                if (item.tentativas or 0) >= (item.max_tentativas or 5):
                    item.status = "dead"
                    item.last_error = msg
                else:
                    item.status = "retry"
                    item.next_attempt_at = _agora_utc() + timedelta(
                        seconds=_calc_backoff_seconds(item.tentativas or 1)
                    )
                    item.last_error = msg
            item.updated_at = _agora_utc()
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            try:
                item_ref = WhatsAppQueueItem.query.get(item.id)
                if item_ref:
                    item_ref.tentativas = (item_ref.tentativas or 0) + 1
                    msg = str(e)[:500]
                    if (item_ref.tentativas or 0) >= (item_ref.max_tentativas or 5):
                        item_ref.status = "dead"
                        item_ref.last_error = msg
                    else:
                        item_ref.status = "retry"
                        item_ref.next_attempt_at = _agora_utc() + timedelta(
                            seconds=_calc_backoff_seconds(item_ref.tentativas or 1)
                        )
                        item_ref.last_error = msg
                    item_ref.updated_at = _agora_utc()
                    db.session.commit()
            except Exception:
                db.session.rollback()
    return len(itens)


def _whatsapp_queue_worker_loop():
    print("[WhatsAppQueueWorker] iniciado")
    while True:
        try:
            with app.app_context():
                picked = _processar_fila_whatsapp_once()
            time.sleep(1.0 if picked else 3.0)
        except Exception as e:
            print(f"[WhatsAppQueueWorker] erro: {e}")
            time.sleep(5)


def _baileys_supervisor_loop():
    print("[BaileysSupervisor] iniciado")
    while True:
        try:
            _garantir_baileys_rodando(motivo="supervisor")
        except Exception as e:
            print(f"[BaileysSupervisor] erro no loop: {e}")
        time.sleep(20)


def _iniciar_background_workers():
    global _BACKGROUND_WORKERS_STARTED
    with _BACKGROUND_WORKERS_LOCK:
        if _BACKGROUND_WORKERS_STARTED:
            return
        _BACKGROUND_WORKERS_STARTED = True
        threading.Thread(target=_baileys_supervisor_loop, daemon=True).start()
        threading.Thread(target=_whatsapp_queue_worker_loop, daemon=True).start()


# ================================================================
# AUTOMACAO: UpSeller + WhatsApp + Agendamentos
# ================================================================

# ----------------------------------------------------------------
# ENDPOINTS - UPSELLER (sessao persistente, sem credenciais)
# ----------------------------------------------------------------

def _get_upseller_session_dir(user_id):
    """Retorna diretorio de sessao do UpSeller para o usuario."""
    d = os.path.join(os.path.expanduser("~"), ".upseller_sessions", str(user_id))
    os.makedirs(d, exist_ok=True)
    return d


def _get_or_create_upseller_config(user_id):
    """Retorna ou cria config do UpSeller (sem precisar de email/senha)."""
    config = UpSellerConfig.query.filter_by(user_id=user_id).first()
    if not config:
        config = UpSellerConfig(
            user_id=user_id,
            email="",
            session_dir=_get_upseller_session_dir(user_id),
        )
        config.password_encrypted = ""
        db.session.add(config)
        db.session.commit()
    # Corrigir session_dir antigo se necessario
    if not config.session_dir or '/tmp/' in config.session_dir or '\\tmp\\' in config.session_dir:
        config.session_dir = _get_upseller_session_dir(user_id)
        db.session.commit()
    return config


# ----------------------------------------------------------------
# MARKETPLACE API DIRETA (Shopee - fase 1)
# ----------------------------------------------------------------

def _get_or_create_marketplace_api_config(user_id, marketplace: str = "shopee"):
    mp = (marketplace or "shopee").strip().lower()
    cfg = MarketplaceApiConfig.query.filter_by(user_id=user_id, marketplace=mp).first()
    if not cfg:
        cfg = MarketplaceApiConfig(
            user_id=user_id,
            marketplace=mp,
            api_base_url="https://openplatform.sandbox.test-stable.shopee.sg",
            status_conexao="nao_configurado",
            ativo=False,
        )
        db.session.add(cfg)
        db.session.commit()
    return cfg


def _set_marketplace_sidebar_cache(user_id, sidebar_info):
    if not hasattr(app, "_marketplace_sidebar_cache"):
        app._marketplace_sidebar_cache = {}
    app._marketplace_sidebar_cache[int(user_id)] = sidebar_info or {}


def _get_marketplace_sidebar_cache(user_id):
    if hasattr(app, "_marketplace_sidebar_cache"):
        return app._marketplace_sidebar_cache.get(int(user_id), {}) or {}
    return {}


def _persistir_lojas_marketplace_api(user_id, lojas):
    """
    Persiste snapshot de lojas da API direta em tabela separada.
    Nao interfere na tabela `lojas` usada pela automacao UpSeller.
    """
    agora = datetime.utcnow()
    lojas = lojas or []

    existentes = MarketplaceLoja.query.filter_by(user_id=user_id, ativo=True).all()
    mapa = {}
    for l in existentes:
        key = f"{(l.marketplace or '').strip().casefold()}::{_norm_loja_nome(l.nome)}"
        mapa[key] = l

    keys_snapshot = set()
    for lj in lojas:
        nome = str(lj.get("nome", "") or "").strip()
        if not nome:
            continue
        marketplace = str(lj.get("marketplace", "Shopee") or "Shopee").strip() or "Shopee"
        key = f"{marketplace.casefold()}::{_norm_loja_nome(nome)}"
        keys_snapshot.add(key)

        try:
            pedidos = max(0, int(lj.get("pedidos", 0) or 0))
        except Exception:
            pedidos = 0
        try:
            notas = max(0, int(lj.get("notas_pendentes", 0) or 0))
        except Exception:
            notas = 0
        try:
            etiquetas = max(0, int(lj.get("etiquetas_pendentes", 0) or 0))
        except Exception:
            etiquetas = 0

        row = mapa.get(key)
        if row:
            row.nome = nome
            row.marketplace = marketplace
            row.pedidos_pendentes = pedidos
            row.notas_pendentes = notas
            row.etiquetas_pendentes = etiquetas
            row.ultima_atualizacao = agora
            row.ativo = True
        else:
            row = MarketplaceLoja(
                user_id=user_id,
                marketplace=marketplace,
                nome=nome,
                pedidos_pendentes=pedidos,
                notas_pendentes=notas,
                etiquetas_pendentes=etiquetas,
                ultima_atualizacao=agora,
                ativo=True,
            )
            db.session.add(row)
            mapa[key] = row

    # Zera as que nao vieram no snapshot atual, mantendo historico da lista.
    for key, row in mapa.items():
        if key not in keys_snapshot:
            row.pedidos_pendentes = 0
            row.notas_pendentes = 0
            row.etiquetas_pendentes = 0
            row.ultima_atualizacao = agora

    db.session.commit()


class _ShopeeOpenApiClient:
    """
    Cliente minimo Shopee Open API v2 para sincronizacao de contagens.
    Fase 1: leitura de loja e pedidos (READY_TO_SHIP).
    """

    def __init__(self, cfg: MarketplaceApiConfig):
        self.cfg = cfg
        self.base_url = (cfg.api_base_url or "https://openplatform.sandbox.test-stable.shopee.sg").strip().rstrip("/")
        self.partner_id = str(cfg.partner_id or "").strip()
        self.partner_key = str(cfg.get_partner_key() or "").strip()
        self.shop_id = str(cfg.shop_id or "").strip()
        self.access_token = str(cfg.get_access_token() or "").strip()
        self.refresh_token = str(cfg.get_refresh_token() or "").strip()

    def _timestamp(self):
        return int(time.time())

    def _sign(self, path: str, timestamp: int, access_token: str = "", shop_id: str = ""):
        base = f"{self.partner_id}{path}{timestamp}{access_token}{shop_id}"
        return hmac.new(
            self.partner_key.encode("utf-8"),
            base.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    def _request(self, method: str, path: str, params=None, body=None, with_auth=True, timeout=30):
        ts = self._timestamp()
        params = dict(params or {})
        sign = self._sign(path, ts, self.access_token if with_auth else "", self.shop_id if with_auth else "")
        params.update({
            "partner_id": self.partner_id,
            "timestamp": ts,
            "sign": sign,
        })
        if with_auth:
            params["access_token"] = self.access_token
            params["shop_id"] = self.shop_id

        url = f"{self.base_url}{path}"
        if method.upper() == "GET":
            resp = requests.get(url, params=params, timeout=timeout)
        else:
            resp = requests.post(url, params=params, json=body or {}, timeout=timeout)

        try:
            data = resp.json()
        except Exception:
            data = {"error": f"http_{resp.status_code}", "message": resp.text[:500]}

        if resp.status_code >= 400:
            return {"ok": False, "http_status": resp.status_code, "data": data}
        err = str((data or {}).get("error") or "").strip()
        if err:
            return {"ok": False, "http_status": resp.status_code, "data": data}
        return {"ok": True, "http_status": resp.status_code, "data": data}

    def refresh_access_token(self):
        """
        Refresh de access token Shopee.
        """
        path = "/api/v2/auth/access_token/get"
        ts = self._timestamp()
        sign = self._sign(path, ts, "", "")
        params = {
            "partner_id": self.partner_id,
            "timestamp": ts,
            "sign": sign,
        }
        body = {
            "shop_id": int(self.shop_id) if str(self.shop_id).isdigit() else self.shop_id,
            "refresh_token": self.refresh_token,
            "partner_id": int(self.partner_id) if str(self.partner_id).isdigit() else self.partner_id,
        }
        url = f"{self.base_url}{path}"
        resp = requests.post(url, params=params, json=body, timeout=30)
        try:
            data = resp.json()
        except Exception:
            data = {"error": f"http_{resp.status_code}", "message": resp.text[:500]}
        if resp.status_code >= 400 or str((data or {}).get("error") or "").strip():
            return {"ok": False, "data": data, "http_status": resp.status_code}

        payload = (data or {}).get("response") or {}
        return {
            "ok": True,
            "access_token": str(payload.get("access_token") or "").strip(),
            "refresh_token": str(payload.get("refresh_token") or "").strip(),
            "expire_in": int(payload.get("expire_in") or 0),
            "shop_id": str(payload.get("shop_id") or self.shop_id).strip(),
            "data": data,
        }

    def get_shop_info(self):
        return self._request("GET", "/api/v2/shop/get_shop_info", with_auth=True)

    def get_order_count(self, order_status: str, days: int = 15, page_size: int = 100):
        """
        Conta pedidos por status usando paginacao do get_order_list.
        Obs: Shopee limita janela de tempo; usamos ultimos `days` dias.
        """
        now_ts = int(time.time())
        from_ts = now_ts - max(1, int(days or 15)) * 24 * 3600
        cursor = ""
        total = 0
        loops = 0

        while loops < 80:
            loops += 1
            params = {
                "time_range_field": "create_time",
                "time_from": from_ts,
                "time_to": now_ts,
                "page_size": min(max(int(page_size or 100), 1), 100),
                "cursor": cursor,
                "order_status": order_status,
            }
            ret = self._request("GET", "/api/v2/order/get_order_list", params=params, with_auth=True)
            if not ret.get("ok"):
                return {"ok": False, "erro": (ret.get("data") or {}).get("message") or (ret.get("data") or {}).get("error") or "falha_get_order_list"}

            resp = ((ret.get("data") or {}).get("response") or {})
            order_list = resp.get("order_list") or []
            total += len(order_list)
            has_next = bool(resp.get("more"))
            cursor = str(resp.get("next_cursor") or "").strip()
            if not has_next or not cursor:
                break

        return {"ok": True, "total": total}


def _marketplace_cfg_to_client(cfg: MarketplaceApiConfig):
    if not cfg:
        return None, "config_nao_encontrada"
    c = _ShopeeOpenApiClient(cfg)
    if not c.partner_id or not c.partner_key or not c.shop_id or not c.access_token:
        return None, "credenciais_incompletas"
    return c, ""


def _marketplace_shopee_sync_snapshot(user_id: int):
    """
    Faz sincronizacao Shopee API e retorna snapshot de lojas do modo API.
    """
    cfg = _get_or_create_marketplace_api_config(user_id, "shopee")
    cli, err = _marketplace_cfg_to_client(cfg)
    if not cli:
        return {"sucesso": False, "erro": err}

    # Tentativa de refresh preventivo quando token estiver perto do vencimento.
    try:
        if cfg.token_expires_at and cfg.token_expires_at <= (datetime.utcnow() + timedelta(minutes=10)):
            ref = cli.refresh_access_token()
            if ref.get("ok"):
                cfg.set_access_token(ref.get("access_token", ""))
                if ref.get("refresh_token"):
                    cfg.set_refresh_token(ref.get("refresh_token", ""))
                cfg.token_expires_at = datetime.utcnow() + timedelta(seconds=max(1, int(ref.get("expire_in") or 0)))
                db.session.commit()
                cli = _ShopeeOpenApiClient(cfg)
    except Exception:
        pass

    shop = cli.get_shop_info()
    if not shop.get("ok"):
        # Se erro de token, tenta refresh e repete uma vez.
        msg = str(((shop.get("data") or {}).get("message") or "")).lower()
        err_code = str(((shop.get("data") or {}).get("error") or "")).lower()
        if "token" in msg or "token" in err_code:
            ref = cli.refresh_access_token()
            if ref.get("ok"):
                cfg.set_access_token(ref.get("access_token", ""))
                if ref.get("refresh_token"):
                    cfg.set_refresh_token(ref.get("refresh_token", ""))
                cfg.token_expires_at = datetime.utcnow() + timedelta(seconds=max(1, int(ref.get("expire_in") or 0)))
                db.session.commit()
                cli = _ShopeeOpenApiClient(cfg)
                shop = cli.get_shop_info()
        if not shop.get("ok"):
            return {
                "sucesso": False,
                "erro": (shop.get("data") or {}).get("message") or (shop.get("data") or {}).get("error") or "falha_shop_info"
            }

    shop_info = ((shop.get("data") or {}).get("response") or {})
    loja_nome = (cfg.loja_nome or "").strip() or str(shop_info.get("shop_name") or "").strip() or f"Shopee Shop {cfg.shop_id}"

    # Fase 1: usamos READY_TO_SHIP como base de pedidos pendentes.
    cnt_ready = cli.get_order_count("READY_TO_SHIP", days=15)
    if not cnt_ready.get("ok"):
        return {"sucesso": False, "erro": cnt_ready.get("erro", "falha_contagem_ready_to_ship")}
    pedidos = int(cnt_ready.get("total") or 0)

    # Shopee API nao expoe diretamente "nota fiscal pendente" no mesmo formato do UpSeller.
    notas = 0
    etiquetas = pedidos

    lojas = [{
        "nome": loja_nome,
        "marketplace": "Shopee",
        "pedidos": pedidos,
        "notas_pendentes": notas,
        "etiquetas_pendentes": etiquetas,
    }]
    sidebar = {
        "Para Enviar": pedidos,
        "Para Emitir": notas,
        "Para Imprimir": etiquetas,
    }

    _persistir_lojas_marketplace_api(user_id, lojas)
    _set_marketplace_sidebar_cache(user_id, sidebar)

    cfg.status_conexao = "ok"
    cfg.ultima_sincronizacao = datetime.utcnow()
    cfg.ativo = True
    db.session.commit()

    return {
        "sucesso": True,
        "lojas": lojas,
        "total_pedidos": pedidos,
        "sidebar_info": sidebar,
        "detalhes": f"Shopee sincronizado: {pedidos} pedido(s) READY_TO_SHIP",
    }


# ----------------------------------------------------------------
# GERENCIADOR GLOBAL DE SCRAPERS UPSELLER (mantém instância viva)
# ----------------------------------------------------------------
# Problema: UpSeller usa cookies de sessão (não-persistentes) que são
# perdidos quando o Playwright fecha. Solução: manter o Playwright
# rodando em background com o navegador minimizado.

import asyncio as _asyncio_global
import threading as _threading_global

class _UpSellerManager:
    """Gerencia instâncias do UpSellerScraper por usuário, mantendo-as vivas."""

    def __init__(self):
        self._scrapers = {}      # user_id -> scraper instance
        self._loops = {}         # user_id -> asyncio event loop
        self._threads = {}       # user_id -> thread running the loop
        self._locks = {}         # user_id -> threading.Lock
        self._global_lock = _threading_global.Lock()

    def _get_lock(self, user_id):
        with self._global_lock:
            if user_id not in self._locks:
                self._locks[user_id] = _threading_global.RLock()
            return self._locks[user_id]

    def _ensure_loop(self, user_id):
        """Garante que existe um event loop rodando para o user_id."""
        if user_id in self._loops and self._loops[user_id].is_running():
            return self._loops[user_id]

        loop = _asyncio_global.new_event_loop()

        def _run_loop():
            _asyncio_global.set_event_loop(loop)
            loop.run_forever()

        t = _threading_global.Thread(target=_run_loop, daemon=True, name=f"upseller-loop-{user_id}")
        t.start()
        self._loops[user_id] = loop
        self._threads[user_id] = t
        return loop

    def _run_async(self, user_id, coro, timeout=300):
        """Executa coroutine no loop do user_id e retorna resultado."""
        loop = self._ensure_loop(user_id)
        future = _asyncio_global.run_coroutine_threadsafe(coro, loop)
        return future.result(timeout=timeout)

    def get_scraper(self, user_id):
        """Retorna scraper existente ou None."""
        return self._scrapers.get(user_id)

    def is_alive(self, user_id):
        """Verifica se o scraper do user_id está vivo e com página aberta."""
        scraper = self._scrapers.get(user_id)
        if not scraper or not scraper._page:
            return False
        try:
            # Testa se a página ainda responde
            self._run_async(user_id, scraper._page.evaluate("1+1"))
            return True
        except Exception:
            return False

    def is_logged_in(self, user_id):
        """Verifica se o scraper está vivo E logado no UpSeller."""
        scraper = self._scrapers.get(user_id)
        if not scraper or not scraper._page:
            return False
        try:
            return self._run_async(user_id, scraper._esta_logado())
        except Exception:
            return False

    def criar_scraper(self, user_id, config_session_dir, download_dir, headless=False):
        """Cria novo scraper, fechando o anterior se existir."""
        from upseller_scraper import UpSellerScraper

        lock = self._get_lock(user_id)
        with lock:
            # Fechar anterior se existir
            self._fechar_interno(user_id)

            scraper = UpSellerScraper({
                "email": "",
                "password": "",
                "profile_dir": config_session_dir,
                "headless": headless,
                "download_dir": download_dir,
            })
            self._scrapers[user_id] = scraper

            # Iniciar navegador
            self._run_async(user_id, scraper._iniciar_navegador())
            return scraper

    def login_manual(self, user_id, timeout=180):
        """Executa login_manual no scraper do user_id."""
        scraper = self._scrapers.get(user_id)
        if not scraper:
            raise RuntimeError("Scraper não inicializado. Chame criar_scraper primeiro.")
        return self._run_async(user_id, scraper.login_manual(timeout_seconds=timeout))

    def listar_lojas(self, user_id):
        """Executa listar_lojas_pendentes no scraper do user_id."""
        scraper = self._scrapers.get(user_id)
        if not scraper:
            raise RuntimeError("Scraper não inicializado")
        return self._run_async(user_id, scraper.listar_lojas_pendentes())

    def contar_loja(self, user_id, nome_loja, pedidos_fallback=0, marketplace_fallback=""):
        """Executa contagem precisa de uma loja no scraper do user_id."""
        scraper = self._scrapers.get(user_id)
        if not scraper:
            raise RuntimeError("Scraper nao inicializado")
        return self._run_async(
            user_id,
            scraper.contar_pedidos_loja(
                nome_loja=nome_loja,
                pedidos_fallback=pedidos_fallback,
                marketplace_fallback=marketplace_fallback,
            ),
        )

    def esta_logado(self, user_id):
        """Verifica login no scraper do user_id."""
        scraper = self._scrapers.get(user_id)
        if not scraper:
            return False
        try:
            return self._run_async(user_id, scraper._esta_logado())
        except Exception:
            return False

    def reconectar(self, user_id, config_session_dir, download_dir):
        """
        Tenta reconectar automaticamente:
        1. Se scraper está vivo e logado → retorna True
        2. Se scraper está vivo mas deslogado → tenta navegar de volta
        3. Se scraper morreu → cria novo com headless=False para login manual
        """
        lock = self._get_lock(user_id)
        with lock:
            # 1. Scraper vivo e logado?
            if self.is_alive(user_id):
                try:
                    logado = self._run_async(user_id, self._scrapers[user_id]._esta_logado())
                    if logado:
                        print(f"[UpSellerManager] User {user_id}: já está logado!")
                        return True
                except Exception:
                    pass

            # 2. Criar novo scraper headless e testar sessão persistente
            try:
                scraper = self.criar_scraper(user_id, config_session_dir, download_dir, headless=True)
                logado = self._run_async(user_id, scraper._esta_logado())
                if logado:
                    print(f"[UpSellerManager] User {user_id}: sessão persistente reutilizada (headless)!")
                    return True
            except Exception as e:
                print(f"[UpSellerManager] Erro tentando headless: {e}")

            # 3. Sessão expirou → abrir visível para login manual
            try:
                self._fechar_interno(user_id)
                scraper = self.criar_scraper(user_id, config_session_dir, download_dir, headless=False)
                logado = self._run_async(user_id, scraper.login_manual(timeout_seconds=180))
                if logado:
                    # Minimizar janela após login bem-sucedido
                    try:
                        self._run_async(user_id, scraper._page.evaluate("""
                            (() => { try { window.resizeTo(1,1); window.moveTo(-2000,-2000); } catch(e){} })()
                        """))
                    except Exception:
                        pass
                    print(f"[UpSellerManager] User {user_id}: login manual concluído, scraper mantido vivo!")
                    return True
                else:
                    return False
            except Exception as e:
                print(f"[UpSellerManager] Erro no login manual: {e}")
                return False

    def _fechar_interno(self, user_id):
        """Fecha scraper e loop sem lock (chamado internamente)."""
        scraper = self._scrapers.pop(user_id, None)
        loop = self._loops.get(user_id)
        if scraper and loop and loop.is_running():
            try:
                future = _asyncio_global.run_coroutine_threadsafe(scraper.fechar(), loop)
                future.result(timeout=10)
            except Exception:
                pass
        # Parar o loop
        if loop and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        self._loops.pop(user_id, None)
        self._threads.pop(user_id, None)

    def fechar(self, user_id):
        """Fecha scraper do user_id (com lock)."""
        lock = self._get_lock(user_id)
        with lock:
            self._fechar_interno(user_id)

    def fechar_todos(self):
        """Fecha todos os scrapers (para shutdown)."""
        for uid in list(self._scrapers.keys()):
            self.fechar(uid)


# Instância global do gerenciador
_upseller_mgr = _UpSellerManager()


def _norm_loja_nome(nome):
    txt = (nome or "").strip().casefold()
    txt = ''.join(ch for ch in unicodedata.normalize('NFD', txt) if unicodedata.category(ch) != 'Mn')
    txt = _re.sub(r"\s+", " ", txt).strip()
    return txt


def _sanitizar_lojas_selecionadas(user_id, lojas_raw):
    """
    Normaliza e valida lista de lojas recebida do frontend.
    Retorna (lojas_validas, lojas_ignoradas) preservando a ordem do usuario.
    """
    lojas_raw = lojas_raw if isinstance(lojas_raw, list) else []
    if not lojas_raw:
        return [], []

    lojas_db = Loja.query.filter_by(user_id=user_id, ativo=True).all()
    mapa_por_key = {}
    for l in lojas_db:
        nome = (l.nome or "").strip()
        key = _norm_loja_nome(nome)
        if key and key not in mapa_por_key:
            mapa_por_key[key] = nome

    validas = []
    ignoradas = []
    vistos = set()
    for item in lojas_raw:
        nome_in = str(item or "").strip()
        if not nome_in:
            continue
        key = _norm_loja_nome(nome_in)
        nome_canon = mapa_por_key.get(key)
        if not nome_canon:
            ignoradas.append(nome_in)
            continue
        if key in vistos:
            continue
        vistos.add(key)
        validas.append(nome_canon)

    return validas, ignoradas


def _agrupar_lojas_por_marketplace(user_id: int, lojas: list) -> list:
    """
    Agrupa lojas por marketplace preservando a ordem de selecao do usuario.
    Retorna lista [{marketplace: str, lojas: [nomes...]}].
    """
    lojas = lojas if isinstance(lojas, list) else []
    if not lojas:
        return []

    lojas_db = Loja.query.filter_by(user_id=user_id, ativo=True).all()
    mapa_db = {_norm_loja_nome(l.nome): l for l in lojas_db if l.nome}

    grupos = {}
    ordem_marketplaces = []
    for nome in lojas:
        nome_txt = str(nome or "").strip()
        if not nome_txt:
            continue
        key = _norm_loja_nome(nome_txt)
        loja_db = mapa_db.get(key)
        nome_canon = (loja_db.nome if loja_db and loja_db.nome else nome_txt).strip()
        marketplace = (loja_db.marketplace if loja_db else "").strip() or "Shopee"
        if marketplace not in grupos:
            grupos[marketplace] = []
            ordem_marketplaces.append(marketplace)
        if nome_canon not in grupos[marketplace]:
            grupos[marketplace].append(nome_canon)

    return [{"marketplace": mp, "lojas": grupos.get(mp, [])} for mp in ordem_marketplaces if grupos.get(mp)]


def _set_sidebar_cache(user_id, sidebar_info):
    if not hasattr(app, '_upseller_sidebar_cache'):
        app._upseller_sidebar_cache = {}
    app._upseller_sidebar_cache[user_id] = sidebar_info or {}


def _get_sidebar_cache(user_id):
    if hasattr(app, '_upseller_sidebar_cache'):
        return app._upseller_sidebar_cache.get(user_id, {}) or {}
    return {}


def _criar_pasta_lote_upseller(pasta_entrada: str, prefixo: str = "lote") -> str:
    """
    Cria pasta de lote isolada para cada execucao do UpSeller.
    Evita que uma geracao reutilize arquivos de execucoes anteriores.
    """
    lotes_base = os.path.join(pasta_entrada, "_upseller_lotes")
    os.makedirs(lotes_base, exist_ok=True)
    nome_lote = f"{prefixo}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    pasta_lote = os.path.join(lotes_base, nome_lote)
    os.makedirs(pasta_lote, exist_ok=True)
    return pasta_lote


def _persistir_lojas_upseller(user_id, lojas):
    """
    Upsert da lista de lojas no banco:
    - mantem todas as lojas ativas visiveis (inclusive com 0 pedidos)
    - atualiza marketplace, pedidos e timestamp
    - lojas nao retornadas no snapshot atual ficam com 0 pedidos
    """
    agora = datetime.utcnow()
    lojas = lojas or []

    existentes = Loja.query.filter_by(user_id=user_id).all()
    mapa_existentes = {_norm_loja_nome(l.nome): l for l in existentes if l.nome}

    nomes_snapshot = set()
    for lj in lojas:
        nome = (lj.get("nome") or "").strip()
        if not nome:
            continue
        key = _norm_loja_nome(nome)
        pedidos = lj.get("pedidos", 0)
        try:
            pedidos = max(0, int(pedidos or 0))
        except Exception:
            pedidos = 0
        marketplace = (lj.get("marketplace") or "Shopee").strip() or "Shopee"

        notas = lj.get("notas_pendentes", 0)
        etiquetas = lj.get("etiquetas_pendentes", 0)
        try:
            notas = max(0, int(notas or 0))
        except Exception:
            notas = 0
        try:
            etiquetas = max(0, int(etiquetas or 0))
        except Exception:
            etiquetas = 0

        nomes_snapshot.add(key)
        loja_db = mapa_existentes.get(key)
        if loja_db:
            loja_db.nome = nome  # atualiza capitalizacao/nome exibido
            loja_db.marketplace = marketplace
            loja_db.pedidos_pendentes = pedidos
            loja_db.notas_pendentes = notas
            loja_db.etiquetas_pendentes = etiquetas
            loja_db.ultima_atualizacao = agora
            loja_db.ativo = True
        else:
            nova = Loja(
                user_id=user_id,
                nome=nome,
                marketplace=marketplace,
                pedidos_pendentes=pedidos,
                notas_pendentes=notas,
                etiquetas_pendentes=etiquetas,
                ultima_atualizacao=agora,
                ativo=True,
            )
            db.session.add(nova)
            mapa_existentes[key] = nova

    # Lojas fora do snapshot atual permanecem salvas, com 0 pedido.
    for key, loja_db in mapa_existentes.items():
        if loja_db.ativo and key not in nomes_snapshot:
            loja_db.pedidos_pendentes = 0
            loja_db.notas_pendentes = 0
            loja_db.etiquetas_pendentes = 0
            loja_db.ultima_atualizacao = agora

    db.session.commit()


def _contagens_lojas_suspeitas(lojas):
    """
    Heuristica para detectar snapshot claramente anomalo:
    muitas lojas com o MESMO valor alto de pedidos.
    """
    try:
        lojas = lojas or []
        if len(lojas) < 4:
            return False
        vals = []
        for lj in lojas:
            try:
                v = int((lj.get("pedidos", 0) if isinstance(lj, dict) else getattr(lj, "pedidos_pendentes", 0)) or 0)
            except Exception:
                v = 0
            if v > 0:
                vals.append(v)
        if len(vals) < 4:
            return False
        freq = {}
        for v in vals:
            freq[v] = freq.get(v, 0) + 1
        val_top, qtd_top = max(freq.items(), key=lambda x: x[1])
        ratio = qtd_top / max(len(vals), 1)
        # Ex.: 393 repetido em quase todas as lojas.
        return val_top >= 50 and ratio >= 0.75
    except Exception:
        return False


def _atualizar_lojas_apos_acao(user_id):
    """
    Rele snapshot atual de lojas no UpSeller e atualiza o banco.
    Usado apos programar/emitir/imprimir/gerar para manter contagens corretas.
    """
    try:
        user = db.session.get(User, user_id)
        config = _get_or_create_upseller_config(user_id)
        download_dir = user.get_pasta_entrada() if user else os.path.join(os.path.expanduser("~"), ".upseller_downloads")

        scraper_vivo = _upseller_mgr.is_alive(user_id)
        scraper_atual = _upseller_mgr.get_scraper(user_id) if scraper_vivo else None
        headless_atual = getattr(scraper_atual, 'headless', True) if scraper_atual else True

        # Para leitura confiavel do SPA (filtro loja + contadores), usar browser visivel.
        if (not scraper_vivo) or headless_atual:
            try:
                _upseller_mgr.criar_scraper(user_id, config.session_dir, download_dir, headless=False)
            except Exception as e:
                return {"sucesso": False, "erro": f"scraper_inativo: {e}"}

        if not _upseller_mgr.esta_logado(user_id):
            return {"sucesso": False, "erro": "sessao_expirada"}

        resultado = _upseller_mgr.listar_lojas(user_id)
        if not resultado or not resultado.get("sucesso"):
            return {"sucesso": False, "erro": (resultado or {}).get("erro", "falha_listar_lojas")}
        lojas_snap = resultado.get("lojas", []) or []
        if _contagens_lojas_suspeitas(lojas_snap):
            return {
                "sucesso": False,
                "erro": "Contagens suspeitas detectadas no UpSeller. Reconecte e tente sincronizar novamente.",
            }
        _persistir_lojas_upseller(user_id, lojas_snap)
        _set_sidebar_cache(user_id, resultado.get("sidebar_info", {}))
        return resultado
    except Exception as e:
        print(f"[Lojas] Falha ao atualizar snapshot apos acao: {e}")
        return {"sucesso": False, "erro": str(e)}


def _set_atualizacao_lojas_em_andamento(user_id, valor: bool):
    if not hasattr(app, "_lojas_atualizando"):
        app._lojas_atualizando = {}
    if not hasattr(app, "_lojas_atualizando_ts"):
        app._lojas_atualizando_ts = {}
    uid = int(user_id)
    ativo = bool(valor)
    app._lojas_atualizando[uid] = ativo
    if ativo:
        app._lojas_atualizando_ts[uid] = datetime.utcnow()
    else:
        app._lojas_atualizando_ts.pop(uid, None)


def _esta_atualizando_lojas(user_id) -> bool:
    if not hasattr(app, "_lojas_atualizando"):
        return False
    uid = int(user_id)
    ativo = bool(app._lojas_atualizando.get(uid, False))
    if not ativo:
        return False
    # Blindagem: se a thread travar, libera automaticamente apos TTL.
    ttl_segundos = 900  # 15 min
    ts = None
    if hasattr(app, "_lojas_atualizando_ts"):
        ts = app._lojas_atualizando_ts.get(uid)
    if isinstance(ts, datetime):
        if (datetime.utcnow() - ts).total_seconds() > ttl_segundos:
            app._lojas_atualizando[uid] = False
            if hasattr(app, "_lojas_atualizando_ts"):
                app._lojas_atualizando_ts.pop(uid, None)
            return False
    return True


def _obter_acao_massa_em_andamento(user_id):
    if not hasattr(app, "_acoes_massa"):
        app._acoes_massa = {}
    return app._acoes_massa.get(int(user_id))


def _iniciar_acao_massa(user_id, acao: str):
    """
    Trava de exclusao mutua para processos em massa.
    Garante 1 processo por usuario por vez (emitir/programar/imprimir/gerar).
    """
    if not hasattr(app, "_acoes_massa"):
        app._acoes_massa = {}
    uid = int(user_id)
    atual = app._acoes_massa.get(uid)
    if atual:
        return False, atual
    novo = {
        "acao": str(acao or "").strip() or "processo",
        "inicio": datetime.utcnow().isoformat() + "Z",
    }
    app._acoes_massa[uid] = novo
    return True, novo


def _finalizar_acao_massa(user_id, acao: str = ""):
    if not hasattr(app, "_acoes_massa"):
        return
    uid = int(user_id)
    atual = app._acoes_massa.get(uid)
    if not atual:
        return
    if acao and atual.get("acao") != acao:
        return
    app._acoes_massa.pop(uid, None)


def _disparar_atualizacao_lojas_background(user_id, status_attr: str = ""):
    """
    Atualiza snapshot de lojas em background sem bloquear retorno da acao principal.
    """
    user_id = int(user_id)
    if _esta_atualizando_lojas(user_id):
        return False

    _set_atualizacao_lojas_em_andamento(user_id, True)

    if status_attr and hasattr(app, status_attr):
        status_map = getattr(app, status_attr)
        st = status_map.get(user_id)
        if isinstance(st, dict):
            st["atualizando_pedidos"] = True
            st["aviso_atualizacao"] = "Atualizando pedidos em segundo plano..."

    def _runner():
        with app.app_context():
            try:
                ret = _atualizar_lojas_apos_acao(user_id)
                if status_attr and hasattr(app, status_attr):
                    status_map = getattr(app, status_attr)
                    st = status_map.get(user_id)
                    if isinstance(st, dict):
                        st["atualizacao_lojas_ok"] = bool((ret or {}).get("sucesso"))
                        if not bool((ret or {}).get("sucesso")):
                            st["aviso_atualizacao"] = (ret or {}).get("erro", "Falha ao atualizar pedidos em segundo plano")
            except Exception as e:
                if status_attr and hasattr(app, status_attr):
                    status_map = getattr(app, status_attr)
                    st = status_map.get(user_id)
                    if isinstance(st, dict):
                        st["atualizacao_lojas_ok"] = False
                        st["aviso_atualizacao"] = str(e)
            finally:
                _set_atualizacao_lojas_em_andamento(user_id, False)
                if status_attr and hasattr(app, status_attr):
                    status_map = getattr(app, status_attr)
                    st = status_map.get(user_id)
                    if isinstance(st, dict):
                        st["atualizando_pedidos"] = False

    threading.Thread(target=_runner, daemon=True).start()
    return True


def _atualizar_loja_individual(user_id, nome_loja):
    """
    Atualiza somente UMA loja no banco via filtro dedicado do UpSeller.
    Mantem as demais lojas intactas.
    """
    nome = (nome_loja or "").strip()
    if not nome:
        return {"sucesso": False, "erro": "loja_obrigatoria"}

    try:
        user = db.session.get(User, user_id)
        config = _get_or_create_upseller_config(user_id)
        download_dir = user.get_pasta_entrada() if user else os.path.join(os.path.expanduser("~"), ".upseller_downloads")

        scraper_vivo = _upseller_mgr.is_alive(user_id)
        scraper_atual = _upseller_mgr.get_scraper(user_id) if scraper_vivo else None
        headless_atual = getattr(scraper_atual, 'headless', True) if scraper_atual else True

        # Para leitura confiavel do SPA usar browser visivel.
        if (not scraper_vivo) or headless_atual:
            try:
                _upseller_mgr.criar_scraper(user_id, config.session_dir, download_dir, headless=False)
            except Exception as e:
                return {"sucesso": False, "erro": f"scraper_inativo: {e}"}

        if not _upseller_mgr.esta_logado(user_id):
            return {"sucesso": False, "erro": "sessao_expirada"}

        loja_db = Loja.query.filter_by(user_id=user_id, nome=nome).first()
        if not loja_db:
            # Busca case-insensitive para evitar duplicidade por capitalizacao.
            loja_db = next(
                (l for l in Loja.query.filter_by(user_id=user_id, ativo=True).all()
                 if _norm_loja_nome(l.nome) == _norm_loja_nome(nome)),
                None
            )

        pedidos_fb = int((loja_db.pedidos_pendentes if loja_db else 0) or 0)
        marketplace_fb = (loja_db.marketplace if loja_db else "Shopee") or "Shopee"

        resultado = _upseller_mgr.contar_loja(
            user_id,
            nome_loja=nome,
            pedidos_fallback=pedidos_fb,
            marketplace_fallback=marketplace_fb,
        )
        if not resultado or not resultado.get("sucesso"):
            return {"sucesso": False, "erro": (resultado or {}).get("erro", "falha_contagem_loja")}

        item = (resultado.get("loja") or {})
        nome_final = (item.get("nome") or nome).strip() or nome
        marketplace = (item.get("marketplace") or marketplace_fb).strip() or "Shopee"
        try:
            pedidos = max(0, int(item.get("pedidos", pedidos_fb) or 0))
        except Exception:
            pedidos = max(0, int(pedidos_fb or 0))

        agora = datetime.utcnow()
        if loja_db:
            loja_db.nome = nome_final
            loja_db.marketplace = marketplace
            loja_db.pedidos_pendentes = pedidos
            loja_db.ultima_atualizacao = agora
            loja_db.ativo = True
        else:
            loja_db = Loja(
                user_id=user_id,
                nome=nome_final,
                marketplace=marketplace,
                pedidos_pendentes=pedidos,
                ultima_atualizacao=agora,
                ativo=True,
            )
            db.session.add(loja_db)

        db.session.commit()

        lojas = Loja.query.filter_by(user_id=user_id, ativo=True).all()
        lojas.sort(key=lambda l: (-(l.pedidos_pendentes or 0), (l.nome or "").casefold()))
        total = sum(int(l.pedidos_pendentes or 0) for l in lojas)

        return {
            "sucesso": True,
            "loja": loja_db.to_dict(),
            "lojas": [l.to_dict() for l in lojas],
            "total_pedidos": total,
            "sidebar_info": _get_sidebar_cache(user_id),
            "fonte": item.get("_src", ""),
        }
    except Exception as e:
        print(f"[Lojas] Falha ao atualizar loja individual '{nome}': {e}")
        return {"sucesso": False, "erro": str(e)}


def _atualizar_todas_lojas_preciso(user_id):
    """
    Atualiza contagem de TODAS as lojas lendo diretamente do UpSeller.
    Usa listar_lojas() que le sidebar + tabela (rapido, sem navegar pagina por pagina).
    """
    try:
        user = db.session.get(User, user_id)
        config = _get_or_create_upseller_config(user_id)
        download_dir = user.get_pasta_entrada() if user else os.path.join(os.path.expanduser("~"), ".upseller_downloads")

        scraper_vivo = _upseller_mgr.is_alive(user_id)
        scraper_atual = _upseller_mgr.get_scraper(user_id) if scraper_vivo else None
        headless_atual = getattr(scraper_atual, 'headless', True) if scraper_atual else True

        if (not scraper_vivo) or headless_atual:
            try:
                _upseller_mgr.criar_scraper(user_id, config.session_dir, download_dir, headless=False)
            except Exception as e:
                return {"sucesso": False, "erro": f"scraper_inativo: {e}"}

        if not _upseller_mgr.esta_logado(user_id):
            return {"sucesso": False, "erro": "sessao_expirada"}

        # Leitura rapida: sidebar + tabela (sem paginacao loja-a-loja)
        snap = _upseller_mgr.listar_lojas(user_id)
        if not snap or not snap.get("sucesso"):
            return {"sucesso": False, "erro": (snap or {}).get("erro", "falha_snapshot_base")}

        lojas_snapshot = snap.get("lojas", []) or []
        _set_sidebar_cache(user_id, snap.get("sidebar_info", {}) or {})

        # Mesclar com lojas do banco para nao "sumirem"
        lojas_db = Loja.query.filter_by(user_id=user_id, ativo=True).all()
        snap_keys = set()
        for lj in lojas_snapshot:
            key = _norm_loja_nome((lj.get("nome") or "").strip())
            if key:
                snap_keys.add(key)
        # Incluir lojas do banco que nao apareceram no snapshot
        for ldb in lojas_db:
            key = _norm_loja_nome((ldb.nome or "").strip())
            if key and key not in snap_keys:
                lojas_snapshot.append({
                    "nome": ldb.nome,
                    "marketplace": ldb.marketplace or "Shopee",
                    "pedidos": 0,
                    "notas_pendentes": 0,
                    "etiquetas_pendentes": 0,
                })

        # Persistir todas as lojas (lista vazia se nao encontrou nada)
        _persistir_lojas_upseller(user_id, lojas_snapshot)

        lojas = Loja.query.filter_by(user_id=user_id, ativo=True).all()
        lojas.sort(key=lambda l: (-(l.pedidos_pendentes or 0), (l.nome or "").casefold()))
        total = sum(int(l.pedidos_pendentes or 0) for l in lojas)
        return {
            "sucesso": True,
            "lojas": [l.to_dict() for l in lojas],
            "total_pedidos": total,
            "sidebar_info": _get_sidebar_cache(user_id),
        }
    except Exception as e:
        err_txt = str(e).strip() or e.__class__.__name__
        if err_txt == "TimeoutError" or e.__class__.__name__.lower() == "timeouterror":
            err_txt = (
                "Timeout na atualizacao de lojas. "
                "Tente novamente com menos abas abertas no UpSeller."
            )
        print(f"[Lojas] Falha na atualizacao precisa de todas as lojas user {user_id}: {err_txt}")
        return {"sucesso": False, "erro": err_txt}


@app.route('/api/upseller/status', methods=['GET'])
@jwt_required()
def api_upseller_status():
    """Retorna status da conexao UpSeller."""
    user_id = int(get_jwt_identity())
    config = UpSellerConfig.query.filter_by(user_id=user_id).first()
    if config and config.status_conexao == "ok":
        return jsonify({
            "status": "ok",
            "ultima_sincronizacao": config.ultima_sincronizacao.strftime("%d/%m/%Y %H:%M") if config.ultima_sincronizacao else ""
        })
    return jsonify({"status": "desconectado", "ultima_sincronizacao": ""})


@app.route('/api/marketplace/status', methods=['GET'])
@jwt_required()
def api_marketplace_status():
    """Status da conexao API direta (fase 1: Shopee)."""
    user_id = int(get_jwt_identity())
    cfg = _get_or_create_marketplace_api_config(user_id, "shopee")
    data = cfg.to_dict()
    status = "ok" if data.get("configurado") and cfg.status_conexao == "ok" else "desconectado"
    return jsonify({
        "status": status,
        "marketplace": "shopee",
        "configurado": bool(data.get("configurado")),
        "loja_nome": data.get("loja_nome", ""),
        "shop_id": data.get("shop_id", ""),
        "ultima_sincronizacao": data.get("ultima_sincronizacao", ""),
        "status_conexao": data.get("status_conexao", "nao_configurado"),
        "api_base_url": data.get("api_base_url", ""),
        "has_partner_key": bool(data.get("has_partner_key")),
        "has_access_token": bool(data.get("has_access_token")),
        "has_refresh_token": bool(data.get("has_refresh_token")),
        "redirect_url": _get_shopee_redirect_url(),
        "redirect_domain": _get_shopee_redirect_domain(),
    })


@app.route('/api/marketplace/config', methods=['GET'])
@jwt_required()
def api_marketplace_config_get():
    """Retorna configuracao da API direta (Shopee)."""
    user_id = int(get_jwt_identity())
    cfg = _get_or_create_marketplace_api_config(user_id, "shopee")
    data = cfg.to_dict()
    data["redirect_url"] = _get_shopee_redirect_url()
    data["redirect_domain"] = _get_shopee_redirect_domain()
    return jsonify(data)


@app.route('/api/marketplace/shopee/redirect-info', methods=['GET'])
@jwt_required()
def api_marketplace_shopee_redirect_info():
    """Info de redirect URL/domain para configurar no painel Shopee."""
    return jsonify({
        "redirect_url": _get_shopee_redirect_url(),
        "redirect_domain": _get_shopee_redirect_domain(),
        "base_publica": _detectar_base_publica(),
    })


@app.route('/api/marketplace/shopee/debug-sign', methods=['POST'])
@jwt_required()
def api_marketplace_shopee_debug_sign():
    """
    Debug de assinatura Shopee v2 para comparacao 1:1.
    Retorna base_string, timestamp, sign e resposta bruta do endpoint auth_partner.
    """
    user_id = int(get_jwt_identity())
    cfg = _get_or_create_marketplace_api_config(user_id, "shopee")
    data = request.get_json(force=True, silent=True) or {}

    partner_id = str(cfg.partner_id or "").strip()
    partner_key = str(cfg.get_partner_key() or "").strip()
    if not partner_id:
        return jsonify({"status": "erro", "erro": "Partner ID nao configurado."}), 400
    if not partner_key:
        return jsonify({"status": "erro", "erro": "Partner Key nao configurada."}), 400

    ts_in = data.get("timestamp")
    try:
        ts = int(ts_in) if ts_in is not None and str(ts_in).strip() else int(time.time())
    except Exception:
        ts = int(time.time())

    path = "/api/v2/shop/auth_partner"
    base_string = f"{partner_id}{path}{ts}"
    sign = hmac.new(
        partner_key.encode("utf-8"),
        base_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    redirect_url = _get_shopee_redirect_url()
    state = _build_shopee_oauth_state(user_id)
    auth_base = (cfg.api_base_url or "https://openplatform.sandbox.test-stable.shopee.sg").strip().rstrip("/")
    query = {
        "partner_id": partner_id,
        "timestamp": ts,
        "sign": sign,
        "redirect": redirect_url,
        "state": state,
    }
    auth_url_preview = f"{auth_base}{path}?{urlencode(query)}"

    test_result = {
        "ok": False,
        "http_status": 0,
        "error": "",
        "message": "",
        "request_id": "",
        "location": "",
        "body_excerpt": "",
    }
    try:
        resp = requests.get(f"{auth_base}{path}", params=query, timeout=20, allow_redirects=False)
        test_result["http_status"] = int(resp.status_code)
        test_result["location"] = str(resp.headers.get("Location") or "")
        ctype = str(resp.headers.get("content-type") or "").lower()
        if "json" in ctype:
            try:
                j = resp.json() or {}
            except Exception:
                j = {}
            test_result["error"] = str(j.get("error") or "").strip()
            test_result["message"] = str(j.get("message") or "").strip()
            test_result["request_id"] = str(j.get("request_id") or "").strip()
            test_result["ok"] = not test_result["error"]
            test_result["body_excerpt"] = str(resp.text or "")[:500]
        else:
            txt = str(resp.text or "")
            test_result["body_excerpt"] = txt[:500]
            test_result["ok"] = resp.status_code in (200, 301, 302, 303, 307, 308)
    except Exception as e:
        test_result["error"] = "network_error"
        test_result["message"] = str(e)

    # Diagnostico detalhado se error_sign
    diagnostico = {}
    if test_result.get("error") == "error_sign":
        key_txt = partner_key
        diagnostico = {
            "causa": "Partner Key NAO corresponde ao Partner ID neste ambiente.",
            "key_format": f"{'v2 (shpk...)' if key_txt.startswith('shpk') else 'desconhecido'}, {len(key_txt)} chars",
            "ambiente": "Test/Sandbox" if "test-stable" in auth_base else "Producao",
            "solucao": (
                "Acesse open.shopee.com > App Management > seu app > "
                "copie App ID e App Key do MESMO app e ambiente."
            ),
        }

    return jsonify({
        "status": "ok",
        "debug": {
            "partner_id": partner_id,
            "api_base_url": auth_base,
            "path": path,
            "timestamp": ts,
            "base_string": base_string,
            "sign": sign,
            "redirect_url": redirect_url,
            "state": state,
            "auth_url_preview": auth_url_preview,
            "test_result": test_result,
            "diagnostico": diagnostico,
        }
    })


def _shopee_exchange_code_for_tokens_fast(
    partner_id: str, partner_key: str, base_url: str,
    code: str, shop_id: str,
):
    """
    Troca authorization code por access/refresh token na Shopee Open API.
    Versao otimizada: recebe credenciais ja prontas (sem DB/Fernet no caminho critico).
    O code da Shopee sandbox expira em ~30s, entao esta funcao deve ser chamada
    o mais rapido possivel apos receber o callback.
    """
    t0 = time.time()
    code_txt = str(code or "").strip()
    shop_txt = str(shop_id or "").strip()
    base_url = (base_url or "https://openplatform.sandbox.test-stable.shopee.sg").strip().rstrip("/")

    if not partner_id:
        return {"ok": False, "erro": "Partner ID nao configurado."}
    if not partner_key:
        return {"ok": False, "erro": "Partner Key nao configurada."}
    if not code_txt:
        return {"ok": False, "erro": "Authorization code ausente no callback."}
    if not shop_txt:
        return {"ok": False, "erro": "shop_id ausente no callback."}

    path = "/api/v2/auth/token/get"
    ts = int(time.time())
    base_string = f"{partner_id}{path}{ts}"
    sign = hmac.new(
        partner_key.encode("utf-8"),
        base_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    pid_int = int(partner_id)
    sid_int = int(shop_txt) if shop_txt.isdigit() else shop_txt
    params = {
        "partner_id": pid_int,
        "timestamp": ts,
        "sign": sign,
    }
    body = {
        "code": code_txt,
        "shop_id": sid_int,
        "partner_id": pid_int,
    }

    url = f"{base_url}{path}"
    t_pre = time.time()
    print(f"[Shopee token/get] POST {url} pid={pid_int} sid={sid_int} code={code_txt[:12]}... ts={ts} prep={int((t_pre-t0)*1000)}ms", flush=True, file=sys.stderr)
    try:
        resp = requests.post(url, params=params, json=body, timeout=30)
    except Exception as e:
        t_err = time.time()
        print(f"[Shopee token/get] REDE FALHOU em {int((t_err-t0)*1000)}ms: {e}", flush=True, file=sys.stderr)
        return {"ok": False, "erro": f"Falha de rede na troca de token: {e}"}

    t_resp = time.time()
    try:
        data = resp.json()
    except Exception:
        data = {"error": f"http_{resp.status_code}", "message": (resp.text or "")[:400]}

    print(f"[Shopee token/get] Status={resp.status_code} error={data.get('error','')} msg={data.get('message','')[:80]} total={int((t_resp-t0)*1000)}ms", flush=True, file=sys.stderr)

    if resp.status_code >= 400:
        msg = (data or {}).get("message") or (data or {}).get("error") or f"http_{resp.status_code}"
        return {"ok": False, "erro": f"Falha Shopee token/get: {msg}", "data": data}

    err = str((data or {}).get("error") or "").strip()
    if err:
        msg = (data or {}).get("message") or err
        return {"ok": False, "erro": f"Erro Shopee token/get: {msg}", "data": data}

    payload = (data or {}).get("response") or {}
    access_token = str(payload.get("access_token") or "").strip()
    refresh_token = str(payload.get("refresh_token") or "").strip()
    shop_final = str(payload.get("shop_id") or shop_txt).strip()
    expire_in = int(payload.get("expire_in") or 0)

    if not access_token:
        return {"ok": False, "erro": "Shopee nao retornou access_token.", "data": data}

    return {
        "ok": True,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "shop_id": shop_final,
        "expire_in": expire_in,
        "data": data,
    }


def _diagnosticar_partner_key_v2(cfg: MarketplaceApiConfig, partner_id: str, ts: int, sign_v2: str, redirect_url: str, state: str):
    """
    Diagnostica erro de assinatura antes de abrir o login.
    Faz um GET simples no auth_partner para checar se a Shopee aceita o sign.
    Sem logica v1/v2 — apenas testa e reporta.
    """
    base_url = (cfg.api_base_url or "https://openplatform.sandbox.test-stable.shopee.sg").strip().rstrip("/")
    path = "/api/v2/shop/auth_partner"
    try:
        r = requests.get(
            f"{base_url}{path}",
            params={
                "partner_id": partner_id,
                "timestamp": ts,
                "sign": sign_v2,
                "redirect": redirect_url,
                "state": state,
            },
            timeout=18,
            allow_redirects=False,
        )
        data = {}
        try:
            data = r.json() if "json" in (r.headers.get("content-type") or "") else {}
        except Exception:
            data = {}
        err = str((data or {}).get("error") or "").strip().lower()
        msg = str((data or {}).get("message") or "").strip()

        if err == "error_sign":
            return {
                "ok": False,
                "erro": (
                    f"Assinatura rejeitada pela Shopee (Wrong sign). "
                    f"Verifique Partner ID ({partner_id}), Partner Key e api_base_url ({base_url})."
                ),
            }

        if err == "invalid_partner_id":
            return {
                "ok": False,
                "erro": f"Partner ID {partner_id} nao existe no ambiente {base_url}.",
            }

        # Qualquer outro erro (ex: error_param sobre redirect) ou redirect 302 = sign OK
        return {"ok": True}

    except Exception:
        # Falha de rede/timeout nao bloqueia o fluxo de login.
        return {"ok": True}


@app.route('/api/marketplace/shopee/login-url', methods=['GET'])
@jwt_required()
def api_marketplace_shopee_login_url():
    """
    Gera URL de login/autorizacao Shopee para o assinante.
    Campos tecnicos devem estar configurados na aba ADM.
    """
    user_id = int(get_jwt_identity())
    cfg = _get_or_create_marketplace_api_config(user_id, "shopee")

    partner_id = str(cfg.partner_id or "").strip()
    partner_key = str(cfg.get_partner_key() or "").strip()
    redirect_url = _get_shopee_redirect_url()

    if not partner_id:
        return jsonify({"status": "erro", "erro": "Partner ID nao configurado na aba Admin."}), 400
    if not partner_key:
        return jsonify({"status": "erro", "erro": "Partner Key nao configurada na aba Admin."}), 400
    if not redirect_url:
        return jsonify({
            "status": "erro",
            "erro": "Redirect URL nao disponivel. Configure NGROK_URL ou SHOPEE_REDIRECT_BASE_URL."
        }), 400

    ts = int(time.time())
    path = "/api/v2/shop/auth_partner"
    base_string = f"{partner_id}{path}{ts}"
    sign = hmac.new(
        partner_key.encode("utf-8"),
        base_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    state = _build_shopee_oauth_state(user_id)
    diag = _diagnosticar_partner_key_v2(
        cfg=cfg,
        partner_id=partner_id,
        ts=ts,
        sign_v2=sign,
        redirect_url=redirect_url,
        state=state,
    )
    if not diag.get("ok"):
        return jsonify({
            "status": "erro",
            "erro": diag.get("erro") or "Falha de assinatura na Shopee API.",
        }), 400

    # Registrar OAuth pendente COM credenciais pre-carregadas.
    # Isso permite que o callback faca o exchange sem query DB/Fernet.
    auth_base = (cfg.api_base_url or "https://openplatform.sandbox.test-stable.shopee.sg").strip().rstrip("/")
    _register_pending_oauth(user_id, partner_id=partner_id, partner_key=partner_key, base_url=auth_base)
    auth_url = (
        f"{auth_base}{path}"
        f"?partner_id={quote_plus(partner_id)}"
        f"&timestamp={ts}"
        f"&sign={sign}"
        f"&redirect={quote_plus(redirect_url)}"
        f"&state={quote_plus(state)}"
    )

    return jsonify({
        "status": "ok",
        "auth_url": auth_url,
        "api_base_url": auth_base,
        "redirect_url": redirect_url,
        "redirect_domain": _get_shopee_redirect_domain(),
    })


@app.route('/api/marketplace/shopee/callback', methods=['GET'])
def api_marketplace_shopee_callback():
    """
    Endpoint de callback OAuth Shopee.
    OTIMIZADO: faz o token exchange ANTES de qualquer query DB,
    usando credenciais pre-carregadas no cache de OAuth pendente.
    O code sandbox da Shopee expira em ~30s.
    """
    t_start = time.time()
    code = str(request.args.get("code", "") or "").strip()
    shop_id = str(request.args.get("shop_id", "") or "").strip()
    state = str(request.args.get("state", "") or "").strip()
    print(f"[CALLBACK] t=0ms code={code[:16] if code else 'VAZIO'} shop_id={shop_id or 'VAZIO'} state={'SET' if state else 'VAZIO'}", flush=True, file=sys.stderr)

    if not code:
        return redirect('/?shopee_login=erro&msg=' + quote_plus("Shopee callback sem code."))

    # --- FASE 1: Identificar usuario e obter credenciais do cache (sem DB) ---
    user_id = None
    cached_creds = {}

    # Tentar state assinado primeiro
    user_id, err_state = _parse_shopee_oauth_state(state)

    # Fallback: cache de OAuth pendente (sandbox pode nao retornar state)
    if not user_id:
        fallback_uid, fallback_info = _find_pending_oauth_user()
        if fallback_uid:
            user_id = fallback_uid
            cached_creds = fallback_info
            print(f"[CALLBACK] t={int((time.time()-t_start)*1000)}ms fallback user_id={user_id}", flush=True, file=sys.stderr)
        else:
            return redirect('/?shopee_login=erro&msg=' + quote_plus(f"State invalido: {err_state}"))
    else:
        # State valido — buscar credenciais no cache pelo user_id
        info = _pending_shopee_oauth.get(int(user_id), {})
        if info:
            cached_creds = info

    # --- FASE 2: Exchange code IMEDIATAMENTE (usando cache se disponivel) ---
    partner_id = cached_creds.get("partner_id", "")
    partner_key = cached_creds.get("partner_key", "")
    base_url = cached_creds.get("base_url", "")

    # Se cache nao tem credenciais, carregar do DB (fallback)
    if not partner_id or not partner_key:
        print(f"[CALLBACK] t={int((time.time()-t_start)*1000)}ms SEM CACHE, carregando do DB...", flush=True, file=sys.stderr)
        cfg = _get_or_create_marketplace_api_config(int(user_id), "shopee")
        partner_id = str(cfg.partner_id or "").strip()
        partner_key = str(cfg.get_partner_key() or "").strip()
        base_url = (cfg.api_base_url or "https://openplatform.sandbox.test-stable.shopee.sg").strip()
        print(f"[CALLBACK] t={int((time.time()-t_start)*1000)}ms DB carregado pid={partner_id}", flush=True, file=sys.stderr)
    else:
        cfg = None  # sera carregado depois para salvar tokens
        print(f"[CALLBACK] t={int((time.time()-t_start)*1000)}ms CACHE OK pid={partner_id}", flush=True, file=sys.stderr)

    # EXCHANGE IMEDIATO — cada ms conta, code expira em ~30s
    troca = _shopee_exchange_code_for_tokens_fast(
        partner_id=partner_id,
        partner_key=partner_key,
        base_url=base_url,
        code=code,
        shop_id=shop_id,
    )
    t_exchange = time.time()
    print(f"[CALLBACK] t={int((t_exchange-t_start)*1000)}ms exchange ok={troca.get('ok')}", flush=True, file=sys.stderr)

    # --- FASE 3: Salvar resultado no DB (depois do exchange) ---
    if cfg is None:
        cfg = _get_or_create_marketplace_api_config(int(user_id), "shopee")

    if not troca.get("ok"):
        try:
            cfg.status_conexao = "erro"
            db.session.commit()
        except Exception:
            pass
        return redirect('/?shopee_login=erro&msg=' + quote_plus(troca.get("erro") or "Falha ao obter token Shopee."))

    cfg.shop_id = str(troca.get("shop_id") or shop_id or cfg.shop_id or "").strip()
    cfg.set_access_token(str(troca.get("access_token") or "").strip())
    refresh_token = str(troca.get("refresh_token") or "").strip()
    if refresh_token:
        cfg.set_refresh_token(refresh_token)
    expire_in = int(troca.get("expire_in") or 0)
    if expire_in > 0:
        cfg.token_expires_at = datetime.utcnow() + timedelta(seconds=expire_in)
    cfg.status_conexao = "ok"
    cfg.ativo = bool(cfg.configurado())
    _consume_pending_oauth(user_id)

    # Tenta preencher nome da loja automaticamente (nao critico).
    try:
        cli = _ShopeeOpenApiClient(cfg)
        info = cli.get_shop_info()
        if info.get("ok"):
            shop_name = str((((info.get("data") or {}).get("response") or {}).get("shop_name")) or "").strip()
            if shop_name:
                cfg.loja_nome = shop_name
    except Exception:
        pass

    db.session.commit()
    return redirect('/?shopee_login=ok')


@app.route('/api/marketplace/config', methods=['POST'])
@jwt_required()
def api_marketplace_config_set():
    """Salva configuracao da API Shopee direta."""
    user_id = int(get_jwt_identity())
    cfg = _get_or_create_marketplace_api_config(user_id, "shopee")
    data = request.get_json(force=True, silent=True) or {}

    cfg.loja_nome = str(data.get("loja_nome", cfg.loja_nome or "") or "").strip()
    cfg.api_base_url = str(data.get("api_base_url", cfg.api_base_url or "https://openplatform.sandbox.test-stable.shopee.sg") or "").strip() or "https://openplatform.sandbox.test-stable.shopee.sg"
    cfg.partner_id = str(data.get("partner_id", cfg.partner_id or "") or "").strip()
    cfg.shop_id = str(data.get("shop_id", cfg.shop_id or "") or "").strip()

    partner_key = data.get("partner_key", None)
    access_token = data.get("access_token", None)
    refresh_token = data.get("refresh_token", None)
    limpar_tokens = _to_bool(data.get("limpar_tokens"), False)

    if partner_key is not None:
        key_txt = str(partner_key or "").strip()
        if key_txt:
            cfg.set_partner_key(key_txt)
    if access_token is not None:
        at_txt = str(access_token or "").strip()
        if at_txt:
            cfg.set_access_token(at_txt)
        elif limpar_tokens:
            cfg.access_token_enc = ""
    if refresh_token is not None:
        rt_txt = str(refresh_token or "").strip()
        if rt_txt:
            cfg.set_refresh_token(rt_txt)
        elif limpar_tokens:
            cfg.refresh_token_enc = ""

    # Atualizar expiração opcional se vier do frontend.
    token_expires_in = data.get("token_expires_in")
    if token_expires_in is not None:
        try:
            sec = max(1, int(token_expires_in))
            cfg.token_expires_at = datetime.utcnow() + timedelta(seconds=sec)
        except Exception:
            pass

    cfg.ativo = bool(cfg.configurado())
    if not cfg.configurado():
        cfg.status_conexao = "nao_configurado"
    db.session.commit()
    data_out = cfg.to_dict()
    data_out["redirect_url"] = _get_shopee_redirect_url()
    data_out["redirect_domain"] = _get_shopee_redirect_domain()
    return jsonify(data_out)


@app.route('/api/marketplace/testar-conexao', methods=['POST'])
@jwt_required()
def api_marketplace_testar():
    """Testa credenciais da API Shopee (shop/get_shop_info)."""
    user_id = int(get_jwt_identity())
    cfg = _get_or_create_marketplace_api_config(user_id, "shopee")
    cli, err = _marketplace_cfg_to_client(cfg)
    if not cli:
        return jsonify({"status": "erro", "erro": err}), 400
    try:
        ret = cli.get_shop_info()
        if not ret.get("ok"):
            cfg.status_conexao = "erro"
            db.session.commit()
            return jsonify({
                "status": "erro",
                "erro": (ret.get("data") or {}).get("message") or (ret.get("data") or {}).get("error") or "falha_shop_info",
            }), 400
        info = ((ret.get("data") or {}).get("response") or {})
        shop_name = str(info.get("shop_name") or "").strip()
        if shop_name and not (cfg.loja_nome or "").strip():
            cfg.loja_nome = shop_name
        cfg.status_conexao = "ok"
        cfg.ativo = True
        db.session.commit()
        return jsonify({
            "status": "ok",
            "mensagem": "Conexao Shopee API validada",
            "shop_name": shop_name,
            "shop_id": cfg.shop_id,
        })
    except Exception as e:
        cfg.status_conexao = "erro"
        db.session.commit()
        return jsonify({"status": "erro", "erro": str(e)}), 500


@app.route('/api/marketplace/reconectar', methods=['POST'])
@jwt_required()
def api_marketplace_reconectar():
    """Alias de teste/revalidacao para API direta."""
    return api_marketplace_testar()


@app.route('/api/marketplace/desconectar', methods=['POST'])
@jwt_required()
def api_marketplace_desconectar():
    """Desativa conexao API direta, com opcao de limpar tokens."""
    user_id = int(get_jwt_identity())
    cfg = _get_or_create_marketplace_api_config(user_id, "shopee")
    data = request.get_json(silent=True) or {}
    limpar = _to_bool(data.get("limpar_tokens"), False)
    cfg.ativo = False
    cfg.status_conexao = "nao_configurado"
    if limpar:
        cfg.access_token_enc = ""
        cfg.refresh_token_enc = ""
    db.session.commit()
    return jsonify({"status": "desconectado", "mensagem": "API direta desativada"})


@app.route('/api/marketplace/lojas', methods=['GET'])
@jwt_required()
def api_marketplace_lojas_listar():
    """Lista lojas sincronizadas via API direta (separadas do UpSeller)."""
    user_id = int(get_jwt_identity())
    lojas = MarketplaceLoja.query.filter_by(user_id=user_id, ativo=True).all()
    lojas.sort(key=lambda l: (-(l.pedidos_pendentes or 0), (l.nome or "").casefold()))
    total = sum(int(l.pedidos_pendentes or 0) for l in lojas)
    return jsonify({
        "lojas": [l.to_dict() for l in lojas],
        "total_pedidos": total,
        "sidebar_info": _get_marketplace_sidebar_cache(user_id),
    })


@app.route('/api/marketplace/sincronizar', methods=['POST'])
@jwt_required()
def api_marketplace_sincronizar():
    """Sincroniza contagens via API direta (Shopee)."""
    user_id = int(get_jwt_identity())
    if not hasattr(app, "_marketplace_sync_em_andamento"):
        app._marketplace_sync_em_andamento = {}
    if not hasattr(app, "_marketplace_sync_status"):
        app._marketplace_sync_status = {}

    if app._marketplace_sync_em_andamento.get(user_id):
        return jsonify({"erro": "Sincronizacao API ja em andamento"}), 409

    def _worker():
        app._marketplace_sync_em_andamento[user_id] = True
        app._marketplace_sync_status[user_id] = {
            "etapa": "iniciando",
            "progresso": 5,
            "detalhes": "Iniciando sincronizacao Shopee API...",
            "em_andamento": True,
        }
        try:
            app._marketplace_sync_status[user_id].update({
                "etapa": "consultando",
                "progresso": 35,
                "detalhes": "Consultando pedidos na Shopee API...",
            })
            ret = _marketplace_shopee_sync_snapshot(user_id)
            if not ret.get("sucesso"):
                app._marketplace_sync_status[user_id].update({
                    "etapa": "erro",
                    "progresso": 0,
                    "detalhes": ret.get("erro", "Falha na sincronizacao API"),
                    "em_andamento": False,
                })
                return

            lojas = ret.get("lojas", []) or []
            total = int(ret.get("total_pedidos") or 0)
            app._marketplace_sync_status[user_id].update({
                "etapa": "concluido",
                "progresso": 100,
                "detalhes": ret.get("detalhes") or f"Concluido: {len(lojas)} loja(s), {total} pedido(s).",
                "em_andamento": False,
                "lojas_encontradas": lojas,
                "total_pedidos": total,
                "sidebar_info": ret.get("sidebar_info", {}),
            })
        except Exception as e:
            app._marketplace_sync_status[user_id].update({
                "etapa": "erro",
                "progresso": 0,
                "detalhes": str(e),
                "em_andamento": False,
            })
        finally:
            app._marketplace_sync_em_andamento[user_id] = False

    threading.Thread(target=_worker, daemon=True).start()
    return jsonify({"sucesso": True, "mensagem": "Sincronizacao API iniciada"})


@app.route('/api/marketplace/sincronizar/status', methods=['GET'])
@jwt_required()
def api_marketplace_sync_status():
    """Status da sincronizacao API direta."""
    user_id = int(get_jwt_identity())
    if hasattr(app, "_marketplace_sync_status") and user_id in app._marketplace_sync_status:
        st = dict(app._marketplace_sync_status[user_id])
        st["em_andamento"] = bool(hasattr(app, "_marketplace_sync_em_andamento") and app._marketplace_sync_em_andamento.get(user_id, False))
        return jsonify(st)
    return jsonify({
        "etapa": "idle",
        "progresso": 0,
        "detalhes": "",
        "em_andamento": False,
    })


@app.route('/api/lojas', methods=['GET'])
@jwt_required()
def api_lojas_listar():
    """Retorna todas as lojas persistidas do user (inclusive com 0 pedidos)."""
    user_id = int(get_jwt_identity())
    lojas = Loja.query.filter_by(user_id=user_id, ativo=True).all()
    lojas.sort(key=lambda l: (-(l.pedidos_pendentes or 0), (l.nome or "").casefold()))
    total = sum(l.pedidos_pendentes for l in lojas)
    return jsonify({
        "lojas": [l.to_dict() for l in lojas],
        "total_pedidos": total,
        "sidebar_info": _get_sidebar_cache(user_id),
    })


@app.route('/api/lojas/atualizar-todas', methods=['POST'])
@jwt_required()
def api_lojas_atualizar_todas():
    """Atualiza snapshot de TODAS as lojas (somente contagens), sem pipeline de geracao."""
    user_id = int(get_jwt_identity())

    # Evitar concorrencia com pipelines que usam o mesmo scraper.
    if (hasattr(app, '_gerar_em_andamento') and app._gerar_em_andamento.get(user_id)) or \
       (hasattr(app, '_imprimir_em_andamento') and app._imprimir_em_andamento.get(user_id)) or \
       (hasattr(app, '_sync_em_andamento') and app._sync_em_andamento.get(user_id)):
        return jsonify({"sucesso": False, "erro": "Aguarde a automacao atual finalizar"}), 409

    ret = _atualizar_todas_lojas_preciso(user_id)
    if not ret.get("sucesso"):
        return jsonify({"sucesso": False, "erro": ret.get("erro", "falha_atualizar_lojas")}), 400
    return jsonify(ret)


@app.route('/api/lojas/atualizar-individual', methods=['POST'])
@jwt_required()
def api_loja_atualizar_individual():
    """Atualiza contagem de uma loja especifica sem reprocessar a lista inteira."""
    user_id = int(get_jwt_identity())
    data = request.get_json(silent=True) or {}
    nome_loja = (data.get("loja") or "").strip()
    if not nome_loja:
        return jsonify({"sucesso": False, "erro": "Nome da loja e obrigatorio"}), 400

    # Evitar concorrencia com pipelines de automacao que usam o mesmo scraper.
    if (hasattr(app, '_gerar_em_andamento') and app._gerar_em_andamento.get(user_id)) or \
       (hasattr(app, '_imprimir_em_andamento') and app._imprimir_em_andamento.get(user_id)) or \
       (hasattr(app, '_sync_em_andamento') and app._sync_em_andamento.get(user_id)):
        return jsonify({"sucesso": False, "erro": "Aguarde a automacao atual finalizar"}), 409

    ret = _atualizar_loja_individual(user_id, nome_loja)
    if not ret.get("sucesso"):
        return jsonify(ret), 400
    return jsonify(ret)


@app.route('/api/upseller/conectar', methods=['POST'])
@jwt_required()
def api_upseller_conectar():
    """
    Abre navegador VISIVEL para login manual no UpSeller.
    MANTÉM o scraper vivo em background após login (cookies de sessão ficam na memória).
    """
    user_id = int(get_jwt_identity())
    config = _get_or_create_upseller_config(user_id)

    try:
        user = db.session.get(User, user_id)
        download_dir = user.get_pasta_entrada() if user else os.path.join(os.path.expanduser("~"), ".upseller_downloads")

        # Verificar se já está logado (scraper vivo)
        if _upseller_mgr.is_alive(user_id):
            try:
                logado = _upseller_mgr.esta_logado(user_id)
                if logado:
                    config.status_conexao = "ok"
                    db.session.commit()
                    return jsonify({"mensagem": "Já conectado ao UpSeller!", "status": "ok"})
            except Exception:
                pass

        # Criar scraper VISÍVEL e fazer login manual
        scraper = _upseller_mgr.criar_scraper(user_id, config.session_dir, download_dir, headless=False)
        logado = _upseller_mgr.login_manual(user_id, timeout=180)

        config.status_conexao = "ok" if logado else "erro"
        db.session.commit()

        if logado:
            # NÃO fecha o scraper! Ele fica vivo em background.
            return jsonify({"mensagem": "Conectado ao UpSeller! Navegador mantido em background.", "status": "ok"})

        # Se não logou, fechar scraper
        _upseller_mgr.fechar(user_id)
        return jsonify({"erro": "Login nao concluido. Tente novamente.", "status": "erro"}), 400

    except Exception as e:
        print(f"[UpSeller] Erro ao conectar: {e}")
        import traceback
        traceback.print_exc()
        _upseller_mgr.fechar(user_id)
        return jsonify({"erro": str(e), "status": "erro"}), 500


@app.route('/api/upseller/testar-conexao', methods=['POST'])
@jwt_required()
def api_upseller_testar():
    """Verifica se scraper está vivo e logado no UpSeller."""
    user_id = int(get_jwt_identity())
    config = _get_or_create_upseller_config(user_id)

    try:
        # Verificar scraper vivo primeiro (sem criar novo)
        if _upseller_mgr.is_alive(user_id):
            logado = _upseller_mgr.esta_logado(user_id)
            config.status_conexao = "ok" if logado else "erro"
            db.session.commit()
            if logado:
                return jsonify({"mensagem": "Sessao ativa!", "status": "ok"})
            return jsonify({"erro": "Sessao expirada. Clique em Reconectar.", "status": "erro"}), 400

        # Scraper não está vivo
        config.status_conexao = "desconectado"
        db.session.commit()
        return jsonify({"erro": "Navegador não está ativo. Clique em Conectar.", "status": "desconectado"}), 400

    except Exception as e:
        return jsonify({"erro": str(e), "status": "erro"}), 500


@app.route('/api/upseller/reconectar', methods=['POST'])
@jwt_required()
def api_upseller_reconectar():
    """
    Reconexão automática ao UpSeller:
    1. Se scraper vivo e logado → retorna OK imediatamente
    2. Se scraper vivo mas deslogado → tenta reconectar
    3. Se scraper morto → tenta criar headless (sessão persistente)
    4. Se tudo falhou → abre navegador visível para login manual
    """
    user_id = int(get_jwt_identity())
    config = _get_or_create_upseller_config(user_id)

    try:
        user = db.session.get(User, user_id)
        download_dir = user.get_pasta_entrada() if user else os.path.join(os.path.expanduser("~"), ".upseller_downloads")

        logado = _upseller_mgr.reconectar(user_id, config.session_dir, download_dir)

        config.status_conexao = "ok" if logado else "erro"
        db.session.commit()

        if logado:
            return jsonify({"mensagem": "Reconectado ao UpSeller!", "status": "ok"})
        return jsonify({"erro": "Não foi possível reconectar. Tente Conectar novamente.", "status": "erro"}), 400

    except Exception as e:
        print(f"[UpSeller] Erro ao reconectar: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"erro": str(e), "status": "erro"}), 500


@app.route('/api/upseller/desconectar', methods=['POST'])
@jwt_required()
def api_upseller_desconectar():
    """
    Desconecta do UpSeller: fecha o navegador/scraper, limpa sessao
    e reseta status. Permite trocar de conta.
    """
    user_id = int(get_jwt_identity())
    config = _get_or_create_upseller_config(user_id)

    try:
        # Fechar scraper (navegador)
        try:
            _upseller_mgr.fechar(user_id)
        except Exception:
            pass

        # Resetar status no banco
        config.status_conexao = "desconectado"
        db.session.commit()

        # Se pediu para limpar sessao, apagar diretorio de cookies
        data = request.get_json(silent=True) or {}
        limpar_sessao = data.get("limpar_sessao", False)
        if limpar_sessao and config.session_dir:
            import shutil
            try:
                shutil.rmtree(config.session_dir, ignore_errors=True)
                os.makedirs(config.session_dir, exist_ok=True)
            except Exception:
                pass

        return jsonify({
            "mensagem": "Desconectado do UpSeller. Clique 'Conectar' para fazer login com outra conta.",
            "status": "desconectado"
        })
    except Exception as e:
        print(f"[UpSeller] Erro ao desconectar: {e}")
        return jsonify({"erro": str(e), "status": "erro"}), 500


@app.route('/api/upseller/sincronizar', methods=['POST'])
@jwt_required()
def api_upseller_sincronizar():
    """
    Sincronizar = APENAS ler pedidos pendentes do UpSeller e mostrar lojas.
    NAO baixa nada, NAO processa nada. Rapido e leve.
    """
    user_id = int(get_jwt_identity())
    config = _get_or_create_upseller_config(user_id)

    if hasattr(app, '_sync_em_andamento') and app._sync_em_andamento.get(user_id):
        return jsonify({"erro": "Sincronizacao ja em andamento"}), 409

    def _sync_leitura():
        if not hasattr(app, '_sync_em_andamento'):
            app._sync_em_andamento = {}
        app._sync_em_andamento[user_id] = True
        if not hasattr(app, '_sync_status'):
            app._sync_status = {}
        app._sync_status[user_id] = {"etapa": "iniciando", "progresso": 0, "detalhes": "Conectando ao UpSeller..."}

        with app.app_context():
            try:
                # Etapa 1: Verificar se scraper está vivo e logado
                app._sync_status[user_id] = {"etapa": "login", "progresso": 20, "detalhes": "Verificando sessao UpSeller..."}

                user = db.session.get(User, user_id)
                download_dir = user.get_pasta_entrada() if user else os.path.join(os.path.expanduser("~"), ".upseller_downloads")

                scraper_vivo = _upseller_mgr.is_alive(user_id)
                scraper_atual = _upseller_mgr.get_scraper(user_id) if scraper_vivo else None
                headless_atual = getattr(scraper_atual, 'headless', True) if scraper_atual else True

                # Para leitura confiavel de lojas/filtros no UpSeller SPA, usar browser visivel.
                precisa_visivel = (not scraper_vivo) or headless_atual

                if precisa_visivel:
                    app._sync_status[user_id] = {"etapa": "reconectando", "progresso": 30, "detalhes": "Abrindo navegador visivel para sincronizacao precisa..."}
                    try:
                        scraper = _upseller_mgr.criar_scraper(user_id, config.session_dir, download_dir, headless=False)
                        logado = _upseller_mgr._run_async(user_id, scraper._esta_logado())
                    except Exception as e:
                        print(f"[Sync] Erro ao criar scraper visivel: {e}")
                        logado = False
                else:
                    # Scraper visivel e vivo: apenas validar sessao
                    logado = _upseller_mgr.esta_logado(user_id)

                if not logado:
                    app._sync_status[user_id] = {
                        "etapa": "erro", "progresso": 0,
                        "detalhes": "Sessao expirada. Clique 'Conectar ao UpSeller' para fazer login."
                    }
                    config.status_conexao = "desconectado"
                    db.session.commit()
                    return

                # Etapa 2: Ler pedidos pendentes usando scraper vivo
                app._sync_status[user_id] = {"etapa": "lendo_pedidos", "progresso": 60, "detalhes": "Lendo pedidos pendentes..."}
                resultado = _upseller_mgr.listar_lojas(user_id)

                # NÃO fecha o scraper! Ele continua vivo para próximas operações.

                # Atualizar config
                config.ultima_sincronizacao = datetime.utcnow()
                config.status_conexao = "ok"
                db.session.commit()

                # Concluir
                if resultado.get("sucesso"):
                    lojas = resultado.get("lojas", [])
                    total = resultado.get("total_pedidos", 0)
                    sidebar = resultado.get("sidebar_info", {})

                    # Persistir snapshot completo de lojas (incluindo lojas com 0 pedido)
                    _persistir_lojas_upseller(user_id, lojas)
                    _set_sidebar_cache(user_id, sidebar)

                    app._sync_status[user_id] = {
                        "etapa": "concluido", "progresso": 100,
                        "detalhes": f"{len(lojas)} lojas, {total} pedidos pendentes",
                        "lojas_encontradas": lojas,
                        "total_pedidos": total,
                        "sidebar_info": sidebar,
                    }
                else:
                    app._sync_status[user_id] = {
                        "etapa": "erro", "progresso": 0,
                        "detalhes": resultado.get("erro", "Erro ao ler pedidos")
                    }

            except Exception as e:
                print(f"[Sync] Erro: {e}")
                import traceback
                traceback.print_exc()
                app._sync_status[user_id] = {"etapa": "erro", "progresso": 0, "detalhes": str(e)}
            finally:
                app._sync_em_andamento[user_id] = False

    thread = threading.Thread(target=_sync_leitura, daemon=True)
    thread.start()
    return jsonify({"mensagem": "Sincronizacao iniciada", "status": "iniciando"})


@app.route('/api/upseller/gerar', methods=['POST'])
@jwt_required()
def api_upseller_gerar():
    """
    Pipeline completo de geracao (atualizado 2026-02-28):

    Fluxo correto POR LOJA:
    1. Verificar/reconectar sessao UpSeller
    2. Atualizar contagens de lojas
    3. Emitir NF-e (com retry de falhas)
    4. Programar envio dos pedidos ("Para Programar")
    5. Aguardar tracking numbers
    6. Baixar etiquetas com DDC (Etiqueta Casada + Declaracao de Conteudo)
    7. Extrair dados de pedidos → XLSX
    8. Mover tudo para pasta_entrada
    9. Processar etiquetas (DDC dados no rodape, organizar por SKU)

    Body JSON (opcional):
    {
        "lojas": ["DAHIANE", "LOJA_X"],  // Lojas para processar (vazio = todas)
    }
    """
    user_id = int(get_jwt_identity())
    config = _get_or_create_upseller_config(user_id)

    if _esta_atualizando_lojas(user_id):
        return jsonify({
            "sucesso": False,
            "mensagem": "Atualizando pedidos, aguarde."
        }), 409

    acao_em_execucao = _obter_acao_massa_em_andamento(user_id)
    if acao_em_execucao:
        return jsonify({
            "sucesso": False,
            "mensagem": (
                f"Processo em massa em andamento ({acao_em_execucao.get('acao', 'processando')}). "
                "Aguarde finalizar."
            )
        }), 409

    if hasattr(app, '_gerar_em_andamento') and app._gerar_em_andamento.get(user_id):
        return jsonify({"erro": "Geracao ja em andamento"}), 409

    # Extrair lojas selecionadas do request
    data = request.get_json(silent=True) or {}
    lojas_raw = data.get("lojas", [])
    lojas_selecionadas, lojas_ignoradas = _sanitizar_lojas_selecionadas(user_id, lojas_raw)
    if isinstance(lojas_raw, list) and lojas_raw and not lojas_selecionadas:
        return jsonify({"erro": "Nenhuma loja valida selecionada para o pipeline."}), 400

    # Pre-setar status ANTES da thread para evitar race condition no polling
    if not hasattr(app, '_gerar_em_andamento'):
        app._gerar_em_andamento = {}
    if not hasattr(app, '_gerar_status'):
        app._gerar_status = {}
    lock_ok, lock_atual = _iniciar_acao_massa(user_id, "gerar")
    if not lock_ok:
        return jsonify({
            "sucesso": False,
            "mensagem": (
                f"Processo em massa em andamento ({(lock_atual or {}).get('acao', 'processando')}). "
                "Aguarde finalizar."
            )
        }), 409
    app._gerar_em_andamento[user_id] = True
    app._gerar_status[user_id] = {"etapa": "iniciando", "progresso": 0, "detalhes": "Iniciando geracao..."}

    def _gerar_pipeline():
        with app.app_context():
            try:
                user = db.session.get(User, user_id)
                if not user:
                    app._gerar_status[user_id] = {"etapa": "erro", "progresso": 0, "detalhes": "Usuario nao encontrado"}
                    return

                pasta_entrada = user.get_pasta_entrada()
                pasta_lote = _criar_pasta_lote_upseller(pasta_entrada, prefixo="gerar")
                download_dir = os.path.join(pasta_entrada, '_upseller_temp')
                os.makedirs(download_dir, exist_ok=True)

                resultado = {"pdfs": [], "xmls": [], "xlsx": "", "sucesso": False}
                lojas_falha_pipeline = []
                grupos_marketplace = []
                if lojas_selecionadas:
                    grupos_marketplace = _agrupar_lojas_por_marketplace(user_id, lojas_selecionadas)
                    if not grupos_marketplace:
                        grupos_marketplace = [{"marketplace": "Shopee", "lojas": list(lojas_selecionadas)}]

                # === Etapa 1: Verificar/reconectar scraper (headless=False para SPA) ===
                app._gerar_status[user_id] = {"etapa": "login", "progresso": 5, "detalhes": "Verificando sessao UpSeller..."}

                scraper_vivo = _upseller_mgr.is_alive(user_id)
                precisa_recriar = False
                if not scraper_vivo:
                    precisa_recriar = True
                else:
                    scraper_atual = _upseller_mgr.get_scraper(user_id)
                    if scraper_atual and getattr(scraper_atual, 'headless', True):
                        precisa_recriar = True
                    elif not _upseller_mgr.esta_logado(user_id):
                        precisa_recriar = True

                if precisa_recriar:
                    app._gerar_status[user_id] = {"etapa": "reconectando", "progresso": 8, "detalhes": "Abrindo navegador para UpSeller..."}
                    try:
                        scraper = _upseller_mgr.criar_scraper(user_id, config.session_dir, download_dir, headless=False)
                        logado = _upseller_mgr._run_async(user_id, scraper._esta_logado())
                        if not logado:
                            app._gerar_status[user_id] = {
                                "etapa": "erro", "progresso": 0,
                                "detalhes": "Sessao expirada. Clique 'Reconectar' para login manual."
                            }
                            config.status_conexao = "desconectado"
                            db.session.commit()
                            return
                        config.status_conexao = "ok"
                        db.session.commit()
                    except Exception as e:
                        print(f"[Gerar] Erro ao criar scraper visivel: {e}")
                        app._gerar_status[user_id] = {"etapa": "erro", "progresso": 0, "detalhes": f"Erro ao abrir navegador: {e}"}
                        return

                scraper = _upseller_mgr.get_scraper(user_id)
                if not scraper:
                    app._gerar_status[user_id] = {"etapa": "erro", "progresso": 0, "detalhes": "Scraper nao disponivel"}
                    return

                # Atualizar download_dir do scraper
                scraper.download_dir = download_dir
                if download_dir:
                    os.makedirs(download_dir, exist_ok=True)

                def _xlsx_download_valido(path):
                    try:
                        if not path or not os.path.exists(path):
                            return False
                        checker = getattr(scraper, "_arquivo_tabulado_valido", None)
                        if callable(checker):
                            return bool(checker(path))
                        with open(path, "rb") as f:
                            head = (f.read(512) or b"").lstrip().lower()
                        if head.startswith(b"<!doctype html") or head.startswith(b"<html"):
                            return False
                        return True
                    except Exception:
                        return False

                # === Etapa 2: Atualizar contagens de lojas ===
                app._gerar_status[user_id] = {
                    "etapa": "atualizando_lojas", "progresso": 7,
                    "detalhes": "Atualizando contagens de lojas..."
                }
                try:
                    if lojas_selecionadas:
                        # Otimizacao: evita varredura completa inicial quando o usuario
                        # ja escolheu lojas especificas para processar.
                        app._gerar_status[user_id]["detalhes"] = (
                            f"Lojas selecionadas ({len(lojas_selecionadas)}), "
                            "pulando varredura completa inicial"
                        )
                    else:
                        _atualizar_lojas_apos_acao(user_id)
                except Exception as e:
                    print(f"[Gerar] Aviso ao atualizar lojas: {e}")

                # === Etapa 3: Emitir NF-e (por loja ou todas) ===
                total_emitidos = 0
                aviso_falhas_nfe = ""
                motivos_falhas_nfe = []
                if lojas_selecionadas:
                    total_grupos = max(1, len(grupos_marketplace))
                    for idx_gp, gp in enumerate(grupos_marketplace, start=1):
                        lojas_gp = list(gp.get("lojas") or [])
                        mp_nome = (gp.get("marketplace") or "Shopee").strip() or "Shopee"
                        if not lojas_gp:
                            continue
                        app._gerar_status[user_id] = {
                            "etapa": "emitindo_nfe",
                            "progresso": 10,
                            "detalhes": (
                                f"Emitindo NF-e ({idx_gp}/{total_grupos}) - "
                                f"{mp_nome}: {len(lojas_gp)} loja(s)"
                            )
                        }
                        try:
                            nfe_result = _upseller_mgr._run_async(
                                user_id, scraper.emitir_nfe(filtro_loja=lojas_gp)
                            )
                            if isinstance(nfe_result, dict):
                                total_emitidos += int(nfe_result.get("total_emitidos", 0) or 0)
                                if nfe_result.get("aviso_falhas"):
                                    aviso_falhas_nfe = (
                                        (aviso_falhas_nfe + " | ") if aviso_falhas_nfe else ""
                                    ) + str(nfe_result.get("aviso_falhas") or "")
                                for m in (nfe_result.get("motivos_falhas") or []):
                                    mt = str(m or "").strip()
                                    if mt and mt not in motivos_falhas_nfe:
                                        motivos_falhas_nfe.append(mt)
                        except Exception as e:
                            print(f"[Gerar] Erro emitir NF-e ({mp_nome}): {e}")
                else:
                    app._gerar_status[user_id] = {
                        "etapa": "emitindo_nfe", "progresso": 10,
                        "detalhes": "Emitindo NF-e dos pedidos..."
                    }
                    try:
                        nfe_result = _upseller_mgr._run_async(user_id, scraper.emitir_nfe())
                        if isinstance(nfe_result, dict):
                            total_emitidos = int(nfe_result.get("total_emitidos", 0) or 0)
                            if nfe_result.get("aviso_falhas"):
                                aviso_falhas_nfe = nfe_result["aviso_falhas"]
                            for m in (nfe_result.get("motivos_falhas") or []):
                                mt = str(m or "").strip()
                                if mt and mt not in motivos_falhas_nfe:
                                    motivos_falhas_nfe.append(mt)
                    except Exception as e:
                        print(f"[Gerar] Erro emitir NF-e: {e}")

                if total_emitidos > 0:
                    app._gerar_status[user_id]["detalhes"] = f"{total_emitidos} NF-e emitidas"
                else:
                    app._gerar_status[user_id]["detalhes"] = "Nenhuma NF-e para emitir"

                # === Etapa 4: Programar envio (por loja ou todas) ===
                total_programados = 0
                if lojas_selecionadas:
                    total_grupos = max(1, len(grupos_marketplace))
                    for idx_gp, gp in enumerate(grupos_marketplace, start=1):
                        lojas_gp = list(gp.get("lojas") or [])
                        mp_nome = (gp.get("marketplace") or "Shopee").strip() or "Shopee"
                        if not lojas_gp:
                            continue
                        app._gerar_status[user_id] = {
                            "etapa": "programando",
                            "progresso": 20,
                            "detalhes": (
                                f"Programando envio ({idx_gp}/{total_grupos}) - "
                                f"{mp_nome}: {len(lojas_gp)} loja(s)"
                            )
                        }
                        try:
                            prog_result = _upseller_mgr._run_async(
                                user_id, scraper.programar_envio(filtro_loja=lojas_gp)
                            )
                            total_programados += int((prog_result or {}).get("total_programados", 0) or 0)
                        except Exception as e:
                            print(f"[Gerar] Erro programar envio ({mp_nome}): {e}")
                else:
                    # Programar todas as lojas de uma vez (sem filtro)
                    app._gerar_status[user_id] = {"etapa": "programando", "progresso": 20, "detalhes": "Programando envio dos pedidos..."}
                    try:
                        prog_result = _upseller_mgr._run_async(user_id, scraper.programar_envio())
                        total_programados = prog_result.get("total_programados", 0)
                    except Exception as e:
                        print(f"[Gerar] Erro programar envio: {e}")

                if total_programados > 0:
                    app._gerar_status[user_id]["detalhes"] = f"{total_programados} pedidos programados"
                else:
                    app._gerar_status[user_id]["detalhes"] = "Nenhum pedido novo para programar"

                # === Etapa 5: Aguardar tracking numbers ===
                app._gerar_status[user_id] = {
                    "etapa": "aguardando_tracking",
                    "progresso": 32,
                    "detalhes": "Aguardando tracking numbers do UpSeller..."
                }

                if total_programados > 0:
                    # Aguardar tracking (poll a cada 10s, max 2 min)
                    try:
                        tracking_ok = _upseller_mgr._run_async(
                            user_id, scraper._aguardar_tracking(timeout_segundos=180)
                        )
                        if tracking_ok:
                            app._gerar_status[user_id]["detalhes"] = "Tracking numbers recebidos!"
                        else:
                            app._gerar_status[user_id]["detalhes"] = "Timeout aguardando tracking, tentando baixar mesmo assim..."
                    except Exception as e:
                        print(f"[Gerar] Erro ao aguardar tracking: {e}")
                        app._gerar_status[user_id]["detalhes"] = "Continuando sem aguardar tracking..."
                else:
                    # Se nao programou nada, esperar um pouco apenas
                    import time
                    time.sleep(3)

                # === Etapa 5.5: Baixar Lista de Resumo (XLSX) antes das etiquetas ===
                app._gerar_status[user_id] = {
                    "etapa": "lista_resumo", "progresso": 38,
                    "detalhes": "Baixando Lista de Resumo (XLSX)..."
                }

                xlsx_paths = []
                try:
                    if lojas_selecionadas:
                        total_grupos = max(1, len(grupos_marketplace))
                        for idx_gp, gp in enumerate(grupos_marketplace, start=1):
                            lojas_gp = list(gp.get("lojas") or [])
                            mp_nome = (gp.get("marketplace") or "Shopee").strip() or "Shopee"
                            if not lojas_gp:
                                continue
                            app._gerar_status[user_id] = {
                                "etapa": "lista_resumo", "progresso": 40,
                                "detalhes": (
                                    f"Lista de Resumo ({idx_gp}/{total_grupos}) - "
                                    f"{mp_nome}: {len(lojas_gp)} loja(s)"
                                )
                            }
                            lista_xlsx = _upseller_mgr._run_async(
                                user_id, scraper.baixar_lista_resumo(filtro_loja=lojas_gp)
                            ) or []
                            if isinstance(lista_xlsx, str):
                                lista_xlsx = [lista_xlsx]
                            for xp in lista_xlsx:
                                if _xlsx_download_valido(xp):
                                    xlsx_paths.append(xp)
                                elif xp:
                                    print(f"[Gerar] Aviso: arquivo de resumo invalido (ignorado): {xp}")
                    else:
                        lista_xlsx = _upseller_mgr._run_async(
                            user_id, scraper.baixar_lista_resumo()
                        ) or []
                        if isinstance(lista_xlsx, str):
                            lista_xlsx = [lista_xlsx]
                        for xp in lista_xlsx:
                            if _xlsx_download_valido(xp):
                                xlsx_paths.append(xp)
                            elif xp:
                                print(f"[Gerar] Aviso: arquivo de resumo invalido (ignorado): {xp}")
                except Exception as e:
                    print(f"[Gerar] Erro lista resumo: {e}")

                # Deduplicar mantendo ordem.
                if xlsx_paths:
                    vistos_xlsx = set()
                    dedup_xlsx = []
                    for xp in xlsx_paths:
                        if not xp or xp in vistos_xlsx:
                            continue
                        vistos_xlsx.add(xp)
                        dedup_xlsx.append(xp)
                    xlsx_paths = dedup_xlsx

                if xlsx_paths:
                    app._gerar_status[user_id]["detalhes"] = f"{len(xlsx_paths)} arquivo(s) de Lista de Resumo baixado(s)"
                else:
                    app._gerar_status[user_id]["detalhes"] = "Lista de Resumo indisponivel, tentando fallback depois das etiquetas"

                # === Etapa 6: Baixar etiquetas com DDC ===
                app._gerar_status[user_id] = {
                    "etapa": "baixando_etiquetas",
                    "progresso": 45,
                    "detalhes": "Baixando etiquetas (Casada + DDC)..."
                }

                if lojas_selecionadas:
                    total_grupos = max(1, len(grupos_marketplace))
                    for idx_gp, gp in enumerate(grupos_marketplace, start=1):
                        lojas_gp = list(gp.get("lojas") or [])
                        mp_nome = (gp.get("marketplace") or "Shopee").strip() or "Shopee"
                        if not lojas_gp:
                            continue
                        app._gerar_status[user_id] = {
                            "etapa": "baixando_etiquetas",
                            "progresso": 48,
                            "detalhes": (
                                f"Baixando etiquetas ({idx_gp}/{total_grupos}) - "
                                f"{mp_nome}: {len(lojas_gp)} loja(s)"
                            )
                        }
                        try:
                            pdfs_lote = _upseller_mgr._run_async(
                                user_id, scraper.baixar_etiquetas(filtro_loja=lojas_gp)
                            ) or []
                            if pdfs_lote:
                                resultado["pdfs"].extend(list(pdfs_lote))
                                continue
                        except Exception as e:
                            print(f"[Gerar] Erro baixar etiquetas ({mp_nome}): {e}")
                            lojas_falha_pipeline.append({
                                "loja": f"lote_{mp_nome}",
                                "etapa": "gerar_etiquetas",
                                "motivo": str(e),
                            })

                        # Resiliencia: se grupo vier vazio/erro, tentar loja a loja.
                        for loja_nome in lojas_gp:
                            try:
                                pdfs_loja = _upseller_mgr._run_async(
                                    user_id, scraper.baixar_etiquetas(filtro_loja=loja_nome)
                                ) or []
                                if pdfs_loja:
                                    resultado["pdfs"].extend(list(pdfs_loja))
                                else:
                                    lojas_falha_pipeline.append({
                                        "loja": loja_nome,
                                        "marketplace": mp_nome,
                                        "etapa": "gerar_etiquetas",
                                        "motivo": "sem_pdf_gerado",
                                    })
                            except Exception as e_loja_pdf:
                                lojas_falha_pipeline.append({
                                    "loja": loja_nome,
                                    "marketplace": mp_nome,
                                    "etapa": "gerar_etiquetas",
                                    "motivo": str(e_loja_pdf),
                                })
                else:
                    # Baixar todas (sem filtro)
                    try:
                        resultado["pdfs"] = _upseller_mgr._run_async(user_id, scraper.baixar_etiquetas())
                    except Exception as e:
                        print(f"[Gerar] Erro baixar etiquetas: {e}")

                app._gerar_status[user_id]["detalhes"] = f"{len(resultado['pdfs'])} PDFs baixados"

                # Sem PDF = nada para processar. Nao continuar para evitar zerar saida/resultado.
                if not resultado["pdfs"]:
                    detalhe_falhas = ""
                    if lojas_falha_pipeline:
                        amostra = " | ".join(
                            f"{x.get('loja', '?')}: {x.get('motivo', 'erro')}"
                            for x in lojas_falha_pipeline[:5]
                        )
                        detalhe_falhas = f" Falhas: {amostra}"
                    app._gerar_status[user_id] = {
                        "etapa": "erro",
                        "progresso": 0,
                        "detalhes": (
                            "Nenhuma etiqueta (PDF) foi baixada do UpSeller. "
                            "Nada para processar." + detalhe_falhas
                        ),
                        "lojas_falha": lojas_falha_pipeline[:20],
                    }
                    # Atualizar sync de lojas para refletir estado atual do UpSeller
                    _atualizar_lojas_apos_acao(user_id)
                    config.ultima_sincronizacao = datetime.utcnow()
                    config.status_conexao = "ok"
                    db.session.commit()
                    return

                # === Etapa 7: Consolidar XLSX (fallback legado apenas se necessario) ===
                app._gerar_status[user_id] = {
                    "etapa": "extraindo_pedidos",
                    "progresso": 60,
                    "detalhes": "Consolidando dados de pedidos (XLSX)..."
                }
                try:
                    if not xlsx_paths:
                        # Fallback legado: extracao via tela apenas se Lista de Resumo falhar.
                        if lojas_selecionadas:
                            app._gerar_status[user_id] = {
                                "etapa": "extraindo_pedidos",
                                "progresso": 64,
                                "detalhes": f"Fallback XLSX em lote ({len(lojas_selecionadas)} lojas)..."
                            }
                            xp = _upseller_mgr._run_async(
                                user_id,
                                scraper.extrair_dados_pedidos(
                                    status_filter="para_imprimir",
                                    filtro_loja=lojas_selecionadas
                                )
                            )
                            if _xlsx_download_valido(xp):
                                xlsx_paths.append(xp)
                        else:
                            xp = _upseller_mgr._run_async(
                                user_id,
                                scraper.extrair_dados_pedidos(status_filter="para_imprimir")
                            )
                            if _xlsx_download_valido(xp):
                                xlsx_paths.append(xp)

                    if xlsx_paths:
                        resultado["xlsx"] = xlsx_paths[0]
                        if len(xlsx_paths) > 1:
                            resultado["xlsx_extra"] = xlsx_paths[1:]
                except Exception as e:
                    print(f"[Gerar] Erro consolidar/extrair XLSX: {e}")

                resultado["sucesso"] = True

                # NÃO fecha o scraper - ele fica vivo!

                # === Etapa 8: Mover arquivos para lote isolado da pasta_entrada ===
                app._gerar_status[user_id] = {"etapa": "movendo", "progresso": 72, "detalhes": "Movendo arquivos para lote atual..."}
                resumo = scraper.mover_para_pasta_entrada(resultado, pasta_lote)
                detalhes_mov = f"{resumo['pdfs_movidos']} PDFs"
                if resumo['xlsx_copiado']:
                    detalhes_mov += ", XLSX"
                if resumo.get("pdfs_movidos", 0) <= 0:
                    app._gerar_status[user_id] = {
                        "etapa": "erro",
                        "progresso": 0,
                        "detalhes": (
                            "Nenhum PDF foi movido para o lote atual. "
                            "Processamento cancelado para preservar o ultimo resultado valido."
                        ),
                        "processado": False,
                    }
                    _atualizar_lojas_apos_acao(user_id)
                    config.ultima_sincronizacao = datetime.utcnow()
                    config.status_conexao = "ok"
                    db.session.commit()
                    return

                # === Etapa 9: Processar etiquetas ===
                app._gerar_status[user_id] = {"etapa": "processando", "progresso": 86, "detalhes": "Processando etiquetas + DDC + produtos..."}
                try:
                    proc_result = _executar_processamento(
                        user_id,
                        sem_recorte=True,
                        resumo_sku_somente=False,
                        pasta_entrada_override=pasta_lote
                    )
                    if not proc_result or not proc_result.get("ok"):
                        raise RuntimeError(
                            (proc_result or {}).get("erro") or
                            "Processamento concluido sem gerar resultado valido."
                        )
                    status_final = {
                        "etapa": "concluido", "progresso": 100,
                        "detalhes": (
                            f"Concluido! {total_emitidos} NF-e | {detalhes_mov} | "
                            f"{proc_result.get('total_etiquetas', 0)} etiquetas em "
                            f"{proc_result.get('total_lojas', 0)} loja(s)"
                        ),
                        "processado": True,
                    }
                    if aviso_falhas_nfe:
                        status_final["aviso_falhas"] = aviso_falhas_nfe
                    if motivos_falhas_nfe:
                        status_final["motivos_falhas"] = motivos_falhas_nfe[:8]
                    if lojas_falha_pipeline:
                        status_final["lojas_falha"] = lojas_falha_pipeline[:20]
                        status_final["aviso_falhas"] = (
                            (status_final.get("aviso_falhas", "") + " | " if status_final.get("aviso_falhas") else "")
                            + f"{len(lojas_falha_pipeline)} loja(s) com erro foram ignoradas."
                        )

                    # Auto-disparo opcional WhatsApp (fila persistente)
                    try:
                        auto_res = _enfileirar_envio_whatsapp_resultado(
                            user_id=user_id,
                            origem="auto",
                            respeitar_toggle_auto=True,
                        )
                        if auto_res.get("ok"):
                            status_final["auto_whatsapp"] = {
                                "enfileirados": auto_res.get("total_entregas", 0),
                                "batch_id": auto_res.get("batch_id", ""),
                            }
                            status_final["detalhes"] += f" | WhatsApp fila: {auto_res.get('total_entregas', 0)}"
                        elif not auto_res.get("ignorado"):
                            status_final["aviso_whatsapp"] = auto_res.get("erro", "Falha ao enfileirar WhatsApp")
                    except Exception as e_auto:
                        status_final["aviso_whatsapp"] = f"Falha no auto-envio WhatsApp: {e_auto}"
                    app._gerar_status[user_id] = status_final
                except Exception as e:
                    print(f"[Gerar] Erro processamento: {e}")
                    import traceback
                    traceback.print_exc()
                    app._gerar_status[user_id] = {
                        "etapa": "parcial", "progresso": 90,
                        "detalhes": f"Download OK ({detalhes_mov}), erro no processamento: {e}",
                        "processado": False,
                    }

                # Re-sincronizar contagens em background para liberar resultado imediatamente.
                _disparar_atualizacao_lojas_background(user_id, status_attr="_gerar_status")

                # Atualizar config
                config.ultima_sincronizacao = datetime.utcnow()
                config.status_conexao = "ok"
                db.session.commit()

                # Limpar pasta temp
                try:
                    import shutil
                    shutil.rmtree(download_dir, ignore_errors=True)
                except:
                    pass

            except Exception as e:
                print(f"[Gerar] Erro geral: {e}")
                import traceback
                traceback.print_exc()
                app._gerar_status[user_id] = {"etapa": "erro", "progresso": 0, "detalhes": str(e)}
            finally:
                app._gerar_em_andamento[user_id] = False
                _finalizar_acao_massa(user_id, "gerar")

    thread = threading.Thread(target=_gerar_pipeline, daemon=True)
    thread.start()
    payload = {"mensagem": "Geracao iniciada", "status": "iniciando"}
    if lojas_ignoradas:
        payload["lojas_ignoradas"] = lojas_ignoradas
    return jsonify(payload)


@app.route('/api/upseller/sincronizar/status', methods=['GET'])
@jwt_required()
def api_upseller_sync_status():
    """Retorna status da sincronizacao em andamento, incluindo lojas escaneadas quando concluido."""
    user_id = int(get_jwt_identity())
    if hasattr(app, '_sync_status') and user_id in app._sync_status:
        status = dict(app._sync_status[user_id])
        em_andamento = hasattr(app, '_sync_em_andamento') and app._sync_em_andamento.get(user_id, False)
        status["em_andamento"] = em_andamento

        # Quando concluido, incluir lojas escaneadas para o usuario escolher
        if status.get("etapa") == "concluido":
            # lojas_encontradas ja estao no status (set pelo pipeline)
            if "lojas_encontradas" not in status:
                status["lojas_encontradas"] = []
            if "total_etiquetas_scan" not in status:
                status["total_etiquetas_scan"] = 0

        return jsonify(status)
    return jsonify({"etapa": "idle", "progresso": 0, "detalhes": "", "em_andamento": False})


@app.route('/api/upseller/gerar/status', methods=['GET'])
@jwt_required()
def api_upseller_gerar_status():
    """Retorna status da geracao em andamento (separado do status de sincronizacao)."""
    user_id = int(get_jwt_identity())
    if hasattr(app, '_gerar_status') and user_id in app._gerar_status:
        status = dict(app._gerar_status[user_id])
        em_andamento = hasattr(app, '_gerar_em_andamento') and app._gerar_em_andamento.get(user_id, False)
        status["em_andamento"] = em_andamento
        status["atualizando_pedidos"] = _esta_atualizando_lojas(user_id) or bool(status.get("atualizando_pedidos"))
        return jsonify(status)
    return jsonify({
        "etapa": "idle",
        "progresso": 0,
        "detalhes": "",
        "em_andamento": False,
        "atualizando_pedidos": _esta_atualizando_lojas(user_id),
    })


# ----------------------------------------------------------------
# ENDPOINTS - ATALHOS INDIVIDUAIS (Emitir, Programar, Imprimir)
# ----------------------------------------------------------------

@app.route('/api/upseller/emitir', methods=['POST'])
@jwt_required()
def api_upseller_emitir():
    """Emite NF-e dos pedidos pendentes no UpSeller (atalho 'Para Emitir')."""
    user_id = int(get_jwt_identity())

    if _esta_atualizando_lojas(user_id):
        return jsonify({"sucesso": False, "mensagem": "Atualizando pedidos, aguarde."}), 409

    acao_em_execucao = _obter_acao_massa_em_andamento(user_id)
    if acao_em_execucao:
        return jsonify({
            "sucesso": False,
            "mensagem": (
                f"Processo em massa em andamento ({acao_em_execucao.get('acao', 'processando')}). "
                "Aguarde finalizar."
            )
        }), 409

    if not _upseller_mgr.is_alive(user_id):
        return jsonify({"sucesso": False, "mensagem": "UpSeller nao conectado. Conecte primeiro."}), 400

    data = request.get_json() or {}
    filtro_loja = (data.get("loja") or "").strip() or None
    lojas_lista_raw = data.get("lojas", [])
    lojas_lista, lojas_ignoradas = _sanitizar_lojas_selecionadas(user_id, lojas_lista_raw)

    if isinstance(lojas_lista_raw, list) and lojas_lista_raw and not lojas_lista:
        return jsonify({
            "sucesso": False,
            "mensagem": "Nenhuma loja valida selecionada para emitir NF-e."
        }), 400
    # Se vier lista de lojas, prioriza SEMPRE execucao em lote
    # (ignora campo legado "loja" para evitar cair em fluxo individual).
    if lojas_lista:
        filtro_loja = None

    if filtro_loja:
        filtro_lista, _ = _sanitizar_lojas_selecionadas(user_id, [filtro_loja])
        if not filtro_lista:
            return jsonify({
                "sucesso": False,
                "mensagem": f"Loja invalida para emissao: {filtro_loja}"
            }), 400
        filtro_loja = filtro_lista[0]

    lock_ok, lock_atual = _iniciar_acao_massa(user_id, "emitir")
    if not lock_ok:
        return jsonify({
            "sucesso": False,
            "mensagem": (
                f"Processo em massa em andamento ({(lock_atual or {}).get('acao', 'processando')}). "
                "Aguarde finalizar."
            )
        }), 409

    try:
        scraper = _upseller_mgr._scrapers.get(user_id)
        if not scraper:
            return jsonify({"sucesso": False, "mensagem": "Scraper nao encontrado"}), 400

        # Se recebeu lista de lojas, executar em lotes separados por marketplace
        # para evitar erro de plataforma mista no UpSeller.
        if lojas_lista:
            grupos_mp = _agrupar_lojas_por_marketplace(user_id, lojas_lista)
            if not grupos_mp:
                grupos_mp = [{"marketplace": "Shopee", "lojas": list(lojas_lista)}]

            total_emitidos_all = 0
            lojas_ok = []
            lojas_falha = []
            motivos_falhas = []
            grupos_processados = []

            for idx_gp, gp in enumerate(grupos_mp, start=1):
                lojas_gp = list(gp.get("lojas") or [])
                mp_nome = (gp.get("marketplace") or "Shopee").strip() or "Shopee"
                if not lojas_gp:
                    continue

                grupos_processados.append({"marketplace": mp_nome, "lojas": lojas_gp})
                try:
                    r_gp = _upseller_mgr._run_async(user_id, scraper.emitir_nfe(filtro_loja=lojas_gp)) or {}
                    if not isinstance(r_gp, dict):
                        r_gp = {"sucesso": False, "mensagem": "retorno_invalido"}

                    if bool(r_gp.get("sucesso")):
                        lojas_ok.extend(lojas_gp)
                        total_emitidos_all += int((r_gp.get("total_emitidos", 0) or 0))
                        for m in (r_gp.get("motivos_falhas") or []):
                            mt = str(m or "").strip()
                            if mt and mt not in motivos_falhas:
                                motivos_falhas.append(mt)
                        continue

                    # Fallback por loja (somente dentro do marketplace atual)
                    if len(lojas_gp) > 1:
                        for loja_nome in lojas_gp:
                            try:
                                r_loja = _upseller_mgr._run_async(user_id, scraper.emitir_nfe(filtro_loja=loja_nome)) or {}
                                if not isinstance(r_loja, dict):
                                    r_loja = {"sucesso": False, "mensagem": "retorno_invalido"}
                                if bool(r_loja.get("sucesso")):
                                    lojas_ok.append(loja_nome)
                                    total_emitidos_all += int((r_loja.get("total_emitidos", 0) or 0))
                                    for m in (r_loja.get("motivos_falhas") or []):
                                        mt = str(m or "").strip()
                                        if mt and mt not in motivos_falhas:
                                            motivos_falhas.append(mt)
                                else:
                                    lojas_falha.append({
                                        "loja": loja_nome,
                                        "marketplace": mp_nome,
                                        "motivo": (r_loja.get("mensagem") or "erro_na_emissao"),
                                    })
                            except Exception as e_loja:
                                lojas_falha.append({
                                    "loja": loja_nome,
                                    "marketplace": mp_nome,
                                    "motivo": str(e_loja),
                                })
                    else:
                        lojas_falha.append({
                            "loja": lojas_gp[0],
                            "marketplace": mp_nome,
                            "motivo": (r_gp.get("mensagem") or "erro_na_emissao"),
                        })
                except Exception as e_gp:
                    for loja_nome in lojas_gp:
                        lojas_falha.append({
                            "loja": loja_nome,
                            "marketplace": mp_nome,
                            "motivo": str(e_gp),
                        })

            resultado = {
                "sucesso": bool(lojas_ok),
                "mensagem": (
                    f"Emissao concluida por marketplace: {len(lojas_ok)} loja(s) ok, "
                    f"{len(lojas_falha)} com erro"
                ),
                "total_emitidos": int(total_emitidos_all),
                "modo_execucao": "lote_marketplace",
                "lojas_processadas": lojas_ok,
                "grupos_marketplace": grupos_processados,
            }
            if motivos_falhas:
                resultado["motivos_falhas"] = motivos_falhas[:8]
            if lojas_falha:
                resultado["lojas_falha"] = lojas_falha[:20]
                resultado["aviso_falhas"] = (
                    f"{len(lojas_falha)} loja(s) com erro foram ignoradas."
                )

            snapshot = _atualizar_lojas_apos_acao(user_id)
            resultado["lojas_atualizadas"] = bool(snapshot.get("sucesso"))
            if lojas_ignoradas:
                resultado["lojas_ignoradas"] = lojas_ignoradas
            return jsonify(resultado)

        resultado = _upseller_mgr._run_async(user_id, scraper.emitir_nfe(filtro_loja=filtro_loja))
        snapshot = _atualizar_lojas_apos_acao(user_id)
        if isinstance(resultado, dict):
            resultado["lojas_atualizadas"] = bool(snapshot.get("sucesso"))
        return jsonify(resultado)
    except Exception as e:
        print(f"[Emitir] Erro: {e}")
        return jsonify({"sucesso": False, "mensagem": str(e)}), 500
    finally:
        _finalizar_acao_massa(user_id, "emitir")


@app.route('/api/upseller/programar', methods=['POST'])
@jwt_required()
def api_upseller_programar():
    """Programa envio dos pedidos pendentes no UpSeller (atalho 'Para Enviar')."""
    user_id = int(get_jwt_identity())

    if _esta_atualizando_lojas(user_id):
        return jsonify({"sucesso": False, "mensagem": "Atualizando pedidos, aguarde."}), 409

    acao_em_execucao = _obter_acao_massa_em_andamento(user_id)
    if acao_em_execucao:
        return jsonify({
            "sucesso": False,
            "mensagem": (
                f"Processo em massa em andamento ({acao_em_execucao.get('acao', 'processando')}). "
                "Aguarde finalizar."
            )
        }), 409

    if not _upseller_mgr.is_alive(user_id):
        return jsonify({"sucesso": False, "mensagem": "UpSeller nao conectado. Conecte primeiro."}), 400

    data = request.get_json() or {}
    filtro_loja = (data.get("loja") or "").strip() or None
    lojas_lista_raw = data.get("lojas", [])
    lojas_lista, lojas_ignoradas = _sanitizar_lojas_selecionadas(user_id, lojas_lista_raw)

    if isinstance(lojas_lista_raw, list) and lojas_lista_raw and not lojas_lista:
        return jsonify({
            "sucesso": False,
            "mensagem": "Nenhuma loja valida selecionada para programar envio."
        }), 400
    # Se vier lista de lojas, prioriza SEMPRE execucao em lote
    # (ignora campo legado "loja" para evitar cair em fluxo individual).
    if lojas_lista:
        filtro_loja = None

    if filtro_loja:
        filtro_lista, _ = _sanitizar_lojas_selecionadas(user_id, [filtro_loja])
        if not filtro_lista:
            return jsonify({
                "sucesso": False,
                "mensagem": f"Loja invalida para programar envio: {filtro_loja}"
            }), 400
        filtro_loja = filtro_lista[0]

    lock_ok, lock_atual = _iniciar_acao_massa(user_id, "programar")
    if not lock_ok:
        return jsonify({
            "sucesso": False,
            "mensagem": (
                f"Processo em massa em andamento ({(lock_atual or {}).get('acao', 'processando')}). "
                "Aguarde finalizar."
            )
        }), 409

    try:
        scraper = _upseller_mgr._scrapers.get(user_id)
        if not scraper:
            return jsonify({"sucesso": False, "mensagem": "Scraper nao encontrado"}), 400

        if lojas_lista:
            grupos_mp = _agrupar_lojas_por_marketplace(user_id, lojas_lista)
            if not grupos_mp:
                grupos_mp = [{"marketplace": "Shopee", "lojas": list(lojas_lista)}]

            total_programados_all = 0
            lojas_ok = []
            lojas_falha = []
            grupos_processados = []

            for idx_gp, gp in enumerate(grupos_mp, start=1):
                lojas_gp = list(gp.get("lojas") or [])
                mp_nome = (gp.get("marketplace") or "Shopee").strip() or "Shopee"
                if not lojas_gp:
                    continue

                grupos_processados.append({"marketplace": mp_nome, "lojas": lojas_gp})
                try:
                    r_gp = _upseller_mgr._run_async(user_id, scraper.programar_envio(filtro_loja=lojas_gp)) or {}
                    if not isinstance(r_gp, dict):
                        r_gp = {"sucesso": False, "mensagem": "retorno_invalido"}

                    if bool(r_gp.get("sucesso")):
                        lojas_ok.extend(lojas_gp)
                        total_programados_all += int((r_gp.get("total_programados", 0) or 0))
                        continue

                    # Fallback por loja dentro do marketplace atual.
                    if len(lojas_gp) > 1:
                        for loja_nome in lojas_gp:
                            try:
                                r_loja = _upseller_mgr._run_async(user_id, scraper.programar_envio(filtro_loja=loja_nome)) or {}
                                if not isinstance(r_loja, dict):
                                    r_loja = {"sucesso": False, "mensagem": "retorno_invalido"}
                                if bool(r_loja.get("sucesso")):
                                    lojas_ok.append(loja_nome)
                                    total_programados_all += int((r_loja.get("total_programados", 0) or 0))
                                else:
                                    lojas_falha.append({
                                        "loja": loja_nome,
                                        "marketplace": mp_nome,
                                        "motivo": (r_loja.get("mensagem") or "erro_na_programacao"),
                                    })
                            except Exception as e_loja:
                                lojas_falha.append({
                                    "loja": loja_nome,
                                    "marketplace": mp_nome,
                                    "motivo": str(e_loja),
                                })
                    else:
                        lojas_falha.append({
                            "loja": lojas_gp[0],
                            "marketplace": mp_nome,
                            "motivo": (r_gp.get("mensagem") or "erro_na_programacao"),
                        })
                except Exception as e_gp:
                    for loja_nome in lojas_gp:
                        lojas_falha.append({
                            "loja": loja_nome,
                            "marketplace": mp_nome,
                            "motivo": str(e_gp),
                        })

            retorno = {
                "sucesso": bool(lojas_ok),
                "mensagem": (
                    f"Programacao concluida por marketplace: {len(lojas_ok)} loja(s) ok, "
                    f"{len(lojas_falha)} com erro"
                ),
                "total_programados": int(total_programados_all),
                "modo_execucao": "lote_marketplace",
                "lojas_processadas": lojas_ok,
                "grupos_marketplace": grupos_processados,
            }
            if lojas_falha:
                retorno["lojas_falha"] = lojas_falha[:20]
                retorno["aviso_falhas"] = (
                    f"{len(lojas_falha)} loja(s) com erro foram ignoradas."
                )

            snapshot = _atualizar_lojas_apos_acao(user_id)
            retorno["lojas_atualizadas"] = bool(snapshot.get("sucesso"))
            if lojas_ignoradas:
                retorno["lojas_ignoradas"] = lojas_ignoradas
            return jsonify(retorno)

        resultado = _upseller_mgr._run_async(user_id, scraper.programar_envio(filtro_loja=filtro_loja))
        snapshot = _atualizar_lojas_apos_acao(user_id)
        if isinstance(resultado, dict):
            resultado["lojas_atualizadas"] = bool(snapshot.get("sucesso"))
        return jsonify(resultado)
    except Exception as e:
        print(f"[Programar] Erro: {e}")
        return jsonify({"sucesso": False, "mensagem": str(e)}), 500
    finally:
        _finalizar_acao_massa(user_id, "programar")


@app.route('/api/upseller/imprimir', methods=['POST'])
@jwt_required()
def api_upseller_imprimir():
    """
    Pipeline 'Gerar Etiquetas' (atalho do UpSeller):
    1. Baixa PDFs de etiquetas do UpSeller (Etiqueta para Impressao)
    2. Move para pasta_entrada do usuario
    3. Processa com regras do Beka MKT (organizar SKU, agrupar loja, numerar, etc.)
    Executa em background com status consultavel via /api/upseller/imprimir/status.
    """
    user_id = int(get_jwt_identity())

    if _esta_atualizando_lojas(user_id):
        return jsonify({"sucesso": False, "mensagem": "Atualizando pedidos, aguarde."}), 409

    acao_em_execucao = _obter_acao_massa_em_andamento(user_id)
    if acao_em_execucao:
        return jsonify({
            "sucesso": False,
            "mensagem": (
                f"Processo em massa em andamento ({acao_em_execucao.get('acao', 'processando')}). "
                "Aguarde finalizar."
            )
        }), 409

    if hasattr(app, '_imprimir_em_andamento') and app._imprimir_em_andamento.get(user_id):
        return jsonify({"sucesso": False, "mensagem": "Geracao de etiquetas ja em andamento"}), 409

    data = request.get_json() or {}
    filtro_loja = (data.get("loja") or "").strip() or None
    lojas_lista_raw = data.get("lojas", [])
    lojas_lista, lojas_ignoradas = _sanitizar_lojas_selecionadas(user_id, lojas_lista_raw)

    if isinstance(lojas_lista_raw, list) and lojas_lista_raw and not lojas_lista:
        return jsonify({
            "sucesso": False,
            "mensagem": "Nenhuma loja valida selecionada para gerar etiquetas."
        }), 400
    # Se vier lista de lojas, prioriza SEMPRE execucao em lote
    # (ignora campo legado "loja" para evitar cair em fluxo individual).
    if lojas_lista:
        filtro_loja = None

    if filtro_loja:
        filtro_lista, _ = _sanitizar_lojas_selecionadas(user_id, [filtro_loja])
        if not filtro_lista:
            return jsonify({
                "sucesso": False,
                "mensagem": f"Loja invalida para gerar etiquetas: {filtro_loja}"
            }), 400
        filtro_loja = filtro_lista[0]

    # Pre-setar status ANTES da thread para evitar race condition no polling
    if not hasattr(app, '_imprimir_em_andamento'):
        app._imprimir_em_andamento = {}
    if not hasattr(app, '_imprimir_status'):
        app._imprimir_status = {}
    lock_ok, lock_atual = _iniciar_acao_massa(user_id, "imprimir")
    if not lock_ok:
        return jsonify({
            "sucesso": False,
            "mensagem": (
                f"Processo em massa em andamento ({(lock_atual or {}).get('acao', 'processando')}). "
                "Aguarde finalizar."
            )
        }), 409
    app._imprimir_em_andamento[user_id] = True
    app._imprimir_status[user_id] = {"etapa": "iniciando", "progresso": 5, "detalhes": "Iniciando..."}

    def _imprimir_pipeline():
        with app.app_context():
            try:
                user = db.session.get(User, user_id)
                if not user:
                    app._imprimir_status[user_id] = {"etapa": "erro", "progresso": 0, "detalhes": "Usuario nao encontrado"}
                    return

                pasta_entrada = user.get_pasta_entrada()
                pasta_lote = _criar_pasta_lote_upseller(pasta_entrada, prefixo="imprimir")
                download_dir = os.path.join(pasta_entrada, '_upseller_temp')
                os.makedirs(download_dir, exist_ok=True)

                # === Auto-reconexao com headless=False (SPA precisa de browser visivel) ===
                config = _get_or_create_upseller_config(user_id)
                scraper_vivo = _upseller_mgr.is_alive(user_id)

                # Para imprimir etiquetas, o UpSeller SPA precisa de browser VISIVEL.
                # Se o scraper esta headless ou morto, recriar com headless=False.
                precisa_recriar = False
                if not scraper_vivo:
                    precisa_recriar = True
                else:
                    scraper_atual = _upseller_mgr.get_scraper(user_id)
                    if scraper_atual and getattr(scraper_atual, 'headless', True):
                        precisa_recriar = True

                if precisa_recriar:
                    app._imprimir_status[user_id] = {"etapa": "reconectando", "progresso": 8, "detalhes": "Abrindo navegador para UpSeller..."}
                    try:
                        scraper = _upseller_mgr.criar_scraper(user_id, config.session_dir, download_dir, headless=False)
                        logado = _upseller_mgr._run_async(user_id, scraper._esta_logado())
                        if not logado:
                            app._imprimir_status[user_id] = {"etapa": "erro", "progresso": 0, "detalhes": "Sessao expirada. Clique 'Reconectar' para login manual."}
                            config.status_conexao = "desconectado"
                            db.session.commit()
                            return
                        config.status_conexao = "ok"
                        db.session.commit()
                    except Exception as e:
                        print(f"[Imprimir] Erro ao criar scraper visivel: {e}")
                        app._imprimir_status[user_id] = {"etapa": "erro", "progresso": 0, "detalhes": f"Erro ao abrir navegador: {e}"}
                        return

                scraper = _upseller_mgr.get_scraper(user_id)
                if not scraper:
                    app._imprimir_status[user_id] = {"etapa": "erro", "progresso": 0, "detalhes": "Scraper nao disponivel apos reconexao"}
                    return

                # Atualizar download_dir do scraper
                scraper.download_dir = download_dir

                # === Etapa 1: Baixar "Lista de Resumo" (XLSX) antes das etiquetas ===
                app._imprimir_status[user_id] = {
                    "etapa": "resumo",
                    "progresso": 12,
                    "detalhes": "Baixando Lista de Resumo (XLSX)..."
                }
                xlsx_paths = []
                lojas_falha = []
                grupos_marketplace = []
                if lojas_lista and not filtro_loja:
                    grupos_marketplace = _agrupar_lojas_por_marketplace(user_id, lojas_lista)
                    if not grupos_marketplace:
                        grupos_marketplace = [{"marketplace": "Shopee", "lojas": list(lojas_lista)}]
                def _xlsx_download_valido(path):
                    try:
                        if not path or not os.path.exists(path):
                            return False
                        checker = getattr(scraper, "_arquivo_tabulado_valido", None)
                        if callable(checker):
                            return bool(checker(path))
                        # Fallback simples: rejeita HTML.
                        with open(path, "rb") as f:
                            head = (f.read(512) or b"").lstrip().lower()
                        if head.startswith(b"<!doctype html") or head.startswith(b"<html"):
                            return False
                        return True
                    except Exception:
                        return False
                try:
                    if lojas_lista and not filtro_loja:
                        total_grupos = max(1, len(grupos_marketplace))
                        for idx_gp, gp in enumerate(grupos_marketplace, start=1):
                            lojas_gp = list(gp.get("lojas") or [])
                            mp_nome = (gp.get("marketplace") or "Shopee").strip() or "Shopee"
                            if not lojas_gp:
                                continue
                            app._imprimir_status[user_id] = {
                                "etapa": "resumo",
                                "progresso": 18,
                                "detalhes": (
                                    f"Lista de resumo ({idx_gp}/{total_grupos}) - "
                                    f"{mp_nome}: {len(lojas_gp)} loja(s)"
                                )
                            }
                            xlsx_lote = _upseller_mgr._run_async(
                                user_id, scraper.baixar_lista_resumo(filtro_loja=lojas_gp)
                            ) or []
                            if isinstance(xlsx_lote, str):
                                xlsx_lote = [xlsx_lote]
                            salvou_gp = False
                            for xp in xlsx_lote:
                                if _xlsx_download_valido(xp):
                                    xlsx_paths.append(xp)
                                    salvou_gp = True
                                elif xp:
                                    print(f"[Imprimir] Aviso: arquivo de resumo invalido (ignorado): {xp}")

                            if (not salvou_gp) and len(lojas_gp) > 1:
                                for loja_nome in lojas_gp:
                                    try:
                                        xlsx_loja = _upseller_mgr._run_async(
                                            user_id, scraper.baixar_lista_resumo(filtro_loja=loja_nome)
                                        ) or []
                                        if isinstance(xlsx_loja, str):
                                            xlsx_loja = [xlsx_loja]
                                        salvou_loja = False
                                        for xp in xlsx_loja:
                                            if _xlsx_download_valido(xp):
                                                xlsx_paths.append(xp)
                                                salvou_loja = True
                                        if not salvou_loja:
                                            lojas_falha.append({
                                                "loja": loja_nome,
                                                "marketplace": mp_nome,
                                                "etapa": "lista_resumo",
                                                "motivo": "sem_xlsx_valido",
                                            })
                                    except Exception as e_xlsx_loja:
                                        lojas_falha.append({
                                            "loja": loja_nome,
                                            "marketplace": mp_nome,
                                            "etapa": "lista_resumo",
                                            "motivo": str(e_xlsx_loja),
                                        })
                    else:
                        xlsx_unico = _upseller_mgr._run_async(
                            user_id, scraper.baixar_lista_resumo(filtro_loja=filtro_loja)
                        ) or []
                        if isinstance(xlsx_unico, str):
                            xlsx_unico = [xlsx_unico]
                        for xp in xlsx_unico:
                            if _xlsx_download_valido(xp):
                                xlsx_paths.append(xp)
                            elif xp:
                                print(f"[Imprimir] Aviso: arquivo de resumo invalido (ignorado): {xp}")
                except Exception as e_resumo:
                    print(f"[Imprimir] Aviso: falha ao baixar Lista de Resumo (XLSX): {e_resumo}")

                # Deduplicar mantendo ordem.
                vistos_xlsx = set()
                xlsx_paths_filtrados = []
                for xp in xlsx_paths:
                    if not xp or xp in vistos_xlsx:
                        continue
                    vistos_xlsx.add(xp)
                    xlsx_paths_filtrados.append(xp)
                xlsx_paths = xlsx_paths_filtrados

                # === Etapa 2: Baixar etiquetas do UpSeller ===
                app._imprimir_status[user_id] = {
                    "etapa": "baixando",
                    "progresso": 28,
                    "detalhes": "Baixando etiquetas do UpSeller..."
                }
                pdfs = []
                try:
                    if lojas_lista and not filtro_loja:
                        total_grupos = max(1, len(grupos_marketplace))
                        for idx_gp, gp in enumerate(grupos_marketplace, start=1):
                            lojas_gp = list(gp.get("lojas") or [])
                            mp_nome = (gp.get("marketplace") or "Shopee").strip() or "Shopee"
                            if not lojas_gp:
                                continue
                            app._imprimir_status[user_id] = {
                                "etapa": "baixando", "progresso": 32,
                                "detalhes": (
                                    f"Baixando etiquetas ({idx_gp}/{total_grupos}) - "
                                    f"{mp_nome}: {len(lojas_gp)} loja(s)"
                                )
                            }
                            pdfs_lote = _upseller_mgr._run_async(
                                user_id, scraper.baixar_etiquetas(filtro_loja=lojas_gp)
                            ) or []
                            if pdfs_lote:
                                pdfs.extend(list(pdfs_lote))
                                continue

                            if len(lojas_gp) > 1:
                                for loja_nome in lojas_gp:
                                    try:
                                        pdfs_loja = _upseller_mgr._run_async(
                                            user_id, scraper.baixar_etiquetas(filtro_loja=loja_nome)
                                        ) or []
                                        if pdfs_loja:
                                            pdfs.extend(list(pdfs_loja))
                                        else:
                                            lojas_falha.append({
                                                "loja": loja_nome,
                                                "marketplace": mp_nome,
                                                "etapa": "gerar_etiquetas",
                                                "motivo": "sem_pdf_gerado",
                                            })
                                    except Exception as e_loja_pdf:
                                        lojas_falha.append({
                                            "loja": loja_nome,
                                            "marketplace": mp_nome,
                                            "etapa": "gerar_etiquetas",
                                            "motivo": str(e_loja_pdf),
                                        })
                            else:
                                lojas_falha.append({
                                    "loja": lojas_gp[0],
                                    "marketplace": mp_nome,
                                    "etapa": "gerar_etiquetas",
                                    "motivo": "sem_pdf_gerado",
                                })
                    else:
                        pdfs = _upseller_mgr._run_async(user_id, scraper.baixar_etiquetas(filtro_loja=filtro_loja))
                except Exception as e:
                    print(f"[Imprimir] Erro ao baixar etiquetas: {e}")
                    if lojas_lista and not filtro_loja:
                        # Fallback por loja mesmo com erro de lote, respeitando marketplace.
                        for gp in grupos_marketplace:
                            lojas_gp = list(gp.get("lojas") or [])
                            mp_nome = (gp.get("marketplace") or "Shopee").strip() or "Shopee"
                            for loja_nome in lojas_gp:
                                try:
                                    pdfs_loja = _upseller_mgr._run_async(
                                        user_id, scraper.baixar_etiquetas(filtro_loja=loja_nome)
                                    ) or []
                                    if pdfs_loja:
                                        pdfs.extend(list(pdfs_loja))
                                    else:
                                        lojas_falha.append({
                                            "loja": loja_nome,
                                            "marketplace": mp_nome,
                                            "etapa": "gerar_etiquetas",
                                            "motivo": "sem_pdf_gerado",
                                        })
                                except Exception as e_loja_pdf:
                                    lojas_falha.append({
                                        "loja": loja_nome,
                                        "marketplace": mp_nome,
                                        "etapa": "gerar_etiquetas",
                                        "motivo": str(e_loja_pdf),
                                    })
                    else:
                        app._imprimir_status[user_id] = {"etapa": "erro", "progresso": 0, "detalhes": f"Erro ao baixar: {e}"}
                        return

                if not pdfs:
                    detalhe_falhas = ""
                    if lojas_falha:
                        amostra = " | ".join(
                            f"{x.get('loja', '?')}: {x.get('motivo', 'erro')}"
                            for x in lojas_falha[:5]
                        )
                        detalhe_falhas = f" Falhas: {amostra}"
                    app._imprimir_status[user_id] = {
                        "etapa": "erro", "progresso": 0,
                        "detalhes": (
                            "Nenhuma etiqueta pendente no UpSeller. "
                            "Verifique a aba 'Etiqueta para Impressão' no UpSeller." + detalhe_falhas
                        ),
                        "lojas_falha": lojas_falha[:20],
                    }
                    return

                # === Etapa 2.5: Fallback antigo de extracao caso Lista de Resumo falhe ===
                if not xlsx_paths:
                    app._imprimir_status[user_id] = {
                        "etapa": "extraindo",
                        "progresso": 50,
                        "detalhes": "Lista de Resumo nao retornou XLSX. Tentando extracao de fallback..."
                    }
                    try:
                        if lojas_lista and not filtro_loja:
                            app._imprimir_status[user_id] = {
                                "etapa": "extraindo", "progresso": 54,
                                "detalhes": f"Fallback XLSX em lote ({len(lojas_lista)} lojas)..."
                            }
                            xp = _upseller_mgr._run_async(
                                user_id,
                                scraper.extrair_dados_pedidos_em_impressao(filtro_loja=lojas_lista)
                            ) or ""
                            if not xp:
                                xp = _upseller_mgr._run_async(
                                    user_id,
                                    scraper.extrair_dados_pedidos(status_filter="para_imprimir", filtro_loja=lojas_lista)
                                ) or ""
                            if not xp:
                                xp = _upseller_mgr._run_async(
                                    user_id,
                                    scraper.extrair_dados_pedidos(status_filter="para_enviar", filtro_loja=lojas_lista)
                                ) or ""
                            if _xlsx_download_valido(xp):
                                xlsx_paths.append(xp)
                        else:
                            xp = _upseller_mgr._run_async(
                                user_id,
                                scraper.extrair_dados_pedidos_em_impressao(filtro_loja=filtro_loja)
                            ) or ""
                            if not xp:
                                xp = _upseller_mgr._run_async(
                                    user_id,
                                    scraper.extrair_dados_pedidos(status_filter="para_imprimir", filtro_loja=filtro_loja)
                                ) or ""
                            if not xp:
                                xp = _upseller_mgr._run_async(
                                    user_id,
                                    scraper.extrair_dados_pedidos(status_filter="para_enviar", filtro_loja=filtro_loja)
                                ) or ""
                            if _xlsx_download_valido(xp):
                                xlsx_paths.append(xp)
                    except Exception as e_xlsx:
                        print(f"[Imprimir] Aviso: falha no fallback de XLSX: {e_xlsx}")

                # Deduplicar novamente apos fallback.
                if xlsx_paths:
                    vistos_xlsx2 = set()
                    dedup_xlsx = []
                    for xp in xlsx_paths:
                        if not xp or xp in vistos_xlsx2:
                            continue
                        vistos_xlsx2.add(xp)
                        dedup_xlsx.append(xp)
                    xlsx_paths = dedup_xlsx

                xlsx_path = xlsx_paths[0] if xlsx_paths else ""
                xlsx_extra = xlsx_paths[1:] if len(xlsx_paths) > 1 else []

                if xlsx_path and os.path.exists(xlsx_path):
                    app._imprimir_status[user_id] = {
                        "etapa": "movendo",
                        "progresso": 58,
                        "detalhes": f"{len(pdfs)} PDF(s) + {len(xlsx_paths)} XLSX, movendo..."
                    }
                else:
                    app._imprimir_status[user_id] = {
                        "etapa": "movendo",
                        "progresso": 58,
                        "detalhes": f"{len(pdfs)} PDF(s) baixado(s), movendo..."
                    }

                # === Etapa 3: Mover arquivos para lote isolado da pasta_entrada ===
                resultado = {"pdfs": pdfs, "xmls": [], "xlsx": xlsx_path or "", "xlsx_extra": xlsx_extra}
                resumo = scraper.mover_para_pasta_entrada(resultado, pasta_lote)
                if resumo.get("pdfs_movidos", 0) <= 0:
                    app._imprimir_status[user_id] = {
                        "etapa": "erro",
                        "progresso": 0,
                        "detalhes": (
                            "Nenhum PDF foi movido para o lote atual. "
                            "Processamento cancelado para preservar o ultimo resultado valido."
                        ),
                        "processado": False,
                    }
                    return

                app._imprimir_status[user_id] = {
                    "etapa": "processando", "progresso": 65,
                    "detalhes": f"{resumo['pdfs_movidos']} PDF(s) na pasta, processando etiquetas..."
                }

                # === Etapa 3: Processar etiquetas com regras do Beka MKT ===
                try:
                    proc_result = _executar_processamento(
                        user_id,
                        sem_recorte=True,
                        resumo_sku_somente=False,
                        pasta_entrada_override=pasta_lote
                    )
                    if not proc_result or not proc_result.get("ok"):
                        raise RuntimeError(
                            (proc_result or {}).get("erro") or
                            "Processamento concluido sem gerar resultado valido."
                        )
                    app._imprimir_status[user_id] = {
                        "etapa": "concluido", "progresso": 100,
                        "detalhes": (
                            f"Concluido! {resumo['pdfs_movidos']} PDF(s) processado(s) | "
                            f"{proc_result.get('total_etiquetas', 0)} etiquetas em "
                            f"{proc_result.get('total_lojas', 0)} loja(s)"
                        ),
                        "processado": True,
                    }

                    # Auto-disparo opcional WhatsApp (fila persistente)
                    try:
                        auto_res = _enfileirar_envio_whatsapp_resultado(
                            user_id=user_id,
                            origem="auto",
                            respeitar_toggle_auto=True,
                        )
                        if auto_res.get("ok"):
                            app._imprimir_status[user_id]["auto_whatsapp"] = {
                                "enfileirados": auto_res.get("total_entregas", 0),
                                "batch_id": auto_res.get("batch_id", ""),
                            }
                            app._imprimir_status[user_id]["detalhes"] += f" | WhatsApp fila: {auto_res.get('total_entregas', 0)}"
                        elif not auto_res.get("ignorado"):
                            app._imprimir_status[user_id]["aviso_whatsapp"] = auto_res.get("erro", "Falha ao enfileirar WhatsApp")
                    except Exception as e_auto:
                        app._imprimir_status[user_id]["aviso_whatsapp"] = f"Falha no auto-envio WhatsApp: {e_auto}"

                    if lojas_falha:
                        app._imprimir_status[user_id]["aviso_falhas"] = (
                            f"{len(lojas_falha)} loja(s) com erro foram ignoradas e o restante foi processado."
                        )
                        app._imprimir_status[user_id]["lojas_falha"] = lojas_falha[:20]
                except Exception as e:
                    print(f"[Imprimir] Erro no processamento: {e}")
                    import traceback
                    traceback.print_exc()
                    app._imprimir_status[user_id] = {
                        "etapa": "parcial", "progresso": 80,
                        "detalhes": f"Download OK ({resumo['pdfs_movidos']} PDFs), erro no processamento: {e}",
                        "processado": False,
                    }
                    if lojas_falha:
                        app._imprimir_status[user_id]["aviso_falhas"] = (
                            f"{len(lojas_falha)} loja(s) com erro foram ignoradas durante o download."
                        )
                        app._imprimir_status[user_id]["lojas_falha"] = lojas_falha[:20]

                # Atualizar snapshot em background para liberar resultado imediatamente.
                _disparar_atualizacao_lojas_background(user_id, status_attr="_imprimir_status")

                # Limpar pasta temp
                try:
                    import shutil
                    shutil.rmtree(download_dir, ignore_errors=True)
                except:
                    pass

            except Exception as e:
                print(f"[Imprimir] Erro geral: {e}")
                import traceback
                traceback.print_exc()
                app._imprimir_status[user_id] = {"etapa": "erro", "progresso": 0, "detalhes": str(e)}
            finally:
                app._imprimir_em_andamento[user_id] = False
                _finalizar_acao_massa(user_id, "imprimir")

    thread = threading.Thread(target=_imprimir_pipeline, daemon=True)
    thread.start()
    payload = {"sucesso": True, "mensagem": "Geracao de etiquetas iniciada", "async": True}
    if lojas_ignoradas:
        payload["lojas_ignoradas"] = lojas_ignoradas
    return jsonify(payload)


@app.route('/api/upseller/imprimir/status', methods=['GET'])
@jwt_required()
def api_upseller_imprimir_status():
    """Retorna status do pipeline 'Gerar Etiquetas' em andamento."""
    user_id = int(get_jwt_identity())
    if hasattr(app, '_imprimir_status') and user_id in app._imprimir_status:
        status = dict(app._imprimir_status[user_id])
        em_andamento = hasattr(app, '_imprimir_em_andamento') and app._imprimir_em_andamento.get(user_id, False)
        status["em_andamento"] = em_andamento
        status["atualizando_pedidos"] = _esta_atualizando_lojas(user_id) or bool(status.get("atualizando_pedidos"))
        return jsonify(status)
    return jsonify({
        "etapa": "idle",
        "progresso": 0,
        "detalhes": "",
        "em_andamento": False,
        "atualizando_pedidos": _esta_atualizando_lojas(user_id),
    })


# ----------------------------------------------------------------
# ENDPOINTS - WHATSAPP
# ----------------------------------------------------------------

@app.route('/api/whatsapp/status', methods=['GET'])
@jwt_required()
def api_whatsapp_status():
    """Verifica status da conexao WhatsApp."""
    _garantir_baileys_rodando(motivo="status")
    wa = WhatsAppService()
    return jsonify(wa.verificar_conexao())


@app.route('/api/whatsapp/qr', methods=['GET'])
@jwt_required()
def api_whatsapp_qr():
    """Retorna QR code para escanear."""
    _garantir_baileys_rodando(motivo="qr")
    wa = WhatsAppService()
    # Iniciar sessao se necessario
    wa.iniciar_sessao()
    return jsonify(wa.get_qr_code())


@app.route('/api/whatsapp/config', methods=['GET'])
@jwt_required()
def api_whatsapp_config_get():
    """Retorna configuracoes do modulo WhatsApp do usuario."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    return jsonify({
        "auto_send_whatsapp": bool(getattr(user, "auto_send_whatsapp", False)) if user else False
    })


@app.route('/api/whatsapp/config', methods=['POST'])
@jwt_required()
def api_whatsapp_config_set():
    """Atualiza configuracoes do modulo WhatsApp do usuario."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if not user:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    data = request.get_json(force=True, silent=True) or {}
    user.auto_send_whatsapp = _to_bool(data.get("auto_send_whatsapp"), False)
    db.session.commit()
    return jsonify({
        "mensagem": "Configuracao salva",
        "auto_send_whatsapp": bool(user.auto_send_whatsapp),
    })


@app.route('/api/whatsapp/contatos', methods=['GET'])
@jwt_required()
def api_whatsapp_contatos_listar():
    """Lista contatos WhatsApp do usuario."""
    user_id = int(get_jwt_identity())
    contatos = WhatsAppContact.query.filter_by(user_id=user_id).order_by(WhatsAppContact.loja_nome).all()
    return jsonify([c.to_dict() for c in contatos])


@app.route('/api/whatsapp/contatos', methods=['POST'])
@jwt_required()
def api_whatsapp_contatos_salvar():
    """Cadastra ou atualiza contato WhatsApp (com selecao de lojas/grupos)."""
    user_id = int(get_jwt_identity())
    data = request.get_json(force=True, silent=True) or {}

    telefone = str(data.get("telefone", "") or "").strip()
    if not telefone:
        return jsonify({"erro": "Telefone e obrigatorio"}), 400

    lojas = data.get("lojas", []) or []
    grupos = data.get("grupos", []) or []
    lojas = [str(x).strip() for x in lojas if str(x).strip()]
    grupos = [str(x).strip() for x in grupos if str(x).strip()]

    loja_cnpj = str(data.get("loja_cnpj", "") or "").strip()
    loja_nome = str(data.get("loja_nome", "") or "").strip()

    if not loja_nome and lojas:
        loja_nome = lojas[0]
    if not loja_cnpj:
        # Mantem compatibilidade com chave legada e evita campo vazio.
        loja_cnpj = "ALVO_CUSTOM"

    contato_id = data.get("id")
    contato = None
    if contato_id:
        contato = WhatsAppContact.query.filter_by(id=int(contato_id), user_id=user_id).first()
    if not contato:
        contato = WhatsAppContact.query.filter_by(
            user_id=user_id,
            loja_cnpj=loja_cnpj,
            telefone=telefone
        ).first()

    if contato:
        # Atualizar existente
        contato.loja_cnpj = loja_cnpj
        contato.loja_nome = loja_nome or contato.loja_nome
        contato.nome_contato = data.get("nome_contato", contato.nome_contato)
        contato.ativo = _to_bool(data.get("ativo"), contato.ativo)
        contato.lojas_json = json.dumps(lojas, ensure_ascii=False)
        contato.grupos_json = json.dumps(grupos, ensure_ascii=False)
    else:
        # Criar novo
        contato = WhatsAppContact(
            user_id=user_id,
            loja_cnpj=loja_cnpj,
            loja_nome=loja_nome,
            telefone=telefone,
            nome_contato=data.get("nome_contato", ""),
            lojas_json=json.dumps(lojas, ensure_ascii=False),
            grupos_json=json.dumps(grupos, ensure_ascii=False),
            ativo=_to_bool(data.get("ativo"), True),
        )
        db.session.add(contato)

    db.session.commit()
    return jsonify({"mensagem": "Contato salvo", **contato.to_dict()})


@app.route('/api/whatsapp/contatos/<int:contato_id>', methods=['DELETE'])
@jwt_required()
def api_whatsapp_contatos_remover(contato_id):
    """Remove um contato WhatsApp."""
    user_id = int(get_jwt_identity())
    contato = WhatsAppContact.query.filter_by(id=contato_id, user_id=user_id).first()
    if not contato:
        return jsonify({"erro": "Contato nao encontrado"}), 404
    db.session.delete(contato)
    db.session.commit()
    return jsonify({"mensagem": "Contato removido"})


@app.route('/api/whatsapp/enviar-teste', methods=['POST'])
@jwt_required()
def api_whatsapp_enviar_teste():
    """Envia mensagem de teste para um numero."""
    data = request.get_json(force=True, silent=True) or {}
    telefone = data.get("telefone", "")
    if not telefone:
        return jsonify({"success": False, "error": "Telefone obrigatorio"}), 400
    try:
        _garantir_baileys_rodando(motivo="teste")
        wa = WhatsAppService()
        status = wa.verificar_conexao()
        if not status.get("connected"):
            return jsonify({
                "success": False,
                "error": "WhatsApp desconectado. Escaneie o QR code para conectar.",
                "status": status,
            }), 400
        resultado = wa.enviar_mensagem(telefone, "Teste Beka MKT - Conexao WhatsApp OK!")
        if resultado.get("success"):
            return jsonify({
                "success": True,
                "message": "Mensagem de teste enviada com sucesso",
                "telefone": telefone,
                "messageId": resultado.get("messageId", ""),
            })
        return jsonify({
            "success": False,
            "error": resultado.get("error", "Falha ao enviar mensagem de teste"),
            "telefone": telefone,
        }), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/whatsapp/enviar-lote', methods=['POST'])
@jwt_required()
def api_whatsapp_enviar_lote():
    """Enfileira envio de arquivos (PDF/IMG/XLSX) para contatos cadastrados manualmente."""
    user_id = int(get_jwt_identity())
    enq = _enfileirar_envio_whatsapp_resultado(
        user_id=user_id,
        origem="manual",
        respeitar_toggle_auto=False,
    )
    if not enq.get("ok"):
        return jsonify({
            "erro": enq.get("erro", "Falha ao enfileirar envio"),
            "diagnostico": enq.get("diagnostico", {}),
            "ignorado": bool(enq.get("ignorado")),
        }), 400

    if not hasattr(app, "_whatsapp_send_status"):
        app._whatsapp_send_status = {}
    app._whatsapp_send_status[user_id] = {
        "batch_id": enq.get("batch_id"),
        "etapa": "enfileirado",
        "em_andamento": True,
        "progresso": 0,
        "detalhes": f"Fila criada com {enq.get('total_entregas', 0)} envio(s).",
        "total": enq.get("total_entregas", 0),
        "enviados": 0,
        "erros": 0,
        "diagnostico": enq.get("diagnostico", {}),
    }
    return jsonify({
        "mensagem": f"Fila WhatsApp criada com {enq.get('total_entregas', 0)} envio(s).",
        "batch_id": enq.get("batch_id"),
        "total_entregas": enq.get("total_entregas", 0),
        "diagnostico": enq.get("diagnostico", {}),
    })


@app.route('/api/whatsapp/enviar-lote/status', methods=['GET'])
@jwt_required()
def api_whatsapp_enviar_lote_status():
    """Status do envio manual de WhatsApp em background."""
    user_id = int(get_jwt_identity())
    status = {}
    if hasattr(app, "_whatsapp_send_status"):
        status = app._whatsapp_send_status.get(user_id, {}) or {}
    batch_id = status.get("batch_id", "") if status else ""
    if batch_id:
        fila = _status_batch_fila_whatsapp(user_id, batch_id)
        fila["diagnostico"] = status.get("diagnostico", {})
        app._whatsapp_send_status[user_id] = {**status, **fila}
        return jsonify({**status, **fila})

    # Sem batch ativo: retorna panorama rapido da fila do usuario.
    totais = defaultdict(int)
    for st in db.session.query(WhatsAppQueueItem.status, db.func.count(WhatsAppQueueItem.id)).filter(
        WhatsAppQueueItem.user_id == user_id
    ).group_by(WhatsAppQueueItem.status):
        totais[st[0] or "pending"] = st[1]
    total_fila = sum(totais.values())
    return jsonify({
        "etapa": "idle",
        "em_andamento": False,
        "progresso": 0,
        "detalhes": "Sem envio manual ativo",
        "total": total_fila,
        "enviados": totais.get("sent", 0),
        "erros": totais.get("dead", 0),
        "retry": totais.get("retry", 0),
        "pendentes": totais.get("pending", 0) + totais.get("sending", 0),
    })


@app.route('/api/whatsapp/fila/status', methods=['GET'])
@jwt_required()
def api_whatsapp_fila_status():
    """Resumo da fila persistente de WhatsApp."""
    user_id = int(get_jwt_identity())
    rows = db.session.query(WhatsAppQueueItem.status, db.func.count(WhatsAppQueueItem.id)).filter(
        WhatsAppQueueItem.user_id == user_id
    ).group_by(WhatsAppQueueItem.status).all()
    counts = defaultdict(int)
    for st, qtd in rows:
        counts[st or "pending"] = qtd

    ultimo_batch = db.session.query(WhatsAppQueueItem.batch_id).filter(
        WhatsAppQueueItem.user_id == user_id
    ).order_by(WhatsAppQueueItem.id.desc()).first()
    batch_id = ultimo_batch[0] if ultimo_batch else ""
    batch = _status_batch_fila_whatsapp(user_id, batch_id) if batch_id else {}

    return jsonify({
        "counts": {
            "pending": counts.get("pending", 0),
            "retry": counts.get("retry", 0),
            "sending": counts.get("sending", 0),
            "sent": counts.get("sent", 0),
            "dead": counts.get("dead", 0),
        },
        "batch_atual": batch,
    })


# ----------------------------------------------------------------
# ENDPOINTS - EMAIL (contatos + envio de etiquetas)
# ----------------------------------------------------------------

def _smtp_campos_usuario(user):
    if not user:
        return {
            "smtp_host": "",
            "smtp_port": 587,
            "smtp_user": "",
            "smtp_from": "",
            "smtp_pass_salva": False,
        }
    try:
        smtp_port = int(getattr(user, "smtp_port", 587) or 587)
    except Exception:
        smtp_port = 587
    return {
        "smtp_host": (getattr(user, "smtp_host", "") or "").strip(),
        "smtp_port": smtp_port,
        "smtp_user": (getattr(user, "smtp_user", "") or "").strip(),
        "smtp_from": (getattr(user, "smtp_from", "") or "").strip(),
        "smtp_pass_salva": bool((getattr(user, "smtp_pass_enc", "") or "").strip()),
    }


def _smtp_config_usuario(user):
    campos = _smtp_campos_usuario(user)
    smtp_pass = ""
    pass_enc = (getattr(user, "smtp_pass_enc", "") or "").strip() if user else ""
    if pass_enc:
        try:
            smtp_pass = decrypt_value(pass_enc)
        except Exception:
            smtp_pass = ""
    cfg = {
        "host": campos["smtp_host"],
        "port": campos["smtp_port"],
        "user": campos["smtp_user"],
        "password": smtp_pass,
        "from_addr": campos["smtp_from"] or campos["smtp_user"],
    }
    if smtp_configurado(cfg):
        return cfg
    return None


def _smtp_config_resolver(user):
    cfg_user = _smtp_config_usuario(user)
    if cfg_user:
        return cfg_user, "usuario"
    cfg_env = get_smtp_config()
    if cfg_env:
        return cfg_env, "ambiente"
    return None, "nao_configurado"


@app.route('/api/email/status', methods=['GET'])
@jwt_required()
def api_email_status():
    """Retorna se SMTP esta configurado."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    smtp_cfg, smtp_origem = _smtp_config_resolver(user)
    smtp_campos = _smtp_campos_usuario(user)
    return jsonify({
        "configurado": bool(smtp_cfg),
        "smtp_origem": smtp_origem,
        "email_remetente": (getattr(user, "email_remetente", "") or "").strip() if user else "",
        "nome_remetente": (getattr(user, "nome_remetente", "") or "").strip() if user else "",
        **smtp_campos,
    })


@app.route('/api/email/config', methods=['GET'])
@jwt_required()
def api_email_config_get():
    """Retorna configuracao de remetente do modulo de email."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    smtp_cfg, smtp_origem = _smtp_config_resolver(user)
    smtp_campos = _smtp_campos_usuario(user)
    return jsonify({
        "configurado": bool(smtp_cfg),
        "smtp_origem": smtp_origem,
        "email_remetente": (getattr(user, "email_remetente", "") or "").strip() if user else "",
        "nome_remetente": (getattr(user, "nome_remetente", "") or "").strip() if user else "",
        **smtp_campos,
    })


@app.route('/api/email/config', methods=['POST'])
@jwt_required()
def api_email_config_set():
    """Atualiza configuracao de remetente do modulo de email."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if not user:
        return jsonify({"erro": "Usuario nao encontrado"}), 404
    data = request.get_json(force=True, silent=True) or {}
    email_rem = str(data.get("email_remetente", "") or "").strip()
    nome_rem = str(data.get("nome_remetente", "") or "").strip()
    if not email_rem:
        return jsonify({"erro": "Email do remetente e obrigatorio"}), 400
    if "@" not in email_rem:
        return jsonify({"erro": "Email do remetente invalido"}), 400
    user.email_remetente = email_rem
    user.nome_remetente = nome_rem

    # SMTP por usuario (opcional no cadastro, obrigatorio para envio real)
    smtp_host_in = data.get("smtp_host", None)
    smtp_port_in = data.get("smtp_port", None)
    smtp_user_in = data.get("smtp_user", None)
    smtp_from_in = data.get("smtp_from", None)
    smtp_pass_in = data.get("smtp_pass", None)
    smtp_limpar_senha = _to_bool(data.get("smtp_limpar_senha"), False)

    if smtp_host_in is not None:
        user.smtp_host = str(smtp_host_in or "").strip()
    elif not (getattr(user, "smtp_host", "") or "").strip():
        user.smtp_host = "smtp.gmail.com"

    if smtp_port_in is not None:
        try:
            smtp_port = int(str(smtp_port_in).strip() or "587")
            if smtp_port <= 0 or smtp_port > 65535:
                raise ValueError()
            user.smtp_port = smtp_port
        except Exception:
            return jsonify({"erro": "Porta SMTP invalida"}), 400
    elif not getattr(user, "smtp_port", None):
        user.smtp_port = 587

    if smtp_user_in is not None:
        user.smtp_user = str(smtp_user_in or "").strip()
    elif not (getattr(user, "smtp_user", "") or "").strip():
        user.smtp_user = email_rem

    if smtp_from_in is not None:
        user.smtp_from = str(smtp_from_in or "").strip()
    elif not (getattr(user, "smtp_from", "") or "").strip():
        user.smtp_from = email_rem

    if smtp_pass_in is not None:
        smtp_pass_txt = str(smtp_pass_in or "").strip()
        if smtp_pass_txt:
            try:
                user.smtp_pass_enc = encrypt_value(smtp_pass_txt)
            except Exception:
                return jsonify({"erro": "Nao foi possivel salvar a senha SMTP"}), 500
        elif smtp_limpar_senha:
            user.smtp_pass_enc = ""

    db.session.commit()
    smtp_cfg, smtp_origem = _smtp_config_resolver(user)
    smtp_campos = _smtp_campos_usuario(user)
    return jsonify({
        "mensagem": "Configuracao de email salva",
        "email_remetente": user.email_remetente or "",
        "nome_remetente": user.nome_remetente or "",
        "configurado": bool(smtp_cfg),
        "smtp_origem": smtp_origem,
        **smtp_campos,
    })


@app.route('/api/email/validar', methods=['POST'])
@jwt_required()
def api_email_validar():
    """Valida login SMTP atual (usuario ou ambiente)."""
    user_id = int(get_jwt_identity())
    user = User.query.get(user_id)
    if not user:
        return jsonify({"sucesso": False, "erro": "Usuario nao encontrado"}), 404

    smtp_cfg, smtp_origem = _smtp_config_resolver(user)
    if not smtp_cfg:
        return jsonify({"sucesso": False, "erro": "SMTP nao configurado"}), 400

    host = str(smtp_cfg.get("host") or "").strip()
    port = int(smtp_cfg.get("port") or 587)
    user_smtp = str(smtp_cfg.get("user") or "").strip()
    pass_smtp = str(smtp_cfg.get("password") or "").strip()

    if not host or not user_smtp or not pass_smtp:
        return jsonify({"sucesso": False, "erro": "Configuracao SMTP incompleta"}), 400

    try:
        if port == 465:
            server = smtplib.SMTP_SSL(host, port, timeout=20)
            server.login(user_smtp, pass_smtp)
            server.quit()
        else:
            server = smtplib.SMTP(host, port, timeout=20)
            server.ehlo()
            if port in (587, 25, 2525):
                server.starttls()
                server.ehlo()
            server.login(user_smtp, pass_smtp)
            server.quit()
    except Exception as e:
        return jsonify({"sucesso": False, "erro": f"Falha SMTP: {e}"}), 400

    return jsonify({
        "sucesso": True,
        "mensagem": f"Remetente validado com sucesso ({smtp_origem}).",
    })


@app.route('/api/email/contatos', methods=['GET'])
@jwt_required()
def api_email_contatos_listar():
    """Lista contatos de email do usuario."""
    user_id = int(get_jwt_identity())
    contatos = EmailContact.query.filter_by(user_id=user_id).all()
    return jsonify([c.to_dict() for c in contatos])


@app.route('/api/email/contatos', methods=['POST'])
@jwt_required()
def api_email_contatos_criar():
    """Cria/atualiza contato de email (com selecao de lojas/grupos)."""
    user_id = int(get_jwt_identity())
    data = request.get_json(force=True, silent=True) or {}
    email_addr = data.get("email", "").strip()
    if not email_addr or "@" not in email_addr:
        return jsonify({"erro": "Email invalido"}), 400

    lojas = data.get("lojas", []) or []
    grupos = data.get("grupos", []) or []
    lojas = [str(x).strip() for x in lojas if str(x).strip()]
    grupos = [str(x).strip() for x in grupos if str(x).strip()]

    loja_cnpj = str(data.get("loja_cnpj", "") or "").strip() or "ALVO_CUSTOM"
    contato_id = data.get("id")
    contato = None
    if contato_id:
        contato = EmailContact.query.filter_by(id=int(contato_id), user_id=user_id).first()
    if not contato:
        contato = EmailContact.query.filter_by(user_id=user_id, email=email_addr, loja_cnpj=loja_cnpj).first()

    if contato:
        contato.nome_contato = data.get("nome_contato", contato.nome_contato)
        contato.ativo = _to_bool(data.get("ativo"), contato.ativo)
        contato.loja_cnpj = loja_cnpj
        contato.lojas_json = json.dumps(lojas, ensure_ascii=False)
        contato.grupos_json = json.dumps(grupos, ensure_ascii=False)
    else:
        contato = EmailContact(
            user_id=user_id,
            email=email_addr,
            loja_cnpj=loja_cnpj,
            nome_contato=data.get("nome_contato", ""),
            lojas_json=json.dumps(lojas, ensure_ascii=False),
            grupos_json=json.dumps(grupos, ensure_ascii=False),
            ativo=True,
        )
        db.session.add(contato)
    db.session.commit()
    return jsonify(contato.to_dict()), 201


@app.route('/api/email/contatos/<int:contato_id>', methods=['DELETE'])
@jwt_required()
def api_email_contatos_deletar(contato_id):
    """Remove contato de email."""
    user_id = int(get_jwt_identity())
    contato = EmailContact.query.filter_by(id=contato_id, user_id=user_id).first()
    if not contato:
        return jsonify({"erro": "Contato nao encontrado"}), 404
    db.session.delete(contato)
    db.session.commit()
    return jsonify({"ok": True})


@app.route('/api/email/enviar-lote', methods=['POST'])
@jwt_required()
def api_email_enviar_lote():
    """Envia arquivos de etiquetas (PDF/IMG/XLSX) por email para contatos cadastrados."""
    user_id = int(get_jwt_identity())
    estado = _get_estado(user_id)
    resultado = estado.get("ultimo_resultado", {})
    user = User.query.get(user_id)
    pasta_saida = user.get_pasta_saida()

    if not resultado or not resultado.get("lojas"):
        return jsonify({"erro": "Nenhum resultado para enviar. Processe as etiquetas primeiro."}), 400

    from_addr_override = (getattr(user, "email_remetente", "") or "").strip()
    if not from_addr_override or "@" not in from_addr_override:
        return jsonify({
            "erro": "Email remetente obrigatorio. Configure em Automacao > Contatos Email > Configurar Remetente."
        }), 400

    smtp_cfg, smtp_origem = _smtp_config_resolver(user)
    if not smtp_cfg:
        return jsonify({
            "erro": "SMTP nao configurado. Preencha host, usuario e senha SMTP em Automacao > Contatos Email > Configurar Remetente."
        }), 400

    contatos = EmailContact.query.filter_by(user_id=user_id, ativo=True).all()
    if not contatos:
        return jsonify({"erro": "Nenhum contato de email cadastrado"}), 400

    envios, diagnostico = montar_destinos_por_resultado(
        resultado=resultado,
        pasta_saida=pasta_saida,
        contatos=contatos,
        destino_attr="email",
        agrupamentos_usuario=(estado or {}).get("agrupamentos", []),
    )

    if not envios:
        return jsonify({
            "erro": "Nenhuma loja com contato email e arquivos encontrados",
            "diagnostico": diagnostico,
        }), 400

    timestamp = resultado.get("timestamp", "")
    from_name_override = (getattr(user, "nome_remetente", "") or "").strip()

    envios_agrupados = {}
    for envio in envios:
        destino = str(envio.get("destino", "") or "").strip()
        loja_nome = str(envio.get("loja", "") or "").strip()
        file_path = str(envio.get("file_path", envio.get("pdf_path", "")) or "").strip()
        if not destino or not file_path:
            continue
        chave = (destino.lower(), loja_nome)
        if chave not in envios_agrupados:
            envios_agrupados[chave] = {
                "destino": destino,
                "loja": loja_nome,
                "arquivos": [],
            }
        if file_path not in envios_agrupados[chave]["arquivos"]:
            envios_agrupados[chave]["arquivos"].append(file_path)

    grupos_envio = list(envios_agrupados.values())
    total_arquivos = sum(len(g.get("arquivos", [])) for g in grupos_envio)
    if not grupos_envio:
        return jsonify({
            "erro": "Nenhum arquivo valido encontrado para envio",
            "diagnostico": diagnostico,
        }), 400

    # Enviar em background
    def _enviar_emails():
        with app.app_context():
            resultados = []
            for envio in grupos_envio:
                res = enviar_email_com_anexos(
                    email_destino=envio["destino"],
                    assunto=f"Arquivos {envio['loja']} - {timestamp}",
                    loja_nome=envio["loja"],
                    timestamp=timestamp,
                    anexos_paths=envio.get("arquivos", []),
                    from_addr_override=from_addr_override,
                    from_name_override=from_name_override,
                    smtp_override=smtp_cfg,
                )
                resultados.append(res)
                import time
                time.sleep(2)  # anti-spam

            # Log
            log_exec = ExecutionLog(
                user_id=user_id,
                tipo="email",
                inicio=datetime.utcnow(),
                fim=datetime.utcnow(),
                status="sucesso" if all(r.get("success") for r in resultados) else "parcial",
                whatsapp_enviados=sum(1 for r in resultados if r.get("success")),
                whatsapp_erros=sum(1 for r in resultados if not r.get("success")),
                detalhes=json.dumps({
                    "envios": len(grupos_envio),
                    "arquivos": total_arquivos,
                    "resultados": resultados,
                    "diagnostico": diagnostico,
                }, ensure_ascii=False),
            )
            db.session.add(log_exec)
            db.session.commit()

    thread = threading.Thread(target=_enviar_emails, daemon=True)
    thread.start()
    return jsonify({
        "mensagem": f"Enviando {len(grupos_envio)} email(s) com {total_arquivos} arquivo(s) em background...",
        "total_envios": len(grupos_envio),
        "total_arquivos": total_arquivos,
        "smtp_origem": smtp_origem,
        "diagnostico": diagnostico,
    })


# ----------------------------------------------------------------
# ENDPOINTS - AGENDAMENTOS
# ----------------------------------------------------------------

@app.route('/api/agendamentos', methods=['GET'])
@jwt_required()
def api_agendamentos_listar():
    """Lista agendamentos do usuario."""
    user_id = int(get_jwt_identity())
    return jsonify(beka_scheduler.listar_agendamentos(user_id))


@app.route('/api/agendamentos', methods=['POST'])
@jwt_required()
def api_agendamentos_criar():
    """Cria novo agendamento."""
    user_id = int(get_jwt_identity())
    data = request.get_json()

    if not data or not data.get("hora"):
        return jsonify({"erro": "Horario obrigatorio"}), 400

    schedule_id = beka_scheduler.adicionar_agendamento(user_id, data)
    if schedule_id:
        return jsonify({"mensagem": "Agendamento criado", "id": schedule_id})
    return jsonify({"erro": "Erro ao criar agendamento"}), 500


@app.route('/api/agendamentos/<int:schedule_id>', methods=['PUT'])
@jwt_required()
def api_agendamentos_atualizar(schedule_id):
    """Atualiza agendamento existente."""
    user_id = int(get_jwt_identity())
    # Verificar propriedade
    sched = Schedule.query.filter_by(id=schedule_id, user_id=user_id).first()
    if not sched:
        return jsonify({"erro": "Agendamento nao encontrado"}), 404

    data = request.get_json()
    if beka_scheduler.atualizar_agendamento(schedule_id, data):
        return jsonify({"mensagem": "Agendamento atualizado"})
    return jsonify({"erro": "Erro ao atualizar"}), 500


@app.route('/api/agendamentos/<int:schedule_id>', methods=['DELETE'])
@jwt_required()
def api_agendamentos_remover(schedule_id):
    """Remove agendamento."""
    user_id = int(get_jwt_identity())
    sched = Schedule.query.filter_by(id=schedule_id, user_id=user_id).first()
    if not sched:
        return jsonify({"erro": "Agendamento nao encontrado"}), 404

    if beka_scheduler.remover_agendamento(schedule_id):
        return jsonify({"mensagem": "Agendamento removido"})
    return jsonify({"erro": "Erro ao remover"}), 500


@app.route('/api/agendamentos/<int:schedule_id>/pausar', methods=['POST'])
@jwt_required()
def api_agendamentos_pausar(schedule_id):
    """Pausa agendamento."""
    user_id = int(get_jwt_identity())
    sched = Schedule.query.filter_by(id=schedule_id, user_id=user_id).first()
    if not sched:
        return jsonify({"erro": "Agendamento nao encontrado"}), 404

    if beka_scheduler.pausar_agendamento(schedule_id):
        return jsonify({"mensagem": "Agendamento pausado"})
    return jsonify({"erro": "Erro ao pausar"}), 500


@app.route('/api/agendamentos/<int:schedule_id>/retomar', methods=['POST'])
@jwt_required()
def api_agendamentos_retomar(schedule_id):
    """Retoma agendamento pausado."""
    user_id = int(get_jwt_identity())
    sched = Schedule.query.filter_by(id=schedule_id, user_id=user_id).first()
    if not sched:
        return jsonify({"erro": "Agendamento nao encontrado"}), 404

    if beka_scheduler.retomar_agendamento(schedule_id):
        return jsonify({"mensagem": "Agendamento retomado"})
    return jsonify({"erro": "Erro ao retomar"}), 500


@app.route('/api/agendamentos/historico', methods=['GET'])
@jwt_required()
def api_agendamentos_historico():
    """Historico de execucoes."""
    user_id = int(get_jwt_identity())
    limite = request.args.get('limite', 20, type=int)
    return jsonify(beka_scheduler.get_historico(user_id, limite))


@app.route('/api/agendamentos/executar-agora', methods=['POST'])
@jwt_required()
def api_agendamentos_executar_agora():
    """Executa pipeline completo manualmente (sem agendamento)."""
    user_id = int(get_jwt_identity())
    beka_scheduler.executar_agora(user_id)
    return jsonify({"mensagem": "Pipeline iniciado em background"})


# Inicia workers globais (fila WhatsApp + supervisor Baileys)
_iniciar_background_workers()


# ----------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------
if __name__ == '__main__':
    print("=" * 60)
    print("DASHBOARD - Beka MultiPlace")
    print("=" * 60)
    print(f"\n  Abra no navegador: http://localhost:5000\n")
    print("=" * 60)

    if getattr(sys, 'frozen', False):
        import webbrowser
        threading.Timer(1.5, lambda: webbrowser.open('http://localhost:5000')).start()

    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, port=port, host='0.0.0.0')
