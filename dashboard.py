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
import re as _re
from datetime import datetime, timedelta
from collections import defaultdict
from flask import Flask, request, jsonify, send_from_directory, send_file, redirect
from flask_cors import CORS
from flask_jwt_extended import JWTManager, jwt_required, get_jwt_identity, get_jwt
import xmltodict
import pandas as pd
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from etiquetas_shopee import ProcessadorEtiquetasShopee
from models import db, bcrypt, User, Session
from auth import auth_bp
from payments import payments_bp

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

# Criar tabelas
with app.app_context():
    _migrate_db()
    db.create_all()

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
    sessao = Session.query.filter_by(user_id=int(user_id), token_id=token_id).first()
    return sessao is None  # True = bloqueado (sessao nao existe mais)

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
    # Carregar ultimo_resultado salvo em disco
    _carregar_resultado_usuario(user_id)


def _resultado_path(user_id):
    """Caminho do arquivo de resultado do usuario."""
    user = User.query.get(int(user_id))
    if not user:
        return None
    pasta = user.get_pasta_saida()
    return os.path.join(pasta, "_ultimo_resultado.json")


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

@app.route('/')
def index():
    """Serve o dashboard (verifica login no frontend)."""
    return send_from_directory('static', 'index.html')


@app.route('/login')
def login_page():
    return send_from_directory('static', 'login.html')


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

    Usa o processador real (carregar_todos_pdfs) de forma lightweight para
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
        todas_etiquetas, cpf_auto, pdfs_shein = proc.carregar_todos_pdfs(pasta_entrada)

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

    thread = threading.Thread(target=_executar_processamento, args=(int(user_id),))
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
    dados = request.get_json()
    estado["agrupamentos"] = dados.get("agrupamentos", [])
    adicionar_log(estado, f"Agrupamentos salvos: {len(estado['agrupamentos'])} grupo(s)", "success")
    return jsonify({"ok": True})


# ----------------------------------------------------------------
# PROCESSAMENTO EM BACKGROUND
# ----------------------------------------------------------------
def _executar_processamento(user_id):
    """Executa o processamento completo em thread separada."""
    with app.app_context():
        estado = _get_estado(user_id)
        if not estado:
            return

        estado["processando"] = True
        estado["logs"] = []
        inicio = time.time()

        try:
            pasta_entrada = estado["configuracoes"]["pasta_entrada"]
            pasta_saida = estado["configuracoes"]["pasta_saida"]

            # Limpar pasta de saida antes de processar (evita duplicatas)
            import shutil
            if os.path.exists(pasta_saida):
                shutil.rmtree(pasta_saida)
            os.makedirs(pasta_saida, exist_ok=True)

            adicionar_log(estado, "Iniciando processamento...", "info")

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
            if proc.dados_xlsx_global:
                adicionar_log(estado, f"XLSX: {len(proc.dados_xlsx_global)} pedidos, {len(proc.dados_xlsx_tracking)} trackings", "success")
            else:
                adicionar_log(estado, "Nenhum XLSX de empacotamento encontrado", "warning")

            adicionar_log(estado, "Carregando etiquetas dos PDFs...", "info")
            todas_etiquetas, cpf_auto_detectadas, pdfs_shein_auto = proc.carregar_todos_pdfs(pasta_entrada)
            adicionar_log(estado, f"Total: {len(todas_etiquetas)} etiquetas extraidas", "success")
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

            if not os.path.exists(pasta_saida):
                os.makedirs(pasta_saida)

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

                    etiq_regular = [e for e in etiquetas_loja if e.get('tipo_especial') != 'cpf']
                    etiq_cpf = [e for e in etiquetas_loja if e.get('tipo_especial') == 'cpf']

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

                    caminho_xlsx = os.path.join(pasta_loja, f"resumo_{nome_loja}_{timestamp}.xlsx")
                    n_skus, total_qtd = proc.gerar_resumo_xlsx(etiquetas_loja, caminho_xlsx, nome_loja)

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
                        proc.gerar_resumo_xlsx(etiquetas_grupo, caminho_xlsx_g, nome_grupo)

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

            # Registrar processamento no contador do usuario
            user = User.query.get(user_id)
            if user:
                user.registrar_processamento()

            adicionar_log(estado, f"Processamento concluido em {duracao}s!", "success")

        except Exception as e:
            adicionar_log(estado, f"ERRO: {str(e)}", "error")
            import traceback
            adicionar_log(estado, traceback.format_exc(), "error")

        finally:
            estado["processando"] = False


def _formatar_tamanho(bytes_val):
    if bytes_val < 1024:
        return f"{bytes_val} B"
    elif bytes_val < 1024 * 1024:
        return f"{bytes_val / 1024:.1f} KB"
    else:
        return f"{bytes_val / (1024 * 1024):.1f} MB"


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
