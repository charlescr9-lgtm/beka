# -*- coding: utf-8 -*-
"""
Modelos do banco de dados - SQLAlchemy + SQLite
Inclui: Users, Sessions, Payments, WhatsAppContacts, Schedules, UpSellerConfig, ExecutionLog
"""

import os
import uuid
import json
from datetime import datetime, timedelta, timezone
from flask_sqlalchemy import SQLAlchemy

# Fuso horario de Brasilia (UTC-3)
_FUSO_BRASILIA = timezone(timedelta(hours=-3))


def _agora_brasil():
    """Retorna datetime atual no fuso de Brasilia (UTC-3), sem tzinfo (naive)."""
    return datetime.now(_FUSO_BRASILIA).replace(tzinfo=None)
from flask_bcrypt import Bcrypt
from cryptography.fernet import Fernet

db = SQLAlchemy()
bcrypt = Bcrypt()

# Chave de criptografia para senhas do UpSeller (fixa para persistir entre reinícios)
_FERNET_KEY = os.environ.get("FERNET_KEY", "cj6k4FwRMbZFA2X7s1vDGqm_UMdd1FWtM-KcTjs2g-k=")
_fernet = Fernet(_FERNET_KEY.encode() if isinstance(_FERNET_KEY, str) else _FERNET_KEY)


def encrypt_value(value: str) -> str:
    """Encripta um valor sensivel (ex: senha UpSeller)."""
    return _fernet.encrypt(value.encode()).decode()


def decrypt_value(encrypted: str) -> str:
    """Decripta um valor sensivel."""
    return _fernet.decrypt(encrypted.encode()).decode()

# Planos disponiveis
PLANOS = {
    "free":         {"nome": "Free",         "max_ips": 1, "limite_proc": 5,  "valor": 0},
    "basico":       {"nome": "Basico",       "max_ips": 1, "limite_proc": -1, "valor": 39.90},
    "pro":          {"nome": "Pro",          "max_ips": 2, "limite_proc": -1, "valor": 59.90},
    "empresarial":  {"nome": "Empresarial",  "max_ips": 5, "limite_proc": -1, "valor": 89.90},
    "unlimited":    {"nome": "Admin",        "max_ips": 99, "limite_proc": -1, "valor": 0},
}


def _json_list(value):
    """Converte valor JSON/lista para lista segura de strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    txt = str(value).strip()
    if not txt:
        return []
    try:
        data = json.loads(txt)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass
    return []


class User(db.Model):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    plano = db.Column(db.String(20), default='free')
    processamentos_mes = db.Column(db.Integer, default=0)
    mes_atual = db.Column(db.String(7), default='')
    created_at = db.Column(db.DateTime, default=_agora_brasil)
    is_active = db.Column(db.Boolean, default=True)

    # Verificacao de email
    email_verified = db.Column(db.Boolean, default=False)
    email_code = db.Column(db.String(6), default='')
    email_code_expires = db.Column(db.DateTime, nullable=True)

    # Recuperacao de senha
    reset_code = db.Column(db.String(6), default='')
    reset_code_expires = db.Column(db.DateTime, nullable=True)

    # Sistema de indicacao
    cupom_indicacao = db.Column(db.String(20), unique=True, nullable=True)
    indicado_por = db.Column(db.Integer, nullable=True)  # user_id de quem indicou
    meses_gratis = db.Column(db.Integer, default=0)  # meses gratis acumulados
    plano_expira = db.Column(db.DateTime, nullable=True)  # quando plano pago expira

    # Google OAuth
    google_id = db.Column(db.String(255), nullable=True, unique=True)
    auto_send_whatsapp = db.Column(db.Boolean, default=False)
    email_remetente = db.Column(db.String(200), default='')
    nome_remetente = db.Column(db.String(200), default='')
    smtp_host = db.Column(db.String(200), default='')
    smtp_port = db.Column(db.Integer, default=587)
    smtp_user = db.Column(db.String(200), default='')
    smtp_pass_enc = db.Column(db.Text, default='')
    smtp_from = db.Column(db.String(200), default='')
    pasta_avulsas = db.Column(db.String(500), default='')

    payments = db.relationship('Payment', backref='user', lazy=True)
    sessions = db.relationship('Session', backref='user', lazy=True)

    def get_plano_info(self):
        return PLANOS.get(self.plano, PLANOS["free"])

    def set_password(self, password):
        self.password_hash = bcrypt.generate_password_hash(password).decode('utf-8')

    def check_password(self, password):
        return bcrypt.check_password_hash(self.password_hash, password)

    def pode_processar(self):
        """Verifica limite de processamentos (free = 5/mes, pagos = ilimitado)."""
        info = self.get_plano_info()
        if info["limite_proc"] == -1:
            return True
        mes = _agora_brasil().strftime('%Y-%m')
        if self.mes_atual != mes:
            self.mes_atual = mes
            self.processamentos_mes = 0
            db.session.commit()
        return self.processamentos_mes < info["limite_proc"]

    def registrar_processamento(self):
        mes = _agora_brasil().strftime('%Y-%m')
        if self.mes_atual != mes:
            self.mes_atual = mes
            self.processamentos_mes = 0
        self.processamentos_mes += 1
        db.session.commit()

    def get_pasta_entrada(self):
        # Usar volume persistente do Railway se disponivel
        volume = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "")
        if volume:
            base = os.path.join(volume, "users")
        else:
            base = os.environ.get("USERS_DATA_DIR", "/tmp/users")
        pasta = os.path.join(base, str(self.id), "entrada")
        os.makedirs(pasta, exist_ok=True)
        return pasta

    def get_pasta_saida(self):
        # Usar volume persistente do Railway se disponivel
        volume = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "")
        if volume:
            base = os.path.join(volume, "users")
        else:
            base = os.environ.get("USERS_DATA_DIR", "/tmp/users")
        pasta = os.path.join(base, str(self.id), "Etiquetas prontas")
        os.makedirs(pasta, exist_ok=True)
        return pasta

    def get_pasta_lucro(self):
        """Pasta separada para arquivos da calculadora de lucros (ZIP/XML + custos)."""
        volume = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "")
        if volume:
            base = os.path.join(volume, "users")
        else:
            base = os.environ.get("USERS_DATA_DIR", "/tmp/users")
        pasta = os.path.join(base, str(self.id), "lucro_entrada")
        os.makedirs(pasta, exist_ok=True)
        return pasta

    def criar_sessao(self, ip):
        """Cria sessao com IP. BLOQUEIA se ja atingiu o limite de IPs do plano."""
        max_ips = self.get_plano_info()["max_ips"]

        # Se ja existe sessao com esse IP, reutiliza
        sessao_existente = Session.query.filter_by(user_id=self.id, ip=ip).first()
        if sessao_existente:
            sessao_existente.token_id = str(uuid.uuid4())
            sessao_existente.last_seen = _agora_brasil()
            db.session.commit()
            return sessao_existente.token_id

        # IP novo - verificar se cabe no plano
        ips_ativos = db.session.query(Session.ip).filter_by(user_id=self.id).distinct().count()
        if ips_ativos >= max_ips:
            return None  # BLOQUEADO - limite de IPs atingido

        nova = Session(user_id=self.id, token_id=str(uuid.uuid4()), ip=ip)
        db.session.add(nova)
        db.session.commit()
        return nova.token_id

    def sessao_valida(self, token_id):
        return Session.query.filter_by(user_id=self.id, token_id=token_id).first() is not None

    def remover_sessao(self, token_id):
        """Invalida o token (logout) mas MANTEM o IP registrado para sempre."""
        sessao = Session.query.filter_by(user_id=self.id, token_id=token_id).first()
        if sessao:
            sessao.token_id = "deslogado_" + str(uuid.uuid4())[:8]
            db.session.commit()

    def to_dict(self):
        info = self.get_plano_info()
        ips_ativos = db.session.query(Session.ip).filter_by(user_id=self.id).distinct().count()
        return {
            "id": self.id,
            "email": self.email,
            "plano": self.plano,
            "plano_nome": info["nome"],
            "processamentos_mes": self.processamentos_mes,
            "limite_mes": info["limite_proc"],
            "dispositivos": ips_ativos,
            "max_dispositivos": info["max_ips"],
            "created_at": self.created_at.strftime("%d/%m/%Y"),
            "email_verified": self.email_verified,
            "cupom_indicacao": self.cupom_indicacao or '',
            "meses_gratis": self.meses_gratis or 0,
            "plano_expira": self.plano_expira.strftime("%d/%m/%Y") if self.plano_expira else '',
            "auto_send_whatsapp": bool(self.auto_send_whatsapp),
            "email_remetente": (self.email_remetente or '').strip(),
            "nome_remetente": (self.nome_remetente or '').strip(),
            "smtp_host": (self.smtp_host or '').strip(),
            "smtp_port": int(self.smtp_port or 587),
            "smtp_user": (self.smtp_user or '').strip(),
            "smtp_from": (self.smtp_from or '').strip(),
            "smtp_configurado": bool((self.smtp_host or '').strip() and (self.smtp_user or '').strip() and (self.smtp_pass_enc or '').strip()),
        }


class Session(db.Model):
    __tablename__ = 'sessions'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    token_id = db.Column(db.String(64), unique=True, nullable=False)
    ip = db.Column(db.String(45), nullable=False)
    created_at = db.Column(db.DateTime, default=_agora_brasil)
    last_seen = db.Column(db.DateTime, default=_agora_brasil)


class Payment(db.Model):
    __tablename__ = 'payments'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    status = db.Column(db.String(30), default='pending')
    mercadopago_id = db.Column(db.String(100), default='')
    plano_contratado = db.Column(db.String(20), default='')
    valor = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=_agora_brasil)
    updated_at = db.Column(db.DateTime, default=_agora_brasil, onupdate=_agora_brasil)


# ============================================================
# NOVAS TABELAS - Automacao (UpSeller + WhatsApp + Agendamento)
# ============================================================

class WhatsAppContact(db.Model):
    """Contatos WhatsApp vinculados a lojas para envio automatico de etiquetas."""
    __tablename__ = 'whatsapp_contacts'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    loja_cnpj = db.Column(db.String(20), nullable=False)    # CNPJ da loja
    loja_nome = db.Column(db.String(200), nullable=False)    # Nome legivel da loja
    telefone = db.Column(db.String(20), nullable=False)      # 5511999999999
    nome_contato = db.Column(db.String(200), default='')     # Nome do destinatario
    lojas_json = db.Column(db.Text, default='[]')            # Lista de lojas alvo (nomes)
    grupos_json = db.Column(db.Text, default='[]')           # Lista de grupos alvo (nomes)
    ativo = db.Column(db.Boolean, default=True)
    horario = db.Column(db.String(5), default='')            # LEGADO - manter para compatibilidade
    dias_semana = db.Column(db.String(50), default='')       # LEGADO - manter para compatibilidade
    horarios_json = db.Column(db.Text, default='[]')         # [{"dias":["seg","ter"],"horas":["07:00","11:30"]}, ...]
    resumo_geral = db.Column(db.Boolean, default=False)      # Recebe resumo consolidado de todas as lojas
    lote_ids_json = db.Column(db.Text, default='[]')         # [1, 3] = IDs de TimeLote
    agendamento_ativo = db.Column(db.Boolean, default=True)  # False = agendamento individual desligado
    created_at = db.Column(db.DateTime, default=_agora_brasil)

    user = db.relationship('User', backref=db.backref('whatsapp_contacts', lazy=True))

    def to_dict(self):
        horarios = []
        try:
            horarios = json.loads(self.horarios_json or '[]')
        except Exception:
            horarios = []
        lote_ids = []
        try:
            lote_ids = json.loads(self.lote_ids_json or '[]')
        except Exception:
            lote_ids = []
        return {
            "id": self.id,
            "loja_cnpj": self.loja_cnpj,
            "loja_nome": self.loja_nome,
            "telefone": self.telefone,
            "nome_contato": self.nome_contato,
            "lojas": _json_list(self.lojas_json),
            "grupos": _json_list(self.grupos_json),
            "ativo": self.ativo,
            "horarios": horarios,
            "resumo_geral": bool(self.resumo_geral),
            "lote_ids": lote_ids,
            "agendamento_ativo": self.agendamento_ativo if self.agendamento_ativo is not None else True,
            "created_at": self.created_at.strftime("%d/%m/%Y %H:%M") if self.created_at else '',
        }


class Schedule(db.Model):
    """Agendamentos de processamento automatico (UpSeller -> Beka -> WhatsApp)."""
    __tablename__ = 'schedules'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    nome = db.Column(db.String(100), nullable=False)         # "Processamento Matutino"
    hora = db.Column(db.String(5), nullable=False)            # "08:00"
    minuto = db.Column(db.Integer, default=0)                 # Minuto extraido de hora
    dias_semana = db.Column(db.String(50), default='todos')   # "seg,ter,qua,qui,sex" ou "todos"
    ativo = db.Column(db.Boolean, default=True)

    # Acoes do agendamento
    baixar_upseller = db.Column(db.Boolean, default=True)
    processar_etiquetas = db.Column(db.Boolean, default=True)
    enviar_whatsapp = db.Column(db.Boolean, default=True)
    enviar_email = db.Column(db.Boolean, default=False)
    lojas_json = db.Column(db.Text, default='[]')            # Lojas alvo do agendamento
    grupos_json = db.Column(db.Text, default='[]')           # Grupos alvo do agendamento
    modo_pipeline = db.Column(db.String(20), default='completo')  # 'completo' | 'direto'

    # Controle interno
    job_id = db.Column(db.String(100), nullable=True)         # ID do APScheduler
    created_at = db.Column(db.DateTime, default=_agora_brasil)
    ultima_execucao = db.Column(db.DateTime, nullable=True)
    ultimo_status = db.Column(db.String(20), default='')      # "sucesso" | "erro" | "parcial"

    user = db.relationship('User', backref=db.backref('schedules', lazy=True))

    def to_dict(self):
        return {
            "id": self.id,
            "nome": self.nome,
            "hora": self.hora,
            "dias_semana": self.dias_semana,
            "ativo": self.ativo,
            "baixar_upseller": self.baixar_upseller,
            "processar_etiquetas": self.processar_etiquetas,
            "enviar_whatsapp": self.enviar_whatsapp,
            "enviar_email": bool(getattr(self, 'enviar_email', False)),
            "modo_pipeline": getattr(self, 'modo_pipeline', 'completo') or 'completo',
            "lojas": _json_list(self.lojas_json),
            "grupos": _json_list(self.grupos_json),
            "job_id": self.job_id or '',
            "ultima_execucao": self.ultima_execucao.strftime("%d/%m/%Y %H:%M") if self.ultima_execucao else '',
            "ultimo_status": self.ultimo_status or '',
            "created_at": self.created_at.strftime("%d/%m/%Y %H:%M") if self.created_at else '',
        }


class UpSellerConfig(db.Model):
    """Configuracao de acesso ao UpSeller por usuario."""
    __tablename__ = 'upseller_config'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), unique=True, nullable=False)
    email = db.Column(db.String(120), nullable=False)         # Login do UpSeller
    password_encrypted = db.Column(db.Text, nullable=False)   # Senha encriptada (Fernet)
    session_dir = db.Column(db.String(500), default='')       # Pasta do profile Playwright
    headless = db.Column(db.Boolean, default=True)
    ultima_sincronizacao = db.Column(db.DateTime, nullable=True)
    status_conexao = db.Column(db.String(20), default='')     # "ok" | "erro" | "nao_testado"
    created_at = db.Column(db.DateTime, default=_agora_brasil)

    user = db.relationship('User', backref=db.backref('upseller_config', uselist=False, lazy=True))

    def set_password(self, password: str):
        """Encripta e salva a senha do UpSeller."""
        self.password_encrypted = encrypt_value(password)

    def get_password(self) -> str:
        """Retorna a senha decriptada."""
        return decrypt_value(self.password_encrypted)

    def to_dict(self):
        return {
            "id": self.id,
            "email": self.email,
            "headless": self.headless,
            "status_conexao": self.status_conexao or 'nao_testado',
            "ultima_sincronizacao": self.ultima_sincronizacao.strftime("%d/%m/%Y %H:%M") if self.ultima_sincronizacao else '',
            # NUNCA retorna senha
        }


class ExecutionLog(db.Model):
    """Log de execucoes do pipeline automatizado."""
    __tablename__ = 'execution_logs'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    schedule_id = db.Column(db.Integer, db.ForeignKey('schedules.id'), nullable=True)
    tipo = db.Column(db.String(20), nullable=False)           # "manual" | "agendado"
    inicio = db.Column(db.DateTime, default=_agora_brasil)
    fim = db.Column(db.DateTime, nullable=True)
    status = db.Column(db.String(20), default='executando')   # "sucesso" | "erro" | "parcial" | "executando"

    # Contadores
    etiquetas_baixadas = db.Column(db.Integer, default=0)
    xmls_baixados = db.Column(db.Integer, default=0)
    etiquetas_processadas = db.Column(db.Integer, default=0)
    whatsapp_enviados = db.Column(db.Integer, default=0)
    whatsapp_erros = db.Column(db.Integer, default=0)

    # Detalhes completos em JSON
    detalhes = db.Column(db.Text, default='{}')

    user = db.relationship('User', backref=db.backref('execution_logs', lazy=True))
    schedule = db.relationship('Schedule', backref=db.backref('execution_logs', lazy=True))

    def to_dict(self):
        return {
            "id": self.id,
            "schedule_id": self.schedule_id,
            "tipo": self.tipo,
            "inicio": self.inicio.strftime("%d/%m/%Y %H:%M:%S") if self.inicio else '',
            "fim": self.fim.strftime("%d/%m/%Y %H:%M:%S") if self.fim else '',
            "status": self.status,
            "etiquetas_baixadas": self.etiquetas_baixadas,
            "xmls_baixados": self.xmls_baixados,
            "etiquetas_processadas": self.etiquetas_processadas,
            "whatsapp_enviados": self.whatsapp_enviados,
            "whatsapp_erros": self.whatsapp_erros,
            "duracao_s": (self.fim - self.inicio).total_seconds() if self.fim and self.inicio else 0,
        }


class Loja(db.Model):
    """Loja persistente — sobrevive reload/restart. Atualizada a cada sync."""
    __tablename__ = 'lojas'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    nome = db.Column(db.String(200), nullable=False)
    marketplace = db.Column(db.String(50), default='Shopee')
    pedidos_pendentes = db.Column(db.Integer, default=0)
    notas_pendentes = db.Column(db.Integer, default=0)       # "Para Emitir"
    etiquetas_pendentes = db.Column(db.Integer, default=0)   # "Para Imprimir"
    ultima_atualizacao = db.Column(db.DateTime, default=_agora_brasil)
    ativo = db.Column(db.Boolean, default=True)

    __table_args__ = (db.UniqueConstraint('user_id', 'nome', name='uq_loja_user_nome'),)

    def to_dict(self):
        return {
            "id": self.id,
            "nome": self.nome,
            "marketplace": self.marketplace,
            "pedidos": self.pedidos_pendentes,
            "notas_pendentes": self.notas_pendentes or 0,
            "etiquetas_pendentes": self.etiquetas_pendentes or 0,
            "ultima_atualizacao": self.ultima_atualizacao.strftime("%d/%m/%Y %H:%M") if self.ultima_atualizacao else "",
            "ativo": self.ativo,
        }


class EmailContact(db.Model):
    """Contato de email para envio de etiquetas por loja."""
    __tablename__ = 'email_contacts'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    email = db.Column(db.String(200), nullable=False)
    loja_cnpj = db.Column(db.String(20), default='')
    nome_contato = db.Column(db.String(200), default='')
    lojas_json = db.Column(db.Text, default='[]')            # Lista de lojas alvo (nomes)
    grupos_json = db.Column(db.Text, default='[]')           # Lista de grupos alvo (nomes)
    ativo = db.Column(db.Boolean, default=True)
    horario = db.Column(db.String(5), default='')            # LEGADO - manter para compatibilidade
    dias_semana = db.Column(db.String(50), default='')       # LEGADO - manter para compatibilidade
    horarios_json = db.Column(db.Text, default='[]')         # [{"dias":["seg","ter"],"horas":["07:00","11:30"]}, ...]
    lote_ids_json = db.Column(db.Text, default='[]')         # [1, 3] = IDs de TimeLote
    agendamento_ativo = db.Column(db.Boolean, default=True)  # False = agendamento individual desligado

    def to_dict(self):
        horarios = []
        try:
            horarios = json.loads(self.horarios_json or '[]')
        except Exception:
            horarios = []
        lote_ids = []
        try:
            lote_ids = json.loads(self.lote_ids_json or '[]')
        except Exception:
            lote_ids = []
        return {
            "id": self.id,
            "email": self.email,
            "loja_cnpj": self.loja_cnpj,
            "nome_contato": self.nome_contato,
            "lojas": _json_list(self.lojas_json),
            "grupos": _json_list(self.grupos_json),
            "ativo": self.ativo,
            "horarios": horarios,
            "lote_ids": lote_ids,
            "agendamento_ativo": self.agendamento_ativo if self.agendamento_ativo is not None else True,
        }


class TimeLote(db.Model):
    """Lote de envio — agrupa contatos para processamento em batch."""
    __tablename__ = 'time_lotes'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    nome = db.Column(db.String(100), default='')
    hora = db.Column(db.String(5), nullable=False)  # "11:00"
    dias_semana = db.Column(db.Text, default='["seg","ter","qua","qui","sex"]')
    ativo = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=_agora_brasil)

    def to_dict(self):
        dias = []
        try:
            dias = json.loads(self.dias_semana or '[]')
        except Exception:
            dias = []
        return {
            "id": self.id, "nome": self.nome, "hora": self.hora,
            "dias_semana": dias, "ativo": self.ativo,
        }


class WhatsAppQueueItem(db.Model):
    """Fila persistente de envios WhatsApp com retry/backoff."""
    __tablename__ = 'whatsapp_queue'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    batch_id = db.Column(db.String(40), nullable=False, index=True)
    origem = db.Column(db.String(20), default='manual')  # manual | auto | agendado

    loja_nome = db.Column(db.String(200), default='')
    telefone = db.Column(db.String(20), nullable=False)
    pdf_path = db.Column(db.String(1000), nullable=False)
    caption = db.Column(db.Text, default='')

    status = db.Column(db.String(20), default='pending', index=True)
    tentativas = db.Column(db.Integer, default=0)
    max_tentativas = db.Column(db.Integer, default=5)
    next_attempt_at = db.Column(db.DateTime, default=_agora_brasil, index=True)
    last_error = db.Column(db.Text, default='')
    message_id = db.Column(db.String(200), default='')

    created_at = db.Column(db.DateTime, default=_agora_brasil)
    updated_at = db.Column(db.DateTime, default=_agora_brasil, onupdate=_agora_brasil)
    sent_at = db.Column(db.DateTime, nullable=True)

    user = db.relationship('User', backref=db.backref('whatsapp_queue', lazy=True))

    def to_dict(self):
        return {
            "id": self.id,
            "batch_id": self.batch_id,
            "origem": self.origem,
            "loja_nome": self.loja_nome,
            "telefone": self.telefone,
            "pdf_path": self.pdf_path,
            "status": self.status,
            "tentativas": self.tentativas,
            "max_tentativas": self.max_tentativas,
            "next_attempt_at": self.next_attempt_at.strftime("%d/%m/%Y %H:%M:%S") if self.next_attempt_at else "",
            "last_error": self.last_error or "",
            "message_id": self.message_id or "",
            "created_at": self.created_at.strftime("%d/%m/%Y %H:%M:%S") if self.created_at else "",
            "sent_at": self.sent_at.strftime("%d/%m/%Y %H:%M:%S") if self.sent_at else "",
        }


class MarketplaceApiConfig(db.Model):
    """Configuracao de API direta por marketplace (inicio: Shopee)."""
    __tablename__ = 'marketplace_api_config'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    marketplace = db.Column(db.String(40), default='shopee', nullable=False)
    loja_nome = db.Column(db.String(200), default='')
    api_base_url = db.Column(db.String(300), default='https://openplatform.sandbox.test-stable.shopee.sg')

    partner_id = db.Column(db.String(80), default='')
    partner_key_enc = db.Column(db.Text, default='')
    shop_id = db.Column(db.String(80), default='')
    access_token_enc = db.Column(db.Text, default='')
    refresh_token_enc = db.Column(db.Text, default='')
    token_expires_at = db.Column(db.DateTime, nullable=True)

    status_conexao = db.Column(db.String(20), default='nao_configurado')  # ok | erro | nao_configurado
    ultima_sincronizacao = db.Column(db.DateTime, nullable=True)
    ativo = db.Column(db.Boolean, default=False)

    # OAuth pendente: timestamp de quando login-url foi chamado.
    # Usado pelo callback para identificar o usuario quando o sandbox
    # nao retorna o state param. Funciona cross-worker (persiste no DB).
    oauth_pending_at = db.Column(db.DateTime, nullable=True)

    created_at = db.Column(db.DateTime, default=_agora_brasil)
    updated_at = db.Column(db.DateTime, default=_agora_brasil, onupdate=_agora_brasil)

    __table_args__ = (
        db.UniqueConstraint('user_id', 'marketplace', name='uq_marketplace_cfg_user_marketplace'),
    )

    def set_partner_key(self, value: str):
        txt = str(value or '').strip()
        self.partner_key_enc = encrypt_value(txt) if txt else ''

    def get_partner_key(self) -> str:
        txt = str(self.partner_key_enc or '').strip()
        if not txt:
            return ''
        try:
            return decrypt_value(txt)
        except Exception:
            return ''

    def set_access_token(self, value: str):
        txt = str(value or '').strip()
        self.access_token_enc = encrypt_value(txt) if txt else ''

    def get_access_token(self) -> str:
        txt = str(self.access_token_enc or '').strip()
        if not txt:
            return ''
        try:
            return decrypt_value(txt)
        except Exception:
            return ''

    def set_refresh_token(self, value: str):
        txt = str(value or '').strip()
        self.refresh_token_enc = encrypt_value(txt) if txt else ''

    def get_refresh_token(self) -> str:
        txt = str(self.refresh_token_enc or '').strip()
        if not txt:
            return ''
        try:
            return decrypt_value(txt)
        except Exception:
            return ''

    def configurado(self) -> bool:
        return bool(
            (self.partner_id or '').strip() and
            (self.get_partner_key() or '').strip() and
            (self.shop_id or '').strip() and
            (self.get_access_token() or '').strip()
        )

    def to_dict(self):
        return {
            "id": self.id,
            "marketplace": self.marketplace,
            "loja_nome": (self.loja_nome or '').strip(),
            "api_base_url": (self.api_base_url or '').strip() or 'https://openplatform.sandbox.test-stable.shopee.sg',
            "partner_id": (self.partner_id or '').strip(),
            "shop_id": (self.shop_id or '').strip(),
            "status_conexao": (self.status_conexao or 'nao_configurado'),
            "ultima_sincronizacao": self.ultima_sincronizacao.strftime("%d/%m/%Y %H:%M") if self.ultima_sincronizacao else "",
            "ativo": bool(self.ativo),
            "configurado": bool(self.configurado()),
            "token_expires_at": self.token_expires_at.strftime("%d/%m/%Y %H:%M:%S") if self.token_expires_at else "",
            "has_partner_key": bool((self.partner_key_enc or '').strip()),
            "has_access_token": bool((self.access_token_enc or '').strip()),
            "has_refresh_token": bool((self.refresh_token_enc or '').strip()),
        }


class MarketplaceLoja(db.Model):
    """Snapshot de lojas por API direta (separado do snapshot do UpSeller)."""
    __tablename__ = 'marketplace_lojas'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    marketplace = db.Column(db.String(40), default='Shopee', nullable=False)
    nome = db.Column(db.String(200), nullable=False)
    pedidos_pendentes = db.Column(db.Integer, default=0)
    notas_pendentes = db.Column(db.Integer, default=0)
    etiquetas_pendentes = db.Column(db.Integer, default=0)
    ultima_atualizacao = db.Column(db.DateTime, default=_agora_brasil)
    ativo = db.Column(db.Boolean, default=True)

    __table_args__ = (
        db.UniqueConstraint('user_id', 'marketplace', 'nome', name='uq_marketplace_loja_user_marketplace_nome'),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "nome": self.nome,
            "marketplace": self.marketplace,
            "pedidos": int(self.pedidos_pendentes or 0),
            "notas_pendentes": int(self.notas_pendentes or 0),
            "etiquetas_pendentes": int(self.etiquetas_pendentes or 0),
            "ultima_atualizacao": self.ultima_atualizacao.strftime("%d/%m/%Y %H:%M") if self.ultima_atualizacao else "",
            "ativo": bool(self.ativo),
        }


# ----------------------------------------------------------------
# AIOS - AI Agent Operating System Config
# ----------------------------------------------------------------

class AIOSConfig(db.Model):
    __tablename__ = 'aios_config'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), unique=True, nullable=False)
    anthropic_key_enc = db.Column(db.Text, default='')
    openai_key_enc = db.Column(db.Text, default='')
    modelo_principal = db.Column(db.String(100), default='claude-sonnet-4-20250514')
    modelo_fallback = db.Column(db.String(100), default='qwen2.5:7b')
    ollama_url = db.Column(db.String(200), default='http://localhost:11434')
    kernel_porta = db.Column(db.Integer, default=8000)
    ativo = db.Column(db.Boolean, default=True)
    criado_em = db.Column(db.DateTime, default=_agora_brasil)
    atualizado_em = db.Column(db.DateTime, default=_agora_brasil, onupdate=_agora_brasil)

    def set_anthropic_key(self, key):
        if key:
            self.anthropic_key_enc = _fernet.encrypt(key.encode()).decode()
        else:
            self.anthropic_key_enc = ''

    def get_anthropic_key(self):
        if self.anthropic_key_enc:
            try:
                return _fernet.decrypt(self.anthropic_key_enc.encode()).decode()
            except Exception:
                return ''
        return ''

    def set_openai_key(self, key):
        if key:
            self.openai_key_enc = _fernet.encrypt(key.encode()).decode()
        else:
            self.openai_key_enc = ''

    def get_openai_key(self):
        if self.openai_key_enc:
            try:
                return _fernet.decrypt(self.openai_key_enc.encode()).decode()
            except Exception:
                return ''
        return ''

    def to_dict(self):
        return {
            "modelo_principal": self.modelo_principal,
            "modelo_fallback": self.modelo_fallback,
            "ollama_url": self.ollama_url,
            "kernel_porta": self.kernel_porta,
            "ativo": bool(self.ativo),
            "tem_anthropic_key": bool(self.anthropic_key_enc),
            "tem_openai_key": bool(self.openai_key_enc),
        }
