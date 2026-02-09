# -*- coding: utf-8 -*-
"""
Modelos do banco de dados - SQLAlchemy + SQLite
"""

import os
import uuid
from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt

db = SQLAlchemy()
bcrypt = Bcrypt()

# Planos disponiveis
PLANOS = {
    "free":         {"nome": "Free",         "max_ips": 1, "limite_proc": 5,  "valor": 0},
    "basico":       {"nome": "Basico",       "max_ips": 1, "limite_proc": -1, "valor": 39.90},
    "pro":          {"nome": "Pro",          "max_ips": 2, "limite_proc": -1, "valor": 59.90},
    "empresarial":  {"nome": "Empresarial",  "max_ips": 5, "limite_proc": -1, "valor": 89.90},
}


class User(db.Model):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    plano = db.Column(db.String(20), default='free')
    processamentos_mes = db.Column(db.Integer, default=0)
    mes_atual = db.Column(db.String(7), default='')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)

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
        mes = datetime.utcnow().strftime('%Y-%m')
        if self.mes_atual != mes:
            self.mes_atual = mes
            self.processamentos_mes = 0
            db.session.commit()
        return self.processamentos_mes < info["limite_proc"]

    def registrar_processamento(self):
        mes = datetime.utcnow().strftime('%Y-%m')
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
            sessao_existente.last_seen = datetime.utcnow()
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
        }


class Session(db.Model):
    __tablename__ = 'sessions'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    token_id = db.Column(db.String(64), unique=True, nullable=False)
    ip = db.Column(db.String(45), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen = db.Column(db.DateTime, default=datetime.utcnow)


class Payment(db.Model):
    __tablename__ = 'payments'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    status = db.Column(db.String(30), default='pending')
    mercadopago_id = db.Column(db.String(100), default='')
    plano_contratado = db.Column(db.String(20), default='')
    valor = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
