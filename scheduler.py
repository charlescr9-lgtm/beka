# -*- coding: utf-8 -*-
"""
Agendador de tarefas automatizadas - APScheduler
Pipeline: UpSeller Scraper -> Beka MKT Processamento -> WhatsApp Sender
"""

import json
import logging
import asyncio
import threading
import os
from datetime import datetime
from typing import Optional, List, Dict

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

# Mapeamento dias da semana PT-BR -> cron
DIAS_MAP = {
    "seg": "mon", "ter": "tue", "qua": "wed",
    "qui": "thu", "sex": "fri", "sab": "sat", "dom": "sun",
    "todos": "mon,tue,wed,thu,fri,sat,sun",
}


def _parse_json_list(value) -> List[str]:
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


def _parse_dias_semana(dias_str: str) -> str:
    """Converte 'seg,ter,qua' para 'mon,tue,wed' (formato cron)."""
    if not dias_str or dias_str.strip().lower() == "todos":
        return "mon,tue,wed,thu,fri,sat,sun"
    partes = [d.strip().lower() for d in dias_str.split(",")]
    cron_dias = []
    for d in partes:
        if d in DIAS_MAP:
            cron_dias.append(DIAS_MAP[d])
        else:
            cron_dias.append(d)  # Ja pode estar em formato cron
    return ",".join(cron_dias)


def _parse_hora(hora_str: str) -> tuple:
    """Converte '08:30' para (8, 30)."""
    partes = hora_str.strip().split(":")
    return int(partes[0]), int(partes[1]) if len(partes) > 1 else 0


class BekaScheduler:
    """Gerenciador de agendamentos do Beka MKT."""

    def __init__(self, app=None):
        self.app = app
        self.scheduler = BackgroundScheduler(
            timezone="America/Sao_Paulo",
            job_defaults={
                'coalesce': True,       # Se perder execucao, roda 1x ao retornar
                'max_instances': 1,      # Nunca rodar 2 instancias do mesmo job
                'misfire_grace_time': 300,  # 5 min de tolerancia
            }
        )
        self._started = False

    def init_app(self, app):
        """Inicializa com Flask app e carrega agendamentos do banco."""
        self.app = app
        if not self._started:
            self.scheduler.start()
            self._started = True
            logger.info("[Scheduler] APScheduler iniciado com sucesso")
            # Carregar agendamentos existentes do banco
            with app.app_context():
                self._carregar_agendamentos_banco()

    def _carregar_agendamentos_banco(self):
        """Recarrega todos os agendamentos ativos do banco de dados."""
        from models import Schedule, db
        try:
            schedules = Schedule.query.filter_by(ativo=True).all()
            for sched in schedules:
                self._registrar_job(sched)
            logger.info(f"[Scheduler] {len(schedules)} agendamento(s) carregados do banco")
        except Exception as e:
            logger.error(f"[Scheduler] Erro ao carregar agendamentos: {e}")

    def _registrar_job(self, schedule_obj) -> Optional[str]:
        """Registra um job no APScheduler a partir de um Schedule do banco."""
        try:
            hora, minuto = _parse_hora(schedule_obj.hora)
            dias_cron = _parse_dias_semana(schedule_obj.dias_semana)

            job_id = f"beka_schedule_{schedule_obj.id}"

            # Remover job anterior se existir
            existing = self.scheduler.get_job(job_id)
            if existing:
                self.scheduler.remove_job(job_id)

            trigger = CronTrigger(
                day_of_week=dias_cron,
                hour=hora,
                minute=minuto,
                timezone="America/Sao_Paulo",
            )

            self.scheduler.add_job(
                func=self._executar_pipeline,
                trigger=trigger,
                id=job_id,
                args=[schedule_obj.user_id, schedule_obj.id],
                name=f"Pipeline: {schedule_obj.nome}",
                replace_existing=True,
            )

            logger.info(f"[Scheduler] Job registrado: {job_id} ({schedule_obj.nome}) "
                        f"- {schedule_obj.hora} dias={schedule_obj.dias_semana}")
            return job_id

        except Exception as e:
            logger.error(f"[Scheduler] Erro ao registrar job para schedule {schedule_obj.id}: {e}")
            return None

    def adicionar_agendamento(self, user_id: int, config: dict) -> Optional[int]:
        """
        Cria agendamento no banco e registra no APScheduler.

        config = {
            "nome": "Processamento Diario 8h",
            "hora": "08:00",
            "dias_semana": "seg,ter,qua,qui,sex",
            "baixar_upseller": True,
            "processar_etiquetas": True,
            "enviar_whatsapp": True,
        }

        Retorna schedule.id ou None em caso de erro.
        """
        from models import Schedule, db

        try:
            hora, minuto = _parse_hora(config["hora"])

            schedule = Schedule(
                user_id=user_id,
                nome=config.get("nome", "Agendamento"),
                hora=config["hora"],
                minuto=minuto,
                dias_semana=config.get("dias_semana", "todos"),
                ativo=True,
                baixar_upseller=config.get("baixar_upseller", True),
                processar_etiquetas=config.get("processar_etiquetas", True),
                enviar_whatsapp=config.get("enviar_whatsapp", True),
                lojas_json=json.dumps(config.get("lojas", []) or [], ensure_ascii=False),
                grupos_json=json.dumps(config.get("grupos", []) or [], ensure_ascii=False),
            )
            db.session.add(schedule)
            db.session.commit()

            # Registrar no APScheduler
            job_id = self._registrar_job(schedule)
            if job_id:
                schedule.job_id = job_id
                db.session.commit()

            logger.info(f"[Scheduler] Agendamento criado: id={schedule.id} user={user_id}")
            return schedule.id

        except Exception as e:
            db.session.rollback()
            logger.error(f"[Scheduler] Erro ao criar agendamento: {e}")
            return None

    def atualizar_agendamento(self, schedule_id: int, config: dict) -> bool:
        """Atualiza agendamento existente."""
        from models import Schedule, db

        try:
            schedule = Schedule.query.get(schedule_id)
            if not schedule:
                return False

            if "nome" in config:
                schedule.nome = config["nome"]
            if "hora" in config:
                schedule.hora = config["hora"]
                _, minuto = _parse_hora(config["hora"])
                schedule.minuto = minuto
            if "dias_semana" in config:
                schedule.dias_semana = config["dias_semana"]
            if "baixar_upseller" in config:
                schedule.baixar_upseller = config["baixar_upseller"]
            if "processar_etiquetas" in config:
                schedule.processar_etiquetas = config["processar_etiquetas"]
            if "enviar_whatsapp" in config:
                schedule.enviar_whatsapp = config["enviar_whatsapp"]
            if "lojas" in config:
                schedule.lojas_json = json.dumps(config.get("lojas", []) or [], ensure_ascii=False)
            if "grupos" in config:
                schedule.grupos_json = json.dumps(config.get("grupos", []) or [], ensure_ascii=False)

            db.session.commit()

            # Re-registrar no APScheduler
            if schedule.ativo:
                self._registrar_job(schedule)

            return True
        except Exception as e:
            db.session.rollback()
            logger.error(f"[Scheduler] Erro ao atualizar agendamento {schedule_id}: {e}")
            return False

    def remover_agendamento(self, schedule_id: int) -> bool:
        """Remove agendamento do banco e do APScheduler."""
        from models import Schedule, db

        try:
            schedule = Schedule.query.get(schedule_id)
            if not schedule:
                return False

            job_id = f"beka_schedule_{schedule_id}"
            if self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)

            db.session.delete(schedule)
            db.session.commit()
            logger.info(f"[Scheduler] Agendamento removido: id={schedule_id}")
            return True
        except Exception as e:
            db.session.rollback()
            logger.error(f"[Scheduler] Erro ao remover agendamento {schedule_id}: {e}")
            return False

    def pausar_agendamento(self, schedule_id: int) -> bool:
        """Pausa um agendamento (remove job do scheduler, mantém no banco)."""
        from models import Schedule, db

        try:
            schedule = Schedule.query.get(schedule_id)
            if not schedule:
                return False

            schedule.ativo = False
            db.session.commit()

            job_id = f"beka_schedule_{schedule_id}"
            if self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)

            logger.info(f"[Scheduler] Agendamento pausado: id={schedule_id}")
            return True
        except Exception as e:
            db.session.rollback()
            return False

    def retomar_agendamento(self, schedule_id: int) -> bool:
        """Retoma um agendamento pausado."""
        from models import Schedule, db

        try:
            schedule = Schedule.query.get(schedule_id)
            if not schedule:
                return False

            schedule.ativo = True
            db.session.commit()

            self._registrar_job(schedule)
            logger.info(f"[Scheduler] Agendamento retomado: id={schedule_id}")
            return True
        except Exception as e:
            db.session.rollback()
            return False

    def listar_agendamentos(self, user_id: int) -> List[dict]:
        """Lista todos os agendamentos de um usuario."""
        from models import Schedule
        schedules = Schedule.query.filter_by(user_id=user_id).order_by(Schedule.created_at.desc()).all()
        result = []
        for s in schedules:
            d = s.to_dict()
            # Adicionar proxima execucao estimada
            job = self.scheduler.get_job(f"beka_schedule_{s.id}")
            if job and job.next_run_time:
                d["proxima_execucao"] = job.next_run_time.strftime("%d/%m/%Y %H:%M")
            else:
                d["proxima_execucao"] = ""
            result.append(d)
        return result

    def executar_agora(self, user_id: int, schedule_id: Optional[int] = None):
        """Dispara execucao manual imediata do pipeline em thread separada."""
        thread = threading.Thread(
            target=self._executar_pipeline,
            args=(user_id, schedule_id),
            daemon=True,
        )
        thread.start()
        logger.info(f"[Scheduler] Execucao manual disparada: user={user_id} schedule={schedule_id}")

    def _executar_pipeline(self, user_id: int, schedule_id: Optional[int] = None):
        """
        Pipeline completo de automacao:
        1. Baixar etiquetas/XMLs do UpSeller (se configurado)
        2. Processar via Beka MKT (pipeline existente)
        3. Enviar PDFs via WhatsApp (se configurado)
        """
        from models import Schedule, ExecutionLog, UpSellerConfig, WhatsAppContact, db

        with self.app.app_context():
            # Criar log de execucao
            log_exec = ExecutionLog(
                user_id=user_id,
                schedule_id=schedule_id,
                tipo="agendado" if schedule_id else "manual",
                inicio=datetime.utcnow(),
                status="executando",
            )
            db.session.add(log_exec)
            db.session.commit()

            # Buscar configuracoes do agendamento
            schedule = Schedule.query.get(schedule_id) if schedule_id else None
            fazer_upseller = schedule.baixar_upseller if schedule else True
            fazer_processamento = schedule.processar_etiquetas if schedule else True
            fazer_whatsapp = schedule.enviar_whatsapp if schedule else True
            lojas_alvo = []

            try:
                if schedule:
                    lojas_cfg = _parse_json_list(getattr(schedule, "lojas_json", "[]"))
                    grupos_cfg = _parse_json_list(getattr(schedule, "grupos_json", "[]"))
                    lojas_alvo.extend(lojas_cfg)
                    if grupos_cfg:
                        from dashboard import _get_estado
                        estado_tmp = _get_estado(user_id) or {}
                        agrup = estado_tmp.get("agrupamentos", []) or []
                        grupos_norm = {str(x).strip().lower() for x in grupos_cfg if str(x).strip()}
                        for g in agrup:
                            nome_g = str((g or {}).get("nome", "") or "").strip().lower()
                            if nome_g and nome_g in grupos_norm:
                                for ln in (g or {}).get("nomes_lojas", []) or []:
                                    ln = str(ln or "").strip()
                                    if ln:
                                        lojas_alvo.append(ln)
                # dedupe preservando ordem
                lojas_alvo = list(dict.fromkeys([x for x in lojas_alvo if str(x).strip()]))
            except Exception as e_tgt:
                logger.warning(f"[Scheduler] Falha ao carregar lojas/grupos alvo: {e_tgt}")
                lojas_alvo = []

            detalhes = {"etapas": []}
            status_final = "sucesso"
            pasta_lote_pipeline = None

            try:
                # ===== ETAPA 1: UpSeller Scraper =====
                if fazer_upseller:
                    try:
                        detalhes["etapas"].append({"etapa": "upseller", "status": "iniciando"})
                        upseller_config = UpSellerConfig.query.filter_by(user_id=user_id).first()
                        if upseller_config:
                            pasta_entrada_user = self._get_pasta_entrada(user_id)
                            lotes_base = os.path.join(pasta_entrada_user, "_upseller_lotes")
                            os.makedirs(lotes_base, exist_ok=True)
                            pasta_lote_pipeline = os.path.join(
                                lotes_base,
                                f"scheduler_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
                            )
                            os.makedirs(pasta_lote_pipeline, exist_ok=True)

                            from upseller_scraper import UpSellerScraper
                            scraper = UpSellerScraper({
                                "email": upseller_config.email,
                                "password": upseller_config.get_password(),
                                "profile_dir": upseller_config.session_dir,
                                "headless": upseller_config.headless,
                                "download_dir": pasta_lote_pipeline,
                            })

                            # Rodar async no thread
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            try:
                                filtro = lojas_alvo if lojas_alvo else None
                                pdfs = loop.run_until_complete(scraper.baixar_etiquetas(filtro_loja=filtro))
                                xmls = []
                                loop.run_until_complete(scraper.fechar())
                            finally:
                                loop.close()

                            log_exec.etiquetas_baixadas = len(pdfs) if pdfs else 0
                            log_exec.xmls_baixados = len(xmls) if xmls else 0
                            detalhes["etapas"][-1]["status"] = "concluido"
                            detalhes["etapas"][-1]["pdfs"] = len(pdfs) if pdfs else 0
                            detalhes["etapas"][-1]["xmls"] = 0
                            detalhes["etapas"][-1]["lote"] = pasta_lote_pipeline
                            if lojas_alvo:
                                detalhes["etapas"][-1]["lojas_alvo"] = lojas_alvo
                        else:
                            detalhes["etapas"][-1]["status"] = "pulado"
                            detalhes["etapas"][-1]["motivo"] = "UpSeller nao configurado"
                    except Exception as e:
                        logger.error(f"[Pipeline] Erro UpSeller: {e}")
                        detalhes["etapas"][-1]["status"] = "erro"
                        detalhes["etapas"][-1]["erro"] = str(e)
                        status_final = "parcial"

                # ===== ETAPA 2: Processamento Beka MKT =====
                if fazer_processamento:
                    try:
                        detalhes["etapas"].append({"etapa": "processamento", "status": "iniciando"})
                        # Importar e executar o processamento existente
                        from dashboard import _executar_processamento, _get_estado
                        estado = _get_estado(user_id)
                        if not estado.get("processando"):
                            _executar_processamento(
                                user_id,
                                sem_recorte=bool(fazer_upseller),
                                resumo_sku_somente=bool(fazer_upseller),
                                pasta_entrada_override=pasta_lote_pipeline if fazer_upseller else None,
                            )
                            # Aguardar conclusao (poll a cada 2s, max 10min)
                            import time
                            timeout = 600
                            elapsed = 0
                            while estado.get("processando") and elapsed < timeout:
                                time.sleep(2)
                                elapsed += 2

                            resultado = estado.get("ultimo_resultado", {})
                            log_exec.etiquetas_processadas = resultado.get("total_etiquetas", 0)
                            detalhes["etapas"][-1]["status"] = "concluido"
                            detalhes["etapas"][-1]["etiquetas"] = resultado.get("total_etiquetas", 0)
                            detalhes["etapas"][-1]["lojas"] = resultado.get("total_lojas", 0)
                        else:
                            detalhes["etapas"][-1]["status"] = "pulado"
                            detalhes["etapas"][-1]["motivo"] = "Processamento ja em andamento"
                    except Exception as e:
                        logger.error(f"[Pipeline] Erro processamento: {e}")
                        detalhes["etapas"][-1]["status"] = "erro"
                        detalhes["etapas"][-1]["erro"] = str(e)
                        status_final = "parcial"

                # ===== ETAPA 3: WhatsApp =====
                if fazer_whatsapp:
                    try:
                        detalhes["etapas"].append({"etapa": "whatsapp", "status": "iniciando"})
                        from dashboard import _enfileirar_envio_whatsapp_resultado, _get_estado
                        estado = _get_estado(user_id)
                        resultado = estado.get("ultimo_resultado", {}) if estado else {}
                        if lojas_alvo and resultado and resultado.get("lojas"):
                            alvo_norm = {str(x).strip().lower() for x in lojas_alvo}
                            lojas_filtradas = [
                                l for l in (resultado.get("lojas", []) or [])
                                if str((l or {}).get("nome", "") or "").strip().lower() in alvo_norm
                            ]
                            resultado = {**resultado, "lojas": lojas_filtradas}
                        enq = _enfileirar_envio_whatsapp_resultado(
                            user_id=user_id,
                            resultado=resultado,
                            origem="agendado",
                            respeitar_toggle_auto=False,
                        )
                        if not enq.get("ok"):
                            detalhes["etapas"][-1]["status"] = "pulado"
                            detalhes["etapas"][-1]["motivo"] = enq.get("erro", "Nenhum envio enfileirado")
                            detalhes["etapas"][-1]["diagnostico"] = enq.get("diagnostico", {})
                            if not enq.get("ignorado") and status_final == "sucesso":
                                status_final = "parcial"
                        else:
                            qtd = int(enq.get("total_entregas", 0) or 0)
                            log_exec.whatsapp_enviados = qtd  # quantidade enfileirada
                            log_exec.whatsapp_erros = 0
                            detalhes["etapas"][-1]["status"] = "concluido"
                            detalhes["etapas"][-1]["enfileirados"] = qtd
                            detalhes["etapas"][-1]["batch_id"] = enq.get("batch_id", "")
                            detalhes["etapas"][-1]["diagnostico"] = enq.get("diagnostico", {})

                    except Exception as e:
                        logger.error(f"[Pipeline] Erro WhatsApp: {e}")
                        detalhes["etapas"][-1]["status"] = "erro"
                        detalhes["etapas"][-1]["erro"] = str(e)
                        status_final = "parcial"

            except Exception as e:
                logger.error(f"[Pipeline] Erro geral: {e}")
                status_final = "erro"
                detalhes["erro_geral"] = str(e)

            # Finalizar log
            log_exec.fim = datetime.utcnow()
            log_exec.status = status_final
            log_exec.detalhes = json.dumps(detalhes, ensure_ascii=False)
            db.session.commit()

            # Atualizar schedule
            if schedule:
                schedule.ultima_execucao = datetime.utcnow()
                schedule.ultimo_status = status_final
                db.session.commit()

            logger.info(f"[Pipeline] Concluido: user={user_id} status={status_final}")

    def _get_pasta_entrada(self, user_id: int) -> str:
        """Retorna pasta de entrada do usuario."""
        from models import User
        user = User.query.get(user_id)
        return user.get_pasta_entrada() if user else ""

    def _get_pasta_saida(self, user_id: int) -> str:
        """Retorna pasta de saida do usuario."""
        from models import User
        user = User.query.get(user_id)
        return user.get_pasta_saida() if user else ""

    def get_historico(self, user_id: int, limite: int = 20) -> List[dict]:
        """Retorna historico de execucoes."""
        from models import ExecutionLog
        logs = ExecutionLog.query.filter_by(user_id=user_id)\
            .order_by(ExecutionLog.inicio.desc())\
            .limit(limite)\
            .all()
        return [l.to_dict() for l in logs]

    def shutdown(self):
        """Para o scheduler graciosamente."""
        if self._started:
            self.scheduler.shutdown(wait=False)
            self._started = False
            logger.info("[Scheduler] APScheduler encerrado")


# Instancia global (singleton)
beka_scheduler = BekaScheduler()
