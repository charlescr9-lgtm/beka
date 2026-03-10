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
import sys
import subprocess as _sp
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict

# Fuso horario de Brasilia (UTC-3)
_FUSO_BRASILIA = timezone(timedelta(hours=-3))


def _agora_brasil():
    """Retorna datetime atual no fuso de Brasilia (UTC-3), sem tzinfo (naive)."""
    return datetime.now(_FUSO_BRASILIA).replace(tzinfo=None)

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
                'misfire_grace_time': 7200,  # 2h de tolerancia — garante execucao apos reinicio
            }
        )
        self._started = False
        # Locks per-user para serializar execucoes (evita conflito no browser/temp dir)
        self._exec_locks = {}                  # user_id -> threading.Lock
        self._exec_locks_guard = threading.Lock()

    def _get_exec_lock(self, user_id):
        """Retorna Lock exclusivo para o user_id (cria se nao existir)."""
        with self._exec_locks_guard:
            if user_id not in self._exec_locks:
                self._exec_locks[user_id] = threading.Lock()
            return self._exec_locks[user_id]

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
                # Recuperar jobs que deveriam ter rodado durante o downtime
                self._recuperar_jobs_perdidos()
            # Registrar auto-wake no Agendador de Tarefas do Windows
            self._configurar_despertar_windows()

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

        # Carregar jobs individuais de contatos (WhatsApp/Email com horario proprio)
        self._carregar_jobs_contatos()

    # ------------------------------------------------------------------
    # Auditoria periodica — recupera jobs perdidos (restart, erro, lock)
    # ------------------------------------------------------------------

    _DIAS_IDX_TO_PT = {0: "seg", 1: "ter", 2: "qua", 3: "qui", 4: "sex", 5: "sab", 6: "dom"}

    def _recuperar_jobs_perdidos(self):
        """Chamada no startup — audita janela de 30min e registra job periodico."""
        self._auditar_jobs(janela_min=30, origem="startup")
        # Registrar job periodico: roda a cada 20 minutos (nos minutos :05, :25, :45)
        # Assim pega logo apos clusters comuns (:00, :08, :15, :20, :30, :35, :40, :45, :50)
        try:
            self.scheduler.add_job(
                func=self._job_auditoria_periodica,
                trigger=CronTrigger(minute="5,25,45", timezone="America/Sao_Paulo"),
                id="__auditoria_periodica__",
                name="Auditoria periodica de jobs perdidos",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
            logger.info("[Scheduler] Job de auditoria periodica registrado (:05, :25, :45)")
        except Exception as e:
            logger.error(f"[Scheduler] Erro ao registrar auditoria periodica: {e}")

    def _job_auditoria_periodica(self):
        """Executada a cada 20min pelo APScheduler — verifica ultimos 25min."""
        with self.app.app_context():
            self._auditar_jobs(janela_min=25, origem="periodica")

    def _auditar_jobs(self, janela_min: int = 25, origem: str = ""):
        """Verifica se algum job de contato deveria ter executado nos ultimos
        `janela_min` minutos mas nao executou. Se encontrar, dispara em thread
        (respeitando exec_lock para fila sequencial).

        Roda tanto no startup quanto periodicamente a cada 20 min.
        """
        from models import WhatsAppContact, EmailContact, ExecutionLog, db

        agora = _agora_brasil()
        janela = timedelta(minutes=janela_min)
        dia_pt = self._DIAS_IDX_TO_PT.get(agora.weekday(), "")

        contatos_perdidos = []  # (contato, tipo_contato, hora_agendada)

        for tipo, Model in [("whatsapp", WhatsAppContact), ("email", EmailContact)]:
            try:
                ativos = Model.query.filter_by(ativo=True).all()
            except Exception:
                continue
            for contato in ativos:
                try:
                    horarios = json.loads(getattr(contato, "horarios_json", "[]") or "[]")
                except Exception:
                    continue
                for slot in horarios:
                    dias = slot.get("dias", [])
                    if dia_pt not in dias:
                        continue
                    for hora_str in slot.get("horas", []):
                        try:
                            h, m = _parse_hora(hora_str)
                        except Exception:
                            continue
                        agendado = agora.replace(hour=h, minute=m, second=0, microsecond=0)
                        # So interessa se esta no passado E dentro da janela
                        if agendado >= agora or (agora - agendado) > janela:
                            continue
                        # Verificar se ja executou (ExecLog com este contato_id proximo do horario)
                        margem = timedelta(minutes=10)
                        exec_existente = ExecutionLog.query.filter(
                            ExecutionLog.user_id == contato.user_id,
                            ExecutionLog.tipo == "contato_individual",
                            ExecutionLog.inicio >= agendado - margem,
                            ExecutionLog.inicio <= agora,
                        ).all()
                        # Checar se algum log e deste contato_id
                        ja_executou = False
                        for ex in exec_existente:
                            try:
                                det = json.loads(ex.detalhes or "{}")
                                if det.get("contato_id") == contato.id:
                                    ja_executou = True
                                    break
                            except Exception:
                                pass
                        if not ja_executou:
                            contatos_perdidos.append((contato, tipo, hora_str))

        if not contatos_perdidos:
            logger.info(f"[Auditoria:{origem}] Nenhum job perdido (janela {janela_min}min)")
            return

        logger.warning(
            f"[Auditoria:{origem}] {len(contatos_perdidos)} job(s) perdido(s) — recuperando"
        )
        for contato, tipo, hora_str in contatos_perdidos:
            nome = getattr(contato, "nome_contato", "") or f"#{contato.id}"
            logger.warning(
                f"[Auditoria:{origem}] Recuperando: {nome} ({tipo}) horario={hora_str}"
            )
            t = threading.Thread(
                target=self._executar_contato_individual,
                args=[contato.user_id, contato.id, tipo],
                daemon=True,
            )
            t.start()

    # ------------------------------------------------------------------
    # Auto-Wake: Agendador de Tarefas do Windows
    # ------------------------------------------------------------------

    def _configurar_despertar_windows(self):
        """Registra tarefas no Agendador de Tarefas do Windows para iniciar
        o Beka MKT automaticamente:
        - No logon do usuario (sempre disponivel)
        - 5 minutos antes de cada horario agendado (despertar sob demanda)

        Usa o script 'iniciar_beka.bat' que verifica se ja esta rodando
        antes de iniciar uma nova instancia.
        """
        if sys.platform != 'win32':
            logger.info("[AutoWake] Sistema nao-Windows, ignorando registro de tarefas")
            return

        projeto_dir = os.path.dirname(os.path.abspath(__file__))
        bat_path = os.path.join(projeto_dir, 'iniciar_beka.bat')

        if not os.path.exists(bat_path):
            logger.warning(f"[AutoWake] iniciar_beka.bat nao encontrado em {projeto_dir}")
            return

        try:
            self._limpar_tasks_antigas()
            self._registrar_task_logon(bat_path)
            self._registrar_tasks_horarios(bat_path)
            logger.info("[AutoWake] Configuracao de despertar automatico concluida")
        except Exception as e:
            logger.error(f"[AutoWake] Erro geral ao configurar auto-wake: {e}")

    def _limpar_tasks_antigas(self):
        """Remove todas as tarefas BekaAutoWake_* anteriores do Agendador."""
        try:
            result = _sp.run(
                ['schtasks', '/query', '/fo', 'csv', '/nh'],
                capture_output=True, text=True, timeout=15,
                creationflags=0x08000000  # CREATE_NO_WINDOW
            )
            for line in result.stdout.split('\n'):
                if 'BekaAutoWake' in line:
                    # CSV: "\\nome_task","..."
                    parts = line.split(',')
                    if parts:
                        task_name = parts[0].strip('"').lstrip('\\')
                        _sp.run(
                            ['schtasks', '/delete', '/tn', task_name, '/f'],
                            capture_output=True, timeout=10,
                            creationflags=0x08000000
                        )
                        logger.info(f"[AutoWake] Task antiga removida: {task_name}")
        except Exception as e:
            logger.warning(f"[AutoWake] Erro ao limpar tasks antigas: {e}")

    def _registrar_task_logon(self, bat_path: str):
        """Registra atalho na pasta Startup do Windows para iniciar Beka MKT
        automaticamente no logon do usuario (nao requer admin)."""
        try:
            startup_dir = os.path.join(
                os.environ.get('APPDATA', ''),
                'Microsoft', 'Windows', 'Start Menu', 'Programs', 'Startup'
            )
            if not os.path.isdir(startup_dir):
                logger.warning(f"[AutoWake] Pasta Startup nao encontrada: {startup_dir}")
                return

            shortcut_path = os.path.join(startup_dir, 'BekaAutoWake.bat')

            # Criar um .bat simples que chama o iniciar_beka.bat
            with open(shortcut_path, 'w', encoding='utf-8') as f:
                f.write(f'@echo off\r\ncall "{bat_path}"\r\n')

            logger.info(f"[AutoWake] Auto-start no logon registrado via pasta Startup")
        except Exception as e:
            logger.warning(f"[AutoWake] Falha ao registrar auto-start no logon: {e}")

    def _registrar_tasks_horarios(self, bat_path: str):
        """Registra tasks diarias 5 minutos antes de cada horario agendado."""
        # Coletar todos os horarios unicos dos jobs APScheduler
        horarios_unicos = set()
        for job in self.scheduler.get_jobs():
            trigger = job.trigger
            if not hasattr(trigger, 'fields'):
                continue
            try:
                # CronTrigger.fields: [year, month, day, week, day_of_week, hour, minute, second]
                field_names = [f.name for f in trigger.fields]
                hour_idx = field_names.index('hour')
                minute_idx = field_names.index('minute')
                hour_field = trigger.fields[hour_idx]
                minute_field = trigger.fields[minute_idx]

                # Cada campo pode ter multiplas expressoes (ex: "8,14" = horas 8 e 14)
                for h_expr in hour_field.expressions:
                    for m_expr in minute_field.expressions:
                        h = int(str(h_expr))
                        m = int(str(m_expr))
                        horarios_unicos.add((h, m))
            except Exception:
                continue

        if not horarios_unicos:
            logger.info("[AutoWake] Nenhum horario agendado encontrado para despertar")
            return

        # Para cada horario, registrar task 5 minutos antes
        registrados = 0
        for hora, minuto in sorted(horarios_unicos):
            wake_dt = datetime(2000, 1, 1, hora, minuto) - timedelta(minutes=5)
            wake_h = wake_dt.hour
            wake_m = wake_dt.minute
            time_str = f'{wake_h:02d}:{wake_m:02d}'
            task_name = f'BekaAutoWake_{wake_h:02d}{wake_m:02d}'

            try:
                result = _sp.run([
                    'schtasks', '/create',
                    '/tn', task_name,
                    '/tr', f'"{bat_path}"',
                    '/sc', 'daily',
                    '/st', time_str,
                    '/rl', 'limited',
                    '/f',
                ], capture_output=True, text=True, timeout=15,
                    creationflags=0x08000000)

                if result.returncode == 0:
                    logger.info(f"[AutoWake] Task '{task_name}' registrada: "
                                f"despertar diario as {time_str} "
                                f"(5 min antes de {hora:02d}:{minuto:02d})")
                    registrados += 1
                else:
                    logger.warning(f"[AutoWake] schtasks {task_name} retornou "
                                   f"{result.returncode}: {result.stderr.strip()}")
            except Exception as e:
                logger.warning(f"[AutoWake] Falha ao registrar task {task_name}: {e}")

        logger.info(f"[AutoWake] {registrados} task(s) de despertar registradas "
                     f"para {len(horarios_unicos)} horario(s) unico(s)")

    # ------------------------------------------------------------------

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
                enviar_email=config.get("enviar_email", False),
                modo_pipeline=config.get("modo_pipeline", "completo"),
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
            if "enviar_email" in config:
                schedule.enviar_email = config["enviar_email"]
            if "modo_pipeline" in config:
                schedule.modo_pipeline = config["modo_pipeline"]
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
                inicio=_agora_brasil(),
                status="executando",
            )
            db.session.add(log_exec)
            db.session.commit()

            # Serializar execucoes do mesmo usuario (evita conflito no browser/temp)
            exec_lock = self._get_exec_lock(user_id)
            logger.info(f"[Pipeline] schedule #{schedule_id}: aguardando fila (user {user_id})...")
            acquired = exec_lock.acquire(timeout=900)
            if not acquired:
                logger.error(f"[Pipeline] TIMEOUT na fila para user {user_id}, schedule #{schedule_id}")
                log_exec.status = "erro"
                log_exec.fim = _agora_brasil()
                log_exec.detalhes = json.dumps(
                    {"erro": "Timeout aguardando fila de execucao (outra execucao demorou mais de 15 min)"},
                    ensure_ascii=False,
                )
                db.session.commit()
                return
            logger.info(f"[Pipeline] schedule #{schedule_id}: fila liberada, executando (user {user_id})")

            # Buscar configuracoes do agendamento
            schedule = Schedule.query.get(schedule_id) if schedule_id else None
            modo_pipeline = getattr(schedule, 'modo_pipeline', 'completo') or 'completo' if schedule else 'completo'
            fazer_upseller = schedule.baixar_upseller if schedule else True
            fazer_processamento = schedule.processar_etiquetas if schedule else True
            fazer_whatsapp = schedule.enviar_whatsapp if schedule else True
            fazer_email = getattr(schedule, 'enviar_email', False) if schedule else False
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

            detalhes = {"etapas": [], "modo_pipeline": modo_pipeline}
            status_final = "sucesso"
            pasta_lote_pipeline = None

            try:
                # ===== MODO DIRETO: Imprimir + Processar + Enviar =====
                if modo_pipeline == 'direto':
                    try:
                        detalhes["etapas"].append({"etapa": "imprimir_direto", "status": "iniciando"})
                        from dashboard import _executar_imprimir_direto
                        resultado_direto = _executar_imprimir_direto(
                            user_id=user_id,
                            lojas_alvo=lojas_alvo,
                        )
                        if resultado_direto.get("ok"):
                            log_exec.etiquetas_baixadas = resultado_direto.get("pdfs_movidos", 0)
                            log_exec.etiquetas_processadas = resultado_direto.get("total_etiquetas", 0)
                            detalhes["etapas"][-1]["status"] = "concluido"
                            detalhes["etapas"][-1]["etiquetas"] = resultado_direto.get("total_etiquetas", 0)
                            detalhes["etapas"][-1]["lojas"] = resultado_direto.get("total_lojas", 0)
                            # Pular upseller e processamento (ja feito no imprimir_direto)
                            fazer_upseller = False
                            fazer_processamento = False
                        else:
                            raise RuntimeError(resultado_direto.get("erro", "Falha no imprimir direto"))
                    except Exception as e:
                        logger.error(f"[Pipeline] Erro imprimir_direto: {e}")
                        detalhes["etapas"][-1]["status"] = "erro"
                        detalhes["etapas"][-1]["erro"] = str(e)
                        status_final = "erro"
                        # Pular tudo se o modo direto falhou
                        fazer_upseller = False
                        fazer_processamento = False

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

                # ===== ETAPA 4: Email =====
                if fazer_email:
                    try:
                        detalhes["etapas"].append({"etapa": "email", "status": "iniciando"})
                        from dashboard import _enviar_email_resultado_agendado, _get_estado
                        estado = _get_estado(user_id)
                        resultado = estado.get("ultimo_resultado", {}) if estado else {}
                        if lojas_alvo and resultado and resultado.get("lojas"):
                            alvo_norm = {str(x).strip().lower() for x in lojas_alvo}
                            lojas_filtradas = [
                                l for l in (resultado.get("lojas", []) or [])
                                if str((l or {}).get("nome", "") or "").strip().lower() in alvo_norm
                            ]
                            resultado = {**resultado, "lojas": lojas_filtradas}
                        email_res = _enviar_email_resultado_agendado(
                            user_id=user_id,
                            resultado=resultado,
                        )
                        if email_res.get("ok"):
                            detalhes["etapas"][-1]["status"] = "concluido"
                            detalhes["etapas"][-1]["enviados"] = email_res.get("total", 0)
                        else:
                            detalhes["etapas"][-1]["status"] = "pulado"
                            detalhes["etapas"][-1]["motivo"] = email_res.get("erro", "Nenhum envio")
                    except Exception as e:
                        logger.error(f"[Pipeline] Erro email: {e}")
                        detalhes["etapas"][-1]["status"] = "erro"
                        detalhes["etapas"][-1]["erro"] = str(e)
                        status_final = "parcial"

            except Exception as e:
                logger.error(f"[Pipeline] Erro geral: {e}")
                status_final = "erro"
                detalhes["erro_geral"] = str(e)

            finally:
                # Liberar fila para proxima execucao do mesmo usuario
                exec_lock.release()

            # Finalizar log
            log_exec.fim = _agora_brasil()
            log_exec.status = status_final
            log_exec.detalhes = json.dumps(detalhes, ensure_ascii=False)
            db.session.commit()

            # Atualizar schedule
            if schedule:
                schedule.ultima_execucao = _agora_brasil()
                schedule.ultimo_status = status_final
                db.session.commit()

            logger.info(f"[Pipeline] Concluido: user={user_id} status={status_final}")

    # ------------------------------------------------------------------
    # Jobs individuais por contato (WhatsApp / Email com horario proprio)
    # ------------------------------------------------------------------

    def registrar_job_contato(self, contato, tipo_contato: str):
        """Cria/atualiza jobs APScheduler para um contato individual.
        Suporta multiplos horarios via horarios_json:
        [{"dias":["seg","ter"],"horas":["07:00","11:30"]}, {"dias":["qua"],"horas":["14:00"]}]
        Cada combinacao dias+hora gera um job separado.

        Args:
            contato: instancia de WhatsAppContact ou EmailContact
            tipo_contato: 'whatsapp' ou 'email'
        """
        prefixo = f"beka_contato_{tipo_contato}_{contato.id}"
        ativo = getattr(contato, 'ativo', False)

        # Remover todos os jobs existentes deste contato
        self._remover_jobs_contato_prefixo(prefixo)

        if not ativo:
            logger.info(f"[Scheduler] Jobs contato removidos (inativo): {prefixo}")
            return

        # Ler horarios_json
        horarios_raw = getattr(contato, 'horarios_json', '[]') or '[]'
        try:
            horarios = json.loads(horarios_raw)
        except Exception:
            horarios = []

        if not horarios or not isinstance(horarios, list):
            logger.info(f"[Scheduler] Sem horarios para contato {prefixo}")
            return

        nome_contato = getattr(contato, 'nome_contato', '') or ''
        idx = 0
        for linha in horarios:
            if not isinstance(linha, dict):
                continue
            dias = linha.get("dias", []) or []
            horas = linha.get("horas", []) or []
            if not dias or not horas:
                continue

            # Converter dias PT-BR para cron
            dias_cron_parts = []
            for d in dias:
                d_lower = str(d).strip().lower()
                if d_lower in DIAS_MAP:
                    dias_cron_parts.append(DIAS_MAP[d_lower])
                elif d_lower:
                    dias_cron_parts.append(d_lower)
            if not dias_cron_parts:
                continue
            dias_cron = ",".join(dias_cron_parts)

            for hora_str in horas:
                hora_str = str(hora_str).strip()
                if not hora_str:
                    continue
                try:
                    hora, minuto = _parse_hora(hora_str)
                    job_id = f"{prefixo}_{idx}"

                    trigger = CronTrigger(
                        day_of_week=dias_cron,
                        hour=hora,
                        minute=minuto,
                        timezone="America/Sao_Paulo",
                    )

                    self.scheduler.add_job(
                        func=self._executar_contato_individual,
                        trigger=trigger,
                        id=job_id,
                        args=[contato.user_id, contato.id, tipo_contato],
                        name=f"Contato {tipo_contato}: {nome_contato or contato.id} [{hora_str}]",
                        replace_existing=True,
                    )

                    logger.info(f"[Scheduler] Job contato registrado: {job_id} "
                                f"- {hora_str} dias={dias_cron}")
                    idx += 1
                except Exception as e:
                    logger.error(f"[Scheduler] Erro ao registrar job contato {prefixo}_{idx}: {e}")
                    idx += 1

        if idx == 0:
            logger.info(f"[Scheduler] Nenhum job criado para contato {prefixo}")
        else:
            logger.info(f"[Scheduler] {idx} job(s) criados para contato {prefixo}")

    def _remover_jobs_contato_prefixo(self, prefixo: str):
        """Remove todos os jobs APScheduler que comecam com o prefixo dado."""
        try:
            jobs_to_remove = [
                j for j in self.scheduler.get_jobs()
                if j.id.startswith(prefixo)
            ]
            for j in jobs_to_remove:
                self.scheduler.remove_job(j.id)
            if jobs_to_remove:
                logger.info(f"[Scheduler] {len(jobs_to_remove)} job(s) removidos com prefixo {prefixo}")
        except Exception as e:
            logger.error(f"[Scheduler] Erro ao remover jobs com prefixo {prefixo}: {e}")

    def remover_job_contato(self, contato_id: int, tipo_contato: str):
        """Remove todos os jobs APScheduler de um contato individual."""
        prefixo = f"beka_contato_{tipo_contato}_{contato_id}"
        self._remover_jobs_contato_prefixo(prefixo)

    def _carregar_jobs_contatos(self):
        """Carrega jobs individuais de todos os contatos ativos com horarios definidos."""
        from models import WhatsAppContact, EmailContact
        total = 0
        try:
            for c in WhatsAppContact.query.filter_by(ativo=True).all():
                horarios_raw = getattr(c, 'horarios_json', '[]') or '[]'
                try:
                    horarios = json.loads(horarios_raw)
                except Exception:
                    horarios = []
                if horarios:
                    self.registrar_job_contato(c, 'whatsapp')
                    total += 1
            for c in EmailContact.query.filter_by(ativo=True).all():
                horarios_raw = getattr(c, 'horarios_json', '[]') or '[]'
                try:
                    horarios = json.loads(horarios_raw)
                except Exception:
                    horarios = []
                if horarios:
                    self.registrar_job_contato(c, 'email')
                    total += 1
            logger.info(f"[Scheduler] {total} contato(s) com jobs individuais carregados")
        except Exception as e:
            logger.error(f"[Scheduler] Erro ao carregar jobs de contatos: {e}")

    def _executar_contato_individual(self, user_id: int, contato_id: int, tipo_contato: str):
        """Pipeline individual: processa e envia apenas para um contato especifico.

        1. Busca contato no banco
        2. Resolve lojas alvo (lojas_json + grupos via agrupamentos)
        3. Executa imprimir_direto para gerar etiquetas
        4. Envia resultado apenas para este contato
        5. Registra log de execucao
        """
        from models import WhatsAppContact, EmailContact, ExecutionLog, db

        with self.app.app_context():
            # Criar log de execucao
            log_exec = ExecutionLog(
                user_id=user_id,
                schedule_id=None,
                tipo="contato_individual",
                inicio=_agora_brasil(),
                status="executando",
            )
            db.session.add(log_exec)
            db.session.commit()

            # Serializar execucoes do mesmo usuario (evita conflito no browser/temp)
            exec_lock = self._get_exec_lock(user_id)
            logger.info(f"[ContatoIndividual] contato #{contato_id}: aguardando fila (user {user_id})...")
            acquired = exec_lock.acquire(timeout=900)  # espera ate 15 min
            if not acquired:
                logger.error(f"[ContatoIndividual] TIMEOUT na fila para user {user_id}, contato #{contato_id}")
                log_exec.status = "erro"
                log_exec.fim = _agora_brasil()
                log_exec.detalhes = json.dumps(
                    {"erro": "Timeout aguardando fila de execucao (outro contato demorou mais de 15 min)"},
                    ensure_ascii=False,
                )
                db.session.commit()
                return
            logger.info(f"[ContatoIndividual] contato #{contato_id}: fila liberada, executando (user {user_id})")

            detalhes = {"etapas": [], "tipo_contato": tipo_contato, "contato_id": contato_id}
            status_final = "sucesso"

            try:
                # 1. Buscar contato
                if tipo_contato == 'whatsapp':
                    contato = WhatsAppContact.query.get(contato_id)
                else:
                    contato = EmailContact.query.get(contato_id)

                if not contato or not contato.ativo:
                    detalhes["erro_geral"] = "Contato nao encontrado ou inativo"
                    status_final = "erro"
                    raise RuntimeError("Contato nao encontrado ou inativo")

                # 2. Resolver lojas alvo (lojas_json + expansao de grupos)
                lojas_alvo = []
                try:
                    lojas_cfg = _parse_json_list(getattr(contato, "lojas_json", "[]"))
                    grupos_cfg = _parse_json_list(getattr(contato, "grupos_json", "[]"))
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
                    logger.warning(f"[Scheduler] Falha ao carregar lojas/grupos do contato: {e_tgt}")
                    lojas_alvo = []

                detalhes["lojas_alvo"] = lojas_alvo

                # 3. Executar imprimir_direto (baixa + processa etiquetas)
                detalhes["etapas"].append({"etapa": "imprimir_direto", "status": "iniciando"})
                from dashboard import _executar_imprimir_direto
                resultado_direto = _executar_imprimir_direto(
                    user_id=user_id,
                    lojas_alvo=lojas_alvo,
                )
                if resultado_direto.get("ok"):
                    log_exec.etiquetas_baixadas = resultado_direto.get("pdfs_movidos", 0)
                    log_exec.etiquetas_processadas = resultado_direto.get("total_etiquetas", 0)
                    detalhes["etapas"][-1]["status"] = "concluido"
                    detalhes["etapas"][-1]["etiquetas"] = resultado_direto.get("total_etiquetas", 0)
                    detalhes["etapas"][-1]["lojas"] = resultado_direto.get("total_lojas", 0)
                else:
                    raise RuntimeError(resultado_direto.get("erro", "Falha no imprimir direto"))

                # Salvar sucesso do processamento IMEDIATAMENTE (resiliencia a restart)
                log_exec.status = "sucesso"
                log_exec.fim = _agora_brasil()
                log_exec.detalhes = json.dumps(detalhes, ensure_ascii=False)
                db.session.commit()

                # 4. Enviar apenas para este contato
                # IMPORTANTE: usa envio direto (sem matching de nomes) porque o download
                # ja filtrou por loja — TODO o conteudo processado pertence a este contato.
                # Matching por nome falha porque nomes UpSeller != nomes marketplace.
                detalhes["etapas"].append({"etapa": f"envio_{tipo_contato}", "status": "iniciando"})
                if tipo_contato == 'whatsapp':
                    from dashboard import (_enfileirar_whatsapp_todos_arquivos_para_contato,
                                           _garantir_baileys_rodando)
                    enq = _enfileirar_whatsapp_todos_arquivos_para_contato(user_id, contato_id)
                    if enq.get("ok"):
                        log_exec.whatsapp_enviados = enq.get("total_entregas", 0)
                        detalhes["etapas"][-1]["status"] = "concluido"
                        detalhes["etapas"][-1]["enfileirados"] = enq.get("total_entregas", 0)
                        detalhes["etapas"][-1]["batch_id"] = enq.get("batch_id", "")
                        _garantir_baileys_rodando(motivo="contato_individual")
                    else:
                        # Fallback: tentar matching por nome (pode funcionar se nomes coincidirem)
                        logger.info(f"[Scheduler] Envio direto falhou ({enq.get('erro')}), tentando matching...")
                        from dashboard import _get_estado
                        estado = _get_estado(user_id)
                        resultado_ref = estado.get("ultimo_resultado", {}) if estado else {}
                        from whatsapp_delivery import montar_entregas_por_resultado
                        from models import WhatsAppQueueItem
                        pasta_saida = self._get_pasta_saida(user_id)
                        agrupamentos = (estado or {}).get("agrupamentos", []) if estado else []
                        entregas, diagnostico = montar_entregas_por_resultado(
                            resultado=resultado_ref,
                            pasta_saida=pasta_saida,
                            contatos=[contato],
                            agrupamentos_usuario=agrupamentos,
                        )
                        if entregas:
                            from dashboard import _agora_utc
                            batch_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                            agora = _agora_utc()
                            enfileirados = 0
                            for ent in entregas:
                                file_path = ent.get("file_path", ent.get("pdf_path", ""))
                                if not file_path:
                                    continue
                                telefone_ent = ent.get("telefone", "")
                                # Cross-batch dedup: pular se pendente OU enviado <2h
                                from dashboard import _ja_na_fila_whatsapp
                                if _ja_na_fila_whatsapp(user_id, telefone_ent, file_path):
                                    continue
                                db.session.add(WhatsAppQueueItem(
                                    user_id=user_id,
                                    batch_id=batch_id,
                                    origem="contato_individual",
                                    loja_nome=ent.get("loja", ""),
                                    telefone=telefone_ent,
                                    pdf_path=file_path,
                                    caption=ent.get("caption", ""),
                                    status="pending",
                                    tentativas=0,
                                    max_tentativas=5,
                                    next_attempt_at=agora,
                                ))
                                enfileirados += 1
                            if enfileirados > 0:
                                db.session.commit()
                                _garantir_baileys_rodando(motivo="contato_individual")
                            log_exec.whatsapp_enviados = enfileirados
                            detalhes["etapas"][-1]["status"] = "concluido"
                            detalhes["etapas"][-1]["enfileirados"] = enfileirados
                            detalhes["etapas"][-1]["batch_id"] = batch_id
                        else:
                            detalhes["etapas"][-1]["status"] = "pulado"
                            detalhes["etapas"][-1]["motivo"] = "Nenhuma entrega valida"
                            detalhes["etapas"][-1]["diagnostico"] = diagnostico

                elif tipo_contato == 'email':
                    from dashboard import _get_estado, _smtp_config_resolver
                    from whatsapp_delivery import montar_destinos_por_resultado
                    from models import User
                    import time as _time

                    user = User.query.get(user_id)
                    estado = _get_estado(user_id)
                    resultado_ref = estado.get("ultimo_resultado", {}) if estado else {}
                    if lojas_alvo and resultado_ref and resultado_ref.get("lojas"):
                        alvo_norm = {str(x).strip().lower() for x in lojas_alvo}
                        lojas_filtradas = [
                            l for l in (resultado_ref.get("lojas", []) or [])
                            if str((l or {}).get("nome", "") or "").strip().lower() in alvo_norm
                        ]
                        resultado_ref = {**resultado_ref, "lojas": lojas_filtradas}

                    from_addr = (getattr(user, "email_remetente", "") or "").strip()
                    smtp_cfg, _ = _smtp_config_resolver(user)
                    if not smtp_cfg or not from_addr:
                        detalhes["etapas"][-1]["status"] = "pulado"
                        detalhes["etapas"][-1]["motivo"] = "SMTP/email remetente nao configurado"
                    else:
                        pasta_saida = self._get_pasta_saida(user_id)
                        agrupamentos = (estado or {}).get("agrupamentos", []) if estado else []
                        envios, diagnostico = montar_destinos_por_resultado(
                            resultado=resultado_ref,
                            pasta_saida=pasta_saida,
                            contatos=[contato],
                            destino_attr="email",
                            agrupamentos_usuario=agrupamentos,
                        )
                        if envios:
                            from dashboard import enviar_email_com_anexos
                            timestamp = resultado_ref.get("timestamp", "")
                            from_name = (getattr(user, "nome_remetente", "") or "").strip()
                            # Agrupar por (destino, loja)
                            envios_agrupados = {}
                            for envio in envios:
                                destino = str(envio.get("destino", "") or "").strip()
                                loja = str(envio.get("loja", "") or "").strip()
                                file_path = str(envio.get("file_path", envio.get("pdf_path", "")) or "").strip()
                                if not destino or not file_path:
                                    continue
                                chave = (destino.lower(), loja)
                                if chave not in envios_agrupados:
                                    envios_agrupados[chave] = {"destino": destino, "loja": loja, "arquivos": []}
                                if file_path not in envios_agrupados[chave]["arquivos"]:
                                    envios_agrupados[chave]["arquivos"].append(file_path)

                            total_ok = 0
                            for grupo in envios_agrupados.values():
                                try:
                                    res = enviar_email_com_anexos(
                                        email_destino=grupo["destino"],
                                        assunto=f"Arquivos {grupo['loja']} - {timestamp}",
                                        loja_nome=grupo["loja"],
                                        timestamp=timestamp,
                                        anexos_paths=grupo["arquivos"],
                                        from_addr_override=from_addr,
                                        from_name_override=from_name,
                                        smtp_override=smtp_cfg,
                                    )
                                    if res.get("success"):
                                        total_ok += 1
                                except Exception as e_mail:
                                    logger.error(f"[ContatoIndividual] Erro email: {e_mail}")
                                _time.sleep(2)

                            detalhes["etapas"][-1]["status"] = "concluido"
                            detalhes["etapas"][-1]["enviados"] = total_ok
                        else:
                            detalhes["etapas"][-1]["status"] = "pulado"
                            detalhes["etapas"][-1]["motivo"] = "Nenhuma entrega email valida"
                            detalhes["etapas"][-1]["diagnostico"] = diagnostico

            except Exception as e:
                logger.error(f"[ContatoIndividual] Erro geral: {e}")
                if status_final != "erro":
                    status_final = "erro"
                detalhes["erro_geral"] = str(e)

            finally:
                # Liberar fila para proximo contato do mesmo usuario
                exec_lock.release()

            # Finalizar log
            log_exec.fim = _agora_brasil()
            log_exec.status = status_final
            log_exec.detalhes = json.dumps(detalhes, ensure_ascii=False)
            db.session.commit()

            logger.info(f"[ContatoIndividual] Concluido: user={user_id} "
                        f"contato={contato_id} tipo={tipo_contato} status={status_final}")

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
