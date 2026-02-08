# -*- coding: utf-8 -*-
"""
Dashboard Interativo - Processador de Etiquetas Shopee
Backend Flask com API REST
"""

import os
import sys
import json
import time
import threading
import re as _re
from datetime import datetime
from collections import defaultdict
from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS
import xmltodict
import pandas as pd
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from etiquetas_shopee import ProcessadorEtiquetasShopee

# PyInstaller frozen path support
if getattr(sys, 'frozen', False):
    _BASE_DIR = sys._MEIPASS
else:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__, static_folder=os.path.join(_BASE_DIR, 'static'))
CORS(app)

# ----------------------------------------------------------------
# ESTADO GLOBAL DO DASHBOARD
# ----------------------------------------------------------------
PASTA_ENTRADA = os.environ.get("PASTA_ENTRADA", os.path.join(os.path.expanduser("~"), "Desktop", "Etiquetas"))
PASTA_SAIDA = os.environ.get("PASTA_SAIDA", os.path.join(os.path.expanduser("~"), "Desktop", "Etiquetas Prontas"))

# Garantir que as pastas existam (necessario no Railway/Linux)
os.makedirs(PASTA_ENTRADA, exist_ok=True)
os.makedirs(PASTA_SAIDA, exist_ok=True)

# Config file fica ao lado do exe (nao dentro do MEIPASS)
if getattr(sys, 'frozen', False):
    CONFIG_FILE = os.path.join(os.path.dirname(sys.executable), "config_dashboard.json")
else:
    CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config_dashboard.json")

estado = {
    "processando": False,
    "logs": [],
    "ultimo_resultado": None,
    "historico": [],
    "agrupamentos": [],  # [{nome: str, cnpjs: [str]}]
    "configuracoes": {
        "pasta_entrada": PASTA_ENTRADA,
        "pasta_saida": PASTA_SAIDA,
        "largura_mm": 150,
        "altura_mm": 230,
        "margem_esq": 8,
        "margem_dir": 8,
        "margem_topo": 5,
        "margem_inf": 5,
        "fonte_produto": 7,
        "perc_declarado": 100,
        "taxa_shopee": 18,
        "imposto_simples": 4,
        "custo_fixo": 3.0,
        "planilha_custos": "",
        "lucro_por_loja": {},  # {"NomeLoja": {"taxa_shopee": 20, ...}, ...}
    }
}


def salvar_config_arquivo():
    """Salva configuracoes em arquivo JSON para persistir entre reinicializacoes."""
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(estado["configuracoes"], f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Aviso: nao foi possivel salvar config: {e}")


def carregar_config_arquivo():
    """Carrega configuracoes do arquivo JSON se existir."""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                config_salva = json.load(f)
            for chave, valor in config_salva.items():
                estado["configuracoes"][chave] = valor
            print(f"Configuracoes carregadas de {CONFIG_FILE}")
    except Exception as e:
        print(f"Aviso: nao foi possivel carregar config: {e}")


# Carregar config salva no startup
carregar_config_arquivo()


def adicionar_log(msg, tipo="info"):
    """Adiciona mensagem ao log com timestamp."""
    estado["logs"].append({
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "mensagem": msg,
        "tipo": tipo  # info, success, warning, error
    })
    # Manter apenas os ultimos 500 logs
    if len(estado["logs"]) > 500:
        estado["logs"] = estado["logs"][-500:]


# ----------------------------------------------------------------
# ROTAS DA API
# ----------------------------------------------------------------

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


@app.route('/api/status')
def api_status():
    """Retorna o status atual do sistema."""
    pasta = estado["configuracoes"]["pasta_entrada"]
    arquivos = []
    if os.path.exists(pasta):
        for f in os.listdir(pasta):
            fp = os.path.join(pasta, f)
            if os.path.isfile(fp):
                ext = os.path.splitext(f)[1].lower()
                if ext in ('.pdf', '.zip', '.xlsx'):
                    tipo_arq = "PDF" if ext == '.pdf' else ("ZIP" if ext == '.zip' else "XLSX")
                    tamanho = os.path.getsize(fp)
                    arquivos.append({
                        "nome": f,
                        "tipo": tipo_arq,
                        "tamanho": tamanho,
                        "tamanho_fmt": _formatar_tamanho(tamanho),
                    })

    # Verificar saida existente
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

    return jsonify({
        "processando": estado["processando"],
        "arquivos_entrada": arquivos,
        "saidas": saidas,
        "ultimo_resultado": estado["ultimo_resultado"],
        "configuracoes": estado["configuracoes"],
        "agrupamentos": estado["agrupamentos"],
        "ultimo_lucro": estado.get("ultimo_lucro"),
    })


@app.route('/api/logs')
def api_logs():
    """Retorna os logs do sistema."""
    desde = request.args.get('desde', 0, type=int)
    return jsonify({
        "logs": estado["logs"][desde:],
        "total": len(estado["logs"]),
    })


@app.route('/api/processar', methods=['POST'])
def api_processar():
    """Inicia o processamento das etiquetas."""
    if estado["processando"]:
        return jsonify({"erro": "Processamento ja em andamento"}), 409

    # Processar em thread separada
    thread = threading.Thread(target=_executar_processamento)
    thread.daemon = True
    thread.start()

    return jsonify({"mensagem": "Processamento iniciado"})


@app.route('/api/configuracoes', methods=['POST'])
def api_configuracoes():
    """Atualiza as configuracoes."""
    dados = request.get_json()
    if dados:
        for chave, valor in dados.items():
            if chave in estado["configuracoes"]:
                estado["configuracoes"][chave] = valor
        salvar_config_arquivo()
        adicionar_log("Configuracoes atualizadas e salvas", "success")
    return jsonify(estado["configuracoes"])


@app.route('/api/configuracoes-lucro-lojas', methods=['GET', 'POST'])
def api_config_lucro_lojas():
    """Gerencia configuracoes de lucro individuais por loja."""
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
        salvar_config_arquivo()
        adicionar_log("Config lucro por loja atualizada", "success")
    return jsonify({"ok": True})


@app.route('/api/historico')
def api_historico():
    """Retorna o historico de processamentos."""
    return jsonify({"historico": estado["historico"]})


@app.route('/api/abrir-pasta', methods=['POST'])
def api_abrir_pasta():
    """Abre uma pasta no explorador de arquivos."""
    dados = request.get_json()
    pasta = dados.get('pasta', '')
    if pasta and os.path.exists(pasta):
        # os.startfile so funciona no Windows
        try:
            os.startfile(pasta)
        except AttributeError:
            pass  # Linux/Mac - ignora silenciosamente
        return jsonify({"ok": True})
    return jsonify({"erro": "Pasta nao encontrada"}), 404


@app.route('/api/upload', methods=['POST'])
def api_upload():
    """Recebe upload de arquivos PDF/ZIP."""
    if 'arquivo' not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado"}), 400

    arquivo = request.files['arquivo']
    if arquivo.filename == '':
        return jsonify({"erro": "Nome de arquivo vazio"}), 400

    ext = os.path.splitext(arquivo.filename)[1].lower()
    if ext not in ('.pdf', '.zip', '.xlsx'):
        return jsonify({"erro": "Tipo de arquivo nao suportado. Use PDF, ZIP ou XLSX."}), 400

    pasta = estado["configuracoes"]["pasta_entrada"]
    caminho = os.path.join(pasta, arquivo.filename)
    arquivo.save(caminho)
    adicionar_log(f"Arquivo recebido: {arquivo.filename}", "success")
    return jsonify({"mensagem": f"Arquivo {arquivo.filename} salvo com sucesso"})


@app.route('/api/remover-arquivo', methods=['POST'])
def api_remover_arquivo():
    """Remove um arquivo da pasta de entrada."""
    dados = request.get_json()
    nome = dados.get('nome', '')
    if not nome:
        return jsonify({"erro": "Nome nao informado"}), 400

    caminho = os.path.join(estado["configuracoes"]["pasta_entrada"], nome)
    if os.path.exists(caminho):
        os.remove(caminho)
        adicionar_log(f"Arquivo removido: {nome}", "warning")
        return jsonify({"ok": True})
    return jsonify({"erro": "Arquivo nao encontrado"}), 404


@app.route('/api/limpar-saida', methods=['POST'])
def api_limpar_saida():
    """Limpa a pasta de saida."""
    import shutil
    pasta = estado["configuracoes"]["pasta_saida"]
    if os.path.exists(pasta):
        shutil.rmtree(pasta)
        adicionar_log("Pasta de saida limpa", "warning")
    return jsonify({"ok": True})


@app.route('/api/download/<loja>/<arquivo>')
def api_download(loja, arquivo):
    """Download de um arquivo gerado."""
    pasta = os.path.join(estado["configuracoes"]["pasta_saida"], loja)
    return send_file(os.path.join(pasta, arquivo), as_attachment=True)


@app.route('/api/download-resumo-geral')
def api_download_resumo_geral():
    """Download do resumo geral XLSX."""
    if not estado["ultimo_resultado"] or "resumo_geral" not in estado["ultimo_resultado"]:
        return jsonify({"erro": "Nenhum resumo geral disponivel"}), 404
    arquivo = estado["ultimo_resultado"]["resumo_geral"]["arquivo"]
    pasta = estado["configuracoes"]["pasta_saida"]
    caminho = os.path.join(pasta, arquivo)
    if os.path.exists(caminho):
        return send_file(caminho, as_attachment=True)
    return jsonify({"erro": "Arquivo nao encontrado"}), 404


@app.route('/api/upload-custos', methods=['POST'])
def api_upload_custos():
    """Recebe upload da planilha de custos XLSX."""
    if 'arquivo' not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado"}), 400

    arquivo = request.files['arquivo']
    if arquivo.filename == '':
        return jsonify({"erro": "Nome de arquivo vazio"}), 400

    ext = os.path.splitext(arquivo.filename)[1].lower()
    if ext != '.xlsx':
        return jsonify({"erro": "Envie um arquivo .xlsx"}), 400

    pasta = estado["configuracoes"]["pasta_entrada"]
    caminho = os.path.join(pasta, "planilha_custos.xlsx")
    arquivo.save(caminho)
    estado["configuracoes"]["planilha_custos"] = caminho
    salvar_config_arquivo()
    adicionar_log(f"Planilha de custos recebida: {arquivo.filename}", "success")
    return jsonify({"mensagem": "Planilha de custos salva", "caminho": caminho})


def _limpar_nome_loja(nome_raw):
    """Limpa nome da loja para uso como nome de pasta/arquivo."""
    nome = _re.sub(r'^\d[\d.]+\s+', '', nome_raw)
    nome = _re.sub(r'\s+\d{11}$', '', nome)
    nome = _re.sub(r'\s+(LTDA|ME|MEI|EPP|EIRELI)\s*$', '', nome, flags=_re.IGNORECASE)
    nome = nome.strip().title()
    nome = _re.sub(r'[<>:"/\\|?*]', '', nome)
    return nome.strip() or 'Loja_Desconhecida'


def _extrair_loja_nfe(nfe):
    """Extrai nome da loja do bloco emit da NFe."""
    emit = nfe.get("emit", {})
    if isinstance(emit, str):
        return "Desconhecida"
    nome_raw = str(emit.get("xNome", "")).strip()
    return _limpar_nome_loja(nome_raw) if nome_raw else "Desconhecida"


def _processar_nfe_lucro(nfe, dict_custos, cfg, cfg_por_loja):
    """Processa uma NFe e retorna (nome_loja, lista_itens, indices_sem_custo)."""
    nome_loja = _extrair_loja_nfe(nfe)

    # Config por loja (override dos defaults globais)
    cfg_loja = cfg_por_loja.get(nome_loja, {})
    perc_declarado = float(cfg_loja.get("perc_declarado", cfg.get("perc_declarado", 100))) / 100
    taxa_shopee = float(cfg_loja.get("taxa_shopee", cfg.get("taxa_shopee", 18))) / 100
    taxa_imposto = float(cfg_loja.get("imposto_simples", cfg.get("imposto_simples", 4))) / 100
    custo_fixo = float(cfg_loja.get("custo_fixo", cfg.get("custo_fixo", 3)))

    dets = nfe.get("det", [])
    if not isinstance(dets, list):
        dets = [dets]

    itens = []
    sem_custo = []
    for item in dets:
        prod = item.get("prod", {})
        sku_xml = str(prod.get("cProd", "")).strip()
        qtd = float(prod.get("qCom", 1))
        sku_busca = sku_xml[:4]

        c_produto_unit = dict_custos.get(sku_busca, 0.0)
        c_produto_total = c_produto_unit * qtd

        eh_sem_custo = sku_busca not in dict_custos
        if eh_sem_custo:
            sem_custo.append(len(itens))

        v_declarado = float(prod.get("vProd", 0))
        v_real = v_declarado / perc_declarado if perc_declarado > 0 else v_declarado

        c_imposto = v_declarado * taxa_imposto
        c_shopee = (v_real * taxa_shopee) + (custo_fixo * qtd)

        lucro = v_real - c_imposto - c_shopee - c_produto_total

        itens.append({
            "SKU": sku_xml,
            "Qtd": qtd,
            "V. Real": round(v_real, 2),
            "V. Decl.": round(v_declarado, 2),
            "Custo": round(c_produto_total, 2),
            "Shopee": round(c_shopee, 2),
            "Imposto": round(c_imposto, 2),
            "LUCRO": round(lucro, 2),
        })

    return nome_loja, itens, sem_custo


@app.route('/api/gerar-lucro', methods=['POST'])
def api_gerar_lucro():
    """Gera relatorio de lucro a partir dos XMLs e planilha de custos, separado por loja."""
    cfg = estado["configuracoes"]
    pasta_entrada = cfg["pasta_entrada"]
    pasta_saida = cfg["pasta_saida"]
    caminho_custos = cfg.get("planilha_custos", "")

    if not caminho_custos or not os.path.exists(caminho_custos):
        return jsonify({"erro": "Planilha de custos nao encontrada. Faca upload primeiro."}), 400

    try:
        adicionar_log("Gerando relatorio de lucro...", "info")

        # Ler planilha de custos — SKU (4 primeiros digitos) -> custo unitario
        df_custos = pd.read_excel(caminho_custos)
        dict_custos = {}
        for _, row in df_custos.iterrows():
            sku_original = str(row.iloc[0]).strip()
            custo = float(row.iloc[1]) if pd.notnull(row.iloc[1]) else 0.0
            sku_chave = sku_original[:4]
            dict_custos[sku_chave] = custo

        cfg_por_loja = cfg.get("lucro_por_loja", {})

        # Agrupar por loja: {nome_loja: {"itens": [...], "linhas_sem_custo": [...]}}
        import zipfile
        loja_dados = defaultdict(lambda: {"itens": [], "linhas_sem_custo": []})

        def _processar_doc(doc):
            if "nfeProc" in doc:
                nfe = doc["nfeProc"]["NFe"]["infNFe"]
            elif "NFe" in doc:
                nfe = doc["NFe"]["infNFe"]
            else:
                return
            nome_loja, itens, sem_custo = _processar_nfe_lucro(nfe, dict_custos, cfg, cfg_por_loja)
            offset = len(loja_dados[nome_loja]["itens"])
            loja_dados[nome_loja]["itens"].extend(itens)
            loja_dados[nome_loja]["linhas_sem_custo"].extend([i + offset for i in sem_custo])

        # XMLs dos ZIPs
        zips = [f for f in os.listdir(pasta_entrada) if f.lower().endswith('.zip')]
        for z in zips:
            caminho_zip = os.path.join(pasta_entrada, z)
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
                adicionar_log(f"Erro ao ler ZIP {z}: {e}", "warning")

        # XMLs avulsos
        xmls_avulsos = [f for f in os.listdir(pasta_entrada) if f.lower().endswith('.xml')]
        for arq in xmls_avulsos:
            caminho_xml = os.path.join(pasta_entrada, arq)
            try:
                with open(caminho_xml, "r", encoding="utf-8") as f:
                    doc = xmltodict.parse(f.read())
                _processar_doc(doc)
            except Exception:
                continue

        if not loja_dados:
            return jsonify({"erro": "Nenhum produto encontrado nos XMLs"}), 400

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(pasta_saida, exist_ok=True)

        # Gerar XLSX por loja
        lojas_lucro = []
        lista_global = []
        linhas_sem_custo_global = []

        for nome_loja in sorted(loja_dados.keys()):
            dados_l = loja_dados[nome_loja]
            itens_l = dados_l["itens"]
            sem_custo_l = dados_l["linhas_sem_custo"]

            if not itens_l:
                continue

            # XLSX por loja
            pasta_loja = os.path.join(pasta_saida, nome_loja)
            os.makedirs(pasta_loja, exist_ok=True)
            caminho_loja_xlsx = os.path.join(pasta_loja, f"lucro_{nome_loja}_{timestamp}.xlsx")

            df_loja = pd.DataFrame(itens_l)
            totais_l = df_loja.sum(numeric_only=True)
            totais_l["SKU"] = "TOTAIS"
            df_loja = pd.concat([df_loja, pd.DataFrame([totais_l])], ignore_index=True)
            df_loja.to_excel(caminho_loja_xlsx, index=False)
            _formatar_excel_lucro(caminho_loja_xlsx, sem_custo_l)

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

            adicionar_log(f"  Lucro {nome_loja}: {len(itens_l)} itens, R$ {lucro_l:.2f}", "info")

            # Acumular para XLSX consolidado
            offset_g = len(lista_global)
            for item in itens_l:
                lista_global.append({"Loja": nome_loja, **item})
            linhas_sem_custo_global.extend([i + offset_g for i in sem_custo_l])

        # XLSX consolidado (todas as lojas)
        df_global = pd.DataFrame(lista_global)
        totais_g = df_global.sum(numeric_only=True)
        totais_g["SKU"] = "TOTAIS"
        totais_g["Loja"] = ""
        df_global = pd.concat([df_global, pd.DataFrame([totais_g])], ignore_index=True)

        caminho_xlsx = os.path.join(pasta_saida, f"relatorio_lucro_{timestamp}.xlsx")
        df_global.to_excel(caminho_xlsx, index=False)
        _formatar_excel_lucro(caminho_xlsx, linhas_sem_custo_global)

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

        adicionar_log(f"Relatorio de lucro gerado: {total_itens} itens, {len(lojas_lucro)} lojas, Lucro: R$ {lucro_total:.2f}", "success")

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
        adicionar_log(f"Erro ao gerar lucro: {str(e)}", "error")
        adicionar_log(traceback.format_exc(), "error")
        return jsonify({"erro": str(e)}), 500


@app.route('/api/download-lucro')
def api_download_lucro():
    """Download do relatorio de lucro XLSX consolidado."""
    lucro = estado.get("ultimo_lucro")
    if not lucro:
        return jsonify({"erro": "Nenhum relatorio de lucro disponivel"}), 404
    pasta = estado["configuracoes"]["pasta_saida"]
    caminho = os.path.join(pasta, lucro["arquivo"])
    if os.path.exists(caminho):
        return send_file(caminho, as_attachment=True)
    return jsonify({"erro": "Arquivo nao encontrado"}), 404


@app.route('/api/download-lucro/<loja>')
def api_download_lucro_loja(loja):
    """Download do relatorio de lucro XLSX por loja."""
    pasta = os.path.join(estado["configuracoes"]["pasta_saida"], loja)
    if not os.path.exists(pasta):
        return jsonify({"erro": "Pasta da loja nao encontrada"}), 404
    arquivos = [f for f in os.listdir(pasta) if f.startswith("lucro_") and f.endswith(".xlsx")]
    if not arquivos:
        return jsonify({"erro": "Arquivo de lucro nao encontrado"}), 404
    arquivo = sorted(arquivos)[-1]
    return send_file(os.path.join(pasta, arquivo), as_attachment=True)


def _formatar_excel_lucro(caminho_arquivo, linhas_sem_custo):
    """Formata o XLSX de lucro com cores e estilos."""
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

    # Header
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    max_row = ws.max_row
    for row in ws.iter_rows(min_row=2, max_row=max_row, min_col=1, max_col=ws.max_column):
        for cell in row:
            cell.border = thin_border
            if cell.col_idx >= 3:
                cell.number_format = 'R$ #,##0.00'

            idx_dados = cell.row - 2
            if idx_dados in linhas_sem_custo:
                cell.fill = alert_fill

            if cell.col_idx == ws.max_column:
                if isinstance(cell.value, (int, float)) and cell.value >= 0:
                    cell.font = lucro_positivo
                elif isinstance(cell.value, (int, float)):
                    cell.font = lucro_negativo

    # Total row
    for cell in ws[max_row]:
        cell.fill = total_fill
        cell.font = Font(bold=True)

    # Auto-width
    for column in ws.columns:
        col_cells = [cell for cell in column]
        max_length = 0
        for cell in col_cells:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except Exception:
                pass
        ws.column_dimensions[get_column_letter(col_cells[0].column)].width = max_length + 2

    wb.save(caminho_arquivo)


@app.route('/api/agrupar', methods=['POST'])
def api_agrupar():
    """Gera PDF agrupado combinando etiquetas de 2+ lojas com mesmos criterios."""
    dados = request.get_json()
    if not dados:
        return jsonify({"erro": "Dados não enviados"}), 400

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
        # Combinar etiquetas raw das lojas selecionadas
        etiquetas_combinadas = []
        nomes_lojas = []
        for cnpj in cnpjs:
            etqs = etiquetas_por_cnpj.get(cnpj, [])
            if etqs:
                etiquetas_combinadas.extend(etqs)
                # Buscar nome da loja
                cfg = estado.get("_proc_config", {})
                nome = cfg.get("cnpj_loja", {}).get(cnpj) or cfg.get("cnpj_nome", {}).get(cnpj, cnpj)
                nomes_lojas.append(nome)

        if len(nomes_lojas) < 2:
            return jsonify({"erro": "Lojas selecionadas nao encontradas"}), 400

        # Criar processador com config salva
        proc = ProcessadorEtiquetasShopee()
        cfg = estado.get("_proc_config", {})
        proc.LARGURA_PT = cfg.get("largura_pt", proc.LARGURA_PT)
        proc.ALTURA_PT = cfg.get("altura_pt", proc.ALTURA_PT)
        proc.MARGEM_ESQUERDA = cfg.get("margem_esquerda", proc.MARGEM_ESQUERDA)
        proc.MARGEM_DIREITA = cfg.get("margem_direita", proc.MARGEM_DIREITA)
        proc.MARGEM_TOPO = cfg.get("margem_topo", proc.MARGEM_TOPO)
        proc.MARGEM_INFERIOR = cfg.get("margem_inferior", proc.MARGEM_INFERIOR)
        proc.fonte_produto = cfg.get("fonte_produto", proc.fonte_produto)
        proc.cnpj_loja = cfg.get("cnpj_loja", {})
        proc.cnpj_nome = cfg.get("cnpj_nome", {})

        # Remover duplicatas cruzadas
        etiquetas_combinadas, duplicadas = proc.remover_duplicatas(etiquetas_combinadas)
        if duplicadas:
            adicionar_log(f"  Agrupamento: {len(duplicadas)} duplicatas removidas", "warning")

        # Separar regular e CPF
        etiq_regular = [e for e in etiquetas_combinadas if e.get('tipo_especial') != 'cpf']
        etiq_cpf = [e for e in etiquetas_combinadas if e.get('tipo_especial') == 'cpf']

        pasta_grupo = os.path.join(pasta_saida, nome_grupo)
        if not os.path.exists(pasta_grupo):
            os.makedirs(pasta_grupo)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        total_pags = 0

        # Gerar PDF com ordenacao (simples primeiro, multi no fim)
        if etiq_regular:
            caminho_pdf = os.path.join(pasta_grupo, f"agrupado_{nome_grupo}_{timestamp}.pdf")
            t, _, _, _, _ = proc.gerar_pdf_loja(etiq_regular, caminho_pdf)
            total_pags += t

        if etiq_cpf:
            caminho_cpf = os.path.join(pasta_grupo, f"cpf_{nome_grupo}_{timestamp}.pdf")
            total_pags += proc.gerar_pdf_cpf(etiq_cpf, caminho_cpf)

        # Gerar XLSX resumo
        caminho_xlsx = os.path.join(pasta_grupo, f"resumo_{nome_grupo}_{timestamp}.xlsx")
        n_skus, total_qtd = proc.gerar_resumo_xlsx(etiquetas_combinadas, caminho_xlsx, nome_grupo)

        adicionar_log(f"Agrupamento '{nome_grupo}': {', '.join(nomes_lojas)}", "success")
        adicionar_log(f"  {total_pags} pags, {n_skus} SKUs, {total_qtd} un.", "info")

        return jsonify({
            "mensagem": f"Agrupamento '{nome_grupo}' gerado com sucesso",
            "lojas": nomes_lojas,
            "total_etiquetas": len(etiquetas_combinadas),
            "arquivo": f"agrupado_{nome_grupo}_{timestamp}.pdf",
            "pasta": nome_grupo,
        })

    except Exception as e:
        adicionar_log(f"ERRO ao agrupar: {str(e)}", "error")
        import traceback
        adicionar_log(traceback.format_exc(), "error")
        return jsonify({"erro": str(e)}), 500


@app.route('/api/agrupamentos', methods=['GET', 'POST'])
def api_agrupamentos():
    """Gerencia agrupamentos pre-configurados."""
    if request.method == 'GET':
        return jsonify({"agrupamentos": estado["agrupamentos"]})
    dados = request.get_json()
    estado["agrupamentos"] = dados.get("agrupamentos", [])
    adicionar_log(f"Agrupamentos salvos: {len(estado['agrupamentos'])} grupo(s)", "success")
    return jsonify({"ok": True})


# ----------------------------------------------------------------
# PROCESSAMENTO EM BACKGROUND
# ----------------------------------------------------------------
def _executar_processamento():
    """Executa o processamento completo em thread separada."""
    estado["processando"] = True
    estado["logs"] = []
    inicio = time.time()

    try:
        pasta_entrada = estado["configuracoes"]["pasta_entrada"]
        pasta_saida = estado["configuracoes"]["pasta_saida"]

        adicionar_log("Iniciando processamento...", "info")

        proc = ProcessadorEtiquetasShopee()

        # Aplicar configuracoes
        proc.LARGURA_PT = estado["configuracoes"]["largura_mm"] * 2.835
        proc.ALTURA_PT = estado["configuracoes"]["altura_mm"] * 2.835
        proc.MARGEM_ESQUERDA = estado["configuracoes"]["margem_esq"]
        proc.MARGEM_DIREITA = estado["configuracoes"]["margem_dir"]
        proc.MARGEM_TOPO = estado["configuracoes"]["margem_topo"]
        proc.MARGEM_INFERIOR = estado["configuracoes"]["margem_inf"]
        proc.fonte_produto = estado["configuracoes"].get("fonte_produto", 7)

        # 1. Carregar XMLs
        adicionar_log("Carregando XMLs dos arquivos ZIP...", "info")
        zips = [f for f in os.listdir(pasta_entrada) if f.lower().endswith('.zip')]
        total_xmls = 0
        for z in zips:
            caminho = os.path.join(pasta_entrada, z)
            n = proc._carregar_zip(caminho)
            total_xmls += n
            adicionar_log(f"  {z}: {n} XMLs", "info")

        adicionar_log(f"Total: {total_xmls} XMLs carregados", "success")
        adicionar_log(f"Lojas identificadas: {len(proc.cnpj_nome)}", "info")
        for cnpj, nome in sorted(proc.cnpj_nome.items(), key=lambda x: x[1]):
            adicionar_log(f"  {nome} [{cnpj}]", "info")

        # 2. Carregar PDFs
        adicionar_log("Carregando etiquetas dos PDFs...", "info")
        todas_etiquetas = proc.carregar_todos_pdfs(pasta_entrada)
        adicionar_log(f"Total: {len(todas_etiquetas)} etiquetas extraidas", "success")

        # 2b. Processar etiquetas especiais (retirada do comprador e CPF)
        adicionar_log("Verificando etiquetas especiais...", "info")
        etiquetas_beka = proc.processar_beka(pasta_entrada)
        if etiquetas_beka:
            todas_etiquetas.extend(etiquetas_beka)
            adicionar_log(f"Retirada do comprador (beka): {len(etiquetas_beka)} etiquetas", "success")

        etiquetas_cpf_especial = proc.processar_cpf(pasta_entrada)
        if etiquetas_cpf_especial:
            todas_etiquetas.extend(etiquetas_cpf_especial)
            adicionar_log(f"CPF: {len(etiquetas_cpf_especial)} etiquetas", "success")

        etiquetas_shein = proc.processar_shein(pasta_entrada)
        if etiquetas_shein:
            adicionar_log(f"Shein: {len(etiquetas_shein)} etiquetas", "success")

        if not etiquetas_beka and not etiquetas_cpf_especial and not etiquetas_shein:
            adicionar_log("Nenhuma etiqueta especial encontrada", "info")

        # 2c. Remover duplicatas
        todas_etiquetas, duplicadas = proc.remover_duplicatas(todas_etiquetas)
        if duplicadas:
            adicionar_log(f"AVISO: {len(duplicadas)} etiquetas duplicadas removidas:", "warning")
            for d in duplicadas:
                adicionar_log(f"  NF duplicada: {d.get('nf', '?')}", "warning")
        else:
            adicionar_log("Nenhuma duplicata encontrada", "info")

        # 3. Separar por loja
        adicionar_log("Separando etiquetas por loja...", "info")
        lojas = proc.separar_por_loja(todas_etiquetas)
        adicionar_log(f"{len(lojas)} lojas para processar", "info")

        # Salvar etiquetas raw e config do processador para uso pelo agrupamento
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

        # 4. Criar pasta de saida
        if not os.path.exists(pasta_saida):
            os.makedirs(pasta_saida)

        # 5. Gerar arquivos por loja
        resultado_lojas = []
        for cnpj, etiquetas_loja in lojas.items():
            nome_loja = proc.get_nome_loja(cnpj)
            n_etiquetas = len(etiquetas_loja)

            adicionar_log(f"Processando: {nome_loja} ({n_etiquetas} etiquetas)...", "info")

            pasta_loja = os.path.join(pasta_saida, nome_loja)
            if not os.path.exists(pasta_loja):
                os.makedirs(pasta_loja)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Separar etiquetas regulares e CPF
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
                adicionar_log(f"  {nome_loja}: {total_cpf} etiquetas CPF", "info")

            # XLSX cobre todas as etiquetas da loja (regular + CPF)
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

            adicionar_log(
                f"  {nome_loja}: {total_pags} pags, {n_skus} SKUs, {total_qtd} un.",
                "success"
            )
            if sem_xml > 0:
                adicionar_log(f"  AVISO: {sem_xml} etiquetas sem XML", "warning")

        # 5a. Gerar resumo geral (todas as lojas)
        adicionar_log("Gerando resumo geral...", "info")
        timestamp_geral = datetime.now().strftime("%Y%m%d_%H%M%S")
        caminho_resumo_geral = os.path.join(pasta_saida, f"resumo_geral_{timestamp_geral}.xlsx")
        n_lojas_rg, total_un_rg = proc.gerar_resumo_geral_xlsx(
            resultado_lojas, dict(lojas), caminho_resumo_geral
        )
        adicionar_log(f"Resumo geral: {n_lojas_rg} lojas, {total_un_rg} unidades total", "success")

        # 5b. (CPF agora e gerado por loja na secao 5 acima)

        # 5c. Gerar PDF Shein separado (formato 150x225mm com barcode vertical)
        if etiquetas_shein:
            adicionar_log("Gerando PDF Shein...", "info")
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
                adicionar_log(f"  Shein {nome_loja_s}: {total_shein} paginas", "success")

        # 5d. Gerar agrupamentos pre-configurados (com mesmos criterios)
        if estado["agrupamentos"] and resultado_lojas:
            adicionar_log("Gerando agrupamentos pre-configurados...", "info")

            for grupo in estado["agrupamentos"]:
                nome_grupo = grupo.get("nome", "Agrupado")
                cnpjs_grupo = grupo.get("cnpjs", [])
                if len(cnpjs_grupo) < 2:
                    continue

                # Combinar etiquetas raw dos CNPJs do grupo
                etiquetas_grupo = []
                nomes_g = []
                for c in cnpjs_grupo:
                    if c in lojas:
                        etiquetas_grupo.extend(lojas[c])
                        nomes_g.append(proc.get_nome_loja(c))

                if len(nomes_g) < 2:
                    adicionar_log(f"  Grupo '{nome_grupo}': lojas insuficientes, pulando", "warning")
                    continue

                try:
                    # Remover duplicatas cruzadas
                    etiquetas_grupo, _ = proc.remover_duplicatas(etiquetas_grupo)

                    # Separar regular e CPF
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

                    # Gerar XLSX resumo
                    caminho_xlsx_g = os.path.join(pasta_grupo, f"resumo_{nome_grupo}_{timestamp_g}.xlsx")
                    proc.gerar_resumo_xlsx(etiquetas_grupo, caminho_xlsx_g, nome_grupo)

                    adicionar_log(f"  Grupo '{nome_grupo}': {', '.join(nomes_g)} ({total_pags_g} pags)", "success")
                except Exception as e_g:
                    adicionar_log(f"  ERRO grupo '{nome_grupo}': {str(e_g)}", "error")

        duracao = round(time.time() - inicio, 1)

        resultado = {
            "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "duracao_s": duracao,
            "total_xmls": total_xmls,
            "total_etiquetas": len(todas_etiquetas),
            "total_lojas": len(lojas),
            "lojas": resultado_lojas,
            "resumo_geral": {
                "arquivo": os.path.basename(caminho_resumo_geral),
                "total_lojas": n_lojas_rg,
                "total_unidades": total_un_rg,
            },
        }

        estado["ultimo_resultado"] = resultado
        estado["historico"].insert(0, resultado)
        # Manter ultimos 20 historicos
        estado["historico"] = estado["historico"][:20]

        adicionar_log(f"Processamento concluido em {duracao}s!", "success")

    except Exception as e:
        adicionar_log(f"ERRO: {str(e)}", "error")
        import traceback
        adicionar_log(traceback.format_exc(), "error")

    finally:
        estado["processando"] = False


def _formatar_tamanho(bytes_val):
    """Formata tamanho em bytes para formato legivel."""
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
    print("DASHBOARD - Processador de Etiquetas Shopee")
    print("=" * 60)
    print(f"\n  Abra no navegador: http://localhost:5000\n")
    print(f"  Pasta entrada: {PASTA_ENTRADA}")
    print(f"  Pasta saida:   {PASTA_SAIDA}")
    print("=" * 60)

    # Auto-abrir browser quando rodando como executavel
    if getattr(sys, 'frozen', False):
        import webbrowser
        threading.Timer(1.5, lambda: webbrowser.open('http://localhost:5000')).start()

    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, port=port, host='0.0.0.0')
