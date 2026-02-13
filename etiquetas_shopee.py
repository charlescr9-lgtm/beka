# -*- coding: utf-8 -*-
"""
Processador de Etiquetas Shopee
- Processa TODOS os PDFs e XMLs da pasta de entrada
- Identifica a loja (CNPJ/emitente) de cada etiqueta via XML
- Separa etiquetas por loja em pastas nomeadas pelo emitente
- Gera PDF final com 1 etiqueta por pagina (150mm x ~230mm)
- Adiciona secao DANFE: codigo de barras + tabela de produtos
- Organiza por SKU, multi-produto no final, numeracao sequencial
- Gera resumo XLSX por SKU para cada loja
- Salva tudo em C:\\Users\\Micro\\Desktop\\Etiquetas Prontas\\<nome_loja>
"""

import fitz  # PyMuPDF
import re
import os
import io
import zipfile
from datetime import datetime
from collections import defaultdict, OrderedDict

import pandas as pd

# python-barcode para gerar Code128
import barcode
from barcode.writer import SVGWriter

# openpyxl para gerar XLSX
import openpyxl
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill

# Marketplace drivers
from marketplaces.registry_bootstrap import bootstrap_drivers
from marketplaces.registry import detect_best, get_driver_by_kind


class ProcessadorEtiquetasShopee:
    # Dimensoes da pagina de saida em pontos (1mm = 2.835pt)
    LARGURA_PT = 425.197   # 150mm
    ALTURA_PT = 651.969    # 230mm

    # Margens de seguranca para evitar cortes na impressao
    MARGEM_ESQUERDA = 8
    MARGEM_DIREITA = 8
    MARGEM_TOPO = 5
    MARGEM_INFERIOR = 5

    def __init__(self):
        self.dados_xml = {}      # nf -> dados completos do XML
        self.cnpj_nome = {}      # cnpj -> nome do emitente (nome limpo)
        self.cnpj_loja = {}      # cnpj -> nome da loja Shopee (extraido do REMETENTE)
        self.fonte_produto = 7   # tamanho da fonte para tabela de produtos (configuravel)
        self.exibicao_produto = 'sku'  # 'sku', 'titulo' ou 'ambos'
        self.dados_xlsx_global = {}    # order_sn -> {produtos, total_itens, total_qtd}
        self.dados_xlsx_tracking = {}  # tracking_number -> order_sn
        self._retirada_map_pedido = {}
        self._retirada_map_track = {}
        self._intercalacao_warnings = []

    # ----------------------------------------------------------------
    # INTERCALAÇÃO / RETIRADA
    # ----------------------------------------------------------------
    def get_intercalacao_warnings(self):
        return list(getattr(self, "_intercalacao_warnings", []))

    def _push_intercalacao_warning(self, msg: str):
        if not hasattr(self, "_intercalacao_warnings"):
            self._intercalacao_warnings = []
        self._intercalacao_warnings.append(msg)

    def _parse_retirada_product_info(self, product_info: str):
        """
        product_info exemplo:
        "[1] Product Name:...; Variation Name:Preto,37/38; ... Quantity: 1; SKU Reference No.: XXX; Parent SKU Reference No.: FA; "
        Retorna lista de dicts no formato esperado por dados_xml['produtos'].
        """
        if not product_info or not isinstance(product_info, str):
            return []
        parts = re.split(r"\[\d+\]\s*", product_info)
        parts = [p.strip() for p in parts if p.strip()]
        produtos = []
        for p in parts:
            fields = {}
            for chunk in p.split(";"):
                chunk = chunk.strip()
                if not chunk or ":" not in chunk:
                    continue
                k, v = chunk.split(":", 1)
                fields[k.strip().lower()] = v.strip()
            nome = fields.get("product name", "") or fields.get("product", "") or ""
            variacao = fields.get("variation name", "") or fields.get("variation", "") or ""
            qtd = fields.get("quantity", "1") or "1"
            sku = fields.get("parent sku reference no.", "") or fields.get("sku reference no.", "") or ""
            try:
                qtd_int = int(float(str(qtd).replace(",", ".")))
            except Exception:
                qtd_int = 1
            produtos.append({
                "codigo": sku.strip(),
                "variacao": variacao.strip(),
                "qtd": str(qtd_int),
                "descricao": (nome.strip()[:80] if nome else ""),
            })
        return produtos

    def carregar_retirada_xlsx(self, pasta_entrada: str):
        """
        Carrega retirada.xlsx (ou arquivos contendo 'retirada' no nome) e cria mapas:
        - pedido(order_sn) -> produtos
        - tracking_number -> produtos
        """
        self._retirada_map_pedido = {}
        self._retirada_map_track = {}
        if not pasta_entrada or not os.path.isdir(pasta_entrada):
            return
        candidatos = []
        for f in os.listdir(pasta_entrada):
            fn = f.lower()
            if fn.endswith(".xlsx") and ("retirada" in fn or "pickup" in fn):
                candidatos.append(os.path.join(pasta_entrada, f))
        if not candidatos:
            p = os.path.join(pasta_entrada, "retirada.xlsx")
            if os.path.exists(p):
                candidatos.append(p)
        for caminho in candidatos:
            try:
                df = pd.read_excel(caminho)
            except Exception as e:
                self._push_intercalacao_warning(f"RETIRADA: falha ao ler XLSX '{os.path.basename(caminho)}': {e}")
                continue
            cols = {c.lower(): c for c in df.columns}
            col_track = cols.get("tracking_number") or cols.get("tracking") or cols.get("tracking number")
            col_pedido = cols.get("order_sn") or cols.get("order") or cols.get("order sn") or cols.get("pedido")
            col_info = cols.get("product_info") or cols.get("product info") or cols.get("products") or cols.get("itens")
            if not col_pedido or not col_info:
                self._push_intercalacao_warning(
                    f"RETIRADA: XLSX '{os.path.basename(caminho)}' sem colunas esperadas (order_sn/product_info)."
                )
                continue
            for _, row in df.iterrows():
                pedido = str(row.get(col_pedido, "")).strip()
                track = str(row.get(col_track, "")).strip() if col_track else ""
                info = str(row.get(col_info, "")).strip()
                if not pedido and not track:
                    continue
                produtos = self._parse_retirada_product_info(info)
                if not produtos:
                    continue
                if pedido:
                    self._retirada_map_pedido[pedido] = produtos
                if track:
                    m = re.search(r"(BR\d{10,})", track, re.IGNORECASE)
                    key = (m.group(1).upper() if m else track.upper())
                    self._retirada_map_track[key] = produtos

    def _is_retirada_pelo_comprador_texto(self, texto: str) -> bool:
        if not texto:
            return False
        return bool(re.search(r"\bRETIRADA\s*(PELO)?\s*COMPRADOR\b", texto, re.IGNORECASE))

    def _extrair_pedido_do_texto(self, texto: str) -> str:
        if not texto:
            return ""
        m = re.search(r"\bPEDIDO\s*[:#]?\s*([A-Z0-9]{8,})\b", texto, re.IGNORECASE)
        return m.group(1).strip() if m else ""

    def _extrair_tracking_do_texto(self, texto: str) -> str:
        if not texto:
            return ""
        m = re.search(r"\b(BR\d{10,})\b", texto, re.IGNORECASE)
        return m.group(1).upper() if m else ""

    def _tentar_injetar_produtos_retirada(self, etiqueta: dict, texto_pagina: str):
        """
        Se etiqueta for de RETIRADA PELO COMPRADOR e nao tiver produtos,
        tenta buscar em retirada.xlsx por pedido ou tracking.
        """
        try:
            if not self._is_retirada_pelo_comprador_texto(texto_pagina):
                return
            dados = etiqueta.get("dados_xml") or {}
            produtos = dados.get("produtos") or []
            if produtos:
                return
            pedido = self._extrair_pedido_do_texto(texto_pagina)
            track = self._extrair_tracking_do_texto(texto_pagina)
            produtos_encontrados = []
            if pedido and getattr(self, "_retirada_map_pedido", None):
                produtos_encontrados = self._retirada_map_pedido.get(pedido, []) or []
            if not produtos_encontrados and track and getattr(self, "_retirada_map_track", None):
                produtos_encontrados = self._retirada_map_track.get(track, []) or []
            if produtos_encontrados:
                dados["produtos"] = produtos_encontrados
                dados["pedido"] = pedido
                dados["tracking"] = track
                dados["modalidade"] = "retirada"
                etiqueta["dados_xml"] = dados
            else:
                self._push_intercalacao_warning(
                    f"INTERCALAÇÃO RETIRADA FALHOU: sem match no retirada.xlsx (pedido={pedido or '-'} track={track or '-'})."
                )
        except Exception as e:
            self._push_intercalacao_warning(f"INTERCALAÇÃO RETIRADA ERRO: {e}")

    # ----------------------------------------------------------------
    # LEITURA DOS XMLs
    # ----------------------------------------------------------------
    def carregar_todos_xmls(self, pasta):
        """Carrega XMLs de TODOS os ZIPs da pasta."""
        zips = [f for f in os.listdir(pasta) if f.lower().endswith('.zip')]
        total = 0
        for z in zips:
            caminho = os.path.join(pasta, z)
            n = self._carregar_zip(caminho)
            total += n
        print(f"  Total: {total} XMLs carregados de {len(zips)} arquivo(s) ZIP")
        return total

    def _carregar_zip(self, caminho_zip):
        """Carrega XMLs de um ZIP."""
        print(f"  Carregando: {os.path.basename(caminho_zip)}")
        contador = 0
        with zipfile.ZipFile(caminho_zip, 'r') as zf:
            for nome in zf.namelist():
                if not nome.lower().endswith('.xml'):
                    continue
                try:
                    conteudo = zf.read(nome)
                    dados = self._parse_xml(conteudo)
                    if dados and dados.get('nf'):
                        nf = dados['nf']
                        # Se ja existe, manter o mais completo
                        if nf not in self.dados_xml:
                            self.dados_xml[nf] = dados
                            contador += 1
                except Exception:
                    pass
        print(f"    {contador} XMLs novos")
        return contador

    def _limpar_nome_emitente(self, nome_raw):
        """Limpa o nome do emitente para usar como nome de pasta."""
        # Remove numeros de CNPJ do inicio tipo "34.847.700 "
        nome = re.sub(r'^\d[\d.]+\s+', '', nome_raw)
        # Remove CPF tipo "11543563619"
        nome = re.sub(r'\s+\d{11}$', '', nome)
        # Limpa LTDA, MEI, etc
        nome = re.sub(r'\s+(LTDA|ME|MEI|EPP|EIRELI)\s*$', '', nome, flags=re.IGNORECASE)
        # Capitaliza
        nome = nome.strip().title()
        # Remove caracteres invalidos para nome de pasta
        nome = re.sub(r'[<>:"/\\|?*]', '', nome)
        return nome.strip() or 'Loja_Desconhecida'

    def _parse_xml(self, conteudo_xml):
        """Extrai dados relevantes do XML da NFe."""
        try:
            xml = conteudo_xml.decode('utf-8', errors='ignore')
            dados = {}

            def get(tag):
                m = re.search(f'<{tag}>([^<]+)</{tag}>', xml)
                return m.group(1).strip() if m else ''

            dados['nf'] = get('nNF')
            dados['serie'] = get('serie') or '1'

            dhEmi = get('dhEmi')
            if dhEmi:
                dt_part = dhEmi[:19]
                try:
                    dt = datetime.strptime(dt_part, '%Y-%m-%dT%H:%M:%S')
                    dados['data_emissao'] = dt.strftime('%d-%m-%Y %H:%M:%S')
                except ValueError:
                    dados['data_emissao'] = dhEmi[:10]
            else:
                dados['data_emissao'] = ''

            # Chave de acesso
            m = re.search(r'Id="NFe(\d+)"', xml)
            dados['chave'] = m.group(1) if m else ''

            # CNPJ e nome do emitente
            cnpj_m = re.search(r'<emit>.*?<CNPJ>([^<]+)</CNPJ>', xml, re.DOTALL)
            nome_m = re.search(r'<emit>.*?<xNome>([^<]+)</xNome>', xml, re.DOTALL)
            dados['cnpj_emitente'] = cnpj_m.group(1) if cnpj_m else ''
            nome_raw = nome_m.group(1) if nome_m else ''
            dados['nome_emitente'] = nome_raw

            # Registra mapeamento CNPJ -> nome limpo
            if dados['cnpj_emitente'] and dados['cnpj_emitente'] not in self.cnpj_nome:
                self.cnpj_nome[dados['cnpj_emitente']] = self._limpar_nome_emitente(nome_raw)

            # Produtos
            produtos = []
            for m in re.finditer(r'<det[^>]*>.*?<prod>(.*?)</prod>', xml, re.DOTALL):
                bloco = m.group(1)
                codigo = re.search(r'<cProd>([^<]+)</cProd>', bloco)
                descricao = re.search(r'<xProd>([^<]+)</xProd>', bloco)
                qtd = re.search(r'<qCom>([^<]+)</qCom>', bloco)

                produtos.append({
                    'codigo': codigo.group(1) if codigo else '',
                    'descricao': descricao.group(1) if descricao else '',
                    'qtd': qtd.group(1) if qtd else '1',
                })

            dados['produtos'] = produtos
            dados['total_itens'] = len(produtos)
            dados['total_qtd'] = sum(int(float(p['qtd'])) for p in produtos)

            return dados if dados.get('nf') else None
        except Exception:
            return None

    # ----------------------------------------------------------------
    # RECORTE DAS ETIQUETAS DO PDF DA SHOPEE
    # ----------------------------------------------------------------
    # ----------------------------------------------------------------
    # CENTRAL DO VENDEDOR (Shopee - DANFE + DECLARAÇÃO)
    # ----------------------------------------------------------------
    def _is_pdf_central_vendedor(self, caminho_pdf: str) -> bool:
        """Detecta PDF 'Central do Vendedor' (DANFE/Declaração)."""
        try:
            doc = fitz.open(caminho_pdf)
            for i in range(min(2, len(doc))):
                t = (doc[i].get_text("text") or "").upper()
                if "DANFE SIMPLIFICADO - ETIQUETA" in t or "DECLARAÇÃO DE CONTEÚDO" in t or "DECLARACAO DE CONTEUDO" in t:
                    doc.close()
                    return True
            doc.close()
        except Exception:
            pass
        return False

    def _extrair_cnpj_do_texto(self, texto: str) -> str:
        """Tenta extrair CNPJ (14 dígitos) do texto."""
        if not texto:
            return ""
        # formato 00.000.000/0000-00
        m = re.search(r"\b(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}\-?\d{2})\b", texto)
        if m:
            c = re.sub(r"\D", "", m.group(1))
            return c if len(c) == 14 else ""
        # 14 dígitos soltos
        m = re.search(r"\b(\d{14})\b", texto)
        if m:
            return m.group(1)
        return ""

    def _extrair_track_do_texto(self, texto: str) -> str:
        """Extrai tracking BR..."""
        m = re.search(r"\b(BR\d{10,})\b", texto or "", re.IGNORECASE)
        return (m.group(1).upper() if m else "")

    def _parse_declaracao_produtos(self, texto: str):
        """
        Extrai produtos da DECLARAÇÃO.
        Retorna lista no formato esperado pelo rodapé (codigo, variacao, qtd, descricao).
        """
        produtos = []
        if not texto:
            return produtos
        linhas = [l.strip() for l in (texto.splitlines() or []) if l.strip()]
        ini = -1
        for i, l in enumerate(linhas):
            u = l.upper()
            if ("CÓDIGO" in u or "CODIGO" in u) and ("DESCRI" in u or "PROD" in u):
                ini = i
                break
        if ini < 0:
            return produtos
        dados = linhas[ini + 1:]
        for l in dados:
            if l.upper().startswith("P.") or "TOTAL" in l.upper():
                break
            m = re.match(r"^\s*(\d+)\s+(\S+)\s+(.+)$", l)
            if not m:
                continue
            _, sku, resto = m.groups()
            nums = re.findall(r"(\d+[.,]?\d*)", resto)
            qtd = "1"
            if len(nums) >= 2:
                qtd = nums[-2]
            resto_limpo = resto
            if len(nums) >= 2:
                resto_limpo = re.sub(r"\s+" + re.escape(nums[-1]) + r"\s*$", "", resto_limpo)
                resto_limpo = re.sub(r"\s+" + re.escape(nums[-2]) + r"\s*$", "", resto_limpo)
            descricao = resto_limpo.strip()
            variacao = "-"
            mvar = re.search(r"(.+)\s+([^\s]+,[^\s]+)\s*$", descricao)
            if mvar:
                descricao = mvar.group(1).strip()
                vv = (mvar.group(2) or "").strip()
                variacao = vv if vv else "-"
            try:
                qtd_int = int(float(qtd.replace(",", ".")))
            except Exception:
                qtd_int = 1
            produtos.append({
                "codigo": sku.strip(),
                "variacao": variacao,
                "qtd": str(qtd_int),
                "descricao": (descricao[:80] if descricao else sku.strip()),
            })
        return produtos

    def _processar_pdf_central_vendedor_um(self, caminho_pdf: str):
        """
        Extrai etiquetas do PDF Central do Vendedor como ETIQUETAS NORMAIS:
        - NÃO usa tipo_especial
        - Preenche cnpj (pra separar_por_loja não ignorar)
        - Preenche dados_xml["produtos"] pra rodapé sair igual
        """
        etiquetas = []
        try:
            doc = fitz.open(caminho_pdf)
        except Exception as e:
            self._push_intercalacao_warning(f"[CENTRAL] Falha ao abrir PDF: {os.path.basename(caminho_pdf)} ({e})")
            return etiquetas

        paginas_danfe = []
        paginas_decl = []
        texto_pag = {}
        track_pag = {}
        cnpj_pag = {}

        for i in range(len(doc)):
            t = doc[i].get_text("text") or ""
            texto_pag[i] = t
            up = t.upper()
            if "DANFE SIMPLIFICADO - ETIQUETA" in up:
                paginas_danfe.append(i)
            if "DECLARAÇÃO DE CONTEÚDO" in up or "DECLARACAO DE CONTEUDO" in up:
                paginas_decl.append(i)
            track_pag[i] = self._extrair_track_do_texto(t)
            cnpj_pag[i] = self._extrair_cnpj_do_texto(t)

        produtos_por_track = defaultdict(list)
        cnpj_por_track = {}

        for i in paginas_decl:
            t = texto_pag.get(i, "")
            tr = track_pag.get(i, "")
            if not tr:
                continue
            prods = self._parse_declaracao_produtos(t)
            if prods:
                produtos_por_track[tr].extend(prods)
            c = cnpj_pag.get(i, "")
            if c:
                cnpj_por_track.setdefault(tr, c)

        # criar etiquetas DANFE
        for i in paginas_danfe:
            t = texto_pag.get(i, "")
            tr = track_pag.get(i, "")
            prods = produtos_por_track.get(tr, []) if tr else []

            # cnpj: tenta do DANFE; senão do track; senão fallback
            cnpj = cnpj_pag.get(i, "") or (cnpj_por_track.get(tr, "") if tr else "")
            if not cnpj:
                cnpj = "CENTRALVENDEDOR000000"

            etq = {
                "cnpj": cnpj,
                "nf": "?",
                "pagina": i,
                "pdf": caminho_pdf,
                "dados_xml": {
                    "produtos": prods,
                    "tracking": tr,
                },
                "tipo_especial": None,  # IMPORTANTÍSSIMO: fluxo normal (rodapé padrão)
            }

            # tentativa de RETIRADA (se tiver o texto)
            try:
                self._tentar_injetar_produtos_retirada(etq, t)
            except Exception:
                pass

            if not prods:
                self._push_intercalacao_warning(
                    f"[CENTRAL] Sem produtos (track={tr or '?'}) em {os.path.basename(caminho_pdf)} pág {i+1}"
                )

            etiquetas.append(etq)

        doc.close()
        return etiquetas

    def processar_central_vendedor(self, pasta_entrada: str):
        """Wrapper para compatibilidade com dashboard.py que chama este método."""
        etiquetas = []
        pdfs = [
            f for f in os.listdir(pasta_entrada)
            if f.lower().endswith(".pdf")
            and not f.startswith("_")
            and not f.lower().startswith("shein")
        ]
        for nome_pdf in pdfs:
            caminho_pdf = os.path.join(pasta_entrada, nome_pdf)
            if self._is_pdf_central_vendedor(caminho_pdf):
                extra = self._processar_pdf_central_vendedor_um(caminho_pdf)
                etiquetas.extend(extra)
        return etiquetas

    def _eh_pdf_shein(self, caminho_pdf):
        """Detecta se um PDF e do tipo Shein (paginas alternadas: etiqueta + DANFE).
        Retorna True se o PDF tem estrutura Shein.
        """
        try:
            doc = fitz.open(caminho_pdf)
            n_pags = len(doc)
            if n_pags < 2:
                doc.close()
                return False

            # Verificar primeiras 2 paginas: par=etiqueta Shein, impar=DANFE
            texto_p0 = doc[0].get_text()
            texto_p1 = doc[1].get_text()
            doc.close()

            p0_shein = self._eh_etiqueta_shein(texto_p0)
            p1_danfe = 'DANFE' in texto_p1.upper() and 'CHAVE' in texto_p1.upper()

            return p0_shein and p1_danfe
        except Exception:
            return False

    def carregar_todos_pdfs(self, pasta):
        """Carrega etiquetas de TODOS os PDFs da pasta (exceto especiais e Shein).
        Retorna (etiquetas_normais, etiquetas_cpf_detectadas, pdfs_shein_detectados).
        Etiquetas CPF detectadas automaticamente (sem NF real) sao separadas
        para processamento especial.
        PDFs Shein detectados por conteudo sao retornados como lista de caminhos.
        """
        especiais_lower = [p.lower() for p in self.PDFS_ESPECIAIS]
        pdfs = [f for f in os.listdir(pasta)
                if f.lower().endswith('.pdf')
                and not f.startswith('etiquetas_prontas')
                and not f.lower().startswith('lanim')  # CPF processado separadamente
                and f.lower() not in especiais_lower]

        # Primeiro, detectar PDFs Shein por conteudo para excluir do processamento normal
        pdfs_shein_detectados = []
        pdfs_normais = []
        for pdf_name in pdfs:
            caminho = os.path.join(pasta, pdf_name)
            if self._eh_pdf_shein(caminho):
                pdfs_shein_detectados.append(caminho)
                print(f"  Detectado como Shein: {pdf_name}")
            else:
                pdfs_normais.append(pdf_name)

        todas_etiquetas = []
        etiquetas_cpf_auto = []

        for pdf_name in pdfs_normais:
            caminho = os.path.join(pasta, pdf_name)

            # --- Central do Vendedor: NÃO PULAR! processar aqui mesmo ---
            if self._is_pdf_central_vendedor(caminho):
                extra_cv = self._processar_pdf_central_vendedor_um(caminho)
                if extra_cv:
                    todas_etiquetas.extend(extra_cv)
                else:
                    self._push_intercalacao_warning(f"[CENTRAL] Detectado mas extraiu 0 etiquetas: {os.path.basename(caminho)}")
                continue

            # Fluxo normal (parser existente)
            etqs = self._carregar_pdf(caminho)
            for etq in etqs:
                # Tentar injetar produtos de retirada no fluxo normal também
                try:
                    doc_tmp = fitz.open(caminho)
                    pg = etq.get('pagina', 0)
                    texto_p = doc_tmp[pg].get_text("text") if pg < len(doc_tmp) else ""
                    doc_tmp.close()
                except Exception:
                    texto_p = ""
                self._tentar_injetar_produtos_retirada(etq, texto_p)

                if etq.get('tipo_especial') == 'cpf':
                    etiquetas_cpf_auto.append(etq)
                else:
                    todas_etiquetas.append(etq)
        if etiquetas_cpf_auto:
            print(f"  CPF detectadas automaticamente: {len(etiquetas_cpf_auto)} etiquetas")
        if pdfs_shein_detectados:
            print(f"  Shein detectados automaticamente: {len(pdfs_shein_detectados)} PDF(s)")
        print(f"  Total: {len(todas_etiquetas)} etiquetas normais de {len(pdfs_normais)} PDF(s)")
        return todas_etiquetas, etiquetas_cpf_auto, pdfs_shein_detectados

    def _carregar_pdf(self, caminho_pdf):
        """Carrega e recorta etiquetas de um PDF."""
        print(f"  Carregando: {os.path.basename(caminho_pdf)}")
        doc = fitz.open(caminho_pdf)
        etiquetas = []

        for num_pag in range(len(doc)):
            pagina = doc[num_pag]
            etqs = self._recortar_pagina(pagina, caminho_pdf)
            etiquetas.extend(etqs)

        doc.close()
        print(f"    {len(etiquetas)} etiquetas")
        return etiquetas

    def _extrair_nf_quadrante(self, pagina, clip):
        """Extrai o numero da NF do texto dentro de um quadrante."""
        texto = pagina.get_text(clip=clip)

        m = re.search(r'Emiss.o:\n(\d+)\n', texto)
        if m:
            return m.group(1)

        # Padrao alternativo: "NF: 12345" (comum em etiquetas de retirada do comprador)
        m = re.search(r'NF:\s*(\d+)', texto)
        if m:
            return m.group(1)

        m = re.search(r'(\d{4,6})\n\d\n\d{2}-\d{2}-\d{4}', texto)
        if m:
            return m.group(1)

        return None

    def _extrair_chave_nfe(self, texto):
        """Extrai a chave de acesso da NFe (44 digitos) do texto da etiqueta."""
        m = re.search(r'(\d{44})', texto)
        return m.group(1) if m else ''

    def _extrair_nome_loja_remetente(self, texto):
        """Extrai o nome da loja do campo REMETENTE da etiqueta Shopee.

        Formato tipico da etiqueta Shopee:
            [dados destinatario]
            [tracking BR...]
            NOME_DA_LOJA          <-- queremos este
            [endereco remetente]
            [CEP remetente]

        Estrategia principal: encontrar o tracking code (BR + digitos + letra)
        e pegar a PRIMEIRA linha seguinte que parece um nome de loja.
        """
        def _eh_endereco_ou_cep(linha):
            """Retorna True se a linha parece ser endereco, CEP ou dado irrelevante."""
            l = linha.strip()
            if not l or len(l) < 3:
                return True
            # CEPs: 12345-678 ou 12345678 ou apenas numeros
            if re.match(r'^[\d\s.,-/]+$', l):
                return True
            if re.match(r'^\d{5}-?\d{3}', l):
                return True
            # Enderecos
            if re.match(r'^(Rua|Avenida|Travessa|Alameda|Estrada|Rodovia|Praca|Largo|R\.|Av\.|Rod\.|Est\.)\s', l, re.IGNORECASE):
                return True
            # Bairro / Complemento com numeros no meio: "Bloco A, 123" etc
            if re.match(r'^(Bloco|Lote|Quadra|Qd|Lt|Sl|Sala|Apto|Apt|Conj|Casa|Galpao|N[°o]?\s)', l, re.IGNORECASE):
                return True
            # Linha que e so "Cidade, Estado" ou "Cidade - UF" ou "UF"
            if re.match(r'^[A-Z]{2}$', l):
                return True
            # Formato "Cidade, Estado" ou "Cidade - UF" (ex: "Italva, Rio de Janeiro")
            if re.match(r'^[A-Za-z\s]+,\s*[A-Za-z\s]+$', l) and len(l.split(',')) == 2:
                parte2 = l.split(',')[1].strip()
                # Se a segunda parte parece um estado brasileiro
                estados = ['acre', 'alagoas', 'amapa', 'amazonas', 'bahia', 'ceara',
                    'distrito federal', 'espirito santo', 'goias', 'maranhao', 'mato grosso',
                    'mato grosso do sul', 'minas gerais', 'para', 'paraiba', 'parana',
                    'pernambuco', 'piaui', 'rio de janeiro', 'rio grande do norte',
                    'rio grande do sul', 'rondonia', 'roraima', 'santa catarina',
                    'sao paulo', 'sergipe', 'tocantins']
                if parte2.lower() in estados or len(parte2) == 2:
                    return True
            # Texto com CEP embutido
            if re.search(r'\d{5}-?\d{3}', l):
                return True
            # Palavras-chave de etiqueta
            if re.match(r'^(envio previsto|peso|volume|frete|destinat|remet|cep)', l, re.IGNORECASE):
                return True
            return False

        # Estrategia 1: Buscar apos tracking code tipo BR261920610412I
        m = re.search(r'BR\d{10,}[A-Z]\s*\n((?:[^\n]+\n){1,5})', texto)
        if m:
            linhas = [l.strip() for l in m.group(1).split('\n') if l.strip()]
            for linha in linhas:
                if not _eh_endereco_ou_cep(linha):
                    return linha

        # Estrategia 2: Buscar apos tracking generico (XX + digitos)
        m2 = re.search(r'[A-Z]{2}\d{9,}[A-Z]?\s*\n((?:[^\n]+\n){1,5})', texto)
        if m2:
            linhas = [l.strip() for l in m2.group(1).split('\n') if l.strip()]
            for linha in linhas:
                if not _eh_endereco_ou_cep(linha):
                    return linha

        return None

    def get_nome_loja(self, cnpj):
        """Retorna nome da loja: primeiro tenta cnpj_loja (Shopee), depois cnpj_nome (XML)."""
        nome = self.cnpj_loja.get(cnpj) or self.cnpj_nome.get(cnpj, 'Loja_Desconhecida')
        # Sanitizar para nome de pasta Windows (remover caracteres ilegais)
        nome = re.sub(r'[<>:"/\\|?*]', '', nome).strip().rstrip('.')
        return nome or 'Loja_Desconhecida'

    def remover_duplicatas(self, etiquetas):
        """Remove etiquetas com NF duplicada.
        Retorna (etiquetas_unicas, lista_de_duplicadas_removidas).
        """
        vistos = set()
        unicas = []
        duplicadas = []
        for etq in etiquetas:
            nf = etq.get('nf', '')
            if not nf:
                unicas.append(etq)
                continue
            if nf in vistos:
                duplicadas.append(etq)
            else:
                vistos.add(nf)
                unicas.append(etq)
        return unicas, duplicadas

    def _contar_etiquetas_regiao(self, pagina, clip):
        """Verifica se uma regiao contem uma etiqueta Shopee.
        Detecta por marcadores: 'Pedido:' ou 'REMETENTE' ou NF numerica.
        """
        texto = pagina.get_text(clip=clip).strip()
        if len(texto) < 10:
            return False
        # Marcadores de etiqueta Shopee
        tem_pedido = 'Pedido:' in texto or 'Pedido\n' in texto
        tem_remetente = 'REMETENTE' in texto
        tem_danfe = 'DANFE' in texto
        tem_nf = self._extrair_nf_quadrante(pagina, clip) is not None
        # Basta ter 1 marcador forte ou NF
        return tem_nf or tem_pedido or (tem_remetente and tem_danfe)

    def _detectar_layout_pagina(self, pagina):
        """Detecta quantas etiquetas ha na pagina analisando o conteudo.
        Retorna lista de clips (regioes) para recortar.
        Layouts possiveis:
          - 4 etiquetas (grid 2x2) - padrao Shopee para pedidos simples e multi-produto
          - 2 etiquetas (2 linhas, largura total) - formato alternativo
          - 1 etiqueta (pagina inteira) - fallback
        Deteccao baseada em marcadores de etiqueta (Pedido:, REMETENTE, DANFE), nao apenas NFs.
        Paginas pequenas (<= 420pt largura) sao sempre 1 etiqueta (tamanho de 1 quadrante A4).
        """
        rect = pagina.rect
        larg = rect.width
        alt = rect.height

        # Pagina pequena = 1 etiqueta (ex: 297x419, tamanho de 1 quadrante A4)
        # A4 = 595x842, metade = ~297x421. Se largura <= 420, e uma etiqueta individual.
        if larg <= 420:
            return [fitz.Rect(0, 0, larg, alt)]

        meio_x = larg / 2
        meio_y = alt / 2

        # Testar grid 2x2 primeiro (padrao Shopee)
        quadrantes_2x2 = [
            fitz.Rect(0, 0, meio_x, meio_y),
            fitz.Rect(meio_x, 0, larg, meio_y),
            fitz.Rect(0, meio_y, meio_x, alt),
            fitz.Rect(meio_x, meio_y, larg, alt),
        ]

        etiquetas_2x2 = sum(1 for clip in quadrantes_2x2 if self._contar_etiquetas_regiao(pagina, clip))

        # Se encontrou 2+ etiquetas no grid 2x2, usar este layout
        if etiquetas_2x2 >= 2:
            return quadrantes_2x2

        # Testar layout 2 etiquetas (metade superior + metade inferior, largura total)
        quadrantes_2x1 = [
            fitz.Rect(0, 0, larg, meio_y),
            fitz.Rect(0, meio_y, larg, alt),
        ]

        etiquetas_2x1 = sum(1 for clip in quadrantes_2x1 if self._contar_etiquetas_regiao(pagina, clip))

        if etiquetas_2x1 >= 2:
            return quadrantes_2x1

        # Se grid 2x2 achou pelo menos 1, usar 2x2 (pode ter quadrantes vazios)
        if etiquetas_2x2 >= 1:
            return quadrantes_2x2

        # Se 2x1 achou pelo menos 1, usar 2x1
        if etiquetas_2x1 >= 1:
            return quadrantes_2x1

        # Fallback: pagina inteira
        return [fitz.Rect(0, 0, larg, alt)]

    def _eh_etiqueta_shein(self, texto):
        """Verifica se o texto pertence a uma etiqueta Shein.
        Etiquetas Shein tem padroes distintos: PUDO-PGK, Ref.No:GSH, Ref.No:GC,
        codigos GC seguidos de numeros longos, etc.
        """
        # Etiqueta de envio Shein
        if 'PUDO-PGK' in texto or 'PUDO' in texto:
            return True
        if 'Ref.No:GSH' in texto or 'Ref.No:GC' in texto:
            return True
        # Codigo GC longo (tracking Shein)
        if re.search(r'GC\d{15,}', texto):
            return True
        return False

    def _detectar_tipo_etiqueta(self, texto, nf_encontrada=None):
        """Detecta o tipo de etiqueta pelo conteudo do texto.
        Retorna: 'retirada', 'shein', 'cnpj' ou 'cpf'

        Criterios:
        - 'retirada': contem "RETIRADA PELO COMPRADOR"
        - 'shein': tem marcadores Shein (PUDO-PGK, Ref.No:GSH, etc.)
        - 'cnpj': tem DANFE SIMPLIFICADO E tem NF numerica real
        - 'cpf': tem DANFE SIMPLIFICADO mas SEM NF numerica (loja CPF/pessoa fisica)
                  OU nao tem DANFE SIMPLIFICADO (declaracao de conteudo)
        """
        texto_upper = texto.upper()
        if 'RETIRADA PELO' in texto_upper and 'COMPRADOR' in texto_upper:
            return 'retirada'
        # Detectar Shein antes de CNPJ/CPF
        if self._eh_etiqueta_shein(texto):
            return 'shein'
        if 'DANFE SIMPLIFICADO' in texto_upper:
            # Tem DANFE SIMPLIFICADO, mas tem NF real?
            if nf_encontrada and nf_encontrada.isdigit():
                return 'cnpj'
            # Sem NF real = loja CPF (pessoa fisica)
            return 'cpf'
        return 'cpf'

    def _recortar_pagina(self, pagina, caminho_pdf):
        """Recorta etiquetas de uma pagina, detectando automaticamente o layout."""
        quadrantes = self._detectar_layout_pagina(pagina)

        etiquetas = []
        for idx, clip in enumerate(quadrantes):
            # Verificar se o quadrante tem conteudo (nao esta vazio)
            texto_quad = pagina.get_text(clip=clip).strip()
            if len(texto_quad) < 10:
                continue  # Quadrante vazio, pular

            # Extrair NF primeiro (necessario para detectar tipo)
            nf = self._extrair_nf_quadrante(pagina, clip)

            # Detectar tipo de etiqueta (passa nf para distinguir CNPJ vs CPF)
            tipo_etiqueta = self._detectar_tipo_etiqueta(texto_quad, nf_encontrada=nf)

            # Gerar etiqueta MESMO sem NF - usar identificador sintetico (incluir nome PDF para unicidade)
            if nf is None:
                pdf_id = os.path.splitext(os.path.basename(caminho_pdf))[0].replace(' ', '_')
                nf = f"SEM_NF_{pdf_id}_p{pagina.number}_q{idx}"
                dados_nf = {}
                print(f"    Pag {pagina.number} Q{idx}: NF nao encontrada (tipo={tipo_etiqueta})")
            else:
                dados_nf = {}

            # Extrair order_sn para usar como NF em etiquetas CPF
            order_sn_txt = self._extrair_pedido_texto(texto_quad)

            # Para etiquetas CPF, usar order_sn como identificador (nao tem NF real)
            if tipo_etiqueta == 'cpf' and order_sn_txt:
                nf = order_sn_txt

            # FONTE PRIMARIA: XLSX (buscar por order_sn ou tracking)
            if self.dados_xlsx_global:
                dados_xlsx, order_sn_xlsx = self._buscar_dados_xlsx(texto_quad)
                if dados_xlsx:
                    dados_nf = {
                        'nf': nf,
                        'serie': '',
                        'data_emissao': '',
                        'chave': self._extrair_chave_nfe(texto_quad),
                        'cnpj_emitente': '',
                        'nome_emitente': '',
                        'produtos': dados_xlsx['produtos'],
                        'total_itens': dados_xlsx['total_itens'],
                        'total_qtd': dados_xlsx['total_qtd'],
                        'fonte_dados': 'xlsx',
                    }
                    print(f"    Pag {pagina.number} Q{idx}: Dados XLSX (order_sn={order_sn_xlsx})")

            # FALLBACK: XML (se XLSX nao encontrou produtos) - apenas para CNPJ
            if tipo_etiqueta == 'cnpj' and not dados_nf.get('produtos') and nf in self.dados_xml:
                dados_nf = self.dados_xml.get(nf, {})

            sku = ''
            num_produtos = 1
            cnpj = dados_nf.get('cnpj_emitente', '')
            if dados_nf.get('produtos'):
                sku = dados_nf['produtos'][0].get('codigo', '')
                num_produtos = len(dados_nf['produtos'])

            # Extrair nome da loja do REMETENTE do texto da etiqueta
            nome_loja = self._extrair_nome_loja_remetente(texto_quad)
            if not cnpj:
                if nome_loja:
                    if tipo_etiqueta == 'cpf':
                        # Loja CPF: prefixo CPF_ para agrupar separadamente
                        cnpj_sintetico = f"CPF_{re.sub(r'[^A-Za-z0-9]', '_', nome_loja)}"
                    else:
                        cnpj_sintetico = f"LOJA_{re.sub(r'[^A-Za-z0-9]', '_', nome_loja)}"
                    cnpj = cnpj_sintetico
                    if cnpj not in self.cnpj_loja:
                        self.cnpj_loja[cnpj] = nome_loja
                        self.cnpj_nome[cnpj] = nome_loja
            elif cnpj not in self.cnpj_loja:
                if nome_loja:
                    self.cnpj_loja[cnpj] = nome_loja

            etiquetas.append({
                'nf': nf,
                'sku': sku,
                'num_produtos': num_produtos,
                'cnpj': cnpj,
                'clip': clip,
                'pagina_idx': pagina.number,
                'caminho_pdf': caminho_pdf,
                'dados_xml': dados_nf,
                'tipo_especial': tipo_etiqueta if tipo_etiqueta != 'cnpj' else None,
            })

        return etiquetas

    # ----------------------------------------------------------------
    # SEPARACAO POR LOJA
    # ----------------------------------------------------------------
    def separar_por_loja(self, etiquetas):
        """Separa etiquetas por CNPJ do emitente (loja)."""
        lojas = defaultdict(list)
        sem_loja = []

        for etq in etiquetas:
            cnpj = etq.get('cnpj', '')
            if cnpj:
                lojas[cnpj].append(etq)
            else:
                sem_loja.append(etq)

        if sem_loja:
            lojas['SEM_CNPJ'] = sem_loja

        return dict(lojas)

    # ----------------------------------------------------------------
    # ORDENACAO
    # ----------------------------------------------------------------
    def _ordenar_etiquetas(self, etiquetas):
        """Ordena etiquetas na mesma ordem do resumo XLSX:
        SKU > Cor > Numero para unitarias (total_qtd=1),
        etiquetas com total_qtd > 1 vao para o final ordenadas por qtd crescente.
        """
        def _chave_etiqueta(etq):
            """Extrai (sku, cor, numero) do primeiro produto da etiqueta."""
            dados = etq.get('dados_xml', {})
            produtos = dados.get('produtos', [])
            if produtos:
                sku = produtos[0].get('codigo', '')
                variacao = produtos[0].get('variacao', '')
            else:
                sku = etq.get('sku', '')
                variacao = ''
            # Separar variacao em cor e numero
            partes = re.split(r',', variacao, maxsplit=1)
            if len(partes) == 2:
                cor = partes[0].strip()
                num_str = partes[1].strip()
            else:
                cor = variacao.strip()
                num_str = ''
            # Extrair valor numerico para ordenacao correta (35 antes de 36)
            m = re.search(r'(\d+)', num_str)
            num_val = int(m.group(1)) if m else 99999
            return (sku, cor, num_val, num_str)

        simples = []
        multiplos = []
        for e in etiquetas:
            total_qtd = e.get('dados_xml', {}).get('total_qtd', 1)
            if total_qtd > 1:
                multiplos.append(e)
            else:
                simples.append(e)

        # Simples: SKU > Cor > Numero (mesma ordem do resumo XLSX)
        simples.sort(key=_chave_etiqueta)

        # Multiplos: por quantidade total crescente (maior qtd mais ao final)
        multiplos.sort(key=lambda e: (
            e.get('dados_xml', {}).get('total_qtd', 1),
            _chave_etiqueta(e)
        ))

        return simples + multiplos, len(simples), len(multiplos)

    @staticmethod
    def _ordenar_produtos(produtos):
        """Ordena produtos na tabela: qtd=1 por SKU > Cor > Numero, qtd>1 no final por qtd crescente.

        A variacao costuma vir como "Cor,Tamanho" (ex: "Preto,35") ou "Cor/Tamanho".
        """
        def _separar_cor_numero(variacao):
            """Separa variacao em (cor, numero) para ordenacao."""
            if not variacao:
                return ('', '')
            # Tentar separar por virgula primeiro, depois barra
            partes = re.split(r'[,/]', variacao, maxsplit=1)
            if len(partes) == 2:
                return (partes[0].strip(), partes[1].strip())
            return (variacao.strip(), '')

        def _numero_sort_key(num_str):
            """Converte numero/tamanho para valor numerico para ordenacao correta."""
            # Extrair primeiro numero encontrado (ex: "BR39/40" -> 39, "35" -> 35)
            m = re.search(r'(\d+)', num_str)
            if m:
                return (0, int(m.group(1)))
            return (1, num_str)  # sem numero vai depois

        unitarios = []
        multiplos = []
        for prod in produtos:
            qtd = int(float(prod.get('qtd', '1')))
            if qtd > 1:
                multiplos.append(prod)
            else:
                unitarios.append(prod)

        # Unitarios: ordenar por SKU > Cor > Numero
        unitarios.sort(key=lambda p: (
            p.get('codigo', ''),
            _separar_cor_numero(p.get('variacao', ''))[0],
            _numero_sort_key(_separar_cor_numero(p.get('variacao', ''))[1]),
        ))

        # Multiplos: ordenar por quantidade crescente (maior qtd mais ao final)
        multiplos.sort(key=lambda p: int(float(p.get('qtd', '1'))))

        return unitarios + multiplos

    # ----------------------------------------------------------------
    # GERACAO DO CODIGO DE BARRAS
    # ----------------------------------------------------------------
    def _gerar_barcode_svg(self, chave):
        """Gera um codigo de barras Code128 como SVG em bytes."""
        code128 = barcode.get('code128', chave, writer=SVGWriter())
        buf = io.BytesIO()
        code128.write(buf, options={
            'module_width': 0.2,
            'module_height': 8,
            'write_text': False,
            'quiet_zone': 1,
        })
        return buf.getvalue()

    # ----------------------------------------------------------------
    # GERACAO DO PDF FINAL (POR LOJA)
    # ----------------------------------------------------------------
    def gerar_pdf_loja(self, etiquetas, caminho_saida):
        """Gera o PDF final para uma loja."""
        etiquetas_ord, n_simples, n_multi = self._ordenar_etiquetas(etiquetas)

        # Agrupar etiquetas por PDF de origem
        pdfs_usados = set()
        for etq in etiquetas_ord:
            pdfs_usados.add(etq['caminho_pdf'])

        # Abrir todos os PDFs necessarios
        docs_abertos = {}
        for pdf_path in pdfs_usados:
            docs_abertos[pdf_path] = fitz.open(pdf_path)

        doc_saida = fitz.open()
        area_util_larg = self.LARGURA_PT - self.MARGEM_ESQUERDA - self.MARGEM_DIREITA

        com_xml = 0
        sem_xml = 0

        for idx, etq in enumerate(etiquetas_ord):
            nf = etq['nf']
            clip = etq['clip']
            pag_idx = etq['pagina_idx']
            dados = etq.get('dados_xml', {})
            numero_ordem = idx + 1
            pdf_path = etq['caminho_pdf']
            doc_entrada = docs_abertos[pdf_path]

            nova_pag = doc_saida.new_page(
                width=self.LARGURA_PT,
                height=self.ALTURA_PT
            )

            quad_larg = clip.width
            quad_alt = clip.height
            escala = area_util_larg / quad_larg
            alt_etiqueta = quad_alt * escala

            # Calcular espaco necessario para tabela de produtos
            num_prods = len(dados.get('produtos', []))
            tem_chave = bool(dados.get('chave'))
            tem_dados_produto = num_prods > 0
            if tem_dados_produto:
                # Espaco necessario: barcode(37 se tem chave) + cabecalho(20) + linhas(12 cada) + margem(15)
                fs_dest = int(round(self.fonte_produto * 1.5))
                line_h = fs_dest + 2
                espaco_barcode = 37 if tem_chave else 0
                espaco_tabela = espaco_barcode + 20 + (min(num_prods, 10) * line_h) + 15
                # Limitar altura da etiqueta para garantir espaco
                alt_max = self.ALTURA_PT - self.MARGEM_TOPO - self.MARGEM_INFERIOR - espaco_tabela
                if alt_etiqueta > alt_max:
                    alt_etiqueta = max(alt_max, self.ALTURA_PT * 0.45)  # minimo 45% da pagina

            dest_rect = fitz.Rect(
                self.MARGEM_ESQUERDA,
                self.MARGEM_TOPO,
                self.LARGURA_PT - self.MARGEM_DIREITA,
                self.MARGEM_TOPO + alt_etiqueta
            )

            nova_pag.show_pdf_page(dest_rect, doc_entrada, pag_idx, clip=clip)

            if tem_dados_produto:
                y_inicio = self.MARGEM_TOPO + alt_etiqueta + 2
                prox_prod = self._desenhar_secao_produtos(nova_pag, dados, y_inicio)
                com_xml += 1

                # Gerar paginas de continuacao se sobraram produtos
                cont_num = 1
                while prox_prod < num_prods:
                    pag_cont = doc_saida.new_page(
                        width=self.LARGURA_PT,
                        height=self.ALTURA_PT
                    )
                    # Cabecalho da continuacao
                    pag_cont.insert_text(
                        (self.MARGEM_ESQUERDA + 2, self.MARGEM_TOPO + 12),
                        f"CONTINUACAO - NF: {nf}",
                        fontsize=8, fontname="hebo", color=(0, 0, 0)
                    )
                    pag_cont.draw_line(
                        (self.MARGEM_ESQUERDA, self.MARGEM_TOPO + 16),
                        (self.LARGURA_PT - self.MARGEM_DIREITA, self.MARGEM_TOPO + 16),
                        color=(0, 0, 0), width=0.5
                    )
                    # Desenhar produtos restantes
                    prox_prod = self._desenhar_secao_produtos(
                        pag_cont, dados, self.MARGEM_TOPO + 20, prod_inicio=prox_prod
                    )
                    # Numero de ordem na continuacao
                    pag_cont.insert_text(
                        (self.MARGEM_ESQUERDA + 2, self.ALTURA_PT - self.MARGEM_INFERIOR - 8),
                        f"p.{numero_ordem} (cont.{cont_num})",
                        fontsize=9, fontname="hebo", color=(0.4, 0.4, 0.4)
                    )
                    cont_num += 1
            else:
                sem_xml += 1

            # Numero de ordem (subido para nao cortar na impressao)
            nova_pag.insert_text(
                (self.MARGEM_ESQUERDA + 2, self.ALTURA_PT - self.MARGEM_INFERIOR - 8),
                f"p.{numero_ordem}",
                fontsize=9,
                fontname="hebo",
                color=(0.4, 0.4, 0.4)
            )

        # Fechar docs de entrada
        for doc in docs_abertos.values():
            doc.close()

        total = len(doc_saida)
        doc_saida.save(caminho_saida)
        doc_saida.close()

        return total, n_simples, n_multi, com_xml, sem_xml

    def _desenhar_secao_produtos(self, pagina, dados, y_inicio, prod_inicio=0):
        """Desenha a secao de codigo de barras + tabela de produtos abaixo da etiqueta.
        prod_inicio: indice do primeiro produto a desenhar (para continuacao).
        Retorna indice do proximo produto nao desenhado (len(produtos) se todos couberam).
        """
        preto = (0, 0, 0)
        fonte = "helv"
        fonte_bold = "hebo"
        margem_esq = self.MARGEM_ESQUERDA
        margem_dir = self.MARGEM_DIREITA
        larg = self.LARGURA_PT
        fs = self.fonte_produto
        fs_destaque = int(round(fs * 1.5))  # 50% maior para SKU
        fs_qtd = int(round(fs_destaque * 1.5))  # quantidade 50% maior que destaque
        line_h = fs_qtd + 2  # espacamento acomoda a fonte da quantidade

        nf = dados.get('nf', '')
        chave = dados.get('chave', '')
        produtos = self._ordenar_produtos(dados.get('produtos', []))
        total_itens = dados.get('total_itens', len(produtos))
        total_qtd = dados.get('total_qtd', sum(int(float(p.get('qtd', 1))) for p in produtos))

        y = y_inicio

        # --- Codigo de barras da chave de acesso (so na primeira pagina) ---
        if chave and prod_inicio == 0:
            try:
                svg_bytes = self._gerar_barcode_svg(chave)
                barcode_rect = fitz.Rect(
                    margem_esq + 5, y,
                    larg - margem_dir - 5, y + 35
                )
                pagina.insert_image(barcode_rect, stream=svg_bytes)
                y += 37
            except Exception:
                y += 5

        # --- Tabela de produtos ---
        col_codigo = margem_esq + 2
        col_prod = margem_esq + 95
        col_qtd = larg - margem_dir - 25

        # Linha superior da tabela
        pagina.draw_line(
            (margem_esq, y), (larg - margem_dir, y),
            color=preto, width=0.8
        )
        y_tabela_topo = y
        y += line_h

        # Cabecalho - depende do modo de exibicao
        modo = getattr(self, 'exibicao_produto', 'sku')

        if modo == 'titulo':
            header_col1 = "PRODUTO"
        elif modo == 'ambos':
            header_col1 = "CODIGO"
        else:
            header_col1 = "CODIGO"

        continuacao_txt = f" (cont. {prod_inicio + 1}-)" if prod_inicio > 0 else ""
        pagina.insert_text(
            (col_codigo, y), header_col1,
            fontsize=fs, fontname=fonte_bold, color=preto
        )

        header_prod = f"VAR. (NF: {nf} T-ITENS: {total_itens} T-QUANT: {total_qtd}){continuacao_txt}"
        pagina.insert_text(
            (col_prod, y), header_prod,
            fontsize=fs, fontname=fonte_bold, color=preto
        )

        pagina.insert_text(
            (col_qtd, y), "Q.",
            fontsize=fs, fontname=fonte_bold, color=preto
        )

        y += 2
        pagina.draw_line(
            (margem_esq, y), (larg - margem_dir, y),
            color=preto, width=0.5
        )
        y += line_h

        # Limite inferior
        y_limite = self.ALTURA_PT - self.MARGEM_INFERIOR - 10

        # Linhas de produtos (sem limite fixo, respeita y_limite)
        ultimo_desenhado = prod_inicio
        for i_abs in range(prod_inicio, len(produtos)):
            prod = produtos[i_abs]
            codigo = prod.get('codigo', '')
            descricao = prod.get('descricao', '')
            variacao = prod.get('variacao', '')
            qtd = str(int(float(prod.get('qtd', '1'))))

            if y + line_h > y_limite:
                break

            if modo == 'titulo':
                # Coluna principal: titulo/descricao
                texto_principal = descricao or codigo
                max_chars = 40
                if len(texto_principal) > max_chars:
                    texto_principal = texto_principal[:max_chars - 2] + '..'
                pagina.insert_text(
                    (col_codigo, y), texto_principal,
                    fontsize=fs_destaque, fontname=fonte_bold, color=preto
                )
                # Coluna 2: variacao (cor/tamanho)
                if variacao:
                    var_trunc = variacao[:30] + '..' if len(variacao) > 30 else variacao
                    pagina.insert_text(
                        (col_prod, y), var_trunc,
                        fontsize=fs_destaque, fontname=fonte_bold, color=preto
                    )
            elif modo == 'ambos':
                # Coluna 1: codigo, Coluna 2: descricao + variacao
                pagina.insert_text(
                    (col_codigo, y), codigo,
                    fontsize=fs_destaque, fontname=fonte_bold, color=preto
                )
                texto2 = variacao or descricao
                max_desc = 30
                if len(texto2) > max_desc:
                    texto2 = texto2[:max_desc - 2] + '..'
                pagina.insert_text(
                    (col_prod, y), texto2,
                    fontsize=fs_destaque, fontname=fonte_bold, color=preto
                )
            else:
                # Modo SKU (padrao): codigo + variacao
                pagina.insert_text(
                    (col_codigo, y), codigo,
                    fontsize=fs_destaque, fontname=fonte_bold, color=preto
                )
                texto2 = variacao or "-"
                if len(texto2) > 30:
                    texto2 = texto2[:28] + '..'
                pagina.insert_text(
                    (col_prod, y), texto2,
                    fontsize=fs_destaque, fontname=fonte_bold, color=preto
                )

            pagina.insert_text(
                (col_qtd, y), qtd,
                fontsize=fs_qtd, fontname=fonte_bold, color=preto
            )
            y += line_h
            ultimo_desenhado = i_abs + 1

            # Linha divisoria entre produtos (exceto apos o ultimo)
            if i_abs < len(produtos) - 1 and y + line_h <= y_limite:
                pagina.draw_line(
                    (margem_esq, y - 1), (larg - margem_dir, y - 1),
                    color=(0.6, 0.6, 0.6), width=0.3
                )

        # Linha inferior da tabela
        pagina.draw_line(
            (margem_esq, y), (larg - margem_dir, y),
            color=preto, width=0.8
        )

        # Linhas verticais da tabela
        y_top = y_tabela_topo + 5
        pagina.draw_line(
            (col_prod - 5, y_top), (col_prod - 5, y),
            color=preto, width=0.5
        )
        pagina.draw_line(
            (col_qtd - 5, y_top), (col_qtd - 5, y),
            color=preto, width=0.5
        )

        return ultimo_desenhado

    # ----------------------------------------------------------------
    # ETIQUETAS ESPECIAIS: RETIRADA DO COMPRADOR (BEKA) E CPF
    # ----------------------------------------------------------------

    # Nomes dos PDFs especiais (nao processados no grid 2x2)
    PDFS_ESPECIAIS = ['lanim.pdf', 'shein crua.pdf', 'shein.pdf']
    # CNPJ e nome fixos para etiquetas CPF (sem XML/DANFE)
    LANIM_CNPJ = 'LANIM_CPF'
    LANIM_NOME = 'CPF'

    def carregar_xlsx_pedidos(self, caminho_xlsx):
        """Carrega dados de pedidos do XLSX (lanim2.xlsx) para etiquetas CPF.
        Retorna dict: order_sn -> {produtos: [...], total_itens, total_qtd}
        """
        import openpyxl as xl
        wb = xl.load_workbook(caminho_xlsx)
        ws = wb.active
        dados_pedidos = {}

        # Descobrir indices das colunas pelo cabecalho
        cabecalho = {}
        for idx, cell in enumerate(ws[1]):
            if cell.value:
                cabecalho[str(cell.value).strip().lower()] = idx

        col_order = cabecalho.get('order_sn', None)
        col_info = cabecalho.get('product_info', None)

        if col_order is None or col_info is None:
            print("  AVISO: Colunas order_sn ou product_info nao encontradas no XLSX")
            wb.close()
            return dados_pedidos

        for row in ws.iter_rows(min_row=2, max_col=ws.max_column, values_only=True):
            if row is None or len(row) <= max(col_order, col_info):
                continue
            order_sn = str(row[col_order] or '').strip()
            product_info = str(row[col_info] or '').strip()

            if not order_sn or not product_info:
                continue

            # Parsear product_info: pode ter multiplos blocos [N]
            produtos = []
            # Dividir por blocos [N]
            blocos = re.split(r'\[\d+\]\s*', product_info)
            for bloco in blocos:
                if not bloco.strip():
                    continue

                # Extrair Parent SKU
                m_sku = re.search(r'Parent SKU Reference No\.:\s*([^;]+)', bloco)
                codigo = m_sku.group(1).strip() if m_sku else ''
                # Fallback: SKU Reference No.
                if not codigo:
                    m_sku2 = re.search(r'SKU Reference No\.:\s*([^;]+)', bloco)
                    codigo = m_sku2.group(1).strip() if m_sku2 else ''

                # Extrair quantidade
                m_qtd = re.search(r'Quantity:\s*(\d+)', bloco)
                qtd = m_qtd.group(1) if m_qtd else '1'

                # Extrair nome do produto
                m_nome = re.search(r'Product Name:\s*([^;]+)', bloco)
                descricao = m_nome.group(1).strip() if m_nome else ''

                # Extrair Variation Name (usado para etiquetas CPF)
                m_var = re.search(r'Variation Name:\s*([^;]+)', bloco)
                variacao = m_var.group(1).strip() if m_var else ''

                if codigo or descricao or variacao:
                    produtos.append({
                        'codigo': codigo,
                        'descricao': descricao,
                        'variacao': variacao,
                        'qtd': qtd,
                    })

            # Acumular apenas produtos NOVOS se o mesmo order_sn aparecer em multiplas linhas
            if order_sn in dados_pedidos:
                existentes = dados_pedidos[order_sn]['produtos']
                chaves_existentes = set()
                for p in existentes:
                    chaves_existentes.add((p.get('codigo', ''), p.get('descricao', ''), p.get('variacao', '')))
                for p in produtos:
                    chave_p = (p.get('codigo', ''), p.get('descricao', ''), p.get('variacao', ''))
                    if chave_p not in chaves_existentes:
                        existentes.append(p)
                        chaves_existentes.add(chave_p)
                dados_pedidos[order_sn]['total_itens'] = len(existentes)
                dados_pedidos[order_sn]['total_qtd'] = sum(
                    int(float(p.get('qtd', 1))) for p in existentes
                )
            else:
                total_qtd = sum(int(float(p.get('qtd', 1))) for p in produtos)
                dados_pedidos[order_sn] = {
                    'produtos': produtos,
                    'total_itens': len(produtos),
                    'total_qtd': total_qtd,
                }

        wb.close()
        print(f"  XLSX: {len(dados_pedidos)} pedidos carregados")
        return dados_pedidos

    def _parsear_product_info(self, product_info):
        """Parseia o campo product_info do XLSX da Shopee.
        Retorna lista de produtos: [{codigo, descricao, variacao, qtd}, ...]
        """
        produtos = []
        blocos = re.split(r'\[\d+\]\s*', product_info)
        for bloco in blocos:
            if not bloco.strip():
                continue

            m_sku = re.search(r'Parent SKU Reference No\.:\s*([^;]+)', bloco)
            codigo = m_sku.group(1).strip() if m_sku else ''
            if not codigo:
                m_sku2 = re.search(r'SKU Reference No\.:\s*([^;]+)', bloco)
                codigo = m_sku2.group(1).strip() if m_sku2 else ''

            m_qtd = re.search(r'Quantity:\s*(\d+)', bloco)
            qtd = m_qtd.group(1) if m_qtd else '1'

            m_nome = re.search(r'Product Name:\s*([^;]+)', bloco)
            descricao = m_nome.group(1).strip() if m_nome else ''

            m_var = re.search(r'Variation Name:\s*([^;]+)', bloco)
            variacao = m_var.group(1).strip() if m_var else ''

            if codigo or descricao or variacao:
                produtos.append({
                    'codigo': codigo,
                    'descricao': descricao,
                    'variacao': variacao,
                    'qtd': qtd,
                })

        return produtos

    def carregar_todos_xlsx(self, pasta):
        """Carrega dados de TODOS os XLSX da pasta para fallback quando XML nao existe.
        Popula self.dados_xlsx_global (order_sn -> dados) e
        self.dados_xlsx_tracking (tracking -> order_sn).
        """
        import openpyxl as xl

        xlsx_files = [f for f in os.listdir(pasta)
                      if f.lower().endswith(('.xlsx', '.xls'))
                      and not f.startswith('_')
                      and not f.startswith('~')
                      and f != 'planilha_custos.xlsx']

        if not xlsx_files:
            return

        for xlsx_nome in sorted(xlsx_files):
            caminho = os.path.join(pasta, xlsx_nome)
            try:
                wb = xl.load_workbook(caminho, read_only=False)
                ws = wb.active

                # Descobrir indices das colunas pelo cabecalho
                cabecalho = {}
                for idx, cell in enumerate(ws[1]):
                    if cell.value:
                        cabecalho[str(cell.value).strip().lower()] = idx

                idx_tracking = cabecalho.get('tracking_number', -1)
                idx_order = cabecalho.get('order_sn', -1)
                idx_product = cabecalho.get('product_info', -1)

                if idx_order == -1 or idx_product == -1:
                    wb.close()
                    continue  # XLSX sem colunas relevantes

                count = 0
                for row in ws.iter_rows(min_row=2, values_only=True):
                    if row is None:
                        continue
                    order_sn = str(row[idx_order] or '').strip() if len(row) > idx_order else ''
                    tracking = str(row[idx_tracking] or '').strip() if idx_tracking >= 0 and len(row) > idx_tracking else ''
                    product_info = str(row[idx_product] or '').strip() if len(row) > idx_product else ''

                    if not order_sn or not product_info:
                        continue

                    produtos = self._parsear_product_info(product_info)

                    if order_sn not in self.dados_xlsx_global:
                        self.dados_xlsx_global[order_sn] = {
                            'produtos': produtos,
                            'total_itens': len(produtos),
                            'total_qtd': sum(int(float(p.get('qtd', 1))) for p in produtos),
                        }
                    else:
                        # Adicionar apenas produtos que ainda nao existem (evita duplicacao)
                        existentes = self.dados_xlsx_global[order_sn]['produtos']
                        chaves_existentes = set()
                        for p in existentes:
                            chaves_existentes.add((p.get('codigo', ''), p.get('descricao', ''), p.get('variacao', '')))
                        for p in produtos:
                            chave_p = (p.get('codigo', ''), p.get('descricao', ''), p.get('variacao', ''))
                            if chave_p not in chaves_existentes:
                                existentes.append(p)
                                chaves_existentes.add(chave_p)
                        self.dados_xlsx_global[order_sn]['total_itens'] = len(existentes)
                        self.dados_xlsx_global[order_sn]['total_qtd'] = sum(
                            int(float(p.get('qtd', 1))) for p in existentes)

                    if tracking:
                        self.dados_xlsx_tracking[tracking] = order_sn

                    count += 1

                wb.close()
                if count > 0:
                    print(f"    {xlsx_nome}: {count} pedidos")

            except Exception as e:
                print(f"    XLSX erro: {xlsx_nome} - {e}")

        print(f"  Total XLSX: {len(self.dados_xlsx_global)} pedidos, {len(self.dados_xlsx_tracking)} trackings")

        # Carregar retirada.xlsx para intercalação de produtos de retirada
        self.carregar_retirada_xlsx(pasta)

    def _extrair_pedido_texto(self, texto):
        """Extrai o numero do pedido (order_sn) do texto da etiqueta."""
        # Padrao Shopee: algo como 2602061BMTVXW0 (alfanumerico ~15 chars)
        m = re.search(r'Pedido[:\s]*\n?([A-Z0-9]{12,20})', texto, re.IGNORECASE)
        if m:
            return m.group(1).strip()
        # Padrao order_sn Shopee: YYMMDD + alfanumerico (ex: 260210A88XUUY8)
        # Aparece em linha propria, 12-16 chars, comeca com 6 digitos (data)
        # Nao confundir com chave NFe (44 digitos puros) nem tracking (BR...)
        m = re.search(r'\n(\d{6}[A-Z0-9]{6,10})\n', texto)
        if m:
            return m.group(1).strip()
        return None

    def _extrair_tracking_quadrante(self, texto):
        """Extrai o tracking number (BR...) do texto de um quadrante."""
        m = re.search(r'(BR\w{10,20})', texto)
        return m.group(1) if m else None

    def _buscar_dados_xlsx(self, texto_quadrante):
        """Busca dados do XLSX usando order_sn ou tracking extraidos do texto.
        Retorna (dados_pedido, order_sn) ou (None, None).
        """
        if not self.dados_xlsx_global:
            return None, None

        # Tentar por order_sn
        order_sn = self._extrair_pedido_texto(texto_quadrante)
        if order_sn and order_sn in self.dados_xlsx_global:
            return self.dados_xlsx_global[order_sn], order_sn

        # Tentar por tracking -> order_sn
        tracking = self._extrair_tracking_quadrante(texto_quadrante)
        if tracking:
            # Match exato primeiro
            if tracking in self.dados_xlsx_tracking:
                order_sn_via_tracking = self.dados_xlsx_tracking[tracking]
                if order_sn_via_tracking in self.dados_xlsx_global:
                    return self.dados_xlsx_global[order_sn_via_tracking], order_sn_via_tracking

            # Match parcial: XLSX pode ter sufixo extra no tracking
            # (ex: etiqueta=BR2699130341698, XLSX=BR2699130341698SPXLM13556762)
            for tracking_xlsx, osn in self.dados_xlsx_tracking.items():
                if tracking_xlsx.startswith(tracking) or tracking.startswith(tracking_xlsx):
                    if osn in self.dados_xlsx_global:
                        return self.dados_xlsx_global[osn], osn

        return None, None

    def carregar_pdf_pagina_inteira(self, caminho_pdf, tipo, dados_xlsx=None):
        """Carrega etiquetas de PDF com 1 etiqueta por pagina (pagina inteira).
        tipo: 'retirada' (beka) ou 'cpf' (lanim)
        dados_xlsx: dict order_sn -> dados (apenas para tipo 'cpf')
        Retorna lista de etiquetas no mesmo formato do pipeline existente.
        """
        print(f"  Carregando ({tipo}): {os.path.basename(caminho_pdf)}")
        doc = fitz.open(caminho_pdf)
        etiquetas = []

        for num_pag in range(len(doc)):
            pagina = doc[num_pag]
            texto = pagina.get_text()

            if tipo == 'cpf':
                # Auto-crop: detectar bounding box do conteudo real
                # (lanim.pdf e A4 mas conteudo esta no canto superior esquerdo)
                blocks = pagina.get_text("blocks")
                if blocks:
                    x0 = min(b[0] for b in blocks)
                    y0 = min(b[1] for b in blocks)
                    x1 = max(b[2] for b in blocks)
                    y1 = max(b[3] for b in blocks)
                    # Pequena margem de seguranca (2pt)
                    clip = fitz.Rect(
                        max(0, x0 - 2), max(0, y0 - 2),
                        min(pagina.rect.width, x1 + 2), min(pagina.rect.height, y1 + 2)
                    )
                else:
                    clip = pagina.rect
            else:
                clip = pagina.rect  # pagina inteira

            if tipo == 'retirada':
                # Extrair NF do texto
                nf = None
                m = re.search(r'Emiss.o:\n(\d+)\n', texto)
                if m:
                    nf = m.group(1)
                else:
                    m = re.search(r'NF:\s*(\d+)', texto)
                    if m:
                        nf = m.group(1)
                    else:
                        m = re.search(r'(\d{4,6})\n\d\n\d{2}-\d{2}-\d{4}', texto)
                        if m:
                            nf = m.group(1)

                if nf is None:
                    pdf_id = os.path.splitext(os.path.basename(caminho_pdf))[0].replace(' ', '_')
                    nf = f"SEM_NF_ret_{pdf_id}_p{num_pag}"
                    dados_nf = {}
                    print(f"    Pag {num_pag}: NF nao encontrada, gerando com ID sintetico")
                else:
                    dados_nf = {}

                # FONTE PRIMARIA: XLSX (buscar por order_sn ou tracking)
                if self.dados_xlsx_global:
                    dados_xlsx_ret, order_sn_xlsx = self._buscar_dados_xlsx(texto)
                    if dados_xlsx_ret:
                        dados_nf = {
                            'nf': nf,
                            'serie': '',
                            'data_emissao': '',
                            'chave': self._extrair_chave_nfe(texto),
                            'cnpj_emitente': '',
                            'nome_emitente': '',
                            'produtos': dados_xlsx_ret['produtos'],
                            'total_itens': dados_xlsx_ret['total_itens'],
                            'total_qtd': dados_xlsx_ret['total_qtd'],
                            'fonte_dados': 'xlsx',
                        }
                        print(f"    Pag {num_pag}: Retirada usando dados XLSX (order_sn={order_sn_xlsx})")

                sku = ''
                num_produtos = 1
                cnpj = dados_nf.get('cnpj_emitente', '')
                if dados_nf.get('produtos'):
                    sku = dados_nf['produtos'][0].get('codigo', '')
                    num_produtos = len(dados_nf['produtos'])

                # Extrair nome da loja do REMETENTE
                if not cnpj:
                    nome_loja = self._extrair_nome_loja_remetente(texto)
                    if nome_loja:
                        cnpj_sintetico = f"LOJA_{re.sub(r'[^A-Za-z0-9]', '_', nome_loja)}"
                        cnpj = cnpj_sintetico
                        if cnpj not in self.cnpj_loja:
                            self.cnpj_loja[cnpj] = nome_loja
                            self.cnpj_nome[cnpj] = nome_loja

                etiquetas.append({
                    'nf': nf,
                    'sku': sku,
                    'num_produtos': num_produtos,
                    'cnpj': cnpj,
                    'clip': clip,
                    'pagina_idx': num_pag,
                    'caminho_pdf': caminho_pdf,
                    'dados_xml': dados_nf,
                    'tipo_especial': 'retirada',
                })

            elif tipo == 'cpf':
                # Extrair order_sn do texto
                order_sn = self._extrair_pedido_texto(texto)

                dados_pedido = {}
                if order_sn and dados_xlsx:
                    dados_pedido = dados_xlsx.get(order_sn, {})

                # Extrair nome real da loja do REMETENTE
                nome_loja_cpf = self._extrair_nome_loja_remetente(texto)
                cpf_cnpj = self.LANIM_CNPJ  # fallback
                cpf_nome = self.LANIM_NOME

                if nome_loja_cpf:
                    # Procurar CNPJ real correspondente ao nome da loja
                    cnpj_encontrado = None
                    for cnpj_real, nome_real in self.cnpj_loja.items():
                        if nome_real.lower().strip() == nome_loja_cpf.lower().strip():
                            cnpj_encontrado = cnpj_real
                            break
                    if cnpj_encontrado:
                        cpf_cnpj = cnpj_encontrado
                        cpf_nome = nome_loja_cpf
                    else:
                        # Nome encontrado mas sem CNPJ correspondente
                        cpf_cnpj = f"CPF_{re.sub(r'[^A-Za-z0-9]', '_', nome_loja_cpf)}"
                        cpf_nome = nome_loja_cpf
                        self.cnpj_loja[cpf_cnpj] = nome_loja_cpf
                        self.cnpj_nome[cpf_cnpj] = nome_loja_cpf

                # Registrar mapeamento fallback se necessario
                if cpf_cnpj == self.LANIM_CNPJ and self.LANIM_CNPJ not in self.cnpj_nome:
                    self.cnpj_nome[self.LANIM_CNPJ] = self.LANIM_NOME

                # Criar dados_xml ficticio com dados do XLSX
                produtos = dados_pedido.get('produtos', [])
                sku = produtos[0].get('codigo', '') if produtos else ''
                num_produtos = len(produtos) if produtos else 1

                # Dados simulados (sem chave/NF real)
                dados_ficticio = {
                    'nf': order_sn or f'CPF_pag{num_pag}',
                    'serie': '',
                    'data_emissao': '',
                    'chave': '',  # sem chave = sem barcode
                    'cnpj_emitente': cpf_cnpj,
                    'nome_emitente': cpf_nome,
                    'produtos': produtos,
                    'total_itens': dados_pedido.get('total_itens', len(produtos)),
                    'total_qtd': dados_pedido.get('total_qtd', 0),
                }

                etiquetas.append({
                    'nf': order_sn or f'CPF_pag{num_pag}',
                    'sku': sku,
                    'num_produtos': num_produtos,
                    'cnpj': cpf_cnpj,
                    'clip': clip,
                    'pagina_idx': num_pag,
                    'caminho_pdf': caminho_pdf,
                    'dados_xml': dados_ficticio,
                    'tipo_especial': 'cpf',
                })

        doc.close()
        print(f"    {len(etiquetas)} etiquetas ({tipo})")
        return etiquetas

    def processar_beka(self, pasta_entrada):
        """Processa etiquetas de retirada do comprador (beka.pdf).
        Retorna lista de etiquetas associadas a lojas via XML/CNPJ.
        """
        caminho = os.path.join(pasta_entrada, 'beka.pdf')
        if not os.path.exists(caminho):
            return []

        print(f"\n  Processando etiquetas RETIRADA DO COMPRADOR...")
        etiquetas = self.carregar_pdf_pagina_inteira(caminho, 'retirada')
        return etiquetas

    def processar_cpf(self, pasta_entrada):
        """Processa etiquetas CPF (lanim*.pdf) usando dados de XLSX de declaracao.
        Detecta automaticamente qualquer XLSX com coluna order_sn + product_info.
        Processa todos os PDFs com nome iniciando em 'lanim' (lanim.pdf, lanim 2.pdf, etc.)

        Todas as etiquetas em lanim*.pdf sao CPF.
        Paginas com grid (2x2/2x1) sao recortadas individualmente.
        Paginas com 1 etiqueta sao processadas como pagina inteira.
        Cada etiqueta recebe dados do XLSX pelo order_sn.

        Retorna lista de etiquetas CPF.
        """
        # Detectar todos os PDFs lanim*.pdf
        pdfs_cpf = []
        for f in os.listdir(pasta_entrada):
            if f.lower().startswith('lanim') and f.lower().endswith('.pdf'):
                pdfs_cpf.append(f)

        if not pdfs_cpf:
            return []

        print(f"\n  Processando etiquetas CPF ({len(pdfs_cpf)} PDF(s))...")

        # Buscar XLSX de declaracao: primeiro lanim2.xlsx, depois qualquer XLSX com order_sn
        dados_xlsx = {}
        caminho_xlsx_especifico = os.path.join(pasta_entrada, 'lanim2.xlsx')
        if os.path.exists(caminho_xlsx_especifico):
            dados_xlsx = self.carregar_xlsx_pedidos(caminho_xlsx_especifico)
            print(f"  Usando lanim2.xlsx")
        else:
            # Buscar qualquer XLSX que nao seja planilha de custos ou config
            xlsx_encontrados = []
            for f in os.listdir(pasta_entrada):
                if f.lower().endswith(('.xlsx', '.xls')) and not f.startswith('_') and f != 'planilha_custos.xlsx':
                    xlsx_encontrados.append(f)

            for xlsx_nome in xlsx_encontrados:
                caminho_xlsx = os.path.join(pasta_entrada, xlsx_nome)
                try:
                    dados_temp = self.carregar_xlsx_pedidos(caminho_xlsx)
                    if dados_temp:
                        dados_xlsx.update(dados_temp)
                        print(f"  Declaracao encontrada: {xlsx_nome} ({len(dados_temp)} pedidos)")
                except Exception as e:
                    print(f"  Erro ao ler {xlsx_nome}: {e}")

            if not dados_xlsx and not xlsx_encontrados:
                print(f"  AVISO: Nenhum XLSX de declaracao encontrado, etiquetas CPF sem dados de produto")

        etiquetas = []
        for pdf_cpf in sorted(pdfs_cpf):
            caminho_pdf = os.path.join(pasta_entrada, pdf_cpf)
            etqs = self._carregar_pdf_cpf_smart(caminho_pdf, dados_xlsx)
            etiquetas.extend(etqs)
            if len(pdfs_cpf) > 1:
                print(f"  {pdf_cpf}: {len(etqs)} etiquetas CPF")
        return etiquetas

    def _carregar_pdf_cpf_smart(self, caminho_pdf, dados_xlsx=None):
        """Carrega etiquetas CPF de um PDF, detectando layout de cada pagina.
        Paginas com grid (2x2/2x1) sao recortadas por quadrante.
        Paginas com 1 etiqueta sao processadas inteiras (com auto-crop).
        Todas sao marcadas como tipo_especial='cpf' e recebem dados do XLSX.
        """
        print(f"  Carregando (cpf): {os.path.basename(caminho_pdf)}")
        doc = fitz.open(caminho_pdf)
        etiquetas = []

        for num_pag in range(len(doc)):
            pagina = doc[num_pag]

            # Detectar layout da pagina
            # Apenas paginas grandes (A4 ~595x842) podem ter grid 2x2/2x1
            # Paginas pequenas (~297x419) sao sempre 1 etiqueta por pagina
            rect = pagina.rect
            eh_pagina_grande = rect.width > 400 and rect.height > 600

            if eh_pagina_grande:
                quadrantes = self._detectar_layout_pagina(pagina)
            else:
                quadrantes = [pagina.rect]  # pagina inteira = 1 etiqueta

            for idx, clip in enumerate(quadrantes):
                texto_quad = pagina.get_text(clip=clip).strip()
                if len(texto_quad) < 10:
                    continue  # Quadrante vazio

                # Extrair order_sn do texto deste quadrante
                order_sn = self._extrair_pedido_texto(texto_quad)

                dados_pedido = {}
                if order_sn and dados_xlsx:
                    dados_pedido = dados_xlsx.get(order_sn, {})

                # Extrair nome da loja do REMETENTE
                nome_loja_cpf = self._extrair_nome_loja_remetente(texto_quad)
                cpf_cnpj = self.LANIM_CNPJ
                cpf_nome = self.LANIM_NOME

                if nome_loja_cpf:
                    cnpj_encontrado = None
                    for cnpj_real, nome_real in self.cnpj_loja.items():
                        if nome_real.lower().strip() == nome_loja_cpf.lower().strip():
                            cnpj_encontrado = cnpj_real
                            break
                    if cnpj_encontrado:
                        cpf_cnpj = cnpj_encontrado
                        cpf_nome = nome_loja_cpf
                    else:
                        cpf_cnpj = f"CPF_{re.sub(r'[^A-Za-z0-9]', '_', nome_loja_cpf)}"
                        cpf_nome = nome_loja_cpf
                        self.cnpj_loja[cpf_cnpj] = nome_loja_cpf
                        self.cnpj_nome[cpf_cnpj] = nome_loja_cpf

                if cpf_cnpj == self.LANIM_CNPJ and self.LANIM_CNPJ not in self.cnpj_nome:
                    self.cnpj_nome[self.LANIM_CNPJ] = self.LANIM_NOME

                produtos = dados_pedido.get('produtos', [])
                sku = produtos[0].get('codigo', '') if produtos else ''
                num_produtos = len(produtos) if produtos else 1

                nf_id = order_sn or f'CPF_pag{num_pag}_q{idx}'

                dados_ficticio = {
                    'nf': nf_id,
                    'serie': '',
                    'data_emissao': '',
                    'chave': '',
                    'cnpj_emitente': cpf_cnpj,
                    'nome_emitente': cpf_nome,
                    'produtos': produtos,
                    'total_itens': dados_pedido.get('total_itens', len(produtos)),
                    'total_qtd': dados_pedido.get('total_qtd', 0),
                }

                etiquetas.append({
                    'nf': nf_id,
                    'sku': sku,
                    'num_produtos': num_produtos,
                    'cnpj': cpf_cnpj,
                    'clip': clip,
                    'pagina_idx': num_pag,
                    'caminho_pdf': caminho_pdf,
                    'dados_xml': dados_ficticio,
                    'tipo_especial': 'cpf',
                })

        doc.close()
        print(f"    {len(etiquetas)} etiquetas (cpf)")
        return etiquetas

    # ----------------------------------------------------------------
    # GERACAO DO PDF CPF - formato 150x225mm
    # ----------------------------------------------------------------
    ALTURA_CPF_PT = 637.795    # 225mm em pontos

    def _desenhar_secao_produtos_cpf(self, pagina, dados, y_inicio, larg_pagina, alt_pagina=None):
        """Desenha tabela de produtos para etiquetas CPF (usa Variation Name)."""
        preto = (0, 0, 0)
        fonte = "helv"
        fonte_bold = "hebo"
        margem_esq = self.MARGEM_ESQUERDA
        margem_dir = self.MARGEM_DIREITA
        fs = self.fonte_produto
        fs_destaque = int(round(fs * 1.5))  # 50% maior para SKU
        fs_qtd = int(round(fs_destaque * 1.5))  # quantidade 50% maior que destaque
        line_h = fs_qtd + 2

        # Limite inferior da pagina para nao cortar rodape
        y_limite = (alt_pagina or self.ALTURA_CPF_PT) - self.MARGEM_INFERIOR - 5

        order_sn = dados.get('nf', '')
        produtos = self._ordenar_produtos(dados.get('produtos', []))
        total_itens = dados.get('total_itens', len(produtos))
        total_qtd = dados.get('total_qtd', sum(int(float(p.get('qtd', 1))) for p in produtos))

        y = y_inicio

        # Cabecalho do pedido
        header_pedido = f"Pedido: {order_sn}    Total Itens: {total_itens}    Total Quantidade: {total_qtd}"
        pagina.insert_text(
            (margem_esq + 2, y), header_pedido,
            fontsize=fs, fontname=fonte_bold, color=preto
        )
        y += 2

        # Modo de exibicao
        modo = getattr(self, 'exibicao_produto', 'sku')

        # Colunas: depende do modo
        col_sku = margem_esq + 2
        col_var = margem_esq + 50
        col_qtd = larg_pagina - margem_dir - 35

        # Linha superior
        pagina.draw_line(
            (margem_esq, y), (larg_pagina - margem_dir, y),
            color=preto, width=0.8
        )
        y += line_h

        # Cabecalho tabela - depende do modo
        if modo == 'titulo':
            header1, header2 = "PRODUTO", "VARIAÇÃO"
        elif modo == 'ambos':
            header1, header2 = "SKU", "PRODUTO"
        else:
            header1, header2 = "SKU", "VARIAÇÃO"

        pagina.insert_text(
            (col_sku, y), header1,
            fontsize=fs, fontname=fonte_bold, color=preto
        )
        pagina.insert_text(
            (col_var, y), header2,
            fontsize=fs, fontname=fonte_bold, color=preto
        )
        pagina.insert_text(
            (col_qtd, y), "Quant",
            fontsize=fs, fontname=fonte_bold, color=preto
        )
        y += 2
        pagina.draw_line(
            (margem_esq, y), (larg_pagina - margem_dir, y),
            color=preto, width=0.5
        )
        y += line_h

        y_top_tabela = y_inicio + 2

        # Linhas de produtos
        for i_prod, prod in enumerate(produtos[:10]):
            if y + line_h > y_limite:
                break

            codigo = prod.get('codigo', '')
            descricao = prod.get('descricao', '')
            variacao = prod.get('variacao', '')
            qtd = str(int(float(prod.get('qtd', '1'))))

            if modo == 'titulo':
                # Col1: descricao/titulo, Col2: variacao
                texto1 = descricao or codigo
                max_t1 = 10
                if len(texto1) > max_t1:
                    texto1 = texto1[:max_t1 - 2] + '..'
                texto2 = variacao
                max_t2 = 45
                if len(texto2) > max_t2:
                    texto2 = texto2[:max_t2 - 2] + '..'
            elif modo == 'ambos':
                # Col1: SKU, Col2: descricao/titulo
                texto1 = codigo
                max_t1 = 10
                if len(texto1) > max_t1:
                    texto1 = texto1[:max_t1 - 2] + '..'
                texto2 = descricao or variacao
                max_t2 = 45
                if len(texto2) > max_t2:
                    texto2 = texto2[:max_t2 - 2] + '..'
            else:
                # Modo SKU (padrao): Col1: SKU, Col2: variacao
                texto1 = codigo
                max_t1 = 10
                if len(texto1) > max_t1:
                    texto1 = texto1[:max_t1 - 2] + '..'
                texto2 = variacao
                max_t2 = 45
                if len(texto2) > max_t2:
                    texto2 = texto2[:max_t2 - 2] + '..'

            pagina.insert_text(
                (col_sku, y), texto1 if texto1 else "-",
                fontsize=fs_destaque, fontname=fonte_bold, color=preto
            )
            pagina.insert_text(
                (col_var, y), texto2.upper() if texto2 else "-",
                fontsize=fs, fontname=fonte, color=preto
            )
            pagina.insert_text(
                (col_qtd, y), qtd,
                fontsize=fs_qtd, fontname=fonte_bold, color=preto
            )
            y += line_h

            # Linha divisoria entre produtos (exceto apos o ultimo)
            if i_prod < len(produtos) - 1 and y + line_h <= y_limite:
                pagina.draw_line(
                    (margem_esq, y - 1), (larg_pagina - margem_dir, y - 1),
                    color=(0.6, 0.6, 0.6), width=0.3
                )

        # Garantir que linhas nao ultrapassem o limite
        y_final = min(y, y_limite)

        # Linha inferior
        pagina.draw_line(
            (margem_esq, y_final), (larg_pagina - margem_dir, y_final),
            color=preto, width=0.8
        )

        # Linhas verticais
        pagina.draw_line(
            (col_var - 5, y_top_tabela), (col_var - 5, y_final),
            color=preto, width=0.5
        )
        pagina.draw_line(
            (col_qtd - 5, y_top_tabela), (col_qtd - 5, y_final),
            color=preto, width=0.5
        )

    def gerar_pdf_cpf(self, etiquetas_cpf, caminho_saida):
        """Gera PDF para etiquetas CPF no formato 150x225mm.
        Renderiza a etiqueta original + tabela de produtos com Variation Name.
        """
        larg = self.LARGURA_PT       # 150mm
        alt = self.ALTURA_CPF_PT     # 225mm
        area_util_larg = larg - self.MARGEM_ESQUERDA - self.MARGEM_DIREITA

        # Abrir PDFs de origem
        pdfs_usados = set(e['caminho_pdf'] for e in etiquetas_cpf)
        docs_abertos = {p: fitz.open(p) for p in pdfs_usados}

        doc_saida = fitz.open()

        for idx, etq in enumerate(etiquetas_cpf):
            clip = etq['clip']
            pag_idx = etq['pagina_idx']
            dados = etq.get('dados_xml', {})
            pdf_path = etq['caminho_pdf']
            doc_entrada = docs_abertos[pdf_path]

            nova_pag = doc_saida.new_page(width=larg, height=alt)

            # Escalar a pagina original para caber na largura util
            quad_larg = clip.width
            quad_alt = clip.height
            escala = area_util_larg / quad_larg
            alt_etiqueta = quad_alt * escala

            # Limitar altura da etiqueta para reservar espaco para tabela de produtos
            min_espaco_tabela = 120  # pt, suficiente para cabecalho + alguns produtos
            alt_max_etiqueta = alt - self.MARGEM_TOPO - self.MARGEM_INFERIOR - min_espaco_tabela
            if alt_etiqueta > alt_max_etiqueta:
                alt_etiqueta = alt_max_etiqueta

            dest_rect = fitz.Rect(
                self.MARGEM_ESQUERDA,
                self.MARGEM_TOPO,
                larg - self.MARGEM_DIREITA,
                self.MARGEM_TOPO + alt_etiqueta
            )

            nova_pag.show_pdf_page(dest_rect, doc_entrada, pag_idx, clip=clip)

            # Tabela de produtos com Variation Name
            y_inicio = self.MARGEM_TOPO + alt_etiqueta + 5
            if dados.get('produtos'):
                self._desenhar_secao_produtos_cpf(nova_pag, dados, y_inicio, larg, alt_pagina=alt)

            # Numero de ordem (subido para nao cortar na impressao)
            nova_pag.insert_text(
                (larg - self.MARGEM_DIREITA - 15, alt - self.MARGEM_INFERIOR - 8),
                f"p.{idx + 1}",
                fontsize=9, fontname="hebo", color=(0.4, 0.4, 0.4)
            )

        for doc in docs_abertos.values():
            doc.close()

        total = len(doc_saida)
        doc_saida.save(caminho_saida)
        doc_saida.close()
        return total

    # ----------------------------------------------------------------
    # PROCESSAMENTO SHEIN
    # ----------------------------------------------------------------

    def _parse_shein_danfe(self, texto):
        """Extrai dados de uma pagina DANFE do shein crua.pdf."""
        dados = {}

        # NF
        m = re.search(r'N[uú]mero:\s*\n?(\d+)', texto, re.IGNORECASE)
        dados['nf'] = m.group(1) if m else ''

        # Chave de acesso (44 digitos)
        m = re.search(r'(\d{44})', texto)
        dados['chave'] = m.group(1) if m else ''

        # CNPJ emitente
        m = re.search(r'CNPJ[^:]*:\s*(\d+)', texto)
        dados['cnpj_emitente'] = m.group(1) if m else ''

        # Nome emitente
        m = re.search(r'NOME/RAZ.O SOCIAL[^:]*:\s*(.+)', texto)
        nome_raw = m.group(1).strip() if m else ''
        dados['nome_emitente'] = nome_raw

        # Registrar CNPJ -> nome
        if dados['cnpj_emitente'] and dados['cnpj_emitente'] not in self.cnpj_nome:
            self.cnpj_nome[dados['cnpj_emitente']] = self._limpar_nome_emitente(nome_raw)

        # Produtos: ITEM | CONTEUDO | ATRIBUTOS | QUANT
        produtos = []
        # Pegar tudo apos "ITEM" header
        m_secao = re.search(r'ITEM\s+CONTE.*?QUANT\.\s*\n(.*)', texto, re.DOTALL)
        if m_secao:
            secao = m_secao.group(1).strip()
            # Linhas quebradas pelo PDF - juntar tudo
            linhas = [l.strip() for l in secao.split('\n') if l.strip()]

            # Estrutura: item_code, descricao (varias linhas), atributos (varias linhas), qtd (ultimo numero)
            # O item_code comeca com I ou l seguido de alfanumerico
            # A qtd e o ultimo numero isolado
            # Os atributos conteem / e nomes de cores/tamanhos

            if linhas:
                item_code = linhas[0]
                # Quantidade e a ultima linha que e so um numero
                qtd_str = '1'
                idx_qtd = len(linhas)
                for j in range(len(linhas) - 1, 0, -1):
                    if re.match(r'^\d+$', linhas[j]):
                        qtd_str = linhas[j]
                        idx_qtd = j
                        break

                # Juntar linhas entre item_code e qtd (sem quebras de linha)
                meio = ''.join(linhas[1:idx_qtd])

                # Separar descricao de atributos
                # Atributos comecam onde aparece algo com / (tipo Rakka/Roxo...)
                m_atrib = re.search(r'([A-Z][a-z]*/.+)$', meio)
                if m_atrib:
                    atrib = m_atrib.group(1)
                    desc = meio[:m_atrib.start()].strip()
                else:
                    atrib = ''
                    desc = meio

                produtos.append({
                    'codigo_item': item_code,
                    'descricao': desc,
                    'atributos': atrib,
                    'qtd': qtd_str,
                })

        dados['produtos_shein'] = produtos
        dados['total_itens'] = len(produtos)
        dados['total_qtd'] = sum(int(float(p.get('qtd', 1))) for p in produtos)

        return dados

    def _gerar_codigo_shein(self, atributos):
        """Converte ATRIBUTOS Shein em codigo limpo.
        Ex: 'Rakka/Roxo(紫色)-BR41/42' -> 'RakkaRoxoBR4142'
        Ex: 'Rakka/Preto/Dourado-L7(黑/金-L7)-BR41/42' -> 'RakkaPretoDouradoL7BR4142'
        """
        if not atributos:
            return ''
        # Remover texto entre parenteses (incluindo chines)
        limpo = re.sub(r'\([^)]*\)', '', atributos)
        # Remover / e espacos
        limpo = limpo.replace('/', '')
        # Remover espacos
        limpo = limpo.replace(' ', '')
        # Manter apenas letras, numeros e hifen
        limpo = re.sub(r'[^A-Za-z0-9\-]', '', limpo)
        # Remover hifens extras
        limpo = re.sub(r'-+', '', limpo)
        return limpo

    def _parsear_atributos_shein(self, atributos):
        """Parseia atributos Shein em modelo, cor e tamanho.
        Ex: 'Rakka/Dourado(金色)-BR39/40' -> ('Rakka', 'Dourado', 'BR39/40')
        Ex: 'Rakka/Rosa/Rosa(粉色/粉色)-BR39/40' -> ('Rakka', 'Rosa/Rosa', 'BR39/40')
        """
        if not atributos:
            return '', '', ''
        # Remover texto chines entre parenteses
        limpo = re.sub(r'\([^)]*\)', '', atributos).strip()
        # Separar tamanho: -BR39/40
        m = re.match(r'^(.+?)(?:-BR(\d+/?\d*))$', limpo)
        if m:
            modelo_cor = m.group(1)
            tamanho = 'BR' + m.group(2)
        else:
            modelo_cor = limpo
            tamanho = ''
        # Separar modelo e cor pelo primeiro /
        partes = modelo_cor.split('/', 1)
        if len(partes) >= 2:
            modelo = partes[0].strip()
            cor = partes[1].strip()
        else:
            modelo = modelo_cor.strip()
            cor = ''
        return modelo, cor, tamanho

    def processar_shein(self, pasta_entrada, pdfs_extras=None):
        """Processa etiquetas Shein de 'shein crua.pdf', 'shein.pdf' ou PDFs auto-detectados.
        O PDF tem paginas alternadas: par=etiqueta Shein, impar=DANFE.
        pdfs_extras: lista de caminhos de PDFs Shein auto-detectados.
        Retorna lista de dicts com dados pareados.
        """
        caminhos_shein = list(pdfs_extras) if pdfs_extras else []

        # Buscar arquivo shein nomeado (case-insensitive para funcionar no Linux/Railway)
        nomes_shein = ['shein crua.pdf', 'shein.pdf']
        for f in os.listdir(pasta_entrada):
            if f.lower() in nomes_shein:
                caminho_fixo = os.path.join(pasta_entrada, f)
                if caminho_fixo not in caminhos_shein:
                    caminhos_shein.append(caminho_fixo)

        if not caminhos_shein:
            return []

        print(f"\n  Processando etiquetas SHEIN ({len(caminhos_shein)} PDF(s))...")
        etiquetas = []

        for caminho in caminhos_shein:
            print(f"    Shein: {os.path.basename(caminho)}")
            doc = fitz.open(caminho)
            n_pags = len(doc)

            # Processar em pares: pag_par (etiqueta) + pag_impar (DANFE)
            for i in range(0, n_pags - 1, 2):
                pag_etiqueta = doc[i]
                pag_danfe = doc[i + 1]

                texto_danfe = pag_danfe.get_text()

                # Verificar se realmente e DANFE
                if 'DANFE' not in texto_danfe and 'CHAVE' not in texto_danfe.upper():
                    print(f"      Par {i}/{i+1}: pagina {i+1} nao parece ser DANFE, pulando")
                    continue

                dados_danfe = self._parse_shein_danfe(texto_danfe)
                nf = dados_danfe.get('nf', '')
                cnpj = dados_danfe.get('cnpj_emitente', '')

                if not nf:
                    print(f"      Par {i}/{i+1}: NF nao encontrada no DANFE, pulando")
                    continue

                # Buscar dados completos do XML se disponivel
                dados_xml = self.dados_xml.get(nf, {})
                if dados_xml:
                    cnpj = dados_xml.get('cnpj_emitente', cnpj)

                etiquetas.append({
                    'nf': nf,
                    'cnpj': cnpj,
                    'pag_etiqueta_idx': i,
                    'pag_danfe_idx': i + 1,
                    'caminho_pdf': caminho,
                    'clip_etiqueta': pag_etiqueta.rect,
                    'clip_danfe': pag_danfe.rect,
                    'dados_danfe': dados_danfe,
                    'dados_xml': dados_xml,
                    'tipo_especial': 'shein',
                })

            doc.close()

        print(f"    {len(etiquetas)} pares etiqueta+DANFE Shein")
        return etiquetas

    def gerar_pdf_shein(self, etiquetas_shein, caminho_saida):
        """Gera PDF final Shein: etiqueta + barcode vertical + tabela de produtos.
        Formato: 150x225mm por pagina.
        """
        larg = self.LARGURA_PT       # 150mm = 425.2pt
        alt = self.ALTURA_CPF_PT     # 225mm = 637.8pt
        margem_esq = self.MARGEM_ESQUERDA
        margem_dir = self.MARGEM_DIREITA
        margem_topo = self.MARGEM_TOPO
        margem_inf = self.MARGEM_INFERIOR
        preto = (0, 0, 0)
        fonte = "helv"
        fonte_bold = "hebo"

        # Largura da faixa lateral do barcode vertical
        faixa_lateral = 28

        # Area util para a etiqueta (sem a faixa lateral)
        area_etiq_larg = larg - margem_esq - margem_dir - faixa_lateral

        # Abrir PDF de origem
        pdfs_usados = set(e['caminho_pdf'] for e in etiquetas_shein)
        docs_abertos = {p: fitz.open(p) for p in pdfs_usados}

        doc_saida = fitz.open()

        for idx, etq in enumerate(etiquetas_shein):
            pdf_path = etq['caminho_pdf']
            doc_entrada = docs_abertos[pdf_path]
            pag_etiq_idx = etq['pag_etiqueta_idx']
            clip_etiq = etq['clip_etiqueta']
            dados_danfe = etq['dados_danfe']
            dados_xml = etq.get('dados_xml', {})

            chave = dados_xml.get('chave', '') or dados_danfe.get('chave', '')
            nf = etq['nf']
            produtos_shein = dados_danfe.get('produtos_shein', [])
            total_itens = dados_danfe.get('total_itens', len(produtos_shein))
            total_qtd = dados_danfe.get('total_qtd', 0)

            nova_pag = doc_saida.new_page(width=larg, height=alt)

            # --- Renderizar etiqueta Shein ---
            etiq_larg = clip_etiq.width
            etiq_alt = clip_etiq.height
            escala = area_etiq_larg / etiq_larg
            alt_etiqueta = etiq_alt * escala

            # Limitar altura para deixar espaco para tabela de produtos
            max_alt_etiq = alt - margem_topo - margem_inf - 60  # espaco para tabela
            if alt_etiqueta > max_alt_etiq:
                escala = max_alt_etiq / etiq_alt
                alt_etiqueta = max_alt_etiq
                area_etiq_larg_real = etiq_larg * escala
            else:
                area_etiq_larg_real = area_etiq_larg

            dest_rect = fitz.Rect(
                margem_esq,
                margem_topo,
                margem_esq + area_etiq_larg_real,
                margem_topo + alt_etiqueta
            )
            nova_pag.show_pdf_page(dest_rect, doc_entrada, pag_etiq_idx, clip=clip_etiq)

            # --- Faixa lateral direita: barcode vertical + texto ---
            x_lateral = larg - margem_dir - faixa_lateral
            if chave:
                # Texto "DANFE Simplificado" rotacionado
                # Usando insert_text com morph para rotacao
                nova_pag.insert_text(
                    (larg - margem_dir - 5, margem_topo + 8),
                    "DANFE",
                    fontsize=7, fontname=fonte_bold, color=preto,
                    rotate=270
                )
                nova_pag.insert_text(
                    (larg - margem_dir - 13, margem_topo + 8),
                    "Simplificado",
                    fontsize=5, fontname=fonte, color=preto,
                    rotate=270
                )

                # Chave formatada rotacionada
                chave_fmt = ' '.join([chave[i:i+4] for i in range(0, len(chave), 4)])
                nova_pag.insert_text(
                    (x_lateral + 5, margem_topo + 8),
                    f"Chave: {chave_fmt}",
                    fontsize=4, fontname=fonte, color=preto,
                    rotate=270
                )

                # Barcode vertical da chave
                try:
                    svg_bytes = self._gerar_barcode_svg(chave)
                    # Barcode na faixa lateral, vertical
                    barcode_rect = fitz.Rect(
                        x_lateral + 8, margem_topo + 10,
                        x_lateral + faixa_lateral - 2, margem_topo + alt_etiqueta - 10
                    )
                    nova_pag.insert_image(barcode_rect, stream=svg_bytes, rotate=90)
                except Exception:
                    pass

            # --- Tabela de produtos no rodape ---
            fs = self.fonte_produto
            line_h = fs + 2
            y = margem_topo + alt_etiqueta + 3
            col_codigo = margem_esq + 2
            col_prod = margem_esq + 95
            col_qtd = larg - margem_dir - 25

            # Linha superior
            nova_pag.draw_line(
                (margem_esq, y), (larg - margem_dir, y),
                color=preto, width=0.8
            )
            y += line_h

            # Cabecalho
            nova_pag.insert_text(
                (col_codigo, y), "CÓDIGO",
                fontsize=fs, fontname=fonte_bold, color=preto
            )
            header_prod = f"PROD. (NF: {nf} T-ITENS: {total_itens} T-QUANT: {total_qtd})"
            nova_pag.insert_text(
                (col_prod, y), header_prod,
                fontsize=fs, fontname=fonte_bold, color=preto
            )
            nova_pag.insert_text(
                (col_qtd, y), "Q.",
                fontsize=fs, fontname=fonte_bold, color=preto
            )
            y += 2
            nova_pag.draw_line(
                (margem_esq, y), (larg - margem_dir, y),
                color=preto, width=0.5
            )
            y_top_tabela = margem_topo + alt_etiqueta + 3
            y += line_h

            # Linhas de produtos
            fs_destaque = int(round(fs * 1.5))
            fs_qtd = int(round(fs_destaque * 1.5))  # quantidade 50% maior
            for prod in produtos_shein[:5]:
                atrib = prod.get('atributos', '')
                modelo, cor, tamanho = self._parsear_atributos_shein(atrib)
                qtd = str(int(float(prod.get('qtd', '1'))))

                # Coluna 1: Modelo
                nova_pag.insert_text(
                    (col_codigo, y), modelo,
                    fontsize=fs_destaque, fontname=fonte_bold, color=preto
                )
                # Coluna 2: Cor, Tamanho
                cor_tam = f"{cor},{tamanho}" if cor and tamanho else (cor or tamanho or '-')
                if len(cor_tam) > 30:
                    cor_tam = cor_tam[:28] + '..'
                nova_pag.insert_text(
                    (col_prod, y), cor_tam,
                    fontsize=fs_destaque, fontname=fonte_bold, color=preto
                )
                # Coluna 3: Quantidade
                nova_pag.insert_text(
                    (col_qtd, y), qtd,
                    fontsize=fs_qtd, fontname=fonte_bold, color=preto
                )
                y += line_h

            # Linha inferior
            nova_pag.draw_line(
                (margem_esq, y), (larg - margem_dir, y),
                color=preto, width=0.8
            )

            # Linhas verticais
            nova_pag.draw_line(
                (col_prod - 5, y_top_tabela), (col_prod - 5, y),
                color=preto, width=0.5
            )
            nova_pag.draw_line(
                (col_qtd - 5, y_top_tabela), (col_qtd - 5, y),
                color=preto, width=0.5
            )

            # Numero de ordem (subido para nao cortar na impressao)
            nova_pag.insert_text(
                (larg - margem_dir - 15, alt - margem_inf - 8),
                f"p.{idx + 1}",
                fontsize=9, fontname="hebo", color=(0.4, 0.4, 0.4)
            )

        for doc in docs_abertos.values():
            doc.close()

        total = len(doc_saida)
        doc_saida.save(caminho_saida)
        doc_saida.close()
        print(f"    Shein PDF: {total} paginas geradas")
        return total

    # ----------------------------------------------------------------
    # GERACAO DO RESUMO XLSX
    # ----------------------------------------------------------------
    def gerar_resumo_xlsx(self, etiquetas, caminho_saida, nome_loja):
        """Gera resumo XLSX com quantidade vendida por SKU + Variacao."""
        # Contar quantidade por (SKU, Variacao)
        sku_var_qtd = defaultdict(int)
        for etq in etiquetas:
            dados = etq.get('dados_xml', {})
            for prod in dados.get('produtos', []):
                codigo = prod.get('codigo', '')
                variacao = prod.get('variacao', '')
                qtd = int(float(prod.get('qtd', '1')))
                if codigo or variacao:
                    chave = (codigo, variacao)
                    sku_var_qtd[chave] += qtd

        # Criar workbook
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = f"Resumo {len(etiquetas)} xmls "

        # Estilos
        header_font = Font(bold=True, size=11)
        header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

        # Cabecalho
        ws['A1'] = 'Cod. SKU'
        ws['B1'] = 'Variacao'
        ws['C1'] = 'Soma Quant.'
        for cell in [ws['A1'], ws['B1'], ws['C1']]:
            cell.font = header_font
            cell.fill = header_fill
            cell.border = border
            cell.alignment = Alignment(horizontal='left')

        # Dados ordenados por SKU > Cor > Numero
        def _sort_var(var):
            partes = re.split(r'[,/]', var or '', maxsplit=1)
            cor = partes[0].strip() if partes else ''
            num_str = partes[1].strip() if len(partes) > 1 else ''
            m = re.search(r'(\d+)', num_str)
            num_val = int(m.group(1)) if m else 99999
            return (cor, num_val, num_str)

        row = 2
        for (sku, var) in sorted(sku_var_qtd.keys(), key=lambda x: (x[0] or '', _sort_var(x[1]))):
            ws.cell(row=row, column=1, value=sku).border = border
            ws.cell(row=row, column=2, value=var).border = border
            ws.cell(row=row, column=3, value=sku_var_qtd[(sku, var)]).border = border
            row += 1

        # Total
        ws.cell(row=row, column=1, value='TOTAL').font = Font(bold=True)
        ws.cell(row=row, column=1).border = border
        ws.cell(row=row, column=2, value='').border = border
        ws.cell(row=row, column=3, value=sum(sku_var_qtd.values())).font = Font(bold=True)
        ws.cell(row=row, column=3).border = border

        # Ajustar largura das colunas
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 40
        ws.column_dimensions['C'].width = 15

        wb.save(caminho_saida)
        return len(sku_var_qtd), sum(sku_var_qtd.values())

    def gerar_resumo_xlsx_shein(self, etiquetas_shein, caminho_saida, nome_loja='Shein'):
        """Gera resumo XLSX de etiquetas Shein com Modelo, Cor, Tamanho, Quantidade."""
        # Agrupar por (modelo, cor, tamanho)
        modelo_cor_tam_qtd = defaultdict(int)
        for etq in etiquetas_shein:
            dados_danfe = etq.get('dados_danfe', {})
            for prod in dados_danfe.get('produtos_shein', []):
                atrib = prod.get('atributos', '')
                qtd = int(float(prod.get('qtd', '1')))
                modelo, cor, tamanho = self._parsear_atributos_shein(atrib)
                chave = (modelo or prod.get('descricao', '-'), cor, tamanho)
                modelo_cor_tam_qtd[chave] += qtd

        if not modelo_cor_tam_qtd:
            return 0, 0

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = f"Resumo Shein {len(etiquetas_shein)} etiq"

        header_font = Font(bold=True, size=11)
        header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

        ws['A1'] = 'Modelo'
        ws['B1'] = 'Cor'
        ws['C1'] = 'Tamanho'
        ws['D1'] = 'Soma Quant.'
        for cell in [ws['A1'], ws['B1'], ws['C1'], ws['D1']]:
            cell.font = header_font
            cell.fill = header_fill
            cell.border = border
            cell.alignment = Alignment(horizontal='left')

        # Ordenar: Modelo > Cor > Tamanho (numerico)
        def _sort_key(chave):
            modelo, cor, tamanho = chave
            m = re.search(r'(\d+)', tamanho)
            num_val = int(m.group(1)) if m else 99999
            return (modelo, cor, num_val, tamanho)

        row = 2
        for (modelo, cor, tamanho) in sorted(modelo_cor_tam_qtd.keys(), key=_sort_key):
            ws.cell(row=row, column=1, value=modelo).border = border
            ws.cell(row=row, column=2, value=cor).border = border
            ws.cell(row=row, column=3, value=tamanho).border = border
            ws.cell(row=row, column=4, value=modelo_cor_tam_qtd[(modelo, cor, tamanho)]).border = border
            row += 1

        # Total
        ws.cell(row=row, column=1, value='TOTAL').font = Font(bold=True)
        ws.cell(row=row, column=1).border = border
        ws.cell(row=row, column=2, value='').border = border
        ws.cell(row=row, column=3, value='').border = border
        ws.cell(row=row, column=4, value=sum(modelo_cor_tam_qtd.values())).font = Font(bold=True)
        ws.cell(row=row, column=4).border = border

        ws.column_dimensions['A'].width = 20
        ws.column_dimensions['B'].width = 25
        ws.column_dimensions['C'].width = 15
        ws.column_dimensions['D'].width = 15

        wb.save(caminho_saida)
        return len(modelo_cor_tam_qtd), sum(modelo_cor_tam_qtd.values())

    def gerar_resumo_geral_xlsx(self, lojas_info, etiquetas_por_cnpj, caminho_saida):
        """Gera resumo geral XLSX com totais de todas as lojas.
        lojas_info: list of dicts com nome, cnpj, etiquetas, skus, total_qtd
        etiquetas_por_cnpj: dict cnpj -> list de etiquetas
        """
        wb = openpyxl.Workbook()

        # Estilos
        header_font = Font(bold=True, size=11)
        header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
        total_fill = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")
        border = Border(
            left=Side(style='thin'), right=Side(style='thin'),
            top=Side(style='thin'), bottom=Side(style='thin')
        )

        # ---- Sheet 1: Resumo por Loja ----
        ws1 = wb.active
        ws1.title = "Resumo Geral"
        headers = ['Loja', 'Etiquetas', 'SKUs', 'Unidades']
        for col, h in enumerate(headers, 1):
            cell = ws1.cell(row=1, column=col, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.border = border
            cell.alignment = Alignment(horizontal='left')

        row = 2
        sum_etiq = sum_skus = sum_un = 0
        for loja in sorted(lojas_info, key=lambda x: x['nome']):
            ws1.cell(row=row, column=1, value=loja['nome']).border = border
            ws1.cell(row=row, column=2, value=loja['etiquetas']).border = border
            ws1.cell(row=row, column=3, value=loja['skus']).border = border
            ws1.cell(row=row, column=4, value=loja['total_qtd']).border = border
            sum_etiq += loja['etiquetas']
            sum_skus += loja['skus']
            sum_un += loja['total_qtd']
            row += 1

        # Linha total
        for col, val in enumerate(['TOTAL', sum_etiq, sum_skus, sum_un], 1):
            cell = ws1.cell(row=row, column=col, value=val)
            cell.font = Font(bold=True, size=11)
            cell.fill = total_fill
            cell.border = border

        ws1.column_dimensions['A'].width = 30
        ws1.column_dimensions['B'].width = 15
        ws1.column_dimensions['C'].width = 12
        ws1.column_dimensions['D'].width = 15

        # ---- Sheet 2: SKUs detalhados por Loja ----
        ws2 = wb.create_sheet("SKUs por Loja")
        headers2 = ['Loja', 'Cod. SKU', 'Quantidade']
        for col, h in enumerate(headers2, 1):
            cell = ws2.cell(row=1, column=col, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.border = border

        row2 = 2
        grand_total = 0
        for cnpj in sorted(etiquetas_por_cnpj.keys(), key=lambda c: self.get_nome_loja(c)):
            nome = self.get_nome_loja(cnpj)
            etiquetas = etiquetas_por_cnpj[cnpj]
            sku_qtd = defaultdict(int)
            for etq in etiquetas:
                dados = etq.get('dados_xml', {})
                for prod in dados.get('produtos', []):
                    codigo = prod.get('codigo', '')
                    qtd = int(float(prod.get('qtd', '1')))
                    if codigo:
                        sku_qtd[codigo] += qtd

            for sku in sorted(sku_qtd.keys()):
                ws2.cell(row=row2, column=1, value=nome).border = border
                ws2.cell(row=row2, column=2, value=sku).border = border
                ws2.cell(row=row2, column=3, value=sku_qtd[sku]).border = border
                grand_total += sku_qtd[sku]
                row2 += 1

        # Total geral
        cell_t1 = ws2.cell(row=row2, column=1, value='TOTAL GERAL')
        cell_t1.font = Font(bold=True)
        cell_t1.fill = total_fill
        cell_t1.border = border
        ws2.cell(row=row2, column=2, value='').border = border
        cell_t3 = ws2.cell(row=row2, column=3, value=grand_total)
        cell_t3.font = Font(bold=True)
        cell_t3.fill = total_fill
        cell_t3.border = border

        ws2.column_dimensions['A'].width = 30
        ws2.column_dimensions['B'].width = 35
        ws2.column_dimensions['C'].width = 15

        wb.save(caminho_saida)
        return len(lojas_info), sum_un


def main():
    """Funcao principal."""

    print("=" * 60)
    print("PROCESSADOR DE ETIQUETAS SHOPEE")
    print("=" * 60)

    pasta_entrada = os.environ.get("PASTA_ENTRADA", os.path.join(os.path.expanduser("~"), "Desktop", "Etiquetas"))
    pasta_saida = os.environ.get("PASTA_SAIDA", os.path.join(os.path.expanduser("~"), "Desktop", "Etiquetas Prontas"))

    if not os.path.exists(pasta_entrada):
        print(f"\nPasta de entrada nao encontrada: {pasta_entrada}")
        return

    # Listar arquivos
    arquivos = os.listdir(pasta_entrada)
    pdfs = [f for f in arquivos if f.lower().endswith('.pdf') and not f.startswith('etiquetas_prontas')]
    zips = [f for f in arquivos if f.lower().endswith('.zip')]

    if not pdfs:
        print(f"\nNenhum PDF encontrado em: {pasta_entrada}")
        return

    print(f"\nArquivos encontrados:")
    for f in pdfs:
        print(f"  [PDF] {f}")
    for f in zips:
        print(f"  [ZIP] {f}")

    proc = ProcessadorEtiquetasShopee()

    # 1. Carregar dados dos XLSX de empacotamento
    print(f"\n{'='*40}")
    print("CARREGANDO XLSX...")
    proc.carregar_todos_xlsx(pasta_entrada)

    # 2. Carregar e recortar TODOS os PDFs
    print(f"\n{'='*40}")
    print("CARREGANDO ETIQUETAS...")
    todas_etiquetas, cpf_auto_detectadas, pdfs_shein_auto = proc.carregar_todos_pdfs(pasta_entrada)

    # 2b. Processar etiquetas especiais (CPF e Shein)
    print(f"\n{'='*40}")
    print("CARREGANDO ETIQUETAS ESPECIAIS...")
    etiquetas_cpf_especial = proc.processar_cpf(pasta_entrada)
    etiquetas_shein = proc.processar_shein(pasta_entrada, pdfs_extras=pdfs_shein_auto)
    # Juntar CPF do lanim*.pdf com CPF auto-detectadas de PDFs genericos
    etiquetas_cpf_especial.extend(cpf_auto_detectadas)
    # CPF e Shein serao gerados com PDFs separados, mas ainda entram no resumo XLSX
    todas_etiquetas.extend(etiquetas_cpf_especial)
    if etiquetas_cpf_especial:
        print(f"  CPF: {len(etiquetas_cpf_especial)} etiquetas ({len(cpf_auto_detectadas)} auto-detectadas)")
    if etiquetas_shein:
        print(f"  Shein: {len(etiquetas_shein)} etiquetas")

    # 2c. Remover duplicatas
    print(f"\n{'='*40}")
    print("VERIFICANDO DUPLICATAS...")
    todas_etiquetas, duplicadas = proc.remover_duplicatas(todas_etiquetas)
    if duplicadas:
        print(f"  AVISO: {len(duplicadas)} etiquetas duplicadas removidas:")
        for d in duplicadas:
            print(f"    NF {d.get('nf','')} (tipo: {d.get('tipo_especial','normal')})")
    else:
        print(f"  Nenhuma duplicata encontrada")

    # 2d. Mostrar nomes das lojas encontradas
    print(f"\n  Lojas identificadas (nomes Shopee):")
    for cnpj in set(e.get('cnpj','') for e in todas_etiquetas if e.get('cnpj')):
        nome = proc.get_nome_loja(cnpj)
        print(f"    [{cnpj}] {nome}")

    # 3. Separar por loja
    print(f"\n{'='*40}")
    print("SEPARANDO POR LOJA...")
    lojas = proc.separar_por_loja(todas_etiquetas)

    # 4. Criar pasta de saida
    if not os.path.exists(pasta_saida):
        os.makedirs(pasta_saida)

    # 5. Gerar PDF e XLSX para cada loja
    print(f"\n{'='*40}")
    print("GERANDO ARQUIVOS POR LOJA...")

    for cnpj, etiquetas_loja in lojas.items():
        nome_loja = proc.get_nome_loja(cnpj)
        n_etiquetas = len(etiquetas_loja)

        print(f"\n  --- {nome_loja} ({n_etiquetas} etiquetas) ---")

        # Criar pasta da loja
        pasta_loja = os.path.join(pasta_saida, nome_loja)
        if not os.path.exists(pasta_loja):
            os.makedirs(pasta_loja)

        # Separar etiquetas regulares e CPF
        etiq_regular = [e for e in etiquetas_loja if e.get('tipo_especial') not in ('cpf',)]
        etiq_cpf_loja = [e for e in etiquetas_loja if e.get('tipo_especial') == 'cpf']

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        total_pags = 0
        n_simples = n_multi = com_xml = sem_xml = 0

        # Gerar PDF regular (CNPJ/retirada)
        if etiq_regular:
            caminho_pdf = os.path.join(pasta_loja, f"etiquetas_{nome_loja}_{timestamp}.pdf")
            total_pags, n_simples, n_multi, com_xml, sem_xml = proc.gerar_pdf_loja(
                etiq_regular, caminho_pdf
            )
            print(f"    PDF: {total_pags} paginas ({n_simples} simples + {n_multi} multi-produto)")
            if sem_xml > 0:
                print(f"    AVISO: {sem_xml} etiquetas sem XML correspondente")

        # Gerar PDF CPF separado (formato 150x225mm)
        if etiq_cpf_loja:
            caminho_cpf_pdf = os.path.join(pasta_loja, f"cpf_{nome_loja}_{timestamp}.pdf")
            total_cpf = proc.gerar_pdf_cpf(etiq_cpf_loja, caminho_cpf_pdf)
            total_pags += total_cpf
            print(f"    PDF CPF: {total_cpf} paginas")

        # Gerar XLSX (inclui regular + CPF)
        caminho_xlsx = os.path.join(pasta_loja, f"resumo_{nome_loja}_{timestamp}.xlsx")
        n_skus, total_qtd = proc.gerar_resumo_xlsx(etiquetas_loja, caminho_xlsx, nome_loja)
        print(f"    XLSX: {n_skus} SKUs, {total_qtd} unidades vendidas")

    # 5b. Gerar PDF Shein separado (formato 150x225mm com barcode vertical)
    if etiquetas_shein:
        print(f"\n  --- Gerando PDF Shein ---")
        # Agrupar shein por CNPJ
        shein_por_cnpj = defaultdict(list)
        for etq in etiquetas_shein:
            shein_por_cnpj[etq.get('cnpj', '')].append(etq)

        for cnpj, etqs_shein in shein_por_cnpj.items():
            nome_loja_shein = proc.get_nome_loja(cnpj)
            pasta_loja_shein = os.path.join(pasta_saida, nome_loja_shein)
            if not os.path.exists(pasta_loja_shein):
                os.makedirs(pasta_loja_shein)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            caminho_shein = os.path.join(pasta_loja_shein, f"shein_{nome_loja_shein}_{timestamp}.pdf")
            total_shein = proc.gerar_pdf_shein(etqs_shein, caminho_shein)
            print(f"    Shein {nome_loja_shein}: {total_shein} paginas")

    # 6. Resumo final
    print(f"\n{'='*60}")
    print("CONCLUIDO!")
    print(f"  Pasta de saida: {pasta_saida}")
    print(f"  Lojas processadas: {len(lojas)}")
    total_etiquetas = sum(len(e) for e in lojas.values())
    print(f"  Total de etiquetas: {total_etiquetas}")
    for cnpj, etiquetas_loja in lojas.items():
        nome = proc.get_nome_loja(cnpj)
        print(f"    {nome}: {len(etiquetas_loja)} etiquetas")
    print("=" * 60)


if __name__ == "__main__":
    main()
