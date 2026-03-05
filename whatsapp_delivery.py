# -*- coding: utf-8 -*-
"""
Helpers de roteamento de envios WhatsApp por loja.

Objetivo:
- Mapear contatos por CNPJ e por nome normalizado (fallback robusto)
- Montar lista de entregas a partir do ultimo resultado processado
- Gerar diagnostico de motivos de "nao envio" para facilitar suporte
"""

import os
import re
import json
import unicodedata
from typing import Dict, List, Tuple, Set


_ALLOWED_FILE_EXTENSIONS = {
    ".pdf",
    ".xlsx",
    ".xls",
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
    ".bmp",
}

_EXT_ORDER = {
    ".pdf": 0,
    ".xlsx": 1,
    ".xls": 1,
    ".png": 2,
    ".jpg": 2,
    ".jpeg": 2,
    ".webp": 2,
    ".bmp": 2,
}


def _normalizar_cnpj_chave(valor: str) -> str:
    """
    Normaliza identificador da loja para chave de lookup.

    Regras:
    - Se parecer CNPJ (>= 11 digitos), usa apenas digitos
    - Caso contrario, usa texto uppercase sem espacos duplicados
    """
    if not valor:
        return ""
    bruto = str(valor).strip()
    if not bruto:
        return ""
    digitos = re.sub(r"\D", "", bruto)
    if len(digitos) >= 11:
        return digitos
    return re.sub(r"\s+", " ", bruto).strip().upper()


def normalizar_nome_loja(nome: str) -> str:
    """Normaliza nome da loja para comparacao robusta."""
    if not nome:
        return ""
    texto = re.sub(r"\s+", " ", str(nome)).strip().lower()
    texto = "".join(
        ch for ch in unicodedata.normalize("NFD", texto)
        if unicodedata.category(ch) != "Mn"
    )
    return texto


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


def _mapa_grupos_lojas(agrupamentos_usuario: List[dict] = None) -> Dict[str, Set[str]]:
    """Mapeia nome de grupo normalizado -> conjunto de lojas normalizadas."""
    mapa: Dict[str, Set[str]] = {}
    for g in (agrupamentos_usuario or []):
        nome_g = normalizar_nome_loja((g or {}).get("nome", ""))
        if not nome_g:
            continue
        nomes_lojas = (g or {}).get("nomes_lojas", []) or []
        lojas_norm = {
            normalizar_nome_loja(x) for x in nomes_lojas
            if normalizar_nome_loja(x)
        }
        if lojas_norm:
            mapa[nome_g] = lojas_norm
    return mapa


def _contato_lojas_alvo_norm(contato: object, grupos_map: Dict[str, Set[str]]) -> Set[str]:
    """
    Resolve lojas alvo de um contato via campos novos:
    - lojas_json
    - grupos_json
    """
    lojas_raw = _parse_json_list(getattr(contato, "lojas_json", "[]"))
    grupos_raw = _parse_json_list(getattr(contato, "grupos_json", "[]"))
    alvo = {normalizar_nome_loja(x) for x in lojas_raw if normalizar_nome_loja(x)}
    for g in grupos_raw:
        gn = normalizar_nome_loja(g)
        if gn and gn in grupos_map:
            alvo.update(grupos_map[gn])
    return alvo


def _arquivo_ext_permitido(path: str) -> bool:
    """Aceita somente arquivos enviados diretamente (sem pasta/zip)."""
    ext = os.path.splitext(str(path or ""))[1].lower()
    return ext in _ALLOWED_FILE_EXTENSIONS


def _listar_arquivos_loja(pasta_saida: str, loja_nome: str, pdf_hint: str = "") -> List[str]:
    """
    Lista arquivos de envio de uma loja:
    - PDF de etiquetas
    - imagens auxiliares
    - XLSX/XLS de resumo
    """
    pasta_loja = os.path.join(pasta_saida, loja_nome)
    arquivos: List[str] = []

    if os.path.isdir(pasta_loja):
        for raiz, _, nomes in os.walk(pasta_loja):
            for nome in nomes:
                caminho = os.path.abspath(os.path.join(raiz, nome))
                if _arquivo_ext_permitido(caminho):
                    arquivos.append(caminho)

    # Compatibilidade: garante o PDF principal do resultado, quando existir.
    if pdf_hint:
        caminho_pdf = os.path.abspath(os.path.join(pasta_loja, pdf_hint))
        if os.path.isfile(caminho_pdf) and _arquivo_ext_permitido(caminho_pdf):
            arquivos.append(caminho_pdf)

    dedupe = []
    vistos = set()
    for p in arquivos:
        if p in vistos:
            continue
        vistos.add(p)
        dedupe.append(p)

    dedupe.sort(key=lambda p: (_EXT_ORDER.get(os.path.splitext(p)[1].lower(), 9), os.path.basename(p).lower()))
    return dedupe


def _dedupe_contatos(contatos: List[object]) -> List[object]:
    """Remove contatos duplicados preservando ordem."""
    vistos = set()
    out = []
    for c in contatos:
        chave = (
            getattr(c, "id", None),
            str(getattr(c, "telefone", "")).strip(),
            _normalizar_cnpj_chave(getattr(c, "loja_cnpj", "")),
            normalizar_nome_loja(getattr(c, "loja_nome", "")),
        )
        if chave in vistos:
            continue
        vistos.add(chave)
        out.append(c)
    return out


def indexar_contatos(contatos: List[object]) -> Tuple[Dict[str, List[object]], Dict[str, List[object]]]:
    """Indexa contatos ativos por CNPJ e por nome normalizado."""
    por_cnpj: Dict[str, List[object]] = {}
    por_nome: Dict[str, List[object]] = {}
    for c in contatos:
        cnpj_key = _normalizar_cnpj_chave(getattr(c, "loja_cnpj", ""))
        if cnpj_key:
            por_cnpj.setdefault(cnpj_key, []).append(c)

        nome_key = normalizar_nome_loja(getattr(c, "loja_nome", ""))
        if nome_key:
            por_nome.setdefault(nome_key, []).append(c)
    return por_cnpj, por_nome


def resolver_contatos_loja(
    loja_cnpj: str,
    loja_nome: str,
    contatos_por_cnpj: Dict[str, List[object]],
    contatos_por_nome: Dict[str, List[object]],
    contatos_todos: List[object] = None,
    grupos_map: Dict[str, Set[str]] = None,
) -> List[object]:
    """
    Resolve contatos de uma loja usando:
    1) CNPJ/identificador normalizado
    2) Nome normalizado (fallback)
    """
    encontrados = []
    cnpj_key = _normalizar_cnpj_chave(loja_cnpj)
    nome_key = normalizar_nome_loja(loja_nome)

    if cnpj_key and cnpj_key in contatos_por_cnpj:
        encontrados.extend(contatos_por_cnpj[cnpj_key])
    if nome_key and nome_key in contatos_por_nome:
        encontrados.extend(contatos_por_nome[nome_key])

    # Novo modelo: contato com selecao explicita de lojas/grupos.
    for c in (contatos_todos or []):
        lojas_alvo = _contato_lojas_alvo_norm(c, grupos_map or {})
        if lojas_alvo and nome_key in lojas_alvo:
            encontrados.append(c)

    return _dedupe_contatos(encontrados)


def montar_entregas_por_resultado(
    resultado: dict,
    pasta_saida: str,
    contatos: List[object],
    agrupamentos_usuario: List[dict] = None,
) -> Tuple[List[dict], dict]:
    """
    Monta entregas WhatsApp a partir de `resultado["lojas"]`,
    enviando todo conteudo util da pasta da loja (PDF + imagem + XLSX).

    Retorna:
    - entregas: lista com telefone/file_path/loja/caption
    - diagnostico: contadores e amostras de lojas ignoradas
    """
    lojas = (resultado or {}).get("lojas", []) or []
    contatos_por_cnpj, contatos_por_nome = indexar_contatos(contatos or [])
    grupos_map = _mapa_grupos_lojas(agrupamentos_usuario or [])

    entregas: List[dict] = []
    diagnostico = {
        "total_lojas_resultado": len(lojas),
        "lojas_sem_pdf": 0,
        "lojas_sem_arquivo": 0,
        "lojas_sem_contato": 0,
        "lojas_com_entrega": 0,
        "arquivos_totais": 0,
        "entregas_totais": 0,
        "amostra_sem_contato": [],
        "amostra_sem_arquivo": [],
    }
    dedupe_entrega = set()

    timestamp = (resultado or {}).get("timestamp", "")
    for loja_info in lojas:
        cnpj = str((loja_info or {}).get("cnpj", "") or "")
        nome = str((loja_info or {}).get("nome", "") or "")
        pdf_nome = str((loja_info or {}).get("pdf", "") or "")
        arquivos_loja = _listar_arquivos_loja(pasta_saida, nome, pdf_hint=pdf_nome)
        if not arquivos_loja:
            diagnostico["lojas_sem_arquivo"] += 1
            if len(diagnostico["amostra_sem_arquivo"]) < 10:
                diagnostico["amostra_sem_arquivo"].append({"loja": nome, "arquivo": pdf_nome or ""})
            continue

        contatos_destino = resolver_contatos_loja(
            loja_cnpj=cnpj,
            loja_nome=nome,
            contatos_por_cnpj=contatos_por_cnpj,
            contatos_por_nome=contatos_por_nome,
            contatos_todos=contatos or [],
            grupos_map=grupos_map,
        )
        if not contatos_destino:
            diagnostico["lojas_sem_contato"] += 1
            if len(diagnostico["amostra_sem_contato"]) < 10:
                diagnostico["amostra_sem_contato"].append({"loja": nome, "cnpj": cnpj})
            continue

        diagnostico["lojas_com_entrega"] += 1
        caption = f"Etiquetas {nome} - {timestamp}"
        for contato in contatos_destino:
            telefone = str(getattr(contato, "telefone", "") or "").strip()
            if not telefone:
                continue
            for file_path in arquivos_loja:
                chave = (telefone, os.path.abspath(file_path))
                if chave in dedupe_entrega:
                    continue
                dedupe_entrega.add(chave)
                entregas.append({
                    "telefone": telefone,
                    "file_path": file_path,
                    # Compatibilidade com campo legado (fila usa pdf_path no DB)
                    "pdf_path": file_path,
                    "file_name": os.path.basename(file_path),
                    "loja": nome,
                    "caption": f"{caption} - {os.path.basename(file_path)}",
                })
                diagnostico["arquivos_totais"] += 1

    diagnostico["entregas_totais"] = len(entregas)
    return entregas, diagnostico


def montar_destinos_por_resultado(
    resultado: dict,
    pasta_saida: str,
    contatos: List[object],
    destino_attr: str,
    agrupamentos_usuario: List[dict] = None,
) -> Tuple[List[dict], dict]:
    """
    Monta lista de destinos generica (WhatsApp/Email) a partir de resultado,
    enviando todo conteudo util da pasta da loja (PDF + imagem + XLSX).

    Retorna lista de dicts:
    - destino
    - file_path
    - loja
    - caption
    """
    lojas = (resultado or {}).get("lojas", []) or []
    contatos_por_cnpj, contatos_por_nome = indexar_contatos(contatos or [])
    grupos_map = _mapa_grupos_lojas(agrupamentos_usuario or [])

    envios: List[dict] = []
    diagnostico = {
        "total_lojas_resultado": len(lojas),
        "lojas_sem_pdf": 0,
        "lojas_sem_arquivo": 0,
        "lojas_sem_contato": 0,
        "lojas_com_entrega": 0,
        "arquivos_totais": 0,
        "entregas_totais": 0,
        "amostra_sem_contato": [],
        "amostra_sem_arquivo": [],
    }
    dedupe = set()
    timestamp = (resultado or {}).get("timestamp", "")

    for loja_info in lojas:
        cnpj = str((loja_info or {}).get("cnpj", "") or "")
        nome = str((loja_info or {}).get("nome", "") or "")
        pdf_nome = str((loja_info or {}).get("pdf", "") or "")
        arquivos_loja = _listar_arquivos_loja(pasta_saida, nome, pdf_hint=pdf_nome)
        if not arquivos_loja:
            diagnostico["lojas_sem_arquivo"] += 1
            if len(diagnostico["amostra_sem_arquivo"]) < 10:
                diagnostico["amostra_sem_arquivo"].append({"loja": nome, "arquivo": pdf_nome or ""})
            continue

        contatos_destino = resolver_contatos_loja(
            loja_cnpj=cnpj,
            loja_nome=nome,
            contatos_por_cnpj=contatos_por_cnpj,
            contatos_por_nome=contatos_por_nome,
            contatos_todos=contatos or [],
            grupos_map=grupos_map,
        )
        if not contatos_destino:
            diagnostico["lojas_sem_contato"] += 1
            if len(diagnostico["amostra_sem_contato"]) < 10:
                diagnostico["amostra_sem_contato"].append({"loja": nome, "cnpj": cnpj})
            continue

        diagnostico["lojas_com_entrega"] += 1
        caption = f"Etiquetas {nome} - {timestamp}"
        for contato in contatos_destino:
            destino = str(getattr(contato, destino_attr, "") or "").strip()
            if not destino:
                continue
            for file_path in arquivos_loja:
                chave = (destino.lower(), os.path.abspath(file_path))
                if chave in dedupe:
                    continue
                dedupe.add(chave)
                envios.append({
                    "destino": destino,
                    "file_path": file_path,
                    # Compatibilidade com fluxo legado que espera pdf_path.
                    "pdf_path": file_path,
                    "file_name": os.path.basename(file_path),
                    "loja": nome,
                    "caption": f"{caption} - {os.path.basename(file_path)}",
                    "nome_contato": str(getattr(contato, "nome_contato", "") or "").strip(),
                })
                diagnostico["arquivos_totais"] += 1

    diagnostico["entregas_totais"] = len(envios)
    return envios, diagnostico
