# -*- coding: utf-8 -*-
from __future__ import annotations
import re
from typing import Optional, List, Dict, Any

import fitz  # PyMuPDF

from .base import DetectedDoc, ExtractedEtiqueta

RE_TRACK = re.compile(r"\b(BR\d{10,})\b", re.IGNORECASE)
RE_PEDIDO = re.compile(r"\bPEDIDO\s*[:#]?\s*([A-Z0-9]{8,})\b", re.IGNORECASE)
RE_RETIRADA = re.compile(r"\bRETIRADA\s*(PELO)?\s*COMPRADOR\b", re.IGNORECASE)


class ShopeeDanfeDriver:
    name = "shopee_danfe"
    kind = "shopee_danfe"

    def detect(self, pdf_path: str) -> Optional[DetectedDoc]:
        try:
            doc = fitz.open(pdf_path)
            sample = ""
            for i in range(min(3, len(doc))):
                sample += "\n" + (doc[i].get_text("text") or "")
            doc.close()

            up = sample.upper()
            score = 0.0

            if "DANFE SIMPLIFICADO - ETIQUETA" in up:
                score += 0.65
            if ("DECLARAÇÃO DE CONTEÚDO" in up) or ("DECLARACAO DE CONTEUDO" in up):
                score += 0.25
            if RE_TRACK.search(sample):
                score += 0.10

            if score >= 0.70:
                tracks = sorted(set(t.upper() for t in RE_TRACK.findall(sample)))
                return DetectedDoc(
                    kind=self.kind,
                    confidence=min(score, 1.0),
                    meta={"tracks": tracks},
                    source_path=pdf_path,
                )
            return None
        except Exception:
            return None

    def extract(self, docdet: DetectedDoc) -> List[ExtractedEtiqueta]:
        pdf_path = docdet.source_path
        doc = fitz.open(pdf_path)

        label_pages = []
        decl_pages = []
        tracks_by_page = {}
        pedido_by_page = {}
        retirada_by_page = {}

        for i in range(len(doc)):
            tx = doc[i].get_text("text") or ""
            up = tx.upper()

            if "DANFE SIMPLIFICADO - ETIQUETA" in up:
                label_pages.append(i)
            if ("DECLARAÇÃO DE CONTEÚDO" in up) or ("DECLARACAO DE CONTEUDO" in up):
                decl_pages.append(i)

            tracks = sorted(set(t.upper() for t in RE_TRACK.findall(tx)))
            if tracks:
                tracks_by_page[i] = tracks

            mp = RE_PEDIDO.search(tx)
            if mp:
                pedido_by_page[i] = mp.group(1).strip()

            retirada_by_page[i] = bool(RE_RETIRADA.search(tx))

        produtos_por_track: Dict[str, List[Dict[str, Any]]] = {}
        produtos_global: List[Dict[str, Any]] = []

        for i in decl_pages:
            tx = doc[i].get_text("text") or ""
            prods = _parse_declaracao_produtos(tx)
            if not prods:
                continue
            tracks = tracks_by_page.get(i, [])
            if tracks:
                for tr in tracks:
                    produtos_por_track.setdefault(tr, []).extend(prods)
            else:
                produtos_global.extend(prods)

        etiquetas: List[ExtractedEtiqueta] = []
        pages = label_pages or list(range(len(doc)))

        for i in pages:
            tx = doc[i].get_text("text") or ""
            tracks = tracks_by_page.get(i, [])
            pedido = pedido_by_page.get(i, "")
            retirada = retirada_by_page.get(i, False)

            produtos = []
            for tr in tracks:
                if tr in produtos_por_track:
                    produtos = produtos_por_track[tr]
                    break
            if not produtos and produtos_global:
                produtos = produtos_global

            dados_xml = {
                "produtos": produtos,
                "pedido": pedido,
                "tracking": (tracks[0] if tracks else ""),
                "modalidade": ("retirada" if retirada else ""),
            }

            etiquetas.append(
                ExtractedEtiqueta(
                    cnpj="",
                    nf=_guess_nf(tx),
                    pagina=i,
                    pdf_path=pdf_path,
                    dados_xml=dados_xml,
                    tipo_especial="normal",
                )
            )

        doc.close()
        return etiquetas


def _guess_nf(text: str) -> str:
    m = re.search(r"\bNF[:\s]*([0-9]{3,})\b", text, re.IGNORECASE)
    return m.group(1) if m else "?"


def _parse_declaracao_produtos(text: str) -> List[Dict[str, Any]]:
    lines = [l.strip() for l in (text or "").splitlines() if l.strip()]

    hdr = -1
    for idx, l in enumerate(lines):
        up = l.upper()
        if ("CODIGO" in up or "CÓDIGO" in up) and ("DESCRICAO" in up or "DESCRIÇÃO" in up) and ("QTD" in up or "Q." in up):
            hdr = idx
            break
        if ("Nº" in up or "NO" in up) and ("CODIGO" in up or "CÓDIGO" in up) and ("DESCRICAO" in up or "DESCRIÇÃO" in up):
            hdr = idx
            break

    if hdr < 0:
        return []

    data = lines[hdr + 1:]
    cleaned = []
    for l in data:
        up = l.upper()
        if up.startswith("P.") or "ASSINATURA" in up:
            break
        cleaned.append(l)

    prods: List[Dict[str, Any]] = []
    for l in cleaned:
        m = re.match(r"^\s*(\d+)\s+(\S+)\s+(.+)$", l)
        if not m:
            continue
        _, sku, tail = m.group(1), m.group(2), m.group(3)
        nums = re.findall(r"(\d+[.,]?\d*)", tail)
        qtd = "1"
        if len(nums) >= 2:
            qtd = nums[-2]

        tail_wo = tail
        if len(nums) >= 2:
            tail_wo = re.sub(r"\s+" + re.escape(nums[-1]) + r"\s*$", "", tail_wo)
            tail_wo = re.sub(r"\s+" + re.escape(nums[-2]) + r"\s*$", "", tail_wo)
        descricao = tail_wo.strip()

        variacao = ""
        mm = re.search(r"(.+)\s+([^\s]+,[^\s]+)\s*$", descricao)
        if mm:
            descricao = mm.group(1).strip()
            variacao = mm.group(2).strip()

        prods.append({
            "codigo": sku.strip(),
            "variacao": variacao,
            "qtd": str(int(float(qtd.replace(",", ".")))) if qtd else "1",
            "descricao": descricao[:80],
        })

    return prods
