# -*- coding: utf-8 -*-
from __future__ import annotations
import re
from typing import Optional, List, Dict, Any

import fitz

from .base import DetectedDoc, ExtractedEtiqueta


class TemuDriver:
    name = "temu"
    kind = "temu"

    def detect(self, pdf_path: str) -> Optional[DetectedDoc]:
        try:
            doc = fitz.open(pdf_path)
            sample = ""
            for i in range(min(2, len(doc))):
                sample += "\n" + (doc[i].get_text("text") or "")
            doc.close()

            up = sample.upper()
            score = 0.0

            if "TEMU" in up:
                score += 0.65
            if "ORDER" in up or "PEDIDO" in up:
                score += 0.10
            if "SHIP" in up or "ENVIO" in up:
                score += 0.05

            if score >= 0.70:
                return DetectedDoc(
                    kind=self.kind,
                    confidence=min(score, 1.0),
                    meta={},
                    source_path=pdf_path,
                )
            return None
        except Exception:
            return None

    def extract(self, docdet: DetectedDoc) -> List[ExtractedEtiqueta]:
        pdf_path = docdet.source_path
        doc = fitz.open(pdf_path)
        etqs: List[ExtractedEtiqueta] = []

        for i in range(len(doc)):
            tx = doc[i].get_text("text") or ""
            produtos = _parse_produtos_temu(tx)
            etqs.append(ExtractedEtiqueta(
                cnpj="",
                nf=_guess_order(tx),
                pagina=i,
                pdf_path=pdf_path,
                dados_xml={"produtos": produtos},
                tipo_especial="normal",
            ))

        doc.close()
        return etqs


def _guess_order(text: str) -> str:
    m = re.search(r"\b(ORDER|PEDIDO)\s*[:#]?\s*([A-Z0-9\-]{6,})\b", text, re.IGNORECASE)
    return m.group(2) if m else "?"


def _parse_produtos_temu(text: str) -> List[Dict[str, Any]]:
    lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
    prods: List[Dict[str, Any]] = []

    for l in lines:
        up = l.upper()
        if "SKU" in up and ("QTY" in up or "QUANTITY" in up or "QTD" in up):
            sku = ""
            qtd = "1"
            msku = re.search(r"SKU\s*[:#]?\s*([A-Z0-9\-_\.]{2,})", l, re.IGNORECASE)
            if msku:
                sku = msku.group(1)
            mq = re.search(r"(QTY|QUANTITY|QTD)\s*[:#]?\s*(\d+)", l, re.IGNORECASE)
            if mq:
                qtd = mq.group(2)
            if sku:
                prods.append({"codigo": sku, "variacao": "", "qtd": qtd, "descricao": ""})

    return prods
