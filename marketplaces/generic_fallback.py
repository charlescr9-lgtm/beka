# -*- coding: utf-8 -*-
from __future__ import annotations
import os
from typing import Optional, List

import fitz

from .base import DetectedDoc, ExtractedEtiqueta


class GenericFallbackDriver:
    name = "generic_fallback"
    kind = "generic_fallback"

    def detect(self, pdf_path: str) -> Optional[DetectedDoc]:
        if not os.path.isfile(pdf_path) or not pdf_path.lower().endswith(".pdf"):
            return None
        return DetectedDoc(kind=self.kind, confidence=0.01, meta={}, source_path=pdf_path)

    def extract(self, doc: DetectedDoc) -> List[ExtractedEtiqueta]:
        pdf_path = doc.source_path
        d = fitz.open(pdf_path)
        etqs = []
        for i in range(len(d)):
            etqs.append(ExtractedEtiqueta(
                cnpj="",
                nf="?",
                pagina=i,
                pdf_path=pdf_path,
                dados_xml={"produtos": []},
                tipo_especial="normal",
            ))
        d.close()
        return etqs
