# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Protocol


@dataclass
class DetectedDoc:
    kind: str
    confidence: float
    meta: Dict[str, Any]
    source_path: str


@dataclass
class ExtractedEtiqueta:
    cnpj: str
    nf: str
    pagina: int
    pdf_path: str
    dados_xml: Dict[str, Any]
    tipo_especial: str = "normal"


class MarketplaceDriver(Protocol):
    name: str

    def detect(self, pdf_path: str) -> Optional[DetectedDoc]:
        ...

    def extract(self, doc: DetectedDoc) -> List[ExtractedEtiqueta]:
        ...
