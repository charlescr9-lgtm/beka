# -*- coding: utf-8 -*-
"""
Normalizador de PDFs DANFE - Shopee Central do Vendedor
Recorta e normaliza PDFs "DANFE SIMPLIFICADO - ETIQUETA" para o padrao 150x230mm,
permitindo misturar com etiquetas de outros formatos no mesmo lote.
"""

import os
import tempfile
import fitz  # PyMuPDF


DANFE_MARKERS = (
    "DANFE SIMPLIFICADO - ETIQUETA",
    "DECLARAÇÃO DE CONTEÚDO",
    "DECLARACAO DE CONTEUDO",
)


def _mm_to_pt(mm: float) -> float:
    return mm * 72.0 / 25.4


def _looks_like_danfe(page: fitz.Page) -> bool:
    text = (page.get_text("text") or "").upper()
    return any(m.upper() in text for m in DANFE_MARKERS)


def _union_content_rect(page: fitz.Page):
    d = page.get_text("dict")
    rects = []
    for b in d.get("blocks", []):
        bbox = b.get("bbox")
        if not bbox:
            continue
        r = fitz.Rect(bbox)
        if r.get_area() < 10:
            continue
        rects.append(r)
    if not rects:
        return None
    u = rects[0]
    for r in rects[1:]:
        u |= r
    return u


def _eh_pdf_shein(caminho_pdf: str) -> bool:
    """Detecta se o PDF e do tipo Shein (nao normalizar)."""
    try:
        doc = fitz.open(caminho_pdf)
        if len(doc) < 2:
            doc.close()
            return False
        p0 = doc[0].get_text()
        p1 = doc[1].get_text()
        doc.close()
        p0_shein = "PUDO-PGK" in p0 or "Ref.No:GSH" in p0 or "Ref.No:GC" in p0
        p1_danfe = "DANFE" in p1.upper() and "CHAVE" in p1.upper()
        return p0_shein and p1_danfe
    except Exception:
        return False


def normalize_pdf_to_labels(
    input_pdf_path: str,
    output_pdf_path: str,
    target_w_mm: float = 150.0,
    target_h_mm: float = 230.0,
    padding_mm: float = 3.0,
    only_when_matches_danfe: bool = True,
) -> dict:
    """
    Normaliza PDF DANFE para paginas target_w_mm x target_h_mm.
    Retorna {"matched": bool, "pages_in": int, "pages_out": int}.
    """
    if not os.path.exists(input_pdf_path):
        raise FileNotFoundError(input_pdf_path)

    src = fitz.open(input_pdf_path)
    n_pages = len(src)

    if only_when_matches_danfe:
        matched = False
        for i in range(min(2, n_pages)):
            if _looks_like_danfe(src[i]):
                matched = True
                break
        if not matched:
            src.close()
            return {"matched": False, "pages_in": n_pages, "pages_out": n_pages}

    out = fitz.open()
    W = _mm_to_pt(target_w_mm)
    H = _mm_to_pt(target_h_mm)
    pad = _mm_to_pt(padding_mm)

    for i in range(n_pages):
        p = src[i]
        content_rect = _union_content_rect(p)
        if content_rect is None:
            out.new_page(width=W, height=H)
            continue

        page_rect = p.rect
        r = fitz.Rect(
            max(page_rect.x0, content_rect.x0 - pad),
            max(page_rect.y0, content_rect.y0 - pad),
            min(page_rect.x1, content_rect.x1 + pad),
            min(page_rect.y1, content_rect.y1 + pad),
        )

        newp = out.new_page(width=W, height=H)

        rw, rh = r.width, r.height
        if rw <= 1 or rh <= 1:
            r = page_rect
            rw, rh = r.width, r.height

        scale = min(W / rw, H / rh)
        dest_w = rw * scale
        dest_h = rh * scale
        x0 = (W - dest_w) / 2
        y0 = (H - dest_h) / 2
        dest = fitz.Rect(x0, y0, x0 + dest_w, y0 + dest_h)

        newp.show_pdf_page(dest, src, i, clip=r)

    out.save(output_pdf_path)
    out.close()
    src.close()

    out_check = fitz.open(output_pdf_path)
    n_out = len(out_check)
    out_check.close()
    return {"matched": True, "pages_in": n_pages, "pages_out": n_out}


def normalize_pdf_to_labels_inplace(
    caminho_pdf: str,
    largura_mm: float = 150.0,
    altura_mm: float = 230.0,
) -> bool:
    """
    Normaliza PDF DANFE in-place (substitui o arquivo original).
    Retorna True se normalizou, False caso contrario.
    """
    if not os.path.isfile(caminho_pdf):
        return False

    if _eh_pdf_shein(caminho_pdf):
        return False

    tmp_fd = None
    tmp_path = None
    try:
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".pdf", prefix="norm_")
        os.close(tmp_fd)

        result = normalize_pdf_to_labels(
            caminho_pdf,
            tmp_path,
            target_w_mm=largura_mm,
            target_h_mm=altura_mm,
            padding_mm=3.0,
            only_when_matches_danfe=True,
        )

        if not result["matched"]:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return False

        os.replace(tmp_path, caminho_pdf)
        return True

    except Exception:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        raise


def normalizar_pdfs_danfe_pasta(pasta, largura_mm=150, altura_mm=230, log_callback=None, error_callback=None):
    """
    Percorre a pasta, normaliza PDFs DANFE e retorna lista de arquivos normalizados.
    log_callback(nome_arquivo) e chamado para cada PDF normalizado com sucesso.
    error_callback(nome, mensagem) e chamado em caso de erro ao normalizar.
    """
    PDFS_ESPECIAIS = ["lanim.pdf", "shein crua.pdf", "shein.pdf"]
    especiais_lower = [p.lower() for p in PDFS_ESPECIAIS]

    pdfs = [
        f for f in os.listdir(pasta)
        if f.lower().endswith(".pdf")
        and not f.startswith("_")
        and not f.startswith("etiquetas_prontas")
        and not f.lower().startswith("lanim")
        and f.lower() not in especiais_lower
    ]

    normalizados = []
    for nome in pdfs:
        caminho = os.path.join(pasta, nome)
        try:
            if normalize_pdf_to_labels_inplace(caminho, largura_mm, altura_mm):
                normalizados.append(nome)
                if log_callback:
                    log_callback(nome)
        except Exception as e:
            if error_callback:
                error_callback(nome, str(e))
    return normalizados
