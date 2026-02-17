#!/usr/bin/env python3
"""
Beka MKT - Sistema de Geração de Etiquetas
Versão modular - Suporta múltiplos marketplaces
"""
import os
import sys
from pathlib import Path
from parsers.shopee_parser import ShopeeParser
from generators.etiqueta_pdf import EtiquetaPDFGenerator


def processar_pasta(pasta_xmls: str, pasta_output: str = "output"):
    """
    Processa todos os XMLs de uma pasta e gera etiquetas em PDF
    
    Args:
        pasta_xmls: Pasta contendo os arquivos XML
        pasta_output: Pasta onde os PDFs serão salvos
    """
    pasta_xmls = Path(pasta_xmls)
    pasta_output = Path(pasta_output)
    
    # Cria pasta de output se não existir
    pasta_output.mkdir(exist_ok=True)
    
    # Procura arquivos XML
    xmls = list(pasta_xmls.glob("*.xml"))
    
    if not xmls:
        print(f"❌ Nenhum arquivo XML encontrado em {pasta_xmls}")
        return
    
    print(f"📁 Encontrados {len(xmls)} arquivos XML")
    print(f"📂 PDFs serão salvos em: {pasta_output.absolute()}\n")
    
    sucesso = 0
    falhas = 0
    
    for xml_path in xmls:
        print(f"⏳ Processando: {xml_path.name}")
        
        try:
            # Parse do XML
            parser = ShopeeParser(str(xml_path))
            if not parser.parse():
                print(f"  ❌ Erro ao fazer parse do XML")
                falhas += 1
                continue
            
            # Info do parse
            resumo = parser.get_resumo()
            print(f"  📦 Marketplace: {resumo['marketplace']}")
            print(f"  📦 Produtos: {resumo['total_produtos']} ({resumo['total_itens']} itens)")
            
            # Gera PDF
            gerador = EtiquetaPDFGenerator(
                produtos=parser.get_produtos(),
                dados_envio=parser.get_dados_envio(),
                nota_fiscal=parser.get_nota_fiscal()
            )
            
            # Nome do PDF baseado no emitente
            nome_emitente = parser.get_nome_emitente_limpo()
            pdf_path = pasta_output / f"etiqueta_{nome_emitente}.pdf"
            
            gerador.gerar(str(pdf_path))
            print(f"  ✅ PDF gerado: {pdf_path.name}\n")
            sucesso += 1
            
        except Exception as e:
            print(f"  ❌ Erro: {str(e)}\n")
            falhas += 1
            continue
    
    # Resumo final
    print("="*50)
    print(f"✅ Sucesso: {sucesso}")
    print(f"❌ Falhas: {falhas}")
    print(f"📊 Total: {len(xmls)}")
    print("="*50)


def main():
    """Ponto de entrada do programa"""
    print("="*50)
    print("  BEKA MKT - Gerador de Etiquetas v2.0")
    print("  Sistema Modular Multi-Marketplace")
    print("="*50)
    print()
    
    if len(sys.argv) < 2:
        print("Uso: python main.py <pasta_xmls> [pasta_output]")
        print()
        print("Exemplos:")
        print("  python main.py xmls_extraidos")
        print("  python main.py xmls_extraidos output_personalizado")
        sys.exit(1)
    
    pasta_xmls = sys.argv[1]
    pasta_output = sys.argv[2] if len(sys.argv) > 2 else "output"
    
    if not os.path.exists(pasta_xmls):
        print(f"❌ Pasta não encontrada: {pasta_xmls}")
        sys.exit(1)
    
    processar_pasta(pasta_xmls, pasta_output)


if __name__ == "__main__":
    main()
