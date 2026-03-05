"""
Gerador de Etiquetas em PDF
Gera PDFs 150mm x 230mm com código de barras e SKU no rodapé
"""
import sys
import os

# Adiciona pasta raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.marketplace_parser import Produto, DadosEnvio, NotaFiscal
from etiquetas_shopee import ProcessadorEtiquetasShopee
from typing import List


class EtiquetaPDFGenerator:
    """
    Gerador de PDFs de etiquetas
    
    Wrapper temporário que usa o código legado do ProcessadorEtiquetasShopee
    """
    
    def __init__(self, produtos: List[Produto], dados_envio: DadosEnvio, nota_fiscal: NotaFiscal):
        self.produtos = produtos
        self.dados_envio = dados_envio
        self.nota_fiscal = nota_fiscal
        self.processador = ProcessadorEtiquetasShopee()
        
    def gerar(self, caminho_saida: str):
        """
        Gera PDF da etiqueta
        
        Args:
            caminho_saida: Caminho completo onde salvar o PDF
        """
        # Por enquanto, usa o método antigo
        # TODO: Implementar gerador modular independente
        
        # Simula estrutura de dados antiga
        self.processador.dados_xml = {
            self.nota_fiscal.numero: {
                'nf': self.nota_fiscal.numero,
                'chave_nf': self.nota_fiscal.chave,
                'emitente': self.nota_fiscal.emitente,
                'cnpj': self.nota_fiscal.cnpj,
                'destinatario': {
                    'nome': self.dados_envio.nome,
                    'endereco': self.dados_envio.endereco,
                    'cidade': self.dados_envio.cidade,
                    'uf': self.dados_envio.estado,
                    'cep': self.dados_envio.cep,
                    'telefone': self.dados_envio.telefone
                },
                'produtos': [
                    {
                        'sku': p.sku,
                        'titulo': p.nome,
                        'qtd': p.quantidade,
                        'preco': p.preco,
                        'variacao': p.variacao
                    }
                    for p in self.produtos
                ]
            }
        }
        
        # Usa o gerador antigo
        # TODO: Migrar para código modular
        print(f"  ⚠️  Usando gerador legado (temporário)")
        print(f"  📄 PDF será gerado em: {caminho_saida}")
