"""
ShopeeParser - Wrapper para ProcessadorEtiquetasShopee
Adaptação temporária do código legado para nova arquitetura
"""
import sys
import os

# Adiciona pasta raiz ao path para importar etiquetas_shopee
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.marketplace_parser import MarketplaceParser, Produto, DadosEnvio, NotaFiscal
from etiquetas_shopee import ProcessadorEtiquetasShopee
import re


class ShopeeParser(MarketplaceParser):
    """
    Parser para arquivos ZIP da Shopee contendo XMLs de nota fiscal
    
    Este é um wrapper do código legado ProcessadorEtiquetasShopee
    adaptado para a nova interface MarketplaceParser.
    """
    
    def __init__(self, arquivo_path: str):
        super().__init__(arquivo_path)
        self.processador = ProcessadorEtiquetasShopee()
        
    def parse(self) -> bool:
        """
        Faz parse do ZIP/pasta contendo XMLs da Shopee
        
        Returns:
            bool: True se sucesso, False se erro
        """
        try:
            # Determina se é ZIP ou pasta
            if os.path.isfile(self.arquivo_path) and self.arquivo_path.lower().endswith('.zip'):
                # É um ZIP
                self.processador._carregar_zip(self.arquivo_path)
            elif os.path.isdir(self.arquivo_path):
                # É uma pasta
                self.processador.carregar_todos_xmls(self.arquivo_path)
            else:
                return False
            
            # Converte dados do processador para o formato padrão
            self._converter_dados()
            
            return len(self._produtos) > 0
            
        except Exception as e:
            print(f"Erro no parse: {e}")
            return False
    
    def _converter_dados(self):
        """Converte dados do processador legado para formato padrão"""
        # Para cada XML carregado, extrai produtos
        for nf, dados in self.processador.dados_xml.items():
            # Produtos
            if 'produtos' in dados:
                for item in dados['produtos']:
                    prod = Produto(
                        sku=item.get('sku', ''),
                        nome=item.get('titulo', ''),
                        quantidade=item.get('qtd', 1),
                        preco=float(item.get('preco', 0)),
                        variacao=item.get('variacao', '')
                    )
                    self._produtos.append(prod)
            
            # Dados de envio (pega do primeiro XML com dados completos)
            if not self._dados_envio and 'destinatario' in dados:
                dest = dados['destinatario']
                self._dados_envio = DadosEnvio(
                    nome=dest.get('nome', ''),
                    endereco=dest.get('endereco', ''),
                    cidade=dest.get('cidade', ''),
                    estado=dest.get('uf', ''),
                    cep=dest.get('cep', ''),
                    telefone=dest.get('telefone', '')
                )
            
            # Nota fiscal (pega da primeira)
            if not self._nota_fiscal:
                self._nota_fiscal = NotaFiscal(
                    numero=dados.get('nf', ''),
                    chave=dados.get('chave_nf', ''),
                    emitente=dados.get('emitente', ''),
                    cnpj=dados.get('cnpj', '')
                )
    
    def get_marketplace_nome(self) -> str:
        """Retorna nome do marketplace"""
        return "Shopee"
    
    def get_nome_emitente_limpo(self) -> str:
        """Retorna nome do emitente limpo para usar em nomes de arquivo"""
        if self._nota_fiscal:
            nome = self._nota_fiscal.emitente
            # Remove números de CNPJ do início
            nome = re.sub(r'^\d[\d.]+\s+', '', nome)
            # Remove CPF
            nome = re.sub(r'\s+\d{11}$', '', nome)
            # Remove LTDA, ME, MEI, etc
            nome = re.sub(r'\s+(LTDA|ME|MEI|EPP|EIRELI)\s*$', '', nome, flags=re.IGNORECASE)
            # Capitaliza
            nome = nome.strip().title()
            # Remove caracteres inválidos
            nome = re.sub(r'[<>:"/\\|?*]', '', nome)
            return nome.strip() or 'Loja_Desconhecida'
        return 'Desconhecido'
