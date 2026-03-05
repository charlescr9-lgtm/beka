"""
Interface base para parsers de marketplaces
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from dataclasses import dataclass


@dataclass
class Produto:
    """Modelo de dados de um produto"""
    sku: str
    nome: str
    quantidade: int
    preco: float = 0.0
    variacao: str = ""
    
    
@dataclass
class DadosEnvio:
    """Dados de envio/destinatário"""
    nome: str
    endereco: str
    cidade: str
    estado: str
    cep: str
    telefone: str = ""
    

@dataclass
class NotaFiscal:
    """Dados da nota fiscal"""
    numero: str
    chave: str
    emitente: str
    cnpj: str
    

class MarketplaceParser(ABC):
    """
    Classe base para parsers de marketplaces.
    
    Cada marketplace (Shopee, Shein, ML, TikTok, Temu) deve implementar esta interface.
    """
    
    def __init__(self, arquivo_path: str):
        self.arquivo_path = arquivo_path
        self._produtos: List[Produto] = []
        self._dados_envio: DadosEnvio = None
        self._nota_fiscal: NotaFiscal = None
        
    @abstractmethod
    def parse(self) -> bool:
        """
        Faz parse do arquivo do marketplace.
        
        Returns:
            bool: True se sucesso, False se erro
        """
        pass
    
    @abstractmethod
    def get_marketplace_nome(self) -> str:
        """Retorna nome do marketplace (ex: 'Shopee', 'Shein')"""
        pass
    
    def get_produtos(self) -> List[Produto]:
        """Retorna lista de produtos parseados"""
        return self._produtos
    
    def get_dados_envio(self) -> DadosEnvio:
        """Retorna dados de envio"""
        return self._dados_envio
    
    def get_nota_fiscal(self) -> NotaFiscal:
        """Retorna dados da nota fiscal"""
        return self._nota_fiscal
    
    def get_resumo(self) -> Dict[str, Any]:
        """Retorna resumo dos dados parseados"""
        return {
            'marketplace': self.get_marketplace_nome(),
            'total_produtos': len(self._produtos),
            'total_itens': sum(p.quantidade for p in self._produtos),
            'tem_nf': self._nota_fiscal is not None,
            'tem_envio': self._dados_envio is not None
        }
