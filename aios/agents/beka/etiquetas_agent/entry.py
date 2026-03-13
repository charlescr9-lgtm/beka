"""
Agente Especialista em Etiquetas - Beka MKT
Analisa, processa e otimiza etiquetas de envio para marketplaces.
"""
from cerebrum.agents.base import BaseAgent
from cerebrum.llm.apis import LLMQuery


SYSTEM_PROMPT = """Voce e um especialista em etiquetas de envio para e-commerce brasileiro.
Sua expertise inclui:
- Processamento de PDFs de etiquetas (Shopee, TikTok Shop, Temu, Mercado Livre)
- Layout e formatacao: margens, fontes, dimensoes de etiqueta
- Deteccao de problemas: texto cortado, codigo de barras ilegivel, informacoes faltantes
- Configuracao ideal para impressoras termicas e jato de tinta
- Regras de cada marketplace para etiquetas

Contexto do sistema Beka MKT:
- Processa etiquetas em lote (PDF com multiplas etiquetas)
- Suporta configuracao de largura/altura em mm, margens, fonte de produto
- Extrai dados do XML da NF-e (DANFE simplificada)
- Organiza por SKU e agrupa por loja
- Gera PDF final com etiqueta + dados do produto + codigo de barras

Responda sempre em portugues brasileiro, de forma pratica e direta.
Quando sugerir configuracoes, use valores numericos especificos."""


class EtiquetasAgent(BaseAgent):
    def __init__(self, agent_name, task_input, config_):
        super().__init__(agent_name, task_input, config_)

    def run(self):
        response = self.send_request(
            agent_name=self.agent_name,
            query=LLMQuery(
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": self.task_input}
                ],
                temperature=0.3,
                max_tokens=2000
            )
        )
        return response
