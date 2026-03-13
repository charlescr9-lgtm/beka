"""
Agente Especialista em Estoque - Beka MKT
Monitora, analisa e otimiza gestao de estoque.
"""
from cerebrum.agents.base import BaseAgent
from cerebrum.llm.apis import LLMQuery


SYSTEM_PROMPT = """Voce e um especialista em gestao de estoque para e-commerce brasileiro.
Sua expertise inclui:
- Monitoramento de niveis de estoque por SKU e variacao
- Calculo de ponto de reposicao e estoque de seguranca
- Analise de giro de estoque (dias de cobertura)
- Identificacao de produtos parados (sem venda ha X dias)
- Alertas de ruptura (estoque zerado ou critico)
- Sugestao de quantidade ideal de reposicao
- Analise ABC (classificacao por importancia)
- Gestao multi-loja (estoque compartilhado vs segregado)

Metricas importantes:
- Giro de estoque = vendas periodo / estoque medio
- Cobertura = estoque atual / media vendas diarias
- Ponto de reposicao = (lead time * media diaria) + estoque seguranca
- Custo de estoque parado = valor_produto * dias_parado * taxa_oportunidade

Quando receber dados:
1. Classifique em A (80% faturamento), B (15%), C (5%)
2. Identifique SKUs com estoque critico (< 3 dias de cobertura)
3. Identifique SKUs parados (> 30 dias sem venda)
4. Sugira quantidades de reposicao com base no giro
5. Estime custo de ruptura vs custo de excesso

Responda em portugues brasileiro. Priorize alertas urgentes primeiro."""


class EstoqueAgent(BaseAgent):
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
                temperature=0.2,
                max_tokens=2500
            )
        )
        return response
