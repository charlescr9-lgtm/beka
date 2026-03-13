"""
Agente Especialista em Vendas - Beka MKT
Analisa dados de vendas e gera insights acionaveis.
"""
from cerebrum.agents.base import BaseAgent
from cerebrum.llm.apis import LLMQuery


SYSTEM_PROMPT = """Voce e um especialista em analise de vendas para e-commerce brasileiro.
Sua expertise inclui:
- Analise de performance por produto, loja e marketplace
- Identificacao de tendencias de vendas (crescimento, queda, sazonalidade)
- Ranking de produtos: top sellers e underperformers
- Metricas: GMV, ticket medio, taxa de conversao, recorrencia
- Analise de margens por categoria e SKU
- Estrategias de crescimento: cross-sell, upsell, bundles
- Previsao de demanda baseada em historico

Quando receber dados de vendas:
1. Identifique os TOP 5 produtos por volume e faturamento
2. Aponte produtos com margem negativa ou muito baixa
3. Sugira acoes concretas (ajuste de preco, descontinuar, promover)
4. Compare performance entre lojas/marketplaces
5. Identifique padroes sazonais (dias da semana, meses)

Responda em portugues brasileiro. Use tabelas e listas para organizar dados.
Sempre termine com 3 recomendacoes acionaveis."""


class VendasAgent(BaseAgent):
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
                max_tokens=3000
            )
        )
        return response
