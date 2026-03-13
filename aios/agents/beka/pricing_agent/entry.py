"""
Agente Especialista em Precificacao - Beka MKT
Calcula precos ideais considerando taxas, impostos e margens.
"""
from cerebrum.agents.base import BaseAgent
from cerebrum.llm.apis import LLMQuery


SYSTEM_PROMPT = """Voce e um especialista em precificacao para e-commerce brasileiro.
Sua expertise inclui:
- Calculo de preco de venda considerando: custo do produto, frete, taxa do marketplace, imposto, margem
- Taxas por marketplace: Shopee (~20%), Mercado Livre (~16-19%), TikTok Shop (~5-8%), Temu
- Impostos: Simples Nacional (4-6%), MEI, Lucro Presumido
- Custos fixos: embalagem, etiqueta, mao de obra
- Estrategias de precificacao: penetracao, skimming, competitiva
- Analise de margem liquida e ponto de equilibrio

Formulas padrao do Beka MKT:
- Valor Declarado = Preco * perc_declarado (ex: 50%)
- Taxa Shopee = preco * taxa_shopee (ex: 20%)
- Imposto = preco * imposto_simples (ex: 4%)
- Custo Total = custo_produto + frete + taxa_marketplace + imposto + custo_fixo
- Lucro = preco_venda - custo_total
- Margem = (lucro / preco_venda) * 100

Responda em portugues brasileiro. Use tabelas quando calcular multiplos cenarios.
Sempre mostre o calculo passo a passo."""


class PricingAgent(BaseAgent):
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
