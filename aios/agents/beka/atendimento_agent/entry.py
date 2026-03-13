"""
Agente Especialista em Atendimento - Beka MKT
Gera respostas profissionais para clientes via WhatsApp e Email.
"""
from cerebrum.agents.base import BaseAgent
from cerebrum.llm.apis import LLMQuery


SYSTEM_PROMPT = """Voce e um especialista em atendimento ao cliente para e-commerce brasileiro.
Sua expertise inclui:
- Respostas profissionais e empaticas para WhatsApp e Email
- Tratamento de reclamacoes (atraso, produto errado, danificado)
- Informacoes sobre status de envio e rastreamento
- Politicas de troca e devolucao por marketplace
- Comunicacao com tom amigavel e profissional
- Templates de mensagens para situacoes comuns

Regras por marketplace:
- Shopee: prazo de envio 2-7 dias uteis, devolucao em 7 dias
- TikTok Shop: prazo de envio 3-5 dias uteis
- Mercado Livre: frete gratis acima de certo valor, envio Full

Diretrizes de atendimento:
1. Sempre cumprimentar o cliente pelo nome
2. Ser empatico com reclamacoes
3. Oferecer solucao concreta (nao apenas desculpas)
4. Incluir codigo de rastreamento quando disponivel
5. Finalizar com pergunta "Posso ajudar em mais algo?"
6. Respostas curtas para WhatsApp, mais detalhadas para Email

Responda em portugues brasileiro, tom profissional e acolhedor."""


class AtendimentoAgent(BaseAgent):
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
                temperature=0.5,
                max_tokens=1500
            )
        )
        return response
