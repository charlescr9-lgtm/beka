# BEKA MULTIPLACE - Planos e Notas do Projeto
**Ultima atualizacao:** 11/02/2026

## Status Atual do Sistema
- **Hospedagem**: Railway (Hobby ~$5/mes) - https://web-production-274ef.up.railway.app/
- **Repositorio**: GitHub (privado)
- **Pagamentos**: Mercado Pago (taxa ~4,99% + R$0,49 por transacao)
- **Email**: Gmail SMTP (variaveis no Railway: SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM)
- **Banco**: SQLite com migracoes manuais em _migrate_db()
- **Stack**: Flask + JWT + SQLAlchemy + openpyxl

---

## Funcionalidades Implementadas
1. Processamento de etiquetas Shopee (PDF) com organizacao por loja
2. Geracao de PDF com codigos de barras e tabelas de produtos
3. Planilha XLSX de lucro (consolidada + por loja) com colunas: Imposto, Custo Fixo
4. Configuracao de lojas (nome, imposto, custo fixo)
5. Sistema de autenticacao (login, registro, JWT)
6. Recuperacao de senha via email (codigo 6 digitos, 15min expiracao)
7. Planos de pagamento: Basico (R$39,90), Profissional (R$69,90), Empresarial (R$99,90)
8. Descontos por periodo: Mensal (0%), Semestral (-20%), Anual (-30%)
9. Sistema de indicacao com cupom (1 mes gratis para indicador e indicado)
10. Painel administrativo (liberar/revogar acesso, listar usuarios)
11. Admin VIP: charlescr9@gmail.com (acesso vitalicio)

---

## Plano Futuro: Integracao com Marketplaces

### Objetivo
Permitir que clientes conectem suas lojas de diversos marketplaces ao sistema para que os pedidos entrem automaticamente, sem precisar fazer upload manual de PDFs.

---

### 1. MERCADO LIVRE (Prioridade ALTA — mais facil de implementar)

#### Por que primeiro?
- API mais madura e bem documentada do Brasil
- **NAO exige CNAE de tecnologia** — qualquer conta de vendedor pode criar app
- Documentacao toda em portugues
- App de desenvolvedor pode ser criada **na hora**, sem aprovacao demorada
- Maior marketplace do Brasil

#### Requisitos
1. Conta de vendedor no Mercado Livre (CNPJ recomendado mas nao obrigatorio)
2. Criar aplicacao em https://developers.mercadolivre.com.br
3. Receber Client ID (App ID) + Secret Key
4. Configurar Redirect URI para OAuth

#### Autenticacao
- **OAuth 2.0 (Authorization Code)**
- URL de autorizacao: `https://auth.mercadolivre.com.br/authorization?response_type=code&client_id={APP_ID}&redirect_uri={REDIRECT_URI}`
- Token valido por **6 horas**, refresh token valido por **6 meses**
- Refresh token e de uso unico (cada refresh gera novo par)
- Scopes: `offline_access`, `write`, `read`

#### Custo
| Item | Custo |
|------|-------|
| Cadastro de desenvolvedor | Gratis |
| Uso da API | Gratis |
| Comissao sobre vendas | 11-16% (cobrado pelo ML, nao pela API) |

#### APIs disponiveis
- Pedidos (orders): listar, detalhes, status
- Produtos: criar, atualizar, estoque, precos
- Logistica/Envios: opcoes de frete, rastreamento
- Pagamentos (via Mercado Pago)
- Mensagens comprador-vendedor
- Perguntas e respostas

#### Portal
- https://developers.mercadolivre.com.br
- Docs: https://developers.mercadolivre.com.br/pt_br/api-docs-pt-br-1

#### Etapas de implementacao
1. Criar modulo `mercadolivre_api.py` com client OAuth 2.0
2. Adicionar campos no banco: ml_app_id, ml_access_token, ml_refresh_token, ml_user_id, ml_token_expires
3. Criar endpoints: /api/ml/connect, /api/ml/callback, /api/ml/orders
4. Adicionar botao "Conectar Mercado Livre" no dashboard
5. Implementar refresh automatico de tokens (a cada 6h)
6. Integrar pedidos com o fluxo de processamento

---

### 2. SHOPEE (Prioridade MEDIA — exige aprovacao como parceiro)

#### Requisitos
1. **Cadastro de desenvolvedor** em https://open.shopee.com
2. **CNPJ com atividade de tecnologia** (CNAE 6201-5/01 ou similar - pode ser atividade secundaria)
3. **Aprovacao da Shopee** como parceiro terceiro (enviar documentacao + URL do sistema HTTPS)
4. **Receber credenciais**: Partner ID + Partner Key
5. Ter produto ja funcional em HTTPS (ja temos no Railway)

#### Autenticacao
- **OAuth 2.0 + HMAC-SHA256** em cada request
- Access token expira em **4 horas**, precisa refresh automatico
- Toda chamada requer: partner_id, timestamp, sign (SHA256), access_token, shop_id

#### Custo
| Item | Custo |
|------|-------|
| Cadastro de desenvolvedor | Gratis |
| Uso da API | Gratis |
| Adicionar CNAE secundario (MEI) | Gratis (Gov.br) |
| Adicionar CNAE secundario (ME/EPP) | R$100-300 (Junta Comercial) |

#### CNAEs de tecnologia recomendados (para cadastro de parceiro Shopee)
- **6201-5/01** - Desenvolvimento de programas de computador sob encomenda (MAIS INDICADO)
- 6202-3/00 - Desenvolvimento e licenciamento de programas customizaveis
- 6203-1/00 - Desenvolvimento e licenciamento de programas nao customizaveis
- 6204-0/00 - Consultoria em tecnologia da informacao
- 6209-1/00 - Suporte tecnico, manutencao e outros servicos em TI

#### Portal
- https://open.shopee.com
- Sandbox: https://partner.test-stable.shopeemobile.com
- Endpoint producao: https://partner.shopeemobile.com/api/v2

#### Etapas de implementacao
1. Criar modulo `shopee_api.py` com client OAuth + assinatura SHA256
2. Adicionar campos no banco: shopee_shop_id, shopee_access_token, shopee_refresh_token, shopee_token_expires
3. Criar endpoints: /api/shopee/connect, /api/shopee/callback, /api/shopee/orders
4. Adicionar botao "Conectar Shopee" no dashboard
5. Implementar job de refresh automatico de tokens (a cada 4h)
6. Integrar pedidos da API com o fluxo existente

---

### 3. SHEIN (Prioridade MEDIA — precisa ser vendedor aprovado)

#### Requisitos
1. Ser **vendedor aprovado na Shein Brasil** (Seller Hub)
2. CNPJ ativo (qualquer atividade, nao precisa CNAE de tecnologia)
3. Credenciais obtidas em: Seller Hub > Personal Center > Third-party Application
4. Receber **Open Key ID** + **Secret Key** via SMS

#### Autenticacao
- **API Key / Secret Key** (mais simples que OAuth)
- Credenciais estaticas, sem necessidade de refresh de token

#### Custo
| Item | Custo |
|------|-------|
| Cadastro de desenvolvedor | Gratis (parte da conta vendedor) |
| Uso da API | Gratis |
| Comissao Shein | 16% (primeiros 3 meses gratis) |

#### APIs disponiveis
- Publicacao de produtos via OpenAPI
- Logistica integrada (agendamento, impressao de etiquetas)
- Upload de tracking numbers
- Gerenciamento de pedidos fulfillment

#### Portal
- https://open.sheincorp.com

#### Etapas de implementacao
1. Criar modulo `shein_api.py` com client API Key/Secret
2. Adicionar campos no banco: shein_key_id, shein_secret_key
3. Criar endpoints: /api/shein/connect, /api/shein/orders
4. Adicionar botao "Conectar Shein" no dashboard
5. Integrar pedidos com o fluxo de processamento

---

### 4. TIKTOK SHOP (Prioridade BAIXA — ainda recente no Brasil)

#### Status
- Lancou no Brasil em **abril 2025**, inicialmente **somente por convite**
- API existe e e completa, mas ecossistema brasileiro ainda amadurecendo
- Brasil e o 2o maior mercado de live-stream shopping do TikTok (111.3M usuarios ativos)

#### Requisitos
1. Ser vendedor aprovado no TikTok Shop Brasil
2. CNPJ + documentacao empresarial
3. Cadastro de desenvolvedor no **Global Partner Portal**: https://partner.tiktokshop.com
4. Receber App ID + App Secret

#### Autenticacao
- **OAuth 2.0**
- Token endpoint: `POST https://open.tiktokapis.com/v2/oauth/token/`
- Parametros: client_key, client_secret, code, grant_type, redirect_uri

#### Custo
| Item | Custo |
|------|-------|
| Cadastro de desenvolvedor | Gratis |
| Uso da API | Gratis (zero taxa para devs) |
| Taxa de listagem | Gratis |

#### Portal
- Partner Center: https://partner.tiktokshop.com
- Developers: https://developers.tiktok.com
- Seller BR: https://seller-br.tiktok.com

---

### 5. TEMU (Prioridade BAIXA — API restrita/documentacao fechada)

#### Status
- Ativo no Brasil desde **junho 2025**, aberto a todos desde **outubro 2025**
- API existe no Partner Platform, mas documentacao e **fechada** (precisa login)
- Aceita **MEI, EIRELI ou SLU** como vendedor

#### Requisitos
1. Ser vendedor registrado: https://br.seller.temu.com
2. CNPJ ativo (qualquer porte, inclusive MEI)
3. Acesso ao Partner Platform: https://partner.temu.com

#### Autenticacao
- **API Key / Secret** (via Partner Platform)

#### Custo
| Item | Custo |
|------|-------|
| Cadastro | Gratis |
| Uso da API | Gratis |
| Comissao | 2-20% (periodos promocionais com 0%) |

---

## Resumo Comparativo

| Marketplace | Prioridade | Dificuldade | CNAE Tech? | Auth | Token Refresh |
|-------------|-----------|-------------|------------|------|---------------|
| **Mercado Livre** | ALTA | Facil | Nao | OAuth 2.0 | 6h (refresh 6 meses) |
| **Shopee** | Media | Media | **Sim** (parceiro) | OAuth + SHA256 | 4h |
| **Shein** | Media | Facil | Nao | API Key/Secret | Nao precisa |
| **TikTok Shop** | Baixa | Media | Nao | OAuth 2.0 | Variavel |
| **Temu** | Baixa | Incerta | Nao | API Key/Secret | Incerto |

### Ordem recomendada de implementacao
1. **Mercado Livre** — mais facil, maior mercado, sem burocracia
2. **Shopee** — ja e o foco do sistema, apos aprovacao como parceiro
3. **Shein** — quando for vendedor aprovado na plataforma
4. **TikTok Shop / Temu** — aguardar amadurecimento no Brasil

> **Descoberta importante:** Nenhum marketplace exige CNAE de tecnologia para usar a API como vendedor.
> A Shopee e a unica que exige para cadastro como **parceiro terceiro** (caso do Beka MultiPlace, que conecta lojas de outros vendedores).
> Para os demais, o CNPJ de vendedor normal basta.

---

## Custos Mensais Estimados
| Item | Custo |
|------|-------|
| Railway (Hobby) | ~R$25-30/mes |
| Gmail SMTP | R$0 |
| Mercado Pago | ~4,99% + R$0,49 por venda |
| APIs dos Marketplaces | R$0 (todas gratuitas) |
| **Total fixo** | **~R$25-30/mes** |

> Railway Pro ($20/mes) pode ser necessario conforme a base de clientes crescer.

---

## Observacoes Tecnicas
- SQLite nao auto-cria colunas novas: sempre adicionar ALTER TABLE em _migrate_db()
- Deploy automatico via push no GitHub (Railway)
- Variaveis de ambiente no Railway: JWT_SECRET, DATABASE_URL, MERCADOPAGO_ACCESS_TOKEN, SMTP_*
- EMAILS_VITALICIO definido em auth.py para controle de admins VIP
- Webhook do Mercado Pago: formato `user_id:plano_id:periodo_id:indicador_id` (retrocompativel)
- Para cada marketplace integrado, sera necessario adicionar variaveis de ambiente com as credenciais (APP_ID, SECRET_KEY, etc.)
