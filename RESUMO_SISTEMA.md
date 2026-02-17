# BEKA MULTIPLACE — Resumo Completo do Sistema
**Ultima atualizacao:** 11/02/2026
**Deploy:** https://web-production-274ef.up.railway.app/
**Repositorio:** https://github.com/charlescr9-lgtm/beka.git
**Local:** C:\Users\Micro\Desktop\Beka MKT

---

## O QUE E
Dashboard web (Flask) para processar etiquetas de envio da Shopee. Recebe PDFs de etiquetas + ZIPs com XMLs de Nota Fiscal, e gera PDFs finais organizados por loja com:
- 1 etiqueta por pagina (150mm x 230mm)
- Codigo de barras da chave NFe
- Tabela de produtos em **negrito** (SKU, titulo ou ambos)
- Paginas de continuacao quando produtos nao cabem
- Resumo XLSX por loja com contagem de SKUs
- Calculo de lucro por produto com colunas: Imposto, Custo Fixo, Custo Shopee
- Sistema de autenticacao com planos pagos, indicacao e painel admin

---

## ARQUITETURA

### Arquivos do projeto
```
C:\Users\Micro\Desktop\Beka MKT\
├── dashboard.py             # App Flask principal (~1954 linhas) — rotas API, processamento, estado
├── etiquetas_shopee.py      # Motor de processamento de PDFs (~2610 linhas) — core do sistema
├── models.py                # Modelos SQLAlchemy (~192 linhas) — User, Session, Payment
├── auth.py                  # Autenticacao JWT (~505 linhas) — login, registro, verificacao email, Google OAuth, recuperacao senha, admin
├── email_utils.py           # Envio de emails SMTP (~109 linhas) — verificacao de email, recuperacao de senha
├── payments.py              # Integracao Mercado Pago (~342 linhas) — pagamento, periodos, cupom indicacao
├── requirements.txt         # Dependencias Python
├── Procfile                 # Deploy Railway: gunicorn dashboard:app
├── PLANOS.md                # Planos futuros e notas do projeto (integracao Shopee API, custos)
├── RESUMO_SISTEMA.md        # Este arquivo
├── static/
│   ├── index.html           # Dashboard principal (~2833 linhas) — SPA vanilla JS
│   ├── login.html           # Tela de login/cadastro/verificacao/recuperacao senha (~597 linhas)
│   └── logo.svg             # Logo
```

### Stack tecnologico
- **Backend:** Flask + Flask-JWT-Extended + Flask-SQLAlchemy + Flask-Bcrypt
- **Banco:** SQLite (persistido via Railway volume) + migracao automatica (`_migrate_db()`)
- **PDF:** PyMuPDF (fitz) — extracao, recorte, escala, insercao de texto
- **Barcode:** python-barcode (Code128, formato SVG)
- **Excel:** openpyxl — resumos e relatorios de lucro com formatacao R$ e auto-width
- **Auth:** Google OAuth (google-auth + Google Identity Services JS)
- **Email:** SMTP (email_utils.py) para verificacao de cadastro e recuperacao de senha
- **Pagamento:** Mercado Pago SDK com periodos (mensal/semestral/anual) e cupom de indicacao
- **Deploy:** Gunicorn no Railway com volume persistente

---

## BANCO DE DADOS (models.py)

### Tabela `users`
| Campo | Tipo | Descricao |
|-------|------|-----------|
| id | Integer PK | ID do usuario |
| email | String UNIQUE | Email (lowercase) |
| password_hash | String | Senha com bcrypt |
| plano | String | free / basico / pro / empresarial |
| processamentos_mes | Integer | Contador mensal (reseta a cada mes) |
| mes_atual | String | "YYYY-MM" para controle de reset |
| created_at | DateTime | Data de criacao |
| is_active | Boolean | Conta ativa |
| email_verified | Boolean | Se email foi verificado |
| email_code | String(6) | Codigo de verificacao 6 digitos |
| email_code_expires | DateTime | Expiracao do codigo de verificacao |
| google_id | String(255) | ID do Google para login via OAuth |
| reset_code | String(6) | Codigo de recuperacao de senha 6 digitos |
| reset_code_expires | DateTime | Expiracao do codigo de reset (15min) |
| cupom_indicacao | String(20) UNIQUE | Cupom unico de indicacao do usuario |
| indicado_por | Integer | user_id de quem indicou |
| meses_gratis | Integer | Meses gratis acumulados por indicacoes |
| plano_expira | DateTime | Data de expiracao do plano pago |

### Tabela `sessions`
| Campo | Tipo | Descricao |
|-------|------|-----------|
| id | Integer PK | - |
| user_id | FK → users | - |
| token_id | String UNIQUE | UUID da sessao (usado no JWT) |
| ip | String | IP do dispositivo |
| created_at / last_seen | DateTime | Controle de atividade |

### Tabela `payments`
| Campo | Tipo | Descricao |
|-------|------|-----------|
| id | Integer PK | - |
| user_id | FK → users | - |
| status | String | pending / approved / rejected |
| mercadopago_id | String | ID da preferencia/pagamento |
| plano_contratado | String | Plano comprado |
| valor | Float | Valor pago |
| created_at / updated_at | DateTime | - |

### Planos
| Plano | Max IPs | Limite proc/mes | Valor |
|-------|---------|-----------------|-------|
| free | 1 | 5 | R$ 0 |
| basico | 1 | ilimitado | R$ 39,90 |
| pro | 2 | ilimitado | R$ 59,90 |
| empresarial | 5 | ilimitado | R$ 89,90 |

### Periodos de assinatura
| Periodo | Meses | Desconto |
|---------|-------|----------|
| Mensal | 1 | 0% |
| Semestral | 6 | 20% |
| Anual | 12 | 30% |

### Migracao de banco (`_migrate_db()`)
- Funcao em `dashboard.py` (linha 62) que roda no startup
- Verifica colunas existentes na tabela `users` via `inspector.get_columns`
- Adiciona automaticamente colunas faltantes via `ALTER TABLE`:
  - `email_verified`, `email_code`, `email_code_expires` (verificacao email)
  - `google_id` (Google OAuth)
  - `reset_code`, `reset_code_expires` (recuperacao de senha)
  - `cupom_indicacao`, `indicado_por`, `meses_gratis`, `plano_expira` (indicacao e planos)
- Necessario porque SQLite no Railway nao suporta `db.create_all()` para adicionar colunas em tabelas existentes

---

## AUTENTICACAO (auth.py)

- **JWT com sessao:** Token inclui `sid` (session ID) validado contra o banco
- **Controle de IPs:** Cada login registra IP; bloqueia se atingiu limite do plano
- **VIP:** Email `charlescr9@gmail.com` sempre tem plano "empresarial" (vitalicio)
- **Token expira:** 30 dias
- **Confirmacao de senha:** Registro exige `senha2` (confirmacao) que deve coincidir com `senha`
- **Verificacao de email:** Codigo de 6 digitos enviado por SMTP; auto-skip se SMTP nao configurado
- **Google OAuth:** Login via ID Token (Google Identity Services JS no frontend → verificacao via `google-auth` no backend)
- **Recuperacao de senha:** Codigo de 6 digitos enviado por email, expira em 15 minutos
- **Admin:** Emails em `EMAILS_VITALICIO` tem acesso ao painel administrativo

### Rotas de autenticacao
| Endpoint | Metodo | Descricao |
|----------|--------|-----------|
| `/api/auth/register` | POST | Cadastro (email + senha + senha2, envia codigo de verificacao) |
| `/api/auth/login` | POST | Login (retorna JWT, bloqueia se limite de IPs) |
| `/api/auth/logout` | POST | Invalida sessao |
| `/api/auth/me` | GET | Dados do usuario logado |
| `/api/auth/verify-email` | POST | Verificar codigo de 6 digitos |
| `/api/auth/resend-code` | POST | Reenviar codigo de verificacao |
| `/api/auth/forgot-password` | POST | Envia codigo de recuperacao de senha (6 digitos, 15min) |
| `/api/auth/reset-password` | POST | Valida codigo e redefine a senha |
| `/api/auth/google` | POST | Login/registro via Google OAuth (ID Token) |
| `/api/auth/google-client-id` | GET | Retorna client ID do Google (publico) |
| `/api/planos` | GET | Lista planos pagos (publico) |

### Rotas de admin (somente EMAILS_VITALICIO)
| Endpoint | Metodo | Descricao |
|----------|--------|-----------|
| `/api/admin/check` | GET | Verifica se usuario logado e admin |
| `/api/admin/usuarios` | GET | Lista todos os usuarios com plano, expiracao, bonus |
| `/api/admin/liberar-acesso` | POST | Libera plano gratuito por N meses a um email |
| `/api/admin/revogar-acesso` | POST | Revoga acesso, voltando para Free |

---

## PAGAMENTO (payments.py)

### Fluxo Mercado Pago
1. Usuario clica "Assinar" → escolhe plano + periodo + cupom (opcional)
2. `POST /api/payment/create` → calcula valor com desconto → cria preferencia no MP → retorna URL de checkout
3. Usuario paga no Mercado Pago
4. `POST /api/payment/webhook` → MP notifica → plano atualizado, expiracao calculada, indicacao processada
5. Usuario volta ao dashboard com plano ativo

### Descontos por periodo
```
Mensal: 0% de desconto
Semestral: 20% de desconto
Anual: 30% de desconto
```

### Sistema de indicacao (cupom)
- Cada usuario tem um cupom unico gerado automaticamente (ex: CHARLE1A2B)
- Cupom e informado no momento do pagamento
- **Quem indicou**: +1 mes gratis (acumula a cada nova indicacao)
- **Quem usou o cupom**: +1 mes gratis (uma unica vez)
- Meses gratis sao aplicados na proxima compra (estendem a expiracao)
- Nao pode usar proprio cupom; cupom so pode ser usado 1 vez por usuario

### Webhook - external_reference
Formato: `user_id:plano_id:periodo_id:indicador_id`
- Retrocompativel com formato antigo `user_id:plano_id`
- `indicador_id = 0` quando sem cupom

### Rotas de pagamento
| Endpoint | Metodo | Descricao |
|----------|--------|-----------|
| `/api/payment/create` | POST | Cria link de pagamento (plano + periodo + cupom) |
| `/api/payment/webhook` | POST | Recebe notificacoes do Mercado Pago |
| `/api/payment/status` | GET | Status do plano + periodos + planos disponiveis |
| `/api/payment/simular` | POST | Simula valor de plano + periodo (frontend) |
| `/api/indicacao/meu-cupom` | GET | Retorna/gera cupom do usuario + stats |
| `/api/indicacao/validar-cupom` | POST | Valida cupom de indicacao |

### Variavel obrigatoria
- `MERCADOPAGO_ACCESS_TOKEN` — Access Token de producao (configurado no Railway)

---

## EMAIL (email_utils.py)

### Funcoes
- `smtp_configurado()` — Retorna True se SMTP esta configurado
- `enviar_codigo_verificacao(email, codigo)` — Email com codigo 6 digitos para verificacao de conta (10min)
- `enviar_codigo_reset_senha(email, codigo)` — Email com codigo 6 digitos para recuperacao de senha (15min)

### Template HTML
- Background escuro (#0a0e1a) com card central (#151929)
- Titulo "Beka MultiPlace" em laranja (#ff6b35)
- Codigo em fonte grande (36px) com letter-spacing

---

## MOTOR DE PROCESSAMENTO (etiquetas_shopee.py)

### Pipeline principal
```
1. Carregar XMLs de todos os ZIPs
   └─ Extrai: NF, chave, CNPJ, nome emitente, produtos
2. Carregar PDFs regulares
   └─ Detecta layout automatico (2x2 / 2x1 / 1x1 / pagina pequena)
   └─ Recorta cada etiqueta
   └─ Associa com XML pela NF
3. Processar PDFs especiais (lanim*.pdf, shein crua.pdf)
4. Remover duplicatas por NF
5. Separar por CNPJ (loja)
6. Para cada loja (com try/except individual):
   ├─ Ordenar: simples primeiro, multi-produto no final
   ├─ Gerar PDF (150x230mm, 1 etiqueta/pagina)
   │   ├─ Imagem da etiqueta original (escalada)
   │   ├─ Codigo de barras Code128 da chave NFe
   │   ├─ Tabela de produtos em NEGRITO (SKU/titulo/ambos + quantidade)
   │   └─ Paginas de continuacao se produtos nao couberem
   └─ Gerar XLSX resumo (contagem por SKU)
7. Gerar resumo geral XLSX (todas as lojas)
```

### Processamento resiliente por loja
- Cada loja e processada dentro de um `try/except` individual
- Se 1 loja falhar, as demais continuam normalmente
- Erro e registrado no log mas nao interrompe o processamento

### Deteccao de layout de pagina
```python
_detectar_layout_pagina(pagina)
├─ Se larg <= 420: pagina inteira (1 etiqueta — pagina pequena/retirada)
├─ Testa grid 2x2 (4 etiquetas) — padrao Shopee
├─ Testa 2x1 (2 etiquetas empilhadas)
└─ Fallback: pagina inteira (1 etiqueta)
```
Usa marcadores de conteudo (Pedido:, REMETENTE, DANFE) em vez de depender apenas de NF.

### Deteccao de etiqueta em regiao
```python
_contar_etiquetas_regiao(pagina, clip)
├─ Verifica texto minimo (>10 chars)
├─ Busca: 'Pedido:', 'REMETENTE', 'DANFE', NF numerica
└─ Retorna True se encontrar NF OU Pedido: OU (REMETENTE + DANFE)
```

### Recorte de etiqueta
```python
_recortar_pagina(pagina)
├─ Usa layout detectado para definir quadrantes
├─ Para cada quadrante com etiqueta:
│   ├─ Extrai NF (ou gera ID sintetico: SEM_NF_{pdf_id}_p{pag}_q{idx})
│   ├─ ID sintetico inclui nome do PDF para evitar colisoes entre multiplos PDFs
│   ├─ Associa com dados XML
│   ├─ Extrai SKU principal
│   ├─ Extrai nome da loja do campo REMETENTE
│   └─ Retorna dict com todos os metadados
```

### Geracao do PDF final
```python
gerar_pdf_loja(etiquetas, caminho_saida, nome_loja)
├─ Pagina 150mm x 230mm (425pt x 652pt)
├─ Etiqueta original: escalada proporcionalmente
│   └─ Se tem tabela de produtos, reduz altura da etiqueta para caber
├─ Codigo de barras: Code128 da chave NFe (37pt de altura)
├─ Tabela de produtos (tudo em NEGRITO — fonte_bold + fs_destaque):
│   ├─ Modo "sku": CODIGO | - | QTD
│   ├─ Modo "titulo": PRODUTO (descricao) | - | QTD
│   ├─ Modo "ambos": CODIGO | DESCRICAO | QTD
│   ├─ Ate 10 produtos por etiqueta (excedente em paginas de continuacao)
│   ├─ Linhas divisorias cinza entre produtos
│   └─ Fonte configuravel (padrao 7pt, destaque 1.5x)
├─ Overflow de produtos:
│   ├─ _desenhar_secao_produtos retorna indice do ultimo produto desenhado
│   ├─ Se nao couberam todos, cria pagina de continuacao
│   └─ Continuacao repete etiqueta + barcode + produtos restantes
├─ Rodape: "p.{numero}" posicionado em ALTURA_PT - MARGEM_INFERIOR - 8
```

### Calculo de espaco para tabela
```python
# Se tem produtos, calcula espaco necessario e reduz etiqueta
espaco_tabela = 37 (barcode) + 20 (header) + (num_prods * line_h) + 15 (margem)
alt_max = ALTURA_PT - MARGEM_TOPO - MARGEM_INFERIOR - espaco_tabela
if alt_etiqueta > alt_max:
    alt_etiqueta = max(alt_max, ALTURA_PT * 0.45)  # minimo 45% da pagina
```

### Ordenacao de etiquetas
```python
_ordenar_etiquetas(etiquetas)
├─ Simples (1 item, qtd=1): ordenadas por SKU → NF
├─ Multiplos (multi-item OU qtd>1): ordenadas por SKU → NF
└─ Resultado: [todas simples] + [todas multiplas]
```

### PDFs especiais

#### lanim*.pdf — Etiquetas CPF (Declaracao de Conteudo)
- Detecta todos: lanim.pdf, lanim 2.pdf, lanim2.pdf, etc.
- Auto-crop ao conteudo (A4 com conteudo no canto)
- **Requer XLSX** com colunas: `order_sn` + `product_info`
- Parseia product_info: `[1] Parent SKU Reference No.: ABC; Quantity: 5; Product Name: ...; Variation Name: P/Blue`
- Tabela mostra: SKU | VARIACAO | Quant (adaptavel ao modo de exibicao)
- Saida: 150mm x 225mm
- Loja fixa: "CPF" com CNPJ sintetico "LANIM_CPF"

#### shein crua.pdf — Shein importacao direta
- Paginas alternadas: Par=Etiqueta, Impar=DANFE
- Extrai NF, chave, CNPJ e produtos do DANFE
- Remove texto chines dos atributos
- Saida: 150mm x 225mm com barra de codigo de barras vertical na direita

> **Nota:** `beka.pdf` foi **removido** de PDFS_ESPECIAIS. Agora e processado como PDF normal (grid 2x2 ou retirada detectada automaticamente pela logica `larg <= 420`).

### XLSX de pedidos (carregar_xlsx_pedidos)
- Le XLSX com colunas: `order_sn`, `product_info`
- Parseia blocos `[N]` com: Parent SKU, SKU Reference No., Quantity, Product Name, Variation Name
- **Acumula** produtos quando mesmo order_sn aparece em multiplas linhas
- Retorna: `{order_sn: {produtos: [...], total_itens, total_qtd}}`

### Extracao de SKU principal
```python
_extrair_sku_principal(sku_completo)
├─ Remove APENAS sufixos de tamanho do FINAL:
│   ├─ Letras: P, M, G, PP, GG, XG, XS, XL, XXL, XXG, EG, EGG
│   ├─ Numeros 2 digitos: 24-56 (numeracao de calcado)
│   └─ Numeros 1 digito: 1-9
├─ Exemplos: "TEN-BO-BR-38" → "TEN-BO-BR"
│            "ABC-M" → "ABC"
│            "PROD-123-GG" → "PROD-123"
```

### Exclusao de PDFs especiais do processamento normal
```python
# carregar_todos_pdfs exclui:
- Arquivos comecando com "etiquetas_prontas"
- Arquivos comecando com "lanim" (processados separadamente como CPF)
- PDFs listados em PDFS_ESPECIAIS: ['lanim.pdf', 'shein crua.pdf']
```

---

## DASHBOARD API (dashboard.py)

### Rotas principais
| Endpoint | Metodo | Descricao |
|----------|--------|-----------|
| `/api/status` | GET | Status completo: processando, arquivos, config, resultado |
| `/api/logs` | GET | Logs em tempo real (paginado por offset) |
| `/api/processar` | POST | Iniciar processamento (thread em background) |
| `/api/upload` | POST | Upload de PDF/ZIP/XLSX/XLS |
| `/api/upload-custos` | POST | Upload de planilha de custos |
| `/api/remover-arquivo` | POST | Remover arquivo da entrada |
| `/api/configuracoes` | POST | Salvar configuracoes do usuario |
| `/api/novo-lote` | POST | Limpar tudo para novo lote |
| `/api/limpar-saida` | POST | Limpar pasta de saida |
| `/api/download-todos` | GET | Baixar ZIP com todos os resultados |
| `/api/download/<loja>/<arquivo>` | GET | Baixar arquivo especifico |
| `/api/download-resumo-geral` | GET | Baixar XLSX resumo geral |
| `/api/gerar-lucro` | POST | Gerar relatorio de lucro |
| `/api/download-lucro` | GET | Baixar ZIP com XLSX de lucro (consolidado + por loja) |
| `/api/download-lucro/<loja>` | GET | Baixar XLSX de lucro por loja |
| `/api/exemplo-custos` | GET | Baixar template de planilha de custos |
| `/api/agrupar` | POST | Agrupar lojas em 1 PDF |
| `/api/agrupamentos` | GET/POST | Salvar/carregar presets de agrupamento |
| `/api/configuracoes-lucro-lojas` | GET/POST | Config de lucro por loja |
| `/api/lojas-lucro` | GET | Leitura de XMLs da pasta lucro (lojas disponiveis) |
| `/api/historico` | GET | Historico de processamentos |

### Estado por usuario (em memoria)
```python
estado = {
    "processando": bool,
    "logs": [{"timestamp", "mensagem", "tipo"}],
    "ultimo_resultado": {...},
    "ultimo_lucro": {...},
    "historico": [...],
    "agrupamentos": [...],
    "configuracoes": {
        "pasta_entrada": path,
        "pasta_saida": path,
        "largura_mm": 150,        # Largura da pagina de saida
        "altura_mm": 230,         # Altura da pagina de saida
        "margem_esq": 8,
        "margem_dir": 8,
        "margem_topo": 5,
        "margem_inf": 5,
        "fonte_produto": 7,        # Tamanho fonte BASE tabela produtos
        "exibicao_produto": "sku", # "sku" | "titulo" | "ambos"
        "perc_declarado": 100,     # % valor declarado vs real
        "taxa_shopee": 18,         # Comissao Shopee %
        "imposto_simples": 4,      # Imposto Simples %
        "custo_fixo": 3.0,         # Custo fixo por unidade
        "planilha_custos": path,
        "lucro_por_loja": {}       # Override de config por loja
    }
}
```

### Persistencia de configuracoes
- Salvo em: `{pasta_entrada}/_config.json`
- Carregado automaticamente ao iniciar o servidor
- **Auto-save:** frontend salva automaticamente 1s apos qualquer alteracao (debounce)
- Tambem persiste: `_ultimo_resultado.json`, `_ultimo_lucro.json`

---

## FRONTEND (static/index.html)

### Abas
1. **Etiquetas** — Upload+Processar, stats, resultado do ultimo processamento, configuracoes, agrupamento
2. **Resultados** — PDFs por loja com botoes de download, resumo geral XLSX
3. **Lucro** — Upload de XMLs/custos, relatorio de lucro por loja
4. **Admin** — (visivel apenas para EMAILS_VITALICIO) Gerenciamento de usuarios e acessos

### Layout da aba Etiquetas
```
[Upload + Processar] ← 1 card horizontal (drag&drop + progresso + botoes)
[PDFs] [XLSX] [Etiquetas] [Lojas] ← Stats grid-4

┌─────────────────────┐ ┌─────────────────────┐
│ Ultimo Processamento│ │ CONFIGURACOES       │
│ (lojas em 2 colunas)│ │ Larg/Alt/Margens    │
│                     │ │ Fonte/Exibicao      │
│ Log do Sistema      │ │ Agrupamento de Lojas│
│                     │ │ Limpar Saida        │
└─────────────────────┘ └─────────────────────┘
← Layout 2 colunas (grid 1fr 1fr) →
```

### Aba Admin (somente admins)
- Formulario: email + plano (select) + meses (input) + botoes Liberar/Revogar
- Tabela de todos os usuarios: email, plano (badge colorido), expiracao, bonus, data cadastro, botao selecionar
- "Vitalicio" para admins, data para quem tem plano_expira, "Sem expiracao" para pagos sem data, vazio para free
- Icone coroa para usuarios admin

### Funcionalidades do frontend
- **Upload:** Aceita .pdf, .zip, .xlsx, .xls (drag & drop no card ou clique no icone Upload)
- **Botao processar:** Desabilitado quando PDFs=0, warning amarelo quando XLSX=0
- **Polling:** Atualiza status e logs a cada 800ms durante processamento, 5s em idle
- **Download automatico:** ZIP baixa automaticamente ao finalizar processamento
- **Modal de planos:** Selecao de plano → periodo (mensal/semestral/anual) → cupom indicacao → valor simulado → pagar
- **Modal de indicacao:** Mostra cupom do usuario, botao copiar, total indicados, meses gratis acumulados
- **Badge de plano:** Mostra plano atual + IPs usados no header
- **Toast notifications:** Sucesso (verde), erro (vermelho), warning (amarelo), info (azul)
- **Aviso sem NF:** Banner vermelho pulsante se houver etiquetas sem Nota Fiscal
- **Auto-save config:** Qualquer alteracao em configuracao salva automaticamente (debounce 1s)
- **Ultimo resultado compacto:** Lojas exibidas em grid de 2 colunas (metade da altura)
- **Config inline:** Todas as configuracoes de etiqueta + agrupamento em 1 card ao lado do resultado
- **Config lojas (Lucro):** Tabela com campos centralizados — "Aliquota" (imposto), Custo Fixo, Taxa Shopee por loja

### Frontend login (static/login.html)
- Formulario de login (email + senha)
- Formulario de cadastro (email + senha + confirmacao)
- Verificacao de email (codigo 6 digitos)
- **Recuperacao de senha:**
  - Link "Esqueci minha senha" apos botao login
  - Formulario de email → envia codigo
  - Formulario de codigo + nova senha + confirmacao
  - Funcoes JS: `solicitarReset()`, `redefinirSenha()`
- Login com Google (condicional — so aparece se GOOGLE_CLIENT_ID configurado)

---

## CALCULO DE LUCRO

### Formula por produto
```
V_Real = V_Declarado / (perc_declarado / 100)
Custo_Imposto = V_Declarado x (imposto_simples / 100)
Custo_Shopee = V_Real x taxa_shopee / 100
Custo_Fixo_Total = custo_fixo x qtd
Lucro = V_Real - Custo_Imposto - Custo_Shopee - Custo_Fixo_Total - Custo_Produto
```

### Colunas do XLSX de lucro
- SKU, Produto, Qtd, V.Declarado, V.Real, Imposto, Custo Fixo, Custo Shopee, Custo Produto, Lucro, Margem%
- Formatacao em R$ (#,##0.00) com auto-width calculando display length do formato "R$ X.XXX,XX"
- XLSX consolidado + 1 XLSX por loja, tudo em um ZIP

### Busca inteligente de custo
```python
_buscar_custo_inteligente(sku_xml, dict_custos)
1. Match exato (uppercase)
2. Match por SKU base (sem variacao)
3. Chave da planilha comeca com SKU base
4. SKU base comeca com chave da planilha
5. Fallback: R$ 0,00 (marca como "sem custo")
```

---

## VARIAVEIS DE AMBIENTE (Railway)

| Variavel | Obrigatoria | Descricao |
|----------|-------------|-----------|
| `MERCADOPAGO_ACCESS_TOKEN` | Sim | Token de producao do Mercado Pago |
| `RAILWAY_VOLUME_MOUNT_PATH` | Auto | Caminho do volume persistente (Railway configura) |
| `SECRET_KEY` | Recomendado | Chave secreta do Flask |
| `JWT_SECRET_KEY` | Recomendado | Chave para assinar JWT |
| `APP_URL` | Recomendado | URL base (default: https://web-production-274ef.up.railway.app) |
| `PORT` | Auto | Porta HTTP (Railway configura) |
| `SMTP_HOST` | Opcional* | Servidor SMTP (ex: smtp.gmail.com) |
| `SMTP_PORT` | Opcional* | Porta SMTP (default: 587) |
| `SMTP_USER` | Opcional* | Usuario SMTP (ex: seuemail@gmail.com) |
| `SMTP_PASS` | Opcional* | Senha de app SMTP (Gmail: Senha de aplicativo) |
| `SMTP_FROM` | Opcional* | Email remetente (default: SMTP_USER) |
| `GOOGLE_CLIENT_ID` | Opcional | Client ID do Google para OAuth |

> **Nota:** Se as variaveis SMTP nao estiverem configuradas, a verificacao de email e ignorada (auto-verifica) e a recuperacao de senha retorna erro "Servico de email nao configurado".
> Se `GOOGLE_CLIENT_ID` nao estiver configurado, o botao Google OAuth nao aparece na tela de login.

### Como configurar Gmail SMTP
1. Ativar verificacao em 2 etapas na conta Google
2. Gerar "Senha de aplicativo" em https://myaccount.google.com/apppasswords
3. Configurar no Railway:
   - `SMTP_HOST` = smtp.gmail.com
   - `SMTP_PORT` = 587
   - `SMTP_USER` = seuemail@gmail.com
   - `SMTP_PASS` = senha de app gerada (16 caracteres)
   - `SMTP_FROM` = seuemail@gmail.com

---

## ESTRUTURA DE PASTAS POR USUARIO

### Entrada (uploads)
```
/volume/users/{user_id}/entrada/
├── etiqueta1.pdf          # PDF de etiquetas Shopee (grid 2x2)
├── notas.zip              # ZIP com XMLs de NF-e
├── pedidos.xlsx           # XLSX com order_sn + product_info
├── beka.pdf               # (opcional) Retirada — processado como PDF normal
├── lanim.pdf              # (opcional) Etiquetas CPF
├── lanim 2.pdf            # (opcional) Mais etiquetas CPF
├── shein crua.pdf         # (opcional) Etiquetas Shein
├── planilha_custos.xlsx   # (opcional) Custos por SKU
└── _config.json           # Config salva do usuario (auto)
```

### Saida (resultados)
```
/volume/users/{user_id}/Etiquetas prontas/
├── Loja_ABC/
│   ├── etiquetas_Loja_ABC_20260211_143022.pdf
│   ├── cpf_Loja_ABC_20260211_143022.pdf        (se houver CPF)
│   ├── shein_Loja_ABC_20260211_143022.pdf      (se houver Shein)
│   └── resumo_Loja_ABC_20260211_143022.xlsx
├── CPF/
│   └── cpf_CPF_20260211_143022.pdf
├── Grupo_Personalizado/                         (agrupamentos)
│   └── agrupado_Grupo_20260211_143022.pdf
├── resumo_geral_20260211_143022.xlsx
├── _ultimo_resultado.json
└── _ultimo_lucro.json
```

### Pasta de lucro (separada)
```
/volume/users/{user_id}/lucro_entrada/
├── notas.zip              # ZIP com XMLs para calculo de lucro
├── planilha_custos.xlsx   # Custos por SKU
└── (resultado em ZIP com XLSX consolidado + por loja)
```

---

## DEPENDENCIAS (requirements.txt)
```
flask, flask-cors, flask-sqlalchemy, flask-bcrypt, flask-jwt-extended
gunicorn, xmltodict, pandas, openpyxl
PyMuPDF, python-barcode, mercadopago, google-auth
```

---

## REGRAS IMPORTANTES (definidas pelo usuario)
1. **"Nao mexer no que ja esta funcionando"** — qualquer alteracao deve preservar funcionalidades existentes
2. Etiquetas sem NF devem ser geradas com ID sintetico (`SEM_NF_{pdf_id}_p{pag}_q{idx}`) — inclui nome do PDF para evitar colisoes entre multiplos PDFs
3. Multi-produto e multi-quantidade vao para o FINAL da impressao
4. Linhas divisorias cinza entre produtos na tabela
5. lanim*.pdf sao CPF e usam dados do XLSX
6. Botao processar: bloqueado durante upload, desabilitado se PDFs=0, warning se XLSX=0
7. Configuracoes auto-salvam ao alterar qualquer campo (debounce 1s)
8. Processamento resiliente: try/except por loja (1 falha nao afeta as demais)
9. Informacoes de produto na tabela sempre em **negrito** (fonte_bold + fs_destaque)
10. beka.pdf nao e mais PDF especial — processado como qualquer PDF normal
11. Nome do sistema: **Beka MultiPlace** (nao "Market Place")
12. XLSX de lucro: coluna "Imposto" (nao "Aliquota"), coluna separada "Custo Fixo"
13. Config lojas no frontend: campo "Aliquota" (nao "Imposto %"), campos centralizados
14. **"Continue trabalhando autonomamente ate finalizar. Nao pare para pedir confirmacao a cada passo."**
