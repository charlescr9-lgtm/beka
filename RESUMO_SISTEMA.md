# BEKA MARKET PLACE — Resumo Completo do Sistema
**Última atualização:** 09/02/2026
**Deploy:** https://web-production-274ef.up.railway.app/
**Repositório:** https://github.com/charlescr9-lgtm/beka.git
**Local:** C:\Users\Micro\Desktop\Beka MKT

---

## O QUE É
Dashboard web (Flask) para processar etiquetas de envio da Shopee. Recebe PDFs de etiquetas + ZIPs com XMLs de Nota Fiscal, e gera PDFs finais organizados por loja com:
- 1 etiqueta por página (150mm x 230mm)
- Código de barras da chave NFe
- Tabela de produtos em **negrito** (SKU, título ou ambos)
- Páginas de continuação quando produtos não cabem
- Resumo XLSX por loja com contagem de SKUs
- Cálculo de lucro por produto

---

## ARQUITETURA

### Arquivos do projeto
```
C:\Users\Micro\Desktop\Beka MKT\
├── dashboard.py             # App Flask principal (~1600 linhas) — rotas API, processamento, estado
├── etiquetas_shopee.py      # Motor de processamento de PDFs (~2200 linhas) — core do sistema
├── models.py                # Modelos SQLAlchemy (~160 linhas) — User, Session, Payment
├── auth.py                  # Autenticação JWT (~200 linhas) — login, registro, verificação email, Google OAuth
├── email_utils.py           # Envio de emails SMTP (~40 linhas) — verificação de email
├── payments.py              # Integração Mercado Pago (~165 linhas) — criar pagamento, webhook
├── requirements.txt         # Dependências Python
├── Procfile                 # Deploy Railway: gunicorn dashboard:app
├── static/
│   ├── index.html           # Dashboard principal (~2400 linhas) — SPA vanilla JS
│   ├── login.html           # Tela de login/cadastro/verificação (~400 linhas)
│   └── logo.svg             # Logo
```

### Stack tecnológico
- **Backend:** Flask + Flask-JWT-Extended + Flask-SQLAlchemy + Flask-Bcrypt
- **Banco:** SQLite (persistido via Railway volume) + migração automática (`_migrate_db()`)
- **PDF:** PyMuPDF (fitz) — extração, recorte, escala, inserção de texto
- **Barcode:** python-barcode (Code128, formato SVG)
- **Excel:** openpyxl — resumos e relatórios de lucro
- **Auth:** Google OAuth (google-auth + Google Identity Services JS)
- **Email:** SMTP (email_utils.py) para verificação de cadastro
- **Pagamento:** Mercado Pago SDK
- **Deploy:** Gunicorn no Railway com volume persistente

---

## BANCO DE DADOS (models.py)

### Tabela `users`
| Campo | Tipo | Descrição |
|-------|------|-----------|
| id | Integer PK | ID do usuário |
| email | String UNIQUE | Email (lowercase) |
| password_hash | String | Senha com bcrypt |
| plano | String | free / basico / pro / empresarial |
| processamentos_mes | Integer | Contador mensal (reseta a cada mês) |
| mes_atual | String | "YYYY-MM" para controle de reset |
| created_at | DateTime | Data de criação |
| is_active | Boolean | Conta ativa |
| email_verified | Boolean | Se email foi verificado |
| email_code | String | Código de verificação 6 dígitos |
| email_code_expires | DateTime | Expiração do código de verificação |
| google_id | String | ID do Google para login via OAuth |

### Tabela `sessions`
| Campo | Tipo | Descrição |
|-------|------|-----------|
| id | Integer PK | - |
| user_id | FK → users | - |
| token_id | String UNIQUE | UUID da sessão (usado no JWT) |
| ip | String | IP do dispositivo |
| created_at / last_seen | DateTime | Controle de atividade |

### Tabela `payments`
| Campo | Tipo | Descrição |
|-------|------|-----------|
| id | Integer PK | - |
| user_id | FK → users | - |
| status | String | pending / approved / rejected |
| mercadopago_id | String | ID da preferência/pagamento |
| plano_contratado | String | Plano comprado |
| valor | Float | Valor pago |
| created_at / updated_at | DateTime | - |

### Planos
| Plano | Max IPs | Limite proc/mês | Valor |
|-------|---------|-----------------|-------|
| free | 1 | 5 | R$ 0 |
| basico | 1 | ilimitado | R$ 39,90 |
| pro | 2 | ilimitado | R$ 59,90 |
| empresarial | 5 | ilimitado | R$ 89,90 |

### Migração de banco (`_migrate_db()`)
- Função em `dashboard.py` que roda no startup
- Verifica colunas existentes na tabela `users` via `PRAGMA table_info`
- Adiciona automaticamente colunas faltantes (`email_verified`, `email_code`, `email_code_expires`, `google_id`) via `ALTER TABLE`
- Necessário porque SQLite no Railway não suporta `db.create_all()` para adicionar colunas em tabelas existentes

---

## AUTENTICAÇÃO (auth.py)

- **JWT com sessão:** Token inclui `sid` (session ID) validado contra o banco
- **Controle de IPs:** Cada login registra IP; bloqueia se atingiu limite do plano
- **VIP:** Email `charlescr9@gmail.com` sempre tem plano "empresarial"
- **Token expira:** 30 dias
- **Confirmação de senha:** Registro exige `senha2` (confirmação) que deve coincidir com `senha`
- **Verificação de email:** Código de 6 dígitos enviado por SMTP; auto-skip se SMTP não configurado
- **Google OAuth:** Login via ID Token (Google Identity Services JS no frontend → verificação via `google-auth` no backend)

### Rotas
- `POST /api/auth/register` — Cadastro (email + senha + senha2, envia código de verificação)
- `POST /api/auth/login` — Login (retorna JWT, bloqueia se limite de IPs)
- `POST /api/auth/logout` — Invalida sessão
- `GET /api/auth/me` — Dados do usuário logado
- `POST /api/auth/verify-email` — Verificar código de 6 dígitos
- `POST /api/auth/resend-code` — Reenviar código de verificação
- `POST /api/auth/google` — Login/registro via Google OAuth (ID Token)
- `GET /api/auth/google-client-id` — Retorna client ID do Google (público)
- `GET /api/planos` — Lista planos pagos (público)

---

## PAGAMENTO (payments.py)

### Fluxo Mercado Pago
1. Usuário clica "Assinar" → escolhe plano
2. `POST /api/payment/create` → cria preferência no MP → retorna URL de checkout
3. Usuário paga no Mercado Pago
4. `POST /api/payment/webhook` → MP notifica → `user.plano` atualizado automaticamente
5. Usuário volta ao dashboard com plano ativo

### Variável obrigatória
- `MERCADOPAGO_ACCESS_TOKEN` — Access Token de produção (configurado no Railway)

---

## MOTOR DE PROCESSAMENTO (etiquetas_shopee.py)

### Pipeline principal
```
1. Carregar XMLs de todos os ZIPs
   └─ Extrai: NF, chave, CNPJ, nome emitente, produtos
2. Carregar PDFs regulares
   └─ Detecta layout automático (2x2 / 2x1 / 1x1 / página pequena)
   └─ Recorta cada etiqueta
   └─ Associa com XML pela NF
3. Processar PDFs especiais (lanim*.pdf, shein crua.pdf)
4. Remover duplicatas por NF
5. Separar por CNPJ (loja)
6. Para cada loja (com try/except individual):
   ├─ Ordenar: simples primeiro, multi-produto no final
   ├─ Gerar PDF (150x230mm, 1 etiqueta/página)
   │   ├─ Imagem da etiqueta original (escalada)
   │   ├─ Código de barras Code128 da chave NFe
   │   ├─ Tabela de produtos em NEGRITO (SKU/título/ambos + quantidade)
   │   └─ Páginas de continuação se produtos não couberem
   └─ Gerar XLSX resumo (contagem por SKU)
7. Gerar resumo geral XLSX (todas as lojas)
```

### Processamento resiliente por loja
- Cada loja é processada dentro de um `try/except` individual
- Se 1 loja falhar, as demais continuam normalmente
- Erro é registrado no log mas não interrompe o processamento

### Detecção de layout de página
```python
_detectar_layout_pagina(pagina)
├─ Se larg <= 420: página inteira (1 etiqueta — página pequena/retirada)
├─ Testa grid 2x2 (4 etiquetas) — padrão Shopee
├─ Testa 2x1 (2 etiquetas empilhadas)
└─ Fallback: página inteira (1 etiqueta)
```
Usa marcadores de conteúdo (Pedido:, REMETENTE, DANFE) em vez de depender apenas de NF.

### Detecção de etiqueta em região
```python
_contar_etiquetas_regiao(pagina, clip)
├─ Verifica texto mínimo (>10 chars)
├─ Busca: 'Pedido:', 'REMETENTE', 'DANFE', NF numérica
└─ Retorna True se encontrar NF OU Pedido: OU (REMETENTE + DANFE)
```

### Recorte de etiqueta
```python
_recortar_pagina(pagina)
├─ Usa layout detectado para definir quadrantes
├─ Para cada quadrante com etiqueta:
│   ├─ Extrai NF (ou gera ID sintético: SEM_NF_{pdf_id}_p{pag}_q{idx})
│   ├─ ID sintético inclui nome do PDF para evitar colisões entre múltiplos PDFs
│   ├─ Associa com dados XML
│   ├─ Extrai SKU principal
│   ├─ Extrai nome da loja do campo REMETENTE
│   └─ Retorna dict com todos os metadados
```

### Geração do PDF final
```python
gerar_pdf_loja(etiquetas, caminho_saida, nome_loja)
├─ Página 150mm x 230mm (425pt x 652pt)
├─ Etiqueta original: escalada proporcionalmente
│   └─ Se tem tabela de produtos, reduz altura da etiqueta para caber
├─ Código de barras: Code128 da chave NFe (37pt de altura)
├─ Tabela de produtos (tudo em NEGRITO — fonte_bold + fs_destaque):
│   ├─ Modo "sku": CODIGO | - | QTD
│   ├─ Modo "titulo": PRODUTO (descrição) | - | QTD
│   ├─ Modo "ambos": CODIGO | DESCRIÇÃO | QTD
│   ├─ Até 10 produtos por etiqueta (excedente em páginas de continuação)
│   ├─ Linhas divisórias cinza entre produtos
│   └─ Fonte configurável (padrão 7pt, destaque 1.5x)
├─ Overflow de produtos:
│   ├─ _desenhar_secao_produtos retorna índice do último produto desenhado
│   ├─ Se não couberam todos, cria página de continuação
│   └─ Continuação repete etiqueta + barcode + produtos restantes
├─ Rodapé: "p.{número}" posicionado em ALTURA_PT - MARGEM_INFERIOR - 8
```

### Cálculo de espaço para tabela
```python
# Se tem produtos, calcula espaço necessário e reduz etiqueta
espaco_tabela = 37 (barcode) + 20 (header) + (num_prods * line_h) + 15 (margem)
alt_max = ALTURA_PT - MARGEM_TOPO - MARGEM_INFERIOR - espaco_tabela
if alt_etiqueta > alt_max:
    alt_etiqueta = max(alt_max, ALTURA_PT * 0.45)  # mínimo 45% da página
```

### Ordenação de etiquetas
```python
_ordenar_etiquetas(etiquetas)
├─ Simples (1 item, qtd=1): ordenadas por SKU → NF
├─ Múltiplos (multi-item OU qtd>1): ordenadas por SKU → NF
└─ Resultado: [todas simples] + [todas múltiplas]
```

### PDFs especiais

#### lanim*.pdf — Etiquetas CPF (Declaração de Conteúdo)
- Detecta todos: lanim.pdf, lanim 2.pdf, lanim2.pdf, etc.
- Auto-crop ao conteúdo (A4 com conteúdo no canto)
- **Requer XLSX** com colunas: `order_sn` + `product_info`
- Parseia product_info: `[1] Parent SKU Reference No.: ABC; Quantity: 5; Product Name: ...; Variation Name: P/Blue`
- Tabela mostra: SKU | VARIAÇÃO | Quant (adaptável ao modo de exibição)
- Saída: 150mm x 225mm
- Loja fixa: "CPF" com CNPJ sintético "LANIM_CPF"

#### shein crua.pdf — Shein importação direta
- Páginas alternadas: Par=Etiqueta, Ímpar=DANFE
- Extrai NF, chave, CNPJ e produtos do DANFE
- Remove texto chinês dos atributos
- Saída: 150mm x 225mm com barra de código de barras vertical na direita

> **Nota:** `beka.pdf` foi **removido** de PDFS_ESPECIAIS. Agora é processado como PDF normal (grid 2x2 ou retirada detectada automaticamente pela lógica `larg <= 420`).

### XLSX de pedidos (carregar_xlsx_pedidos)
- Lê XLSX com colunas: `order_sn`, `product_info`
- Parseia blocos `[N]` com: Parent SKU, SKU Reference No., Quantity, Product Name, Variation Name
- **Acumula** produtos quando mesmo order_sn aparece em múltiplas linhas
- Retorna: `{order_sn: {produtos: [...], total_itens, total_qtd}}`

### Extração de SKU principal
```python
_extrair_sku_principal(sku_completo)
├─ Remove APENAS sufixos de tamanho do FINAL:
│   ├─ Letras: P, M, G, PP, GG, XG, XS, XL, XXL, XXG, EG, EGG
│   ├─ Números 2 dígitos: 24-56 (numeração de calçado)
│   └─ Números 1 dígito: 1-9
├─ Exemplos: "TEN-BO-BR-38" → "TEN-BO-BR"
│            "ABC-M" → "ABC"
│            "PROD-123-GG" → "PROD-123"
```

### Exclusão de PDFs especiais do processamento normal
```python
# carregar_todos_pdfs exclui:
- Arquivos começando com "etiquetas_prontas"
- Arquivos começando com "lanim" (processados separadamente como CPF)
- PDFs listados em PDFS_ESPECIAIS: ['lanim.pdf', 'shein crua.pdf']
```

---

## DASHBOARD API (dashboard.py)

### Rotas principais
| Endpoint | Método | Descrição |
|----------|--------|-----------|
| `/api/status` | GET | Status completo: processando, arquivos, config, resultado |
| `/api/logs` | GET | Logs em tempo real (paginado por offset) |
| `/api/processar` | POST | Iniciar processamento (thread em background) |
| `/api/upload` | POST | Upload de PDF/ZIP/XLSX/XLS |
| `/api/upload-custos` | POST | Upload de planilha de custos |
| `/api/remover-arquivo` | POST | Remover arquivo da entrada |
| `/api/configuracoes` | POST | Salvar configurações do usuário |
| `/api/novo-lote` | POST | Limpar tudo para novo lote |
| `/api/limpar-saida` | POST | Limpar pasta de saída |
| `/api/download-todos` | GET | Baixar ZIP com todos os resultados |
| `/api/download/<loja>/<arquivo>` | GET | Baixar arquivo específico |
| `/api/download-resumo-geral` | GET | Baixar XLSX resumo geral |
| `/api/gerar-lucro` | POST | Gerar relatório de lucro |
| `/api/download-lucro` | GET | Baixar XLSX de lucro geral |
| `/api/download-lucro/<loja>` | GET | Baixar XLSX de lucro por loja |
| `/api/exemplo-custos` | GET | Baixar template de planilha de custos |
| `/api/agrupar` | POST | Agrupar lojas em 1 PDF |
| `/api/agrupamentos` | GET/POST | Salvar/carregar presets de agrupamento |
| `/api/configuracoes-lucro-lojas` | GET/POST | Config de lucro por loja |
| `/api/lojas-lucro` | GET | Leitura de XMLs da pasta lucro (lojas disponíveis) |
| `/api/historico` | GET | Histórico de processamentos |

### Estado por usuário (em memória)
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
        "largura_mm": 150,        # Largura da página de saída
        "altura_mm": 230,         # Altura da página de saída
        "margem_esq": 8,
        "margem_dir": 8,
        "margem_topo": 5,
        "margem_inf": 5,
        "fonte_produto": 7,        # Tamanho fonte BASE tabela produtos
        "exibicao_produto": "sku", # "sku" | "titulo" | "ambos"
        "perc_declarado": 100,     # % valor declarado vs real
        "taxa_shopee": 18,         # Comissão Shopee %
        "imposto_simples": 4,      # Imposto Simples %
        "custo_fixo": 3.0,         # Custo fixo por unidade
        "planilha_custos": path,
        "lucro_por_loja": {}       # Override de config por loja
    }
}
```

### Persistência de configurações
- Salvo em: `{pasta_entrada}/_config.json`
- Carregado automaticamente ao iniciar o servidor
- **Auto-save:** frontend salva automaticamente 1s após qualquer alteração (debounce)
- Também persiste: `_ultimo_resultado.json`, `_ultimo_lucro.json`

---

## FRONTEND (static/index.html)

### Abas
1. **Etiquetas** — Upload+Processar, stats, resultado do último processamento, configurações, agrupamento
2. **Resultados** — PDFs por loja com botões de download, resumo geral XLSX
3. **Lucro** — Upload de XMLs/custos, relatório de lucro por loja

### Layout da aba Etiquetas
```
[Upload + Processar] ← 1 card horizontal (drag&drop + progresso + botões)
[PDFs] [XLSX] [Etiquetas] [Lojas] ← Stats grid-4

┌─────────────────────┐ ┌─────────────────────┐
│ Ultimo Processamento│ │ CONFIGURAÇÕES       │
│ (lojas em 2 colunas)│ │ Larg/Alt/Margens    │
│                     │ │ Fonte/Exibição      │
│ Log do Sistema      │ │ Agrupamento de Lojas│
│                     │ │ Limpar Saída        │
└─────────────────────┘ └─────────────────────┘
← Layout 2 colunas (grid 1fr 1fr) →
```

### Funcionalidades do frontend
- **Upload:** Aceita .pdf, .zip, .xlsx, .xls (drag & drop no card ou clique no ícone Upload)
- **Botão processar:** Desabilitado quando PDFs=0, warning amarelo quando XLSX=0
- **Polling:** Atualiza status e logs a cada 800ms durante processamento, 5s em idle
- **Download automático:** ZIP baixa automaticamente ao finalizar processamento
- **Modal de planos:** Botão "Assinar" (coroa) abre modal com planos disponíveis
- **Badge de plano:** Mostra plano atual + IPs usados no header
- **Toast notifications:** Sucesso (verde), erro (vermelho), warning (amarelo), info (azul)
- **Aviso sem NF:** Banner vermelho pulsante se houver etiquetas sem Nota Fiscal
- **Auto-save config:** Qualquer alteração em configuração salva automaticamente (debounce 1s)
- **Ultimo resultado compacto:** Lojas exibidas em grid de 2 colunas (metade da altura)
- **Config inline:** Todas as configurações de etiqueta + agrupamento em 1 card ao lado do resultado

---

## CÁLCULO DE LUCRO

### Fórmula por produto
```
V_Real = V_Declarado / (perc_declarado / 100)
Custo_Imposto = V_Declarado × (imposto_simples / 100)
Custo_Shopee = (V_Real × taxa_shopee / 100) + (custo_fixo × qtd)
Lucro = V_Real - Custo_Imposto - Custo_Shopee - Custo_Produto
```

### Busca inteligente de custo
```python
_buscar_custo_inteligente(sku_xml, dict_custos)
1. Match exato (uppercase)
2. Match por SKU base (sem variação)
3. Chave da planilha começa com SKU base
4. SKU base começa com chave da planilha
5. Fallback: R$ 0,00 (marca como "sem custo")
```

---

## VARIÁVEIS DE AMBIENTE (Railway)

| Variável | Obrigatória | Descrição |
|----------|-------------|-----------|
| `MERCADOPAGO_ACCESS_TOKEN` | Sim | Token de produção do Mercado Pago |
| `RAILWAY_VOLUME_MOUNT_PATH` | Auto | Caminho do volume persistente (Railway configura) |
| `SECRET_KEY` | Recomendado | Chave secreta do Flask |
| `JWT_SECRET_KEY` | Recomendado | Chave para assinar JWT |
| `APP_URL` | Recomendado | URL base (default: https://web-production-274ef.up.railway.app) |
| `PORT` | Auto | Porta HTTP (Railway configura) |
| `SMTP_HOST` | Opcional | Servidor SMTP para verificação de email |
| `SMTP_PORT` | Opcional | Porta SMTP (default: 587) |
| `SMTP_USER` | Opcional | Usuário SMTP |
| `SMTP_PASS` | Opcional | Senha SMTP |
| `GOOGLE_CLIENT_ID` | Opcional | Client ID do Google para OAuth |

> **Nota:** Se as variáveis SMTP não estiverem configuradas, a verificação de email é ignorada automaticamente (usuário cadastra sem verificar). Se `GOOGLE_CLIENT_ID` não estiver configurado, o botão Google OAuth não aparece na tela de login.

---

## ESTRUTURA DE PASTAS POR USUÁRIO

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
└── _config.json           # Config salva do usuário (auto)
```

### Saída (resultados)
```
/volume/users/{user_id}/Etiquetas prontas/
├── Loja_ABC/
│   ├── etiquetas_Loja_ABC_20260209_143022.pdf
│   ├── cpf_Loja_ABC_20260209_143022.pdf        (se houver CPF)
│   ├── shein_Loja_ABC_20260209_143022.pdf      (se houver Shein)
│   └── resumo_Loja_ABC_20260209_143022.xlsx
├── CPF/
│   └── cpf_CPF_20260209_143022.pdf
├── Grupo_Personalizado/                         (agrupamentos)
│   └── agrupado_Grupo_20260209_143022.pdf
├── resumo_geral_20260209_143022.xlsx
├── _ultimo_resultado.json
└── _ultimo_lucro.json
```

---

## DEPENDÊNCIAS (requirements.txt)
```
flask, flask-cors, flask-sqlalchemy, flask-bcrypt, flask-jwt-extended
gunicorn, xmltodict, pandas, openpyxl
PyMuPDF, python-barcode, mercadopago, google-auth
```

---

## REGRAS IMPORTANTES (definidas pelo usuário)
1. **"Não mexer no que já está funcionando"** — qualquer alteração deve preservar funcionalidades existentes
2. Etiquetas sem NF devem ser geradas com ID sintético (`SEM_NF_{pdf_id}_p{pag}_q{idx}`) — inclui nome do PDF para evitar colisões entre múltiplos PDFs
3. Multi-produto e multi-quantidade vão para o FINAL da impressão
4. Linhas divisórias cinza entre produtos na tabela
5. lanim*.pdf são CPF e usam dados do XLSX
6. Botão processar: bloqueado durante upload, desabilitado se PDFs=0, warning se XLSX=0
7. Configurações auto-salvam ao alterar qualquer campo (debounce 1s)
8. Processamento resiliente: try/except por loja (1 falha não afeta as demais)
9. Informações de produto na tabela sempre em **negrito** (fonte_bold + fs_destaque)
10. beka.pdf não é mais PDF especial — processado como qualquer PDF normal
