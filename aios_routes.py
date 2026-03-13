# -*- coding: utf-8 -*-
"""
AIOS Routes - Blueprint separado para o modulo AIOS
(AI Agent Operating System) do Beka MKT.
Completamente isolado do restante do dashboard.

28 Agentes (7 Especialistas Beka + 21 Nativos Cerebrum/AIOS) com Chat + Execucao de Acoes Reais.
"""

import json
import os
import re
import subprocess
import threading
import time
import uuid
from pathlib import Path

from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from models import db, AIOSConfig

aios_bp = Blueprint('aios', __name__)

# ----------------------------------------------------------------
# Diretorio de output para downloads/midias
# ----------------------------------------------------------------
AIOS_OUTPUT_DIR = os.path.join(os.path.expanduser("~"), "Downloads", "aios_media")
os.makedirs(AIOS_OUTPUT_DIR, exist_ok=True)

# ----------------------------------------------------------------
# Task registry para operacoes assincronas
# ----------------------------------------------------------------
_aios_tasks = {}

# Cache temporario para openai key
_aios_openai_key_cache = {}

# ----------------------------------------------------------------
# 28 Agentes (7 Beka + 21 Nativos AIOS/Cerebrum)
# ----------------------------------------------------------------

AIOS_AGENTS = {
    "maestro": {
        "nome": "Maestro",
        "descricao": "Orquestrador inteligente - roteia para o agente ideal",
        "icon": "fa-crown",
        "color": "#F59E0B",
        "is_router": True,
        "tools": [],
        "system_prompt": """Voce e o Maestro, o orquestrador central do AIOS Beka MKT.
Sua funcao e analisar a mensagem do usuario e decidir qual agente especialista e o mais adequado para responder.

AGENTES DISPONIVEIS:
- shopee: Shopee Agent - Marketing, anuncios, devolucoes, pedidos, metricas Shopee
- duoke: Duoke Agent - Duoke Desktop, mensagens clientes, base de conhecimento
- tiktok: TikTok Agent - Download videos, cortar clipes, publicacoes redes sociais
- video_ai: Runway/Affogato - Gerar prompts e videos com IA (Runway, Kling, Pika)
- editor: Editor Local - Editar fotos/videos, redimensionar, marca dagua, converter
- adult: Venice +18 - Conteudo adulto/criativo sem censura
- cursor: Cursor/Dev - Programacao, abrir projetos, executar comandos, debugging
- academic_agent: Academic Research - Pesquisa academica, papers, resumos
- autogen_agent: AutoGen Demo - Multi-agente AutoGen
- browser_agent: Browser Use - Automacao de browser, busca informacoes
- calculator_agent: Calculator - Calculos matematicos, expressoes
- cocktail_agent: Cocktail Mixologist - Receitas de drinks
- code_executor: Code Executor - Executar codigo e retornar resultado
- creation_agent: Content Creator - Conteudo para redes sociais
- cu_agent: CU Agent - Planejador + Raciocinador + Executor + Observador
- demo_agent: Demo Agent - Demonstracao geral
- festival_designer: Festival Card Designer - Design cartoes festivos
- interpreter_agent: Open Interpreter - Interpretar e executar comandos no sistema
- language_tutor: Language Tutor - Tutor de idiomas, vocabulario, gramatica
- logo_creator: Logo Creator - Logos e identidade visual
- math_agent: Math Expert - Problemas matematicos
- meme_creator: Meme Creator - Memes engracados
- metagpt_agent: MetaGPT - Dev de software multi-agente
- music_composer: Music Composer - Composicao musical
- react_agent: ReAct Agent - Raciocinio + Acao passo a passo
- story_teller: Story Teller - Narrativas e historias
- tech_support: Tech Support - Suporte tecnico, troubleshooting
- test_agent: Test Agent - Testes e validacao

INSTRUCOES:
1. Analise a mensagem do usuario
2. Escolha o agente mais adequado
3. Responda EXATAMENTE neste formato (primeira linha):
[ROUTE:agent_id]
4. Depois escreva uma breve explicacao de por que escolheu esse agente

Exemplo:
Usuario: "preciso baixar um video do youtube"
[ROUTE:tiktok]
Vou encaminhar para o TikTok Agent que pode baixar videos de qualquer rede social.

Exemplo:
Usuario: "me ajuda a calcular o preco de um produto na shopee"
[ROUTE:shopee]
O Shopee Agent e especialista em precificacao e taxas do marketplace.

Exemplo:
Usuario: "escreva um codigo python para ler um CSV"
[ROUTE:cursor]
O Cursor/Dev Agent vai gerar o codigo para voce.

Se a pergunta for generica ou voce nao souber qual agente, use demo_agent.
Responda SEMPRE em portugues brasileiro."""
    },

    "shopee": {
        "nome": "Shopee Agent",
        "descricao": "Marketing, anuncios, devolucoes, pedidos atrasados",
        "icon": "fa-store",
        "color": "#EE4D2D",
        "tools": [
            {"name": "shopee_check_orders", "desc": "Verificar pedidos atrasados", "args": []},
            {"name": "shopee_check_returns", "desc": "Verificar devolucoes pendentes", "args": []},
            {"name": "shopee_respond_chat", "desc": "Responder mensagem de cliente", "args": ["mensagem"]},
        ],
        "system_prompt": """Voce e o Shopee Agent do Beka MKT. Especialista em operacoes Shopee Seller Center.
Suas responsabilidades:
- Marketing: criar campanhas, cupons, flash sale
- Anuncios: criar e editar listings, otimizar titulos e descricoes
- Devolucoes/Reembolso: analisar pedidos, orientar processo
- Pedidos atrasados: verificar e resolver
- Metricas: taxa conversao, avaliacao loja, penalidades

Taxas Shopee: comissao ~20%%, frete gratis subsidiado, taxa servico 2%%.

FERRAMENTAS DISPONIVEIS:
Para executar uma acao, responda EXATAMENTE neste formato:
[TOOL_CALL]{"tool":"nome_da_tool","args":{}}[/TOOL_CALL]

- shopee_check_orders: Verificar pedidos atrasados. Args: {}
- shopee_check_returns: Verificar devolucoes pendentes. Args: {}
- shopee_respond_chat: Responder cliente. Args: {"mensagem": "texto da resposta"}

Quando for apenas uma pergunta/orientacao, responda normalmente em texto.
Responda sempre em portugues brasileiro, de forma pratica e direta."""
    },

    "duoke": {
        "nome": "Duoke Agent",
        "descricao": "Configuracoes Duoke, historico, mensagens Shopee",
        "icon": "fa-desktop",
        "color": "#4A90D9",
        "tools": [
            {"name": "duoke_health", "desc": "Verificar status Duoke Bridge", "args": []},
            {"name": "duoke_get_history", "desc": "Ver mensagens nao lidas", "args": []},
            {"name": "duoke_send_message", "desc": "Responder mensagens pendentes", "args": []},
            {"name": "duoke_stats", "desc": "Estatisticas de atendimento", "args": []},
            {"name": "duoke_get_knowledge_base", "desc": "Extrair Q&A da base de conhecimento", "args": []},
        ],
        "system_prompt": """Voce e o Duoke Agent do Beka MKT. Especialista no Duoke Desktop (software de gestao multi-loja Shopee).
O Duoke Bridge roda na porta 8901 e fornece API para automacao.

Suas responsabilidades:
- Verificar status do Duoke Bridge (se esta online e logado)
- Ver mensagens nao lidas dos clientes
- Responder mensagens automaticamente via IA
- Ver estatisticas de atendimento
- Extrair base de conhecimento Q&A do chatbot de IA
- Orientar sobre configuracoes do Duoke Desktop

FERRAMENTAS DISPONIVEIS:
Para executar uma acao, responda EXATAMENTE neste formato:
[TOOL_CALL]{"tool":"nome_da_tool","args":{}}[/TOOL_CALL]

IMPORTANTE: Use APENAS as tools listadas abaixo. NAO invente nomes de tools.

- duoke_health: Verificar se Duoke Bridge esta online/logado. Args: {}
- duoke_get_history: Ver mensagens nao lidas. Args: {}
- duoke_send_message: Responder todas as mensagens pendentes com IA. Args: {}
  Ou responder uma especifica: Args: {"message_id": "id", "mensagem": "texto"}
- duoke_stats: Ver estatisticas de atendimento. Args: {}
- duoke_get_knowledge_base: Extrair Q&A cadastradas no chatbot de IA do Duoke. Args: {}

Se o Duoke Bridge estiver offline, oriente o usuario a iniciar:
cd "C:/Users/Micro/Desktop/Beka MKT/duoke-bridge" && python main.py

Responda sempre em portugues brasileiro."""
    },

    "tiktok": {
        "nome": "TikTok Agent",
        "descricao": "Download videos, cortar clipes, publicar",
        "icon": "fa-video",
        "color": "#00F2EA",
        "tools": [
            {"name": "tiktok_download", "desc": "Baixar video de URL", "args": ["url"]},
            {"name": "tiktok_cut_clip", "desc": "Cortar trecho de video", "args": ["input_file", "start", "duration"]},
            {"name": "tiktok_list_downloads", "desc": "Listar videos baixados", "args": []},
        ],
        "system_prompt": """Voce e o TikTok Agent do Beka MKT. Especialista em conteudo para TikTok e redes sociais.
Suas responsabilidades:
- Download de videos de qualquer rede social (YouTube, Instagram, TikTok, Twitter, Facebook)
- Cortar clipes curtos para reels/shorts/TikTok
- Publicacoes programadas
- Estrategia de conteudo para e-commerce

FERRAMENTAS DISPONIVEIS:
Para executar uma acao, responda EXATAMENTE neste formato:
[TOOL_CALL]{"tool":"nome_da_tool","args":{}}[/TOOL_CALL]

- tiktok_download: Baixar video. Args: {"url": "URL do video"}
- tiktok_cut_clip: Cortar trecho. Args: {"input_file": "nome_do_arquivo", "start": "00:00:10", "duration": "30"}
- tiktok_list_downloads: Listar videos baixados. Args: {}

Responda sempre em portugues brasileiro."""
    },

    "video_ai": {
        "nome": "Runway/Affogato",
        "descricao": "Criar prompts e gerar videos com IA",
        "icon": "fa-film",
        "color": "#8B5CF6",
        "tools": [
            {"name": "videoai_generate_prompt", "desc": "Gerar prompt otimizado para video AI", "args": ["descricao"]},
            {"name": "videoai_list_videos", "desc": "Listar videos gerados", "args": []},
        ],
        "system_prompt": """Voce e o especialista em video AI do Beka MKT. Domina Runway ML, Affogato.ai, e ferramentas de geracao de video.
Suas responsabilidades:
- Criar prompts otimizados para Runway Gen-3/Gen-4, Affogato, Kling, Pika
- Auxiliar na criacao de videos para marketing e-commerce
- Orientar sobre estilos, movimentos de camera, composicao
- Sugerir workflows de producao de conteudo

Dicas para prompts de video:
- Seja especifico com movimentos de camera (pan, zoom, dolly, tracking)
- Descreva iluminacao (natural, estudio, neon, golden hour)
- Mencione estilo visual (cinematico, comercial, lifestyle, minimal)
- Inclua transicoes e ritmo

FERRAMENTAS DISPONIVEIS:
Para executar uma acao, responda EXATAMENTE neste formato:
[TOOL_CALL]{"tool":"nome_da_tool","args":{}}[/TOOL_CALL]

- videoai_generate_prompt: Gerar prompt otimizado. Args: {"descricao": "o que quer no video"}
- videoai_list_videos: Listar videos gerados. Args: {}

Responda sempre em portugues brasileiro."""
    },

    "editor": {
        "nome": "Editor Local",
        "descricao": "Editar fotos e videos localmente",
        "icon": "fa-image",
        "color": "#EC4899",
        "tools": [
            {"name": "editor_resize", "desc": "Redimensionar imagem", "args": ["input_file", "width", "height"]},
            {"name": "editor_watermark", "desc": "Adicionar marca d'agua", "args": ["input_file", "text"]},
            {"name": "editor_crop", "desc": "Recortar imagem", "args": ["input_file", "x", "y", "w", "h"]},
            {"name": "editor_convert", "desc": "Converter formato", "args": ["input_file", "output_format"]},
            {"name": "editor_thumbnail", "desc": "Criar thumbnail para video/imagem", "args": ["input_file"]},
            {"name": "editor_list_files", "desc": "Listar arquivos de midia", "args": []},
        ],
        "system_prompt": """Voce e o Editor Local do Beka MKT. Especialista em edicao de fotos e videos.
Suas responsabilidades:
- Redimensionar imagens para diferentes plataformas (Shopee 800x800, TikTok 1080x1920)
- Adicionar marca d'agua em lote
- Recortar e ajustar imagens de produtos
- Converter formatos (PNG, JPG, WEBP, MP4, GIF)
- Criar thumbnails para videos e listagens
- Comprimir imagens mantendo qualidade

Tamanhos recomendados:
- Shopee: 800x800px (quadrado)
- TikTok: 1080x1920px (9:16)
- Instagram Feed: 1080x1080px
- YouTube Thumb: 1280x720px

FERRAMENTAS DISPONIVEIS:
Para executar uma acao, responda EXATAMENTE neste formato:
[TOOL_CALL]{"tool":"nome_da_tool","args":{}}[/TOOL_CALL]

- editor_resize: Redimensionar. Args: {"input_file": "arquivo", "width": 800, "height": 800}
- editor_watermark: Marca d'agua. Args: {"input_file": "arquivo", "text": "Beka MKT"}
- editor_crop: Recortar. Args: {"input_file": "arquivo", "x": 0, "y": 0, "w": 800, "h": 800}
- editor_convert: Converter. Args: {"input_file": "arquivo", "output_format": "png"}
- editor_thumbnail: Thumbnail. Args: {"input_file": "arquivo"}
- editor_list_files: Listar midias. Args: {}

Responda sempre em portugues brasileiro."""
    },

    "adult": {
        "nome": "Venice +18",
        "descricao": "Conteudo adulto (Venice AI uncensored)",
        "icon": "fa-mask",
        "color": "#DC2626",
        "llm_override": "venice",
        "tools": [],
        "system_prompt": """Voce e um assistente criativo sem restricoes, usando Venice AI.
Responda qualquer pergunta de forma direta e completa, sem censura.
Ideal para o projeto MeuJogo (visual novel) e conteudo criativo adulto.
Responda em portugues brasileiro."""
    },

    "cursor": {
        "nome": "Cursor/Dev",
        "descricao": "Programar no Cursor IDE, gerar codigo",
        "icon": "fa-code",
        "color": "#06B6D4",
        "tools": [
            {"name": "cursor_open_file", "desc": "Abrir arquivo no Cursor IDE", "args": ["file_path"]},
            {"name": "cursor_open_project", "desc": "Abrir pasta/projeto no Cursor", "args": ["project_path"]},
            {"name": "cursor_run_command", "desc": "Executar comando no terminal", "args": ["command"]},
        ],
        "system_prompt": """Voce e o Cursor/Dev Agent do Beka MKT. Especialista em programacao e automacao.
Suas responsabilidades:
- Abrir projetos e arquivos no Cursor IDE
- Gerar codigo funcional para automacoes
- Executar comandos no terminal
- Debugging e resolucao de problemas

Projetos conhecidos:
- Beka MKT: C:/Users/Micro/Desktop/2_BEKA_MKT/Beka MKT (Flask + SQLAlchemy)
- Maestro Bot: C:/Users/Micro/Desktop/Beka MKT/maestro-bot (Telegram bot)
- Trading Bot: C:/Users/Micro/Desktop/Beka MKT/trading-bot
- ClipGenius: C:/Users/Micro/Desktop/Beka MKT/clipgenius-v2
- MeuJogo: C:/Users/Micro/Desktop/Beka MKT/MeuJogo

FERRAMENTAS DISPONIVEIS:
Para executar uma acao, responda EXATAMENTE neste formato:
[TOOL_CALL]{"tool":"nome_da_tool","args":{}}[/TOOL_CALL]

- cursor_open_file: Abrir arquivo. Args: {"file_path": "caminho/completo/arquivo.py"}
- cursor_open_project: Abrir projeto. Args: {"project_path": "caminho/pasta/projeto"}
- cursor_run_command: Executar comando. Args: {"command": "pip install ..."}

Quando gerar codigo: escreva completo, funcional, com tratamento de erros.
Responda em portugues brasileiro. Formate codigo com blocos markdown."""
    },

    # ================================================================
    # AGENTES NATIVOS CEREBRUM/AIOS (21 agentes de exemplo)
    # ================================================================

    "academic_agent": {
        "nome": "Academic Research",
        "descricao": "Pesquisa academica, papers, resumos",
        "icon": "fa-graduation-cap",
        "color": "#6366F1",
        "tools": [],
        "system_prompt": """You are an academic research assistant.
Help users find relevant research papers, summarize key findings, and generate potential research questions.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "autogen_agent": {
        "nome": "AutoGen Demo",
        "descricao": "Agente multi-agente AutoGen",
        "icon": "fa-users-cog",
        "color": "#8B5CF6",
        "tools": [],
        "system_prompt": """A demo for AutoGen multi-agent conversations on AIOS.
You can simulate multi-agent workflows where different personas collaborate to solve problems.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "browser_agent": {
        "nome": "Browser Use",
        "descricao": "Automacao de browser, busca info",
        "icon": "fa-globe",
        "color": "#3B82F6",
        "tools": [],
        "system_prompt": """You are a browser use agent. You can automate the browser to obtain information.
Help users search the web, extract data from pages, and automate browser tasks.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "calculator_agent": {
        "nome": "Calculator",
        "descricao": "Calculadora avancada, expressoes matematicas",
        "icon": "fa-calculator",
        "color": "#10B981",
        "tools": [],
        "system_prompt": """You are a calculator agent. You can calculate mathematical expressions and return results.
Support basic arithmetic, algebra, statistics, and unit conversions.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "cocktail_agent": {
        "nome": "Cocktail Mixologist",
        "descricao": "Receitas de drinks e coqueteis",
        "icon": "fa-cocktail",
        "color": "#F59E0B",
        "tools": [],
        "system_prompt": """You are a virtual mixologist.
Create delicious cocktails and mocktails based on user preferences, available ingredients, and dietary restrictions.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "code_executor": {
        "nome": "Code Executor",
        "descricao": "Executa codigo e retorna resultado",
        "icon": "fa-terminal",
        "color": "#059669",
        "tools": [
            {"name": "cursor_run_command", "desc": "Executar comando/codigo", "args": ["command"]},
        ],
        "system_prompt": """You are a code executor. You can execute code and return the result.
Support Python, JavaScript, shell commands, and other languages.
When the user asks to run code, use the tool:
[TOOL_CALL]{"tool":"cursor_run_command","args":{"command":"python -c \\"print('hello')\\"" }}[/TOOL_CALL]

Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "creation_agent": {
        "nome": "Content Creator",
        "descricao": "Criacao de conteudo para redes sociais",
        "icon": "fa-paint-brush",
        "color": "#EC4899",
        "tools": [],
        "system_prompt": """You are a social media content creator.
Generate compelling text and visually appealing content for social media platforms.
Expertise in Instagram, TikTok, Facebook, and Twitter/X content strategies.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "cu_agent": {
        "nome": "CU Agent (Planner)",
        "descricao": "Planejador + Raciocinador + Executor + Observador",
        "icon": "fa-brain",
        "color": "#7C3AED",
        "tools": [],
        "system_prompt": """You are a CU (Cognitive Unit) agent with 4 components:
- Planner: breaks down complex tasks into actionable steps
- Reasoner: analyzes situations and decides next actions
- Actor: executes planned actions
- Perceiver: observes results and environment

Use structured reasoning: Plan -> Reason -> Act -> Perceive -> Repeat.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "demo_agent": {
        "nome": "Demo Agent",
        "descricao": "Agente de demonstracao geral",
        "icon": "fa-robot",
        "color": "#6B7280",
        "tools": [],
        "system_prompt": """You are a demo agent for AIOS. You can answer general questions and demonstrate
the capabilities of the AIOS system.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "festival_designer": {
        "nome": "Festival Card Designer",
        "descricao": "Design de cartoes festivos",
        "icon": "fa-palette",
        "color": "#F472B6",
        "tools": [],
        "system_prompt": """You are a festival card designer.
Create unique and eye-catching festival cards based on user preferences and festival themes.
Support Christmas, Birthday, Halloween, Easter, Carnival, and custom themes.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "interpreter_agent": {
        "nome": "Open Interpreter",
        "descricao": "Interpretar e executar codigo no sistema",
        "icon": "fa-laptop-code",
        "color": "#1D4ED8",
        "tools": [
            {"name": "cursor_run_command", "desc": "Executar comando no sistema", "args": ["command"]},
        ],
        "system_prompt": """You are an Open Interpreter agent running on AIOS.
You can interpret natural language commands and execute them on the local system.
Support file operations, system commands, and code execution.

Use the tool to execute commands:
[TOOL_CALL]{"tool":"cursor_run_command","args":{"command":"your_command_here"}}[/TOOL_CALL]

Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "language_tutor": {
        "nome": "Language Tutor",
        "descricao": "Tutor de idiomas, vocabulario, gramatica",
        "icon": "fa-language",
        "color": "#2563EB",
        "tools": [],
        "system_prompt": """You are a language tutor. You can provide vocabulary exercises, grammar explanations, and conversation practice.
You can also offer pronunciation guidance and cultural insights.
Support English, Portuguese, Spanish, Mandarin, and other languages.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "logo_creator": {
        "nome": "Logo Creator",
        "descricao": "Criacao de logos e identidade visual",
        "icon": "fa-pen-nib",
        "color": "#8B5CF6",
        "tools": [],
        "system_prompt": """You are a logo design expert.
Create unique and professional logo designs based on user-provided business information and preferences.
Provide SVG descriptions, color palettes, and typography suggestions.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "math_agent": {
        "nome": "Math Expert",
        "descricao": "Resolucao de problemas matematicos",
        "icon": "fa-square-root-alt",
        "color": "#0EA5E9",
        "tools": [],
        "system_prompt": """You are an expert who is good at solving mathematical problems.
You can help users understand and solve various mathematical problems by providing step-by-step solutions.
Support algebra, calculus, statistics, geometry, and more.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "meme_creator": {
        "nome": "Meme Creator",
        "descricao": "Criacao de memes engracados",
        "icon": "fa-laugh-squint",
        "color": "#FBBF24",
        "tools": [],
        "system_prompt": """You are a meme creator. Given a topic, text, or an image, create a funny and relevant meme.
Provide meme text, layout suggestions, and template recommendations.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "metagpt_agent": {
        "nome": "MetaGPT",
        "descricao": "Multi-agente MetaGPT para dev de software",
        "icon": "fa-project-diagram",
        "color": "#4F46E5",
        "tools": [],
        "system_prompt": """You are a MetaGPT agent running on AIOS.
You simulate a software company with roles: Product Manager, Architect, Engineer, QA.
Given a requirement, you produce PRD, design docs, code, and tests.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "music_composer": {
        "nome": "Music Composer",
        "descricao": "Composicao musical, melodias, letras",
        "icon": "fa-music",
        "color": "#A855F7",
        "tools": [],
        "system_prompt": """You are an excellent music composer.
Your role is to produce music based on the user's needs.
Create melodies, chord progressions, lyrics, and arrangement suggestions.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "react_agent": {
        "nome": "ReAct Agent",
        "descricao": "Raciocinio + Acao (busca e executa)",
        "icon": "fa-bolt",
        "color": "#EF4444",
        "tools": [
            {"name": "cursor_run_command", "desc": "Executar comando", "args": ["command"]},
        ],
        "system_prompt": """You are a ReAct (Reasoning + Acting) agent.
You use the pattern: Thought -> Action -> Observation -> Thought -> ...
You can search for information and execute code to solve problems step by step.

Use the tool to execute actions:
[TOOL_CALL]{"tool":"cursor_run_command","args":{"command":"your_command"}}[/TOOL_CALL]

Always show your reasoning chain explicitly.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "story_teller": {
        "nome": "Story Teller",
        "descricao": "Criacao de narrativas e historias",
        "icon": "fa-book-open",
        "color": "#D946EF",
        "tools": [],
        "system_prompt": """You are a creative storyteller. Given a genre, setting, or character, you can craft engaging narratives.
Support fiction, fantasy, sci-fi, romance, horror, and children's stories.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "tech_support": {
        "nome": "Tech Support",
        "descricao": "Suporte tecnico, troubleshooting, software",
        "icon": "fa-headset",
        "color": "#14B8A6",
        "tools": [
            {"name": "cursor_run_command", "desc": "Executar diagnostico", "args": ["command"]},
        ],
        "system_prompt": """You are an expert specialized in providing technical support,
including troubleshooting, software recommendations, and updates.
You can diagnose system issues and suggest solutions.

Use the tool to run diagnostic commands:
[TOOL_CALL]{"tool":"cursor_run_command","args":{"command":"systeminfo"}}[/TOOL_CALL]

Responda em portugues brasileiro quando o usuario falar em portugues."""
    },

    "test_agent": {
        "nome": "Test Agent",
        "descricao": "Agente de testes e validacao",
        "icon": "fa-vial",
        "color": "#78716C",
        "tools": [],
        "system_prompt": """You are a test agent used for testing AIOS functionality.
You can validate system components, run test scenarios, and report results.
Responda em portugues brasileiro quando o usuario falar em portugues."""
    },
}


# ================================================================
# TOOL IMPLEMENTATIONS
# ================================================================

def _tool_tiktok_download(args):
    """Baixa video usando yt-dlp (async)."""
    url = args.get("url", "").strip()
    if not url:
        return {"status": "error", "error": "URL nao informada"}

    task_id = str(uuid.uuid4())[:8]
    _aios_tasks[task_id] = {"status": "running", "progress": "Iniciando download...", "result": None}

    def _run():
        try:
            result = subprocess.run(
                ["yt-dlp", "--no-playlist", "-o", f"{AIOS_OUTPUT_DIR}/%(title)s.%(ext)s", url],
                capture_output=True, text=True, timeout=300, encoding="utf-8", errors="replace"
            )
            if result.returncode == 0:
                # Encontrar o arquivo baixado
                lines = result.stdout.split("\n")
                filename = ""
                for line in lines:
                    if "Destination:" in line:
                        filename = line.split("Destination:")[-1].strip()
                    elif "[download]" in line and "has already been downloaded" in line:
                        filename = line.split("[download]")[1].split("has already")[0].strip()
                    elif "Merging formats" in line or "[Merger]" in line:
                        for l2 in lines:
                            if "Destination:" in l2:
                                filename = l2.split("Destination:")[-1].strip()
                _aios_tasks[task_id] = {
                    "status": "done",
                    "result": f"Video baixado com sucesso!\nArquivo: {filename or 'ver pasta Downloads/aios_media'}",
                }
            else:
                _aios_tasks[task_id] = {"status": "error", "error": result.stderr[:500] or "Erro no download"}
        except subprocess.TimeoutExpired:
            _aios_tasks[task_id] = {"status": "error", "error": "Timeout: download demorou mais de 5 minutos"}
        except FileNotFoundError:
            _aios_tasks[task_id] = {"status": "error", "error": "yt-dlp nao encontrado. Execute: pip install yt-dlp"}
        except Exception as e:
            _aios_tasks[task_id] = {"status": "error", "error": str(e)}

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "started", "task_id": task_id, "message": f"Download iniciado: {url}"}


def _tool_tiktok_cut_clip(args):
    """Corta trecho de video com ffmpeg (async)."""
    input_file = args.get("input_file", "").strip()
    start = args.get("start", "00:00:00")
    duration = args.get("duration", "30")

    # Procurar arquivo na pasta de midias
    full_path = input_file
    if not os.path.isabs(input_file):
        full_path = os.path.join(AIOS_OUTPUT_DIR, input_file)
    if not os.path.exists(full_path):
        return {"status": "error", "error": f"Arquivo nao encontrado: {input_file}"}

    task_id = str(uuid.uuid4())[:8]
    _aios_tasks[task_id] = {"status": "running", "progress": "Cortando video...", "result": None}

    def _run():
        try:
            base = Path(full_path).stem
            ext = Path(full_path).suffix
            output = os.path.join(AIOS_OUTPUT_DIR, f"{base}_clip_{start.replace(':','')}{ext}")
            result = subprocess.run(
                ["ffmpeg", "-y", "-i", full_path, "-ss", start, "-t", str(duration), "-c", "copy", output],
                capture_output=True, text=True, timeout=120, encoding="utf-8", errors="replace"
            )
            if result.returncode == 0:
                _aios_tasks[task_id] = {"status": "done", "result": f"Clip criado: {output}"}
            else:
                _aios_tasks[task_id] = {"status": "error", "error": result.stderr[:500]}
        except FileNotFoundError:
            _aios_tasks[task_id] = {"status": "error", "error": "ffmpeg nao encontrado. Instale ffmpeg no sistema."}
        except Exception as e:
            _aios_tasks[task_id] = {"status": "error", "error": str(e)}

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "started", "task_id": task_id, "message": f"Cortando: {start} ({duration}s)"}


def _tool_tiktok_list_downloads(args):
    """Lista videos baixados."""
    files = []
    if os.path.exists(AIOS_OUTPUT_DIR):
        for f in sorted(os.listdir(AIOS_OUTPUT_DIR), key=lambda x: os.path.getmtime(os.path.join(AIOS_OUTPUT_DIR, x)), reverse=True):
            fp = os.path.join(AIOS_OUTPUT_DIR, f)
            if os.path.isfile(fp):
                size_mb = os.path.getsize(fp) / (1024 * 1024)
                files.append(f"{f} ({size_mb:.1f} MB)")
    if not files:
        return {"status": "done", "result": "Nenhum arquivo encontrado em " + AIOS_OUTPUT_DIR}
    return {"status": "done", "result": f"Arquivos em {AIOS_OUTPUT_DIR}:\n" + "\n".join(files[:20])}


def _tool_editor_resize(args):
    """Redimensiona imagem com Pillow."""
    try:
        from PIL import Image
    except ImportError:
        return {"status": "error", "error": "Pillow nao instalado. Execute: pip install Pillow"}

    input_file = args.get("input_file", "").strip()
    width = int(args.get("width", 800))
    height = int(args.get("height", 800))

    full_path = input_file if os.path.isabs(input_file) else os.path.join(AIOS_OUTPUT_DIR, input_file)
    if not os.path.exists(full_path):
        return {"status": "error", "error": f"Arquivo nao encontrado: {input_file}"}

    try:
        img = Image.open(full_path)
        img_resized = img.resize((width, height), Image.LANCZOS)
        base = Path(full_path).stem
        ext = Path(full_path).suffix or ".png"
        output = os.path.join(AIOS_OUTPUT_DIR, f"{base}_{width}x{height}{ext}")
        img_resized.save(output, quality=95)
        return {"status": "done", "result": f"Imagem redimensionada: {output} ({width}x{height})"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _tool_editor_watermark(args):
    """Adiciona marca d'agua na imagem."""
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        return {"status": "error", "error": "Pillow nao instalado"}

    input_file = args.get("input_file", "").strip()
    text = args.get("text", "Beka MKT")

    full_path = input_file if os.path.isabs(input_file) else os.path.join(AIOS_OUTPUT_DIR, input_file)
    if not os.path.exists(full_path):
        return {"status": "error", "error": f"Arquivo nao encontrado: {input_file}"}

    try:
        img = Image.open(full_path).convert("RGBA")
        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        font_size = max(20, img.width // 20)
        try:
            font = ImageFont.truetype("arial.ttf", font_size)
        except Exception:
            font = ImageFont.load_default()
        bbox = draw.textbbox((0, 0), text, font=font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        x = img.width - tw - 20
        y = img.height - th - 20
        draw.text((x, y), text, fill=(255, 255, 255, 128), font=font)
        result = Image.alpha_composite(img, overlay).convert("RGB")
        base = Path(full_path).stem
        ext = Path(full_path).suffix or ".png"
        output = os.path.join(AIOS_OUTPUT_DIR, f"{base}_wm{ext}")
        result.save(output, quality=95)
        return {"status": "done", "result": f"Marca d'agua adicionada: {output}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _tool_editor_crop(args):
    """Recorta imagem."""
    try:
        from PIL import Image
    except ImportError:
        return {"status": "error", "error": "Pillow nao instalado"}

    input_file = args.get("input_file", "").strip()
    x = int(args.get("x", 0))
    y = int(args.get("y", 0))
    w = int(args.get("w", 800))
    h = int(args.get("h", 800))

    full_path = input_file if os.path.isabs(input_file) else os.path.join(AIOS_OUTPUT_DIR, input_file)
    if not os.path.exists(full_path):
        return {"status": "error", "error": f"Arquivo nao encontrado: {input_file}"}

    try:
        img = Image.open(full_path)
        cropped = img.crop((x, y, x + w, y + h))
        base = Path(full_path).stem
        ext = Path(full_path).suffix or ".png"
        output = os.path.join(AIOS_OUTPUT_DIR, f"{base}_crop{ext}")
        cropped.save(output, quality=95)
        return {"status": "done", "result": f"Imagem recortada: {output} ({w}x{h})"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _tool_editor_convert(args):
    """Converte formato de imagem."""
    try:
        from PIL import Image
    except ImportError:
        return {"status": "error", "error": "Pillow nao instalado"}

    input_file = args.get("input_file", "").strip()
    output_format = args.get("output_format", "png").lower().strip(".")

    full_path = input_file if os.path.isabs(input_file) else os.path.join(AIOS_OUTPUT_DIR, input_file)
    if not os.path.exists(full_path):
        return {"status": "error", "error": f"Arquivo nao encontrado: {input_file}"}

    try:
        img = Image.open(full_path)
        if output_format in ("jpg", "jpeg") and img.mode == "RGBA":
            img = img.convert("RGB")
        base = Path(full_path).stem
        output = os.path.join(AIOS_OUTPUT_DIR, f"{base}.{output_format}")
        img.save(output, quality=95)
        return {"status": "done", "result": f"Convertido: {output}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _tool_editor_thumbnail(args):
    """Cria thumbnail 320x320."""
    try:
        from PIL import Image
    except ImportError:
        return {"status": "error", "error": "Pillow nao instalado"}

    input_file = args.get("input_file", "").strip()
    full_path = input_file if os.path.isabs(input_file) else os.path.join(AIOS_OUTPUT_DIR, input_file)
    if not os.path.exists(full_path):
        return {"status": "error", "error": f"Arquivo nao encontrado: {input_file}"}

    try:
        img = Image.open(full_path)
        img.thumbnail((320, 320), Image.LANCZOS)
        base = Path(full_path).stem
        output = os.path.join(AIOS_OUTPUT_DIR, f"{base}_thumb.png")
        img.save(output, quality=90)
        return {"status": "done", "result": f"Thumbnail criado: {output} ({img.width}x{img.height})"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _tool_editor_list_files(args):
    """Lista arquivos de midia no diretorio de output."""
    return _tool_tiktok_list_downloads(args)


def _tool_cursor_open_file(args):
    """Abre arquivo no Cursor IDE."""
    file_path = args.get("file_path", "").strip()
    if not file_path:
        return {"status": "error", "error": "Caminho do arquivo nao informado"}
    try:
        subprocess.Popen(["cursor", file_path], shell=True)
        return {"status": "done", "result": f"Abrindo no Cursor: {file_path}"}
    except Exception as e:
        return {"status": "error", "error": f"Erro ao abrir Cursor: {e}"}


def _tool_cursor_open_project(args):
    """Abre pasta/projeto no Cursor IDE."""
    project_path = args.get("project_path", "").strip()
    if not project_path:
        return {"status": "error", "error": "Caminho do projeto nao informado"}
    try:
        subprocess.Popen(["cursor", project_path], shell=True)
        return {"status": "done", "result": f"Abrindo projeto no Cursor: {project_path}"}
    except Exception as e:
        return {"status": "error", "error": f"Erro ao abrir Cursor: {e}"}


def _tool_cursor_run_command(args):
    """Executa comando no terminal (async)."""
    command = args.get("command", "").strip()
    if not command:
        return {"status": "error", "error": "Comando nao informado"}

    # Bloquear comandos perigosos
    blocked = ["rm -rf /", "format ", "del /s /q", "shutdown", "taskkill"]
    for b in blocked:
        if b in command.lower():
            return {"status": "error", "error": f"Comando bloqueado por seguranca: {command}"}

    task_id = str(uuid.uuid4())[:8]
    _aios_tasks[task_id] = {"status": "running", "progress": f"Executando: {command}", "result": None}

    def _run():
        try:
            result = subprocess.run(
                command, shell=True, capture_output=True, text=True,
                timeout=60, encoding="utf-8", errors="replace"
            )
            output = result.stdout[:2000]
            if result.stderr:
                output += "\n[STDERR] " + result.stderr[:500]
            _aios_tasks[task_id] = {
                "status": "done" if result.returncode == 0 else "error",
                "result": output or "(sem output)",
                "error": result.stderr[:500] if result.returncode != 0 else None,
            }
        except subprocess.TimeoutExpired:
            _aios_tasks[task_id] = {"status": "error", "error": "Timeout: comando demorou mais de 60s"}
        except Exception as e:
            _aios_tasks[task_id] = {"status": "error", "error": str(e)}

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "started", "task_id": task_id, "message": f"Executando: {command}"}


def _tool_shopee_check_orders(args):
    """Placeholder - verificar pedidos atrasados Shopee."""
    return {"status": "done", "result": "Funcao de verificacao Shopee sera implementada com Playwright.\nPor enquanto, acesse: https://seller.shopee.com.br/portal/sale/order"}


def _tool_shopee_check_returns(args):
    """Placeholder - verificar devolucoes Shopee."""
    return {"status": "done", "result": "Funcao de devolucoes Shopee sera implementada com Playwright.\nAcesse: https://seller.shopee.com.br/portal/sale/return"}


def _tool_shopee_respond_chat(args):
    """Placeholder - responder chat Shopee."""
    return {"status": "done", "result": "Funcao de resposta automatica sera implementada com Playwright.\nAcesse: https://seller.shopee.com.br/portal/chatroom"}


def _tool_duoke_send_message(args):
    """Responder mensagem via Duoke Bridge API (porta 8901)."""
    import requests as req
    message_id = args.get("message_id", "").strip()
    mensagem = args.get("mensagem", "").strip()

    try:
        r = req.get("http://localhost:8901/api/health", timeout=3)
        if r.status_code != 200:
            return {"status": "error", "error": "Duoke Bridge offline. Inicie o servidor: cd duoke-bridge && python main.py"}
    except Exception:
        return {"status": "error", "error": "Duoke Bridge nao esta rodando na porta 8901.\nInicie: cd \"C:/Users/Micro/Desktop/Beka MKT/duoke-bridge\" && python main.py"}

    if message_id and mensagem:
        try:
            r = req.post("http://localhost:8901/api/messages/respond", json={"message_id": message_id, "text": mensagem}, timeout=60)
            if r.status_code == 200:
                return {"status": "done", "result": f"Mensagem respondida: {r.json()}"}
            return {"status": "error", "error": f"Erro {r.status_code}: {r.text[:300]}"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # Se nao tem message_id, responder todas pendentes
    task_id = str(uuid.uuid4())[:8]
    _aios_tasks[task_id] = {"status": "running", "progress": "Respondendo mensagens via Duoke...", "result": None}

    def _run():
        try:
            r = req.post("http://localhost:8901/api/messages/respond-all", json={
                "max_messages": 10, "auto_send": True, "dry_run": False
            }, timeout=600)
            if r.status_code == 200:
                _aios_tasks[task_id] = {"status": "done", "result": f"Respostas enviadas:\n{json.dumps(r.json(), indent=2, ensure_ascii=False)[:1000]}"}
            else:
                _aios_tasks[task_id] = {"status": "error", "error": f"Erro {r.status_code}: {r.text[:300]}"}
        except Exception as e:
            _aios_tasks[task_id] = {"status": "error", "error": str(e)}

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "started", "task_id": task_id, "message": "Respondendo todas as mensagens pendentes via Duoke..."}


def _tool_duoke_get_history(args):
    """Buscar mensagens nao lidas via Duoke Bridge API."""
    import requests as req
    try:
        r = req.get("http://localhost:8901/api/health", timeout=3)
        if r.status_code != 200:
            return {"status": "error", "error": "Duoke Bridge offline"}
        health = r.json()
    except Exception:
        return {"status": "error", "error": "Duoke Bridge nao esta rodando na porta 8901.\nInicie: cd \"C:/Users/Micro/Desktop/Beka MKT/duoke-bridge\" && python main.py"}

    try:
        r = req.get("http://localhost:8901/api/messages/unread", timeout=30)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and len(data) > 0:
                lines = [f"Logado: {health.get('logged_in', '?')}", f"Mensagens nao lidas: {len(data)}", ""]
                for msg in data[:10]:
                    lines.append(f"- [{msg.get('id', '?')}] {msg.get('customer_name', '?')}: {msg.get('last_message', '')[:100]}")
                return {"status": "done", "result": "\n".join(lines)}
            return {"status": "done", "result": f"Logado: {health.get('logged_in', '?')}\nNenhuma mensagem nao lida."}
        return {"status": "error", "error": f"Erro {r.status_code}: {r.text[:300]}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def _tool_duoke_stats(args):
    """Obter estatisticas de atendimento via Duoke Bridge."""
    import requests as req
    try:
        r = req.get("http://localhost:8901/api/stats", timeout=10)
        if r.status_code == 200:
            return {"status": "done", "result": json.dumps(r.json(), indent=2, ensure_ascii=False)[:1000]}
        return {"status": "error", "error": f"Erro {r.status_code}"}
    except Exception:
        return {"status": "error", "error": "Duoke Bridge offline (porta 8901)"}


def _tool_duoke_health(args):
    """Verificar status da Duoke Bridge."""
    import requests as req
    try:
        r = req.get("http://localhost:8901/api/health", timeout=3)
        if r.status_code == 200:
            data = r.json()
            return {"status": "done", "result": f"Duoke Bridge: ONLINE\nLogado: {data.get('logged_in', '?')}\nURL: {data.get('url', '?')}\nVersao: {data.get('version', '?')}"}
        return {"status": "error", "error": f"Duoke Bridge retornou {r.status_code}"}
    except Exception:
        return {"status": "error", "error": "Duoke Bridge OFFLINE.\nInicie: cd \"C:/Users/Micro/Desktop/Beka MKT/duoke-bridge\" && python main.py"}


def _tool_duoke_get_knowledge_base(args):
    """Extrair base de conhecimento Q&A do chatbot de IA do Duoke."""
    import requests as req
    try:
        r = req.get("http://localhost:8901/api/knowledge/extract", timeout=120)
        if r.status_code == 200:
            data = r.json()
            items = data.get("knowledge", [])
            if not items:
                return {"status": "done", "result": "Nenhum Q&A encontrado na base de conhecimento do Duoke."}
            # Formatar resumo
            lines = [f"Base de conhecimento: {len(items)} Q&As encontradas\n"]
            for i, item in enumerate(items[:30], 1):
                product = item.get("product", "?")
                question = item.get("question", "")
                response = item.get("response", "")
                if question and response:
                    lines.append(f"{i}. [{product}] P: {question[:80]}")
                    lines.append(f"   R: {response[:120]}")
                elif item.get("raw_text"):
                    lines.append(f"{i}. [{product}] {item['raw_text'][:150]}")
            if len(items) > 30:
                lines.append(f"\n... e mais {len(items) - 30} Q&As")
            return {"status": "done", "result": "\n".join(lines)}
        return {"status": "error", "error": f"Erro {r.status_code}"}
    except req.exceptions.Timeout:
        return {"status": "error", "error": "Timeout ao extrair knowledge base (pode demorar alguns minutos). Tente novamente."}
    except Exception:
        return {"status": "error", "error": "Duoke Bridge offline (porta 8901).\nInicie: cd \"C:/Users/Micro/Desktop/Beka MKT/duoke-bridge\" && python main.py"}


def _tool_videoai_generate_prompt(args):
    """Gera prompt otimizado para video AI (usa LLM)."""
    descricao = args.get("descricao", "")
    return {"status": "needs_llm", "result": descricao, "meta": "generate_video_prompt"}


def _tool_videoai_list_videos(args):
    """Lista videos na pasta de output."""
    video_exts = {".mp4", ".avi", ".mov", ".mkv", ".webm"}
    files = []
    if os.path.exists(AIOS_OUTPUT_DIR):
        for f in sorted(os.listdir(AIOS_OUTPUT_DIR), key=lambda x: os.path.getmtime(os.path.join(AIOS_OUTPUT_DIR, x)), reverse=True):
            if Path(f).suffix.lower() in video_exts:
                fp = os.path.join(AIOS_OUTPUT_DIR, f)
                size_mb = os.path.getsize(fp) / (1024 * 1024)
                files.append(f"{f} ({size_mb:.1f} MB)")
    if not files:
        return {"status": "done", "result": "Nenhum video encontrado em " + AIOS_OUTPUT_DIR}
    return {"status": "done", "result": "Videos:\n" + "\n".join(files[:20])}


# ----------------------------------------------------------------
# Tool Registry - mapeia nome -> funcao
# ----------------------------------------------------------------

TOOL_REGISTRY = {
    "tiktok_download": _tool_tiktok_download,
    "tiktok_cut_clip": _tool_tiktok_cut_clip,
    "tiktok_list_downloads": _tool_tiktok_list_downloads,
    "editor_resize": _tool_editor_resize,
    "editor_watermark": _tool_editor_watermark,
    "editor_crop": _tool_editor_crop,
    "editor_convert": _tool_editor_convert,
    "editor_thumbnail": _tool_editor_thumbnail,
    "editor_list_files": _tool_editor_list_files,
    "cursor_open_file": _tool_cursor_open_file,
    "cursor_open_project": _tool_cursor_open_project,
    "cursor_run_command": _tool_cursor_run_command,
    "shopee_check_orders": _tool_shopee_check_orders,
    "shopee_check_returns": _tool_shopee_check_returns,
    "shopee_respond_chat": _tool_shopee_respond_chat,
    "duoke_send_message": _tool_duoke_send_message,
    "duoke_get_history": _tool_duoke_get_history,
    "duoke_stats": _tool_duoke_stats,
    "duoke_health": _tool_duoke_health,
    "duoke_get_knowledge_base": _tool_duoke_get_knowledge_base,
    "videoai_generate_prompt": _tool_videoai_generate_prompt,
    "videoai_list_videos": _tool_videoai_list_videos,
}


# ================================================================
# LLM CALL FUNCTIONS
# ================================================================

def _aios_get_config(user_id):
    """Retorna AIOSConfig do usuario ou cria default."""
    cfg = AIOSConfig.query.filter_by(user_id=user_id).first()
    if not cfg:
        cfg = AIOSConfig(user_id=user_id)
        db.session.add(cfg)
        db.session.commit()
    return cfg


def _aios_call_venice(messages, max_tokens=2000, temperature=0.7):
    """Chama Venice AI direto (para agente adult). Bypass do fallback chain."""
    import requests as req
    venice_key = "VENICE-INFERENCE-KEY-zyGu9lHlEfU0GCXSwVc2SUO_UIYhPtwepXgMOpU9cP"
    try:
        r = req.post("https://api.venice.ai/api/v1/chat/completions", json={
            "model": "llama-3.3-70b",
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "venice_parameters": {"include_venice_system_prompt": False},
        }, headers={
            "Authorization": f"Bearer {venice_key}",
            "Content-Type": "application/json",
        }, timeout=120)
        if r.status_code == 200:
            data = r.json()
            return {"sucesso": True, "resposta": data["choices"][0]["message"]["content"], "modelo": "Llama 3.3 70B (Venice uncensored)"}
        return {"sucesso": False, "resposta": f"Venice API erro: {r.status_code}", "modelo": None}
    except Exception as e:
        return {"sucesso": False, "resposta": f"Venice API falhou: {e}", "modelo": None}


def _build_user_message_with_image(text, imagem):
    """Constroi mensagem multimodal (texto + imagem) para APIs que suportam vision.
    Para APIs que nao suportam, retorna apenas texto com indicacao de imagem.
    imagem: dict com {base64: "data:image/...;base64,...", name: "...", type: "..."}
    """
    if not imagem:
        return text

    # Extrair base64 puro (remover prefixo data:image/...;base64,)
    b64_data = imagem.get("base64", "")
    media_type = imagem.get("type", "image/png")
    if ";base64," in b64_data:
        b64_data = b64_data.split(";base64,")[1]
        # Extrair media_type do prefixo
        prefix = imagem["base64"].split(";base64,")[0]
        if prefix.startswith("data:"):
            media_type = prefix[5:]

    return {
        "_multimodal": True,
        "text": text,
        "image_b64": b64_data,
        "media_type": media_type,
        "image_name": imagem.get("name", "imagem.png"),
    }


def _format_messages_for_api(messages, api_type="openai"):
    """Converte mensagens com _multimodal para o formato de cada API.
    api_type: 'openai' (OpenAI-compatible), 'anthropic' (Messages API)
    """
    formatted = []
    for msg in messages:
        content = msg.get("content", "")
        if isinstance(content, dict) and content.get("_multimodal"):
            if api_type == "anthropic":
                # Anthropic Messages API format
                formatted.append({
                    "role": msg["role"],
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": content["media_type"],
                                "data": content["image_b64"],
                            }
                        },
                        {"type": "text", "text": content["text"]},
                    ]
                })
            elif api_type == "openai":
                # OpenAI / compatible format
                formatted.append({
                    "role": msg["role"],
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{content['media_type']};base64,{content['image_b64']}"
                            }
                        },
                        {"type": "text", "text": content["text"]},
                    ]
                })
            else:
                # Fallback: texto simples com indicacao de imagem
                formatted.append({
                    "role": msg["role"],
                    "content": f"{content['text']}\n[Imagem anexada: {content['image_name']}]"
                })
        else:
            formatted.append(msg)
    return formatted


def _aios_call_llm(api_key, modelo, messages, max_tokens=2000, temperature=0.3):
    """Chama LLM via API direta. Fallback: Claude Max (proxy) > Anthropic API > Groq > Venice > OpenAI > Ollama."""
    import requests as req

    # Pre-formatar mensagens para cada tipo de API
    msgs_openai = _format_messages_for_api(messages, "openai")
    msgs_anthropic = _format_messages_for_api(messages, "anthropic")
    msgs_text = _format_messages_for_api(messages, "text")

    # --- Tentar Claude Max (proxy local na porta 3456) ---
    try:
        r = req.post("http://localhost:3456/v1/chat/completions", json={
            "model": modelo if modelo.startswith("claude") else "claude-sonnet-4-20250514",
            "messages": msgs_openai,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }, headers={
            "Authorization": "Bearer not-needed",
            "Content-Type": "application/json",
        }, timeout=(2, 120))
        if r.status_code == 200:
            data = r.json()
            resp_text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if resp_text:
                return {"sucesso": True, "resposta": resp_text, "modelo": f"{modelo} (Max)"}
        else:
            print(f"[AIOS] Claude Max proxy erro {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[AIOS] Claude Max proxy indisponivel: {e}")

    # --- Tentar Anthropic API direta ---
    if api_key and modelo.startswith("claude"):
        try:
            non_system = [m for m in msgs_anthropic if m["role"] != "system"]
            system_content = next((m["content"] for m in msgs_anthropic if m["role"] == "system"), "")
            r = req.post("https://api.anthropic.com/v1/messages", json={
                "model": modelo,
                "max_tokens": max_tokens,
                "messages": non_system,
                "system": system_content if isinstance(system_content, str) else "",
                "temperature": temperature,
            }, headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }, timeout=120)
            if r.status_code == 200:
                data = r.json()
                return {"sucesso": True, "resposta": data["content"][0]["text"], "modelo": modelo}
            else:
                print(f"[AIOS] Anthropic erro {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"[AIOS] Anthropic falhou: {e}")

    # --- Tentar Groq (Kimi K2) --- (sem suporte a imagem, usar texto)
    try:
        groq_key = os.environ.get("GROQ_API_KEY", "")
        r = req.post("https://api.groq.com/openai/v1/chat/completions", json={
            "model": "moonshotai/kimi-k2-instruct",
            "messages": msgs_text,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }, headers={
            "Authorization": f"Bearer {groq_key}",
            "Content-Type": "application/json",
        }, timeout=60)
        if r.status_code == 200:
            data = r.json()
            resp_text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if resp_text:
                return {"sucesso": True, "resposta": resp_text, "modelo": "Kimi K2 (Groq)"}
    except Exception as e:
        print(f"[AIOS] Groq/Kimi falhou: {e}")

    # --- Tentar Venice AI --- (sem suporte a imagem, usar texto)
    try:
        venice_key = "VENICE-INFERENCE-KEY-zyGu9lHlEfU0GCXSwVc2SUO_UIYhPtwepXgMOpU9cP"
        r = req.post("https://api.venice.ai/api/v1/chat/completions", json={
            "model": "llama-3.3-70b",
            "messages": msgs_text,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }, headers={
            "Authorization": f"Bearer {venice_key}",
            "Content-Type": "application/json",
        }, timeout=60)
        if r.status_code == 200:
            data = r.json()
            resp_text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if resp_text:
                return {"sucesso": True, "resposta": resp_text, "modelo": "Llama 3.3 70B (Venice)"}
    except Exception as e:
        print(f"[AIOS] Venice falhou: {e}")

    # --- Tentar OpenAI --- (suporta imagem via openai format)
    try:
        openai_key = _aios_openai_key_cache.get("key", "")
        if openai_key:
            r = req.post("https://api.openai.com/v1/chat/completions", json={
                "model": "gpt-4o",
                "messages": msgs_openai,
                "max_tokens": max_tokens,
                "temperature": temperature,
            }, headers={
                "Authorization": f"Bearer {openai_key}",
                "Content-Type": "application/json",
            }, timeout=120)
            if r.status_code == 200:
                data = r.json()
                return {"sucesso": True, "resposta": data["choices"][0]["message"]["content"], "modelo": "gpt-4o"}
    except Exception as e:
        print(f"[AIOS] OpenAI falhou: {e}")

    # --- Fallback: Ollama local ---
    try:
        ollama_model = "qwen2.5-coder:latest"
        try:
            tags_r = req.get("http://localhost:11434/api/tags", timeout=5)
            if tags_r.status_code == 200:
                models = [m["name"] for m in tags_r.json().get("models", [])]
                for preferred in ["qwen2.5-coder:latest", "dolphin-mistral:latest"]:
                    if preferred in models:
                        ollama_model = preferred
                        break
                else:
                    if models:
                        ollama_model = models[0]
        except Exception:
            pass
        r = req.post("http://localhost:11434/api/chat", json={
            "model": ollama_model,
            "messages": messages,
            "stream": False,
            "options": {"temperature": temperature, "num_predict": max_tokens}
        }, timeout=120)
        if r.status_code == 200:
            data = r.json()
            return {"sucesso": True, "resposta": data.get("message", {}).get("content", ""), "modelo": f"{ollama_model} (local)"}
    except Exception as e:
        print(f"[AIOS] Ollama falhou: {e}")

    return {"sucesso": False, "resposta": "Nenhum modelo LLM disponivel. Configure uma API key ou instale Ollama.", "modelo": None}


# ================================================================
# TOOL_CALL PARSER
# ================================================================

def _parse_tool_calls(text):
    """Extrai [TOOL_CALL]...[/TOOL_CALL] do texto do LLM."""
    pattern = r'\[TOOL_CALL\](.*?)\[/TOOL_CALL\]'
    matches = re.findall(pattern, text, re.DOTALL)
    calls = []
    for match in matches:
        try:
            data = json.loads(match.strip())
            calls.append(data)
        except json.JSONDecodeError:
            pass
    # Texto limpo (sem os blocos TOOL_CALL)
    clean_text = re.sub(pattern, '', text, flags=re.DOTALL).strip()
    return calls, clean_text


def _execute_tool(tool_name, tool_args, agent_id):
    """Executa uma tool e retorna resultado."""
    # Validar que a tool pertence ao agente
    agent = AIOS_AGENTS.get(agent_id, {})
    agent_tools = [t["name"] for t in agent.get("tools", [])]
    if tool_name not in agent_tools:
        return {"status": "error", "error": f"Tool '{tool_name}' nao pertence ao agente '{agent_id}'"}

    func = TOOL_REGISTRY.get(tool_name)
    if not func:
        return {"status": "error", "error": f"Tool '{tool_name}' nao implementada"}

    return func(tool_args or {})


# ================================================================
# ROTAS
# ================================================================

@aios_bp.route('/api/aios/status', methods=['GET'])
@jwt_required()
def api_aios_status():
    """Status do AIOS e agentes disponiveis."""
    user_id = int(get_jwt_identity())
    cfg = _aios_get_config(user_id)

    backends = []
    import requests as req

    # Testar Claude Max proxy
    try:
        r = req.get("http://localhost:3456/v1/models", timeout=1)
        if r.status_code == 200:
            backends.append({"nome": "Claude Max (Proxy)", "modelo": cfg.modelo_principal, "status": "online"})
    except Exception:
        pass
    if cfg.get_anthropic_key():
        backends.append({"nome": "Anthropic (API)", "modelo": cfg.modelo_principal, "status": "configurado"})
    if cfg.get_openai_key():
        backends.append({"nome": "OpenAI (GPT)", "modelo": "gpt-4o", "status": "configurado"})
    backends.append({"nome": "Groq (Kimi K2)", "modelo": "moonshotai/kimi-k2-instruct", "status": "configurado"})
    backends.append({"nome": "Venice AI", "modelo": "llama-3.3-70b", "status": "configurado"})

    ollama_ok = False
    try:
        r = req.get(f"{cfg.ollama_url}/api/tags", timeout=1)
        ollama_ok = r.status_code == 200
        if ollama_ok:
            models = [m["name"] for m in r.json().get("models", [])]
            backends.append({"nome": "Ollama (Local)", "modelo": ", ".join(models[:3]) or "nenhum modelo", "status": "online"})
    except Exception:
        pass

    agents_list = []
    for key, agent in AIOS_AGENTS.items():
        agents_list.append({
            "id": key,
            "nome": agent["nome"],
            "descricao": agent["descricao"],
            "icon": agent["icon"],
            "color": agent["color"],
            "tools": agent.get("tools", []),
        })

    return jsonify({
        "ativo": cfg.ativo,
        "backends": backends,
        "agentes": agents_list,
        "config": cfg.to_dict(),
        "tem_backend": len(backends) > 0,
        "ollama_online": ollama_ok,
    })


@aios_bp.route('/api/aios/chat', methods=['POST'])
@jwt_required()
def api_aios_chat():
    """Chat com um agente especialista. Suporta tool calling."""
    user_id = int(get_jwt_identity())
    cfg = _aios_get_config(user_id)
    data = request.get_json() or {}

    agent_id = data.get("agent_id", "shopee")
    mensagem = data.get("mensagem", "").strip()
    historico = data.get("historico", [])
    imagem = data.get("imagem")  # {base64: "data:image/...;base64,...", name: "...", type: "..."}

    if not mensagem and not imagem:
        return jsonify({"sucesso": False, "mensagem": "Mensagem vazia"}), 400
    if not mensagem and imagem:
        mensagem = "Analise esta imagem."

    agent = AIOS_AGENTS.get(agent_id)
    if not agent:
        return jsonify({"sucesso": False, "mensagem": f"Agente '{agent_id}' nao encontrado"}), 404

    api_key = cfg.get_anthropic_key()
    _aios_openai_key_cache["key"] = cfg.get_openai_key()

    # Construir conteudo multimodal se houver imagem
    user_content = _build_user_message_with_image(mensagem, imagem) if imagem else mensagem

    # ============================================================
    # MAESTRO ROUTER: analisa mensagem e delega ao agente correto
    # ============================================================
    if agent.get("is_router"):
        # Passo 1: Perguntar ao LLM qual agente usar
        route_messages = [{"role": "system", "content": agent["system_prompt"]}]
        # Incluir historico recente para contexto de roteamento
        for msg in historico[-6:]:
            route_messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
        # Router nao precisa de imagem, so texto para decidir roteamento
        route_messages.append({"role": "user", "content": mensagem})

        route_result = _aios_call_llm(api_key, cfg.modelo_principal, route_messages)

        routed_agent_id = "demo_agent"
        route_explanation = ""

        if route_result["sucesso"]:
            route_text = route_result["resposta"]
            # Extrair [ROUTE:agent_id]
            route_match = re.search(r'\[ROUTE:(\w+)\]', route_text)
            if route_match:
                candidate = route_match.group(1)
                if candidate in AIOS_AGENTS and candidate != "maestro":
                    routed_agent_id = candidate
            # Extrair explicacao (tudo apos o [ROUTE:...])
            route_explanation = re.sub(r'\[ROUTE:\w+\]\s*', '', route_text).strip()

        # Passo 2: Chamar o agente escolhido com a mensagem original
        target_agent = AIOS_AGENTS.get(routed_agent_id, AIOS_AGENTS.get("demo_agent"))
        target_id = routed_agent_id

        target_messages = [{"role": "system", "content": target_agent["system_prompt"]}]
        for msg in historico[-20:]:
            target_messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
        target_messages.append({"role": "user", "content": user_content})

        # Venice bypass se agente target for adult
        if target_agent.get("llm_override") == "venice":
            resultado = _aios_call_venice(target_messages)
        else:
            resultado = _aios_call_llm(api_key, cfg.modelo_principal, target_messages)

        actions = []
        resposta = resultado["resposta"]

        if resultado["sucesso"]:
            tool_calls, clean_text = _parse_tool_calls(resposta)
            if tool_calls:
                for tc in tool_calls:
                    tool_name = tc.get("tool", "")
                    tool_args = tc.get("args", {})
                    tool_result = _execute_tool(tool_name, tool_args, target_id)
                    actions.append({"tool": tool_name, "args": tool_args, "result": tool_result})
                if clean_text:
                    resposta = clean_text
                else:
                    parts = []
                    for a in actions:
                        r = a["result"]
                        if r.get("status") == "done": parts.append(r.get("result", "OK"))
                        elif r.get("status") == "started": parts.append(r.get("message", "Tarefa iniciada"))
                        elif r.get("status") == "error": parts.append(f"Erro: {r.get('error', 'desconhecido')}")
                    resposta = "\n".join(parts) if parts else "Acoes executadas."

        # Prefixar com info de roteamento
        route_header = f"🎯 **{target_agent['nome']}**"
        if route_explanation:
            route_header += f" — {route_explanation}"
        resposta = route_header + "\n\n" + resposta

        return jsonify({
            "sucesso": resultado["sucesso"],
            "resposta": resposta,
            "modelo": resultado["modelo"],
            "agente": f"Maestro → {target_agent['nome']}",
            "routed_to": target_id,
            "actions": actions,
        })

    # ============================================================
    # AGENTE NORMAL (nao-router)
    # ============================================================
    messages = [{"role": "system", "content": agent["system_prompt"]}]
    for msg in historico[-20:]:
        messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
    messages.append({"role": "user", "content": user_content})

    # Venice bypass para agente adult
    if agent.get("llm_override") == "venice":
        resultado = _aios_call_venice(messages)
        return jsonify({
            "sucesso": resultado["sucesso"],
            "resposta": resultado["resposta"],
            "modelo": resultado["modelo"],
            "agente": agent["nome"],
            "actions": [],
        })

    # Chamar LLM normal
    resultado = _aios_call_llm(api_key, cfg.modelo_principal, messages)

    actions = []
    resposta = resultado["resposta"]

    if resultado["sucesso"]:
        # Parsear TOOL_CALLs da resposta
        tool_calls, clean_text = _parse_tool_calls(resposta)
        if tool_calls:
            for tc in tool_calls:
                tool_name = tc.get("tool", "")
                tool_args = tc.get("args", {})
                tool_result = _execute_tool(tool_name, tool_args, agent_id)
                actions.append({
                    "tool": tool_name,
                    "args": tool_args,
                    "result": tool_result,
                })
            # Se tem texto limpo alem dos tool calls, usar ele
            if clean_text:
                resposta = clean_text
            else:
                # Montar resposta baseada nos resultados
                parts = []
                for a in actions:
                    r = a["result"]
                    if r.get("status") == "done":
                        parts.append(r.get("result", "OK"))
                    elif r.get("status") == "started":
                        parts.append(r.get("message", "Tarefa iniciada"))
                    elif r.get("status") == "error":
                        parts.append(f"Erro: {r.get('error', 'desconhecido')}")
                resposta = "\n".join(parts) if parts else "Acoes executadas."

    return jsonify({
        "sucesso": resultado["sucesso"],
        "resposta": resposta,
        "modelo": resultado["modelo"],
        "agente": agent["nome"],
        "actions": actions,
    })


@aios_bp.route('/api/aios/execute', methods=['POST'])
@jwt_required()
def api_aios_execute():
    """Execucao direta de tool (botoes de acao rapida)."""
    data = request.get_json() or {}
    agent_id = data.get("agent_id", "")
    tool_name = data.get("tool_name", "")
    tool_args = data.get("args", {})

    if not tool_name:
        return jsonify({"sucesso": False, "error": "tool_name nao informado"}), 400

    result = _execute_tool(tool_name, tool_args, agent_id)
    return jsonify({"sucesso": True, "result": result})


@aios_bp.route('/api/aios/task/<task_id>', methods=['GET'])
@jwt_required()
def api_aios_task(task_id):
    """Polling de task async."""
    task = _aios_tasks.get(task_id)
    if not task:
        return jsonify({"status": "not_found", "error": "Task nao encontrada"}), 404
    return jsonify(task)


@aios_bp.route('/api/aios/config', methods=['GET'])
@jwt_required()
def api_aios_config_get():
    """Retorna configuracao AIOS do usuario."""
    user_id = int(get_jwt_identity())
    cfg = _aios_get_config(user_id)
    return jsonify(cfg.to_dict())


@aios_bp.route('/api/aios/config', methods=['PUT'])
@jwt_required()
def api_aios_config_put():
    """Atualiza configuracao AIOS do usuario."""
    user_id = int(get_jwt_identity())
    cfg = _aios_get_config(user_id)
    data = request.get_json() or {}

    if "anthropic_key" in data and data["anthropic_key"]:
        cfg.set_anthropic_key(data["anthropic_key"])
    if "openai_key" in data and data["openai_key"]:
        cfg.set_openai_key(data["openai_key"])
    if "modelo_principal" in data:
        cfg.modelo_principal = data["modelo_principal"]
    if "modelo_fallback" in data:
        cfg.modelo_fallback = data["modelo_fallback"]
    if "ollama_url" in data:
        cfg.ollama_url = data["ollama_url"]
    if "ativo" in data:
        cfg.ativo = bool(data["ativo"])

    db.session.commit()
    return jsonify({"sucesso": True, "config": cfg.to_dict()})


@aios_bp.route('/api/aios/agents', methods=['GET'])
@jwt_required()
def api_aios_agents():
    """Lista todos os agentes disponiveis."""
    agents_list = []
    for key, agent in AIOS_AGENTS.items():
        agents_list.append({
            "id": key,
            "nome": agent["nome"],
            "descricao": agent["descricao"],
            "icon": agent["icon"],
            "color": agent["color"],
            "tools": agent.get("tools", []),
        })
    return jsonify({"agentes": agents_list})
