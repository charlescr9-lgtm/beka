/**
 * AIOS - Central de Inteligencia (AI Agent Operating System)
 * Modulo completamente isolado do restante do dashboard.
 * 7 Agentes Especialistas com Chat + Execucao de Acoes Reais.
 */

(function() {
    'use strict';

    // ========== Estado interno ==========
    var _currentAgent = null;
    var _chatHistory = [];
    var _agents = [];
    var _pollingTimers = {};
    var _pendingImage = null; // {base64: string, name: string, type: string}

    // ========== Memoria persistente (localStorage) ==========
    var MEMORY_KEY_PREFIX = 'aios_chat_';
    var MEMORY_MAX_MESSAGES = 50; // max mensagens salvas por agente

    function _saveMemory(agentId) {
        if (!agentId || _chatHistory.length === 0) return;
        try {
            var toSave = _chatHistory.slice(-MEMORY_MAX_MESSAGES);
            localStorage.setItem(MEMORY_KEY_PREFIX + agentId, JSON.stringify(toSave));
            // Salvar HTML do chat tambem para restaurar visual
            var msgs = document.getElementById('aiosChatMessages');
            if (msgs) localStorage.setItem(MEMORY_KEY_PREFIX + agentId + '_html', msgs.innerHTML);
        } catch(e) { console.warn('[AIOS] Erro ao salvar memoria:', e); }
    }

    function _loadMemory(agentId) {
        if (!agentId) return { history: [], html: '' };
        try {
            var stored = localStorage.getItem(MEMORY_KEY_PREFIX + agentId);
            var html = localStorage.getItem(MEMORY_KEY_PREFIX + agentId + '_html') || '';
            return { history: stored ? JSON.parse(stored) : [], html: html };
        } catch(e) { return { history: [], html: '' }; }
    }

    function _clearMemory(agentId) {
        if (!agentId) return;
        try {
            localStorage.removeItem(MEMORY_KEY_PREFIX + agentId);
            localStorage.removeItem(MEMORY_KEY_PREFIX + agentId + '_html');
        } catch(e) {}
    }

    // Expor funcoes globais para HTML
    window._aiosSelectAgent = selectAgent;
    window._aiosSendMessage = sendMessage;
    window._aiosClearChat = clearChat;
    window._aiosSaveConfig = saveConfig;
    window._aiosExecuteTool = executeTool;
    window._aiosPromptTool = promptTool;
    window._aiosHandleKeydown = handleKeydown;
    window._aiosHandleFileSelect = handleFileSelect;
    window._aiosRemoveImage = removeImage;
    window.carregarAIOS = carregarAIOS;

    // ========== Carregar AIOS ==========
    async function carregarAIOS() {
        try {
            var data = await fetchAPI('/api/aios/status');
            if (!data) return;

            // Status badge
            var badge = document.getElementById('aiosStatusBadge');
            if (badge) {
                if (data.tem_backend) {
                    badge.style.background = 'rgba(37,211,102,0.15)';
                    badge.style.color = '#25D366';
                    badge.innerHTML = '<i class="fas fa-check-circle"></i> Online';
                } else {
                    badge.style.background = 'rgba(231,76,60,0.15)';
                    badge.style.color = '#e74c3c';
                    badge.innerHTML = '<i class="fas fa-exclamation-circle"></i> Sem backend';
                }
            }

            // Backends list
            var backendsEl = document.getElementById('aiosBackendsList');
            if (backendsEl) {
                backendsEl.innerHTML = data.backends.map(function(b) {
                    var statusColor = (b.status === 'online' || b.status === 'configurado') ? '#25D366' : '#e74c3c';
                    return '<div style="background:var(--bg-tertiary);border:1px solid var(--border);border-radius:6px;padding:6px 12px;font-size:11px;">' +
                        '<span style="color:' + statusColor + ';"><i class="fas fa-circle" style="font-size:7px;"></i></span> ' +
                        '<b>' + b.nome + '</b> - ' + b.modelo + '</div>';
                }).join('');
                if (data.backends.length === 0) {
                    backendsEl.innerHTML = '<div style="font-size:11px;color:var(--text-muted);padding:8px;">Nenhum backend configurado.</div>';
                }
            }

            // Agents grid
            _agents = data.agentes || [];
            var grid = document.getElementById('aiosAgentsGrid');
            if (grid) {
                grid.innerHTML = _agents.map(function(a) {
                    var toolCount = (a.tools || []).length;
                    var isSelected = _currentAgent === a.id;
                    return '<div onclick="_aiosSelectAgent(\'' + a.id + '\')" style="' +
                        'padding:10px 6px; border-radius:8px; text-align:center;' +
                        'background:var(--bg-secondary); border:2px solid ' + (isSelected ? a.color : 'var(--border)') + ';' +
                        'cursor:pointer; transition:all 0.2s; min-width:0;"' +
                        ' onmouseover="this.style.borderColor=\'' + a.color + '\';this.style.transform=\'scale(1.03)\'"' +
                        ' onmouseout="this.style.borderColor=\'' + (isSelected ? a.color : 'var(--border)') + '\';this.style.transform=\'scale(1)\'"' +
                        ' id="aios-agent-card-' + a.id + '">' +
                        '<div style="font-size:18px;color:' + a.color + ';margin-bottom:2px;"><i class="fas ' + a.icon + '"></i></div>' +
                        '<div style="font-size:10px;font-weight:700;color:var(--text-primary);margin-bottom:1px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="' + a.nome + '">' + a.nome + '</div>' +
                        '<div style="font-size:8px;color:var(--text-muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="' + a.descricao + '">' + a.descricao + '</div>' +
                        (toolCount > 0 ? '<div style="font-size:8px;color:' + a.color + ';margin-top:2px;"><i class="fas fa-tools"></i> ' + toolCount + '</div>' : '') +
                        '</div>';
                }).join('');
            }

            // Select dropdown
            var select = document.getElementById('aiosAgentSelect');
            if (select) {
                var currentVal = select.value;
                select.innerHTML = '<option value="">Selecione um agente...</option>' +
                    _agents.map(function(a) {
                        return '<option value="' + a.id + '"' + (currentVal === a.id ? ' selected' : '') + '>' + a.nome + '</option>';
                    }).join('');
            }

            // Config form
            var cfg = data.config || {};
            var mp = document.getElementById('aiosModeloPrincipal');
            if (mp && cfg.modelo_principal) mp.value = cfg.modelo_principal;
            var mf = document.getElementById('aiosModeloFallback');
            if (mf && cfg.modelo_fallback) mf.value = cfg.modelo_fallback;
            var ou = document.getElementById('aiosOllamaUrl');
            if (ou && cfg.ollama_url) ou.value = cfg.ollama_url;

            // Registrar paste handler para Ctrl+V imagem
            var chatInput = document.getElementById('aiosChatInput');
            if (chatInput && !chatInput._aiosPasteAdded) {
                chatInput.addEventListener('paste', handlePaste);
                chatInput._aiosPasteAdded = true;
            }

        } catch (e) {
            console.error('[AIOS] Erro ao carregar:', e);
        }
    }

    // ========== Selecionar agente ==========
    function selectAgent(agentId) {
        if (!agentId) return;

        // Salvar memoria do agente anterior antes de trocar
        if (_currentAgent && _chatHistory.length > 0) {
            _saveMemory(_currentAgent);
        }

        _currentAgent = agentId;

        // Carregar memoria do novo agente
        var memory = _loadMemory(agentId);
        _chatHistory = memory.history;

        var agent = _agents.find(function(a) { return a.id === agentId; });
        if (!agent) return;

        // Header
        var title = document.getElementById('aiosChatTitle');
        if (title) title.textContent = 'Chat - ' + agent.nome;
        var icon = document.getElementById('aiosChatIcon');
        if (icon) icon.className = 'fas ' + agent.icon;
        var sel = document.getElementById('aiosAgentSelect');
        if (sel) sel.value = agentId;

        // Highlight card
        _agents.forEach(function(a) {
            var card = document.getElementById('aios-agent-card-' + a.id);
            if (card) card.style.borderColor = (a.id === agentId) ? a.color : 'var(--border)';
        });

        // Tools panel
        var toolsPanel = document.getElementById('aiosToolsPanel');
        var toolsList = document.getElementById('aiosToolsList');
        var tools = agent.tools || [];
        if (toolsPanel && toolsList) {
            if (tools.length > 0) {
                toolsPanel.style.display = 'block';
                toolsList.innerHTML = tools.map(function(t) {
                    var argsList = (t.args || []);
                    if (argsList.length > 0) {
                        return '<button onclick="_aiosPromptTool(\'' + agent.id + '\',\'' + t.name + '\')" ' +
                            'class="btn" style="font-size:11px;padding:5px 12px;border:1px solid ' + agent.color + '44;background:' + agent.color + '11;border-radius:6px;cursor:pointer;color:var(--text-primary);">' +
                            '<i class="fas fa-play" style="color:' + agent.color + ';font-size:9px;"></i> ' + t.desc + '</button>';
                    } else {
                        return '<button onclick="_aiosExecuteTool(\'' + agent.id + '\',\'' + t.name + '\',{})" ' +
                            'class="btn" style="font-size:11px;padding:5px 12px;border:1px solid ' + agent.color + '44;background:' + agent.color + '11;border-radius:6px;cursor:pointer;color:var(--text-primary);">' +
                            '<i class="fas fa-play" style="color:' + agent.color + ';font-size:9px;"></i> ' + t.desc + '</button>';
                    }
                }).join('');
            } else {
                toolsPanel.style.display = 'none';
            }
        }

        // Restaurar chat salvo ou mostrar boas-vindas
        var msgs = document.getElementById('aiosChatMessages');
        if (msgs) {
            if (memory.html && _chatHistory.length > 0) {
                // Restaurar conversa anterior
                msgs.innerHTML = memory.html;
                msgs.scrollTop = msgs.scrollHeight;
            } else {
                // Welcome message
                msgs.innerHTML = renderAgentBubble(agent, 'Ola! Sou o <b>' + agent.nome + '</b>. ' + agent.descricao + '. Como posso ajudar?');
            }
        }

        var input = document.getElementById('aiosChatInput');
        if (input) input.focus();
    }

    // ========== Prompt para tool com argumentos ==========
    function promptTool(agentId, toolName) {
        var agent = _agents.find(function(a) { return a.id === agentId; });
        if (!agent) return;
        var tool = (agent.tools || []).find(function(t) { return t.name === toolName; });
        if (!tool) return;

        var args = tool.args || [];
        if (args.length === 0) {
            executeTool(agentId, toolName, {});
            return;
        }

        // Criar modal inline no chat
        var msgs = document.getElementById('aiosChatMessages');
        var modalId = 'aios-tool-modal-' + Date.now();
        var inputsHtml = args.map(function(arg) {
            var placeholder = arg;
            if (arg === 'url') placeholder = 'https://www.tiktok.com/...';
            if (arg === 'file_path' || arg === 'project_path' || arg === 'input_file') placeholder = 'C:/caminho/arquivo';
            if (arg === 'command') placeholder = 'npm install ...';
            if (arg === 'width' || arg === 'height') placeholder = '800';
            if (arg === 'start') placeholder = '00:00:10';
            if (arg === 'duration') placeholder = '30';
            if (arg === 'text') placeholder = 'Beka MKT';
            if (arg === 'output_format') placeholder = 'png';
            if (arg === 'descricao') placeholder = 'Descreva o video...';
            if (arg === 'mensagem') placeholder = 'Digite a mensagem...';
            return '<div style="margin-bottom:6px;">' +
                '<label style="font-size:10px;color:var(--text-muted);text-transform:uppercase;">' + arg + '</label>' +
                '<input type="text" id="' + modalId + '-' + arg + '" placeholder="' + placeholder + '" ' +
                'style="width:100%;padding:6px 10px;border:1px solid var(--border);border-radius:6px;background:var(--bg-input);color:var(--text-primary);font-size:12px;box-sizing:border-box;">' +
                '</div>';
        }).join('');

        msgs.innerHTML +=
            '<div id="' + modalId + '" style="background:var(--bg-tertiary);border:1px solid ' + agent.color + '44;border-radius:10px;padding:12px;margin-bottom:12px;">' +
            '<div style="font-size:12px;font-weight:700;margin-bottom:8px;color:' + agent.color + ';"><i class="fas fa-tools"></i> ' + tool.desc + '</div>' +
            inputsHtml +
            '<div style="display:flex;gap:8px;margin-top:8px;">' +
            '<button onclick="(function(){var a={};' +
            args.map(function(arg) {
                return 'a[\'' + arg + '\']=document.getElementById(\'' + modalId + '-' + arg + '\').value;';
            }).join('') +
            'document.getElementById(\'' + modalId + '\').remove();_aiosExecuteTool(\'' + agentId + '\',\'' + toolName + '\',a);})()" ' +
            'style="padding:5px 16px;background:' + agent.color + ';color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:11px;"><i class="fas fa-play"></i> Executar</button>' +
            '<button onclick="document.getElementById(\'' + modalId + '\').remove()" ' +
            'style="padding:5px 16px;background:var(--bg-secondary);color:var(--text-muted);border:1px solid var(--border);border-radius:6px;cursor:pointer;font-size:11px;">Cancelar</button>' +
            '</div></div>';

        msgs.scrollTop = msgs.scrollHeight;
        // Focus first input
        setTimeout(function() {
            var firstInput = document.getElementById(modalId + '-' + args[0]);
            if (firstInput) firstInput.focus();
        }, 100);
    }

    // ========== Executar tool diretamente ==========
    async function executeTool(agentId, toolName, args) {
        var agent = _agents.find(function(a) { return a.id === agentId; });
        var msgs = document.getElementById('aiosChatMessages');
        if (!agent || !msgs) return;

        // Mostrar que esta executando
        var cardId = 'aios-action-' + Date.now();
        msgs.innerHTML +=
            '<div id="' + cardId + '" style="background:var(--bg-tertiary);border-left:3px solid ' + agent.color + ';border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:12px;">' +
            '<div style="color:' + agent.color + ';font-weight:700;margin-bottom:4px;"><i class="fas fa-cog fa-spin"></i> Executando: ' + toolName + '</div>' +
            '<div style="color:var(--text-muted);">Aguarde...</div></div>';
        msgs.scrollTop = msgs.scrollHeight;

        try {
            var res = await fetchAPI('/api/aios/execute', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({agent_id: agentId, tool_name: toolName, args: args || {}})
            });

            var card = document.getElementById(cardId);
            if (!card) return;

            if (res && res.sucesso) {
                var result = res.result || {};
                if (result.status === 'started' && result.task_id) {
                    // Async task - start polling
                    card.innerHTML =
                        '<div style="color:' + agent.color + ';font-weight:700;margin-bottom:4px;"><i class="fas fa-spinner fa-spin"></i> ' + (result.message || toolName) + '</div>' +
                        '<div id="' + cardId + '-progress" style="color:var(--text-muted);">Em andamento...</div>';
                    pollTask(result.task_id, cardId, agent.color);
                } else if (result.status === 'done') {
                    card.innerHTML =
                        '<div style="color:#25D366;font-weight:700;margin-bottom:4px;"><i class="fas fa-check-circle"></i> ' + toolName + '</div>' +
                        '<div style="color:var(--text-primary);white-space:pre-wrap;">' + escapeHtml(result.result || 'OK') + '</div>';
                } else if (result.status === 'error') {
                    card.innerHTML =
                        '<div style="color:#e74c3c;font-weight:700;margin-bottom:4px;"><i class="fas fa-times-circle"></i> Erro</div>' +
                        '<div style="color:#e74c3c;">' + escapeHtml(result.error || 'Erro desconhecido') + '</div>';
                }
            } else {
                card.innerHTML =
                    '<div style="color:#e74c3c;font-weight:700;"><i class="fas fa-times-circle"></i> Erro: ' + escapeHtml((res && res.error) || 'Falha na execucao') + '</div>';
            }
        } catch (e) {
            var card2 = document.getElementById(cardId);
            if (card2) card2.innerHTML = '<div style="color:#e74c3c;"><i class="fas fa-times-circle"></i> Erro: ' + e.message + '</div>';
        }
    }

    // ========== Polling de task async ==========
    function pollTask(taskId, cardId, color) {
        var interval = setInterval(async function() {
            try {
                var res = await fetchAPI('/api/aios/task/' + taskId);
                var card = document.getElementById(cardId);
                if (!card) { clearInterval(interval); return; }

                if (res && res.status === 'done') {
                    clearInterval(interval);
                    card.innerHTML =
                        '<div style="color:#25D366;font-weight:700;margin-bottom:4px;"><i class="fas fa-check-circle"></i> Concluido</div>' +
                        '<div style="color:var(--text-primary);white-space:pre-wrap;">' + escapeHtml(res.result || 'OK') + '</div>';
                } else if (res && res.status === 'error') {
                    clearInterval(interval);
                    card.innerHTML =
                        '<div style="color:#e74c3c;font-weight:700;margin-bottom:4px;"><i class="fas fa-times-circle"></i> Erro</div>' +
                        '<div style="color:#e74c3c;">' + escapeHtml(res.error || 'Erro') + '</div>';
                } else if (res && res.progress) {
                    var prog = document.getElementById(cardId + '-progress');
                    if (prog) prog.textContent = res.progress;
                }
            } catch (e) {
                // Silently retry
            }
        }, 3000);

        // Max 5 min polling
        setTimeout(function() { clearInterval(interval); }, 300000);
    }

    // ========== Keyboard handler (Shift+Enter / Enter) ==========
    function handleKeydown(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
        // Shift+Enter = nova linha (comportamento padrao do textarea)
    }

    // ========== Ctrl+V paste imagem ==========
    function handlePaste(e) {
        var items = (e.clipboardData || e.originalEvent.clipboardData || {}).items;
        if (!items) return;
        for (var i = 0; i < items.length; i++) {
            if (items[i].type.indexOf('image') !== -1) {
                e.preventDefault();
                var file = items[i].getAsFile();
                if (file) _processImageFile(file);
                return;
            }
        }
    }

    // ========== File select (botao galeria) ==========
    function handleFileSelect(input) {
        if (input.files && input.files[0]) {
            _processImageFile(input.files[0]);
            input.value = ''; // reset para permitir selecionar o mesmo arquivo
        }
    }

    // ========== Processar arquivo de imagem ==========
    function _processImageFile(file) {
        if (!file.type.startsWith('image/')) {
            toast('Apenas imagens sao suportadas', 'warning');
            return;
        }
        if (file.size > 10 * 1024 * 1024) {
            toast('Imagem muito grande (max 10MB)', 'warning');
            return;
        }
        var reader = new FileReader();
        reader.onload = function(ev) {
            _pendingImage = {
                base64: ev.target.result, // data:image/...;base64,...
                name: file.name,
                type: file.type,
                size: file.size
            };
            // Mostrar preview
            var preview = document.getElementById('aiosImagePreview');
            var thumb = document.getElementById('aiosImageThumb');
            if (preview && thumb) {
                thumb.src = ev.target.result;
                preview.style.display = 'block';
            }
        };
        reader.readAsDataURL(file);
    }

    // ========== Remover imagem pendente ==========
    function removeImage() {
        _pendingImage = null;
        var preview = document.getElementById('aiosImagePreview');
        if (preview) preview.style.display = 'none';
    }

    // ========== Enviar mensagem ==========
    async function sendMessage() {
        var input = document.getElementById('aiosChatInput');
        var mensagem = (input ? input.value.trim() : '');
        var hasImage = !!_pendingImage;

        if (!mensagem && !hasImage) return;
        if (!_currentAgent) {
            toast('Selecione um agente primeiro', 'warning');
            return;
        }

        var agent = _agents.find(function(a) { return a.id === _currentAgent; });
        var msgs = document.getElementById('aiosChatMessages');
        var sendBtn = document.getElementById('aiosSendBtn');

        // Capturar imagem pendente e limpar
        var imageData = _pendingImage;
        _pendingImage = null;
        var preview = document.getElementById('aiosImagePreview');
        if (preview) preview.style.display = 'none';

        // User bubble (com imagem se houver)
        var userBubbleContent = '';
        if (imageData) {
            userBubbleContent += '<img src="' + imageData.base64 + '" style="max-width:200px;max-height:150px;border-radius:6px;margin-bottom:6px;display:block;">';
            userBubbleContent += '<div style="font-size:9px;color:rgba(255,255,255,0.7);margin-bottom:4px;"><i class="fas fa-image"></i> ' + escapeHtml(imageData.name) + '</div>';
        }
        if (mensagem) {
            userBubbleContent += escapeHtml(mensagem).replace(/\n/g, '<br>');
        }

        msgs.innerHTML +=
            '<div style="display:flex;gap:10px;margin-bottom:12px;justify-content:flex-end;">' +
            '<div style="background:var(--primary);color:#fff;border-radius:12px;padding:10px 14px;max-width:80%;font-size:13px;line-height:1.5;">' +
            userBubbleContent + '</div>' +
            '<div style="width:32px;height:32px;border-radius:50%;background:var(--primary);display:flex;align-items:center;justify-content:center;flex-shrink:0;">' +
            '<i class="fas fa-user" style="color:#fff;font-size:14px;"></i></div></div>';

        // Historico: incluir descricao da imagem no conteudo
        var historyContent = mensagem || '';
        if (imageData) {
            historyContent = (mensagem ? mensagem + '\n' : '') + '[Imagem anexada: ' + imageData.name + ']';
        }
        _chatHistory.push({role: 'user', content: historyContent});

        input.value = '';
        input.style.height = 'auto';
        input.disabled = true;
        if (sendBtn) { sendBtn.disabled = true; sendBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>'; }

        // Typing
        var typingId = 'aios-typing-' + Date.now();
        msgs.innerHTML +=
            '<div id="' + typingId + '" style="display:flex;gap:10px;margin-bottom:12px;">' +
            '<div style="width:32px;height:32px;border-radius:50%;background:' + agent.color + '22;display:flex;align-items:center;justify-content:center;flex-shrink:0;">' +
            '<i class="fas ' + agent.icon + '" style="color:' + agent.color + ';font-size:14px;"></i></div>' +
            '<div style="background:var(--bg-tertiary);border-radius:12px;padding:10px 14px;font-size:13px;">' +
            '<i class="fas fa-ellipsis-h fa-fade" style="color:var(--text-muted);"></i> Pensando...</div></div>';
        msgs.scrollTop = msgs.scrollHeight;

        try {
            var chatPayload = {
                agent_id: _currentAgent,
                mensagem: mensagem || (imageData ? 'Analise esta imagem.' : ''),
                historico: _chatHistory.slice(-20)
            };
            if (imageData) {
                chatPayload.imagem = {
                    base64: imageData.base64,
                    name: imageData.name,
                    type: imageData.type
                };
            }
            var res = await fetchAPI('/api/aios/chat', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(chatPayload)
            });

            var typing = document.getElementById(typingId);
            if (typing) typing.remove();

            if (res && res.sucesso) {
                _chatHistory.push({role: 'assistant', content: res.resposta});

                // Format response text
                var formatted = escapeHtml(res.resposta)
                    .replace(/\*\*(.*?)\*\*/g, '<b>$1</b>')
                    .replace(/```([\s\S]*?)```/g, '<pre style="background:var(--bg-input);padding:8px;border-radius:6px;overflow-x:auto;font-size:12px;margin:6px 0;">$1</pre>')
                    .replace(/`([^`]+)`/g, '<code style="background:var(--bg-input);padding:1px 4px;border-radius:3px;font-size:12px;">$1</code>')
                    .replace(/\n/g, '<br>');

                msgs.innerHTML +=
                    '<div style="display:flex;gap:10px;margin-bottom:12px;">' +
                    '<div style="width:32px;height:32px;border-radius:50%;background:' + agent.color + '22;display:flex;align-items:center;justify-content:center;flex-shrink:0;">' +
                    '<i class="fas ' + agent.icon + '" style="color:' + agent.color + ';font-size:14px;"></i></div>' +
                    '<div style="flex:1;">' +
                    '<div style="background:var(--bg-tertiary);border-radius:12px;padding:10px 14px;max-width:95%;font-size:13px;line-height:1.6;">' +
                    formatted + '</div>' +
                    '<div style="font-size:9px;color:var(--text-muted);margin-top:2px;padding-left:4px;">' +
                    (res.modelo || '?') + ' | ' + agent.nome + '</div></div></div>';

                // Render action results if any
                var actions = res.actions || [];
                for (var i = 0; i < actions.length; i++) {
                    var action = actions[i];
                    var r = action.result || {};
                    var acId = 'aios-action-' + Date.now() + '-' + i;
                    if (r.status === 'started' && r.task_id) {
                        msgs.innerHTML +=
                            '<div id="' + acId + '" style="background:var(--bg-tertiary);border-left:3px solid ' + agent.color + ';border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:12px;">' +
                            '<div style="color:' + agent.color + ';font-weight:700;margin-bottom:4px;"><i class="fas fa-spinner fa-spin"></i> ' + (r.message || action.tool) + '</div>' +
                            '<div id="' + acId + '-progress" style="color:var(--text-muted);">Em andamento...</div></div>';
                        pollTask(r.task_id, acId, agent.color);
                    } else if (r.status === 'done') {
                        msgs.innerHTML +=
                            '<div style="background:var(--bg-tertiary);border-left:3px solid #25D366;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:12px;">' +
                            '<div style="color:#25D366;font-weight:700;margin-bottom:4px;"><i class="fas fa-check-circle"></i> ' + action.tool + '</div>' +
                            '<div style="color:var(--text-primary);white-space:pre-wrap;">' + escapeHtml(r.result || 'OK') + '</div></div>';
                    } else if (r.status === 'error') {
                        msgs.innerHTML +=
                            '<div style="background:#e74c3c11;border-left:3px solid #e74c3c;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:12px;">' +
                            '<div style="color:#e74c3c;font-weight:700;margin-bottom:4px;"><i class="fas fa-times-circle"></i> ' + action.tool + '</div>' +
                            '<div style="color:#e74c3c;">' + escapeHtml(r.error || 'Erro') + '</div></div>';
                    }
                }
            } else {
                msgs.innerHTML +=
                    '<div style="display:flex;gap:10px;margin-bottom:12px;">' +
                    '<div style="width:32px;height:32px;border-radius:50%;background:#e74c3c22;display:flex;align-items:center;justify-content:center;flex-shrink:0;">' +
                    '<i class="fas fa-exclamation-triangle" style="color:#e74c3c;font-size:14px;"></i></div>' +
                    '<div style="background:#e74c3c11;border:1px solid #e74c3c33;border-radius:12px;padding:10px 14px;max-width:80%;font-size:13px;color:#e74c3c;">' +
                    (res ? (res.resposta || res.mensagem || 'Erro') : 'Falha na comunicacao') + '</div></div>';
            }
        } catch (e) {
            var typing2 = document.getElementById(typingId);
            if (typing2) typing2.remove();
            msgs.innerHTML += '<div style="padding:8px;font-size:12px;color:#e74c3c;text-align:center;">Erro: ' + e.message + '</div>';
        }

        input.disabled = false;
        if (sendBtn) { sendBtn.disabled = false; sendBtn.innerHTML = '<i class="fas fa-paper-plane"></i> Enviar'; }
        msgs.scrollTop = msgs.scrollHeight;
        input.focus();

        // Salvar memoria apos cada interacao
        _saveMemory(_currentAgent);
    }

    // ========== Limpar chat ==========
    function clearChat() {
        _chatHistory = [];
        _clearMemory(_currentAgent);
        var agent = _agents.find(function(a) { return a.id === _currentAgent; });
        var msgs = document.getElementById('aiosChatMessages');
        if (!msgs) return;
        if (agent) {
            msgs.innerHTML = renderAgentBubble(agent, 'Chat limpo! Como posso ajudar?');
        } else {
            msgs.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:60px 20px;font-size:13px;">' +
                '<i class="fas fa-brain" style="font-size:40px;opacity:0.3;display:block;margin-bottom:12px;"></i>' +
                'Selecione um especialista acima para iniciar o chat</div>';
        }
    }

    // ========== Salvar config ==========
    async function saveConfig() {
        var payload = {
            modelo_principal: document.getElementById('aiosModeloPrincipal').value,
            modelo_fallback: document.getElementById('aiosModeloFallback').value,
            ollama_url: document.getElementById('aiosOllamaUrl').value,
        };
        var ak = document.getElementById('aiosAnthropicKey').value.trim();
        if (ak) payload.anthropic_key = ak;
        var ok = document.getElementById('aiosOpenaiKey').value.trim();
        if (ok) payload.openai_key = ok;

        var res = await fetchAPI('/api/aios/config', {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        if (res && res.sucesso) {
            toast('Configuracao AIOS salva!', 'success');
            document.getElementById('aiosAnthropicKey').value = '';
            document.getElementById('aiosOpenaiKey').value = '';
            carregarAIOS();
        } else {
            toast('Erro ao salvar configuracao', 'error');
        }
    }

    // ========== Helpers ==========
    function renderAgentBubble(agent, html) {
        return '<div style="display:flex;gap:10px;margin-bottom:12px;">' +
            '<div style="width:32px;height:32px;border-radius:50%;background:' + agent.color + '22;display:flex;align-items:center;justify-content:center;flex-shrink:0;">' +
            '<i class="fas ' + agent.icon + '" style="color:' + agent.color + ';font-size:14px;"></i></div>' +
            '<div style="background:var(--bg-tertiary);border-radius:12px;padding:10px 14px;max-width:80%;font-size:13px;line-height:1.5;">' +
            html + '</div></div>';
    }

    function escapeHtml(text) {
        var div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

})();
