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
    var _pendingMediaFiles = []; // File[] para upload multiplo no envio
    var _isSending = false;
    var _cursorProjects = [];
    var CURSOR_PROJECT_KEY = 'aios_cursor_project';

    // ========== Memoria persistente (cache local + backend) ==========
    var MEMORY_KEY_PREFIX = 'aios_chat_';
    var MEMORY_MAX_MESSAGES = 50; // max mensagens salvas por agente

    function _sleep(ms) {
        return new Promise(function(resolve) {
            setTimeout(resolve, ms);
        });
    }

    function _saveMemory(agentId, historyOverride, htmlOverride) {
        var historyToPersist = Array.isArray(historyOverride) ? historyOverride.slice() : _chatHistory.slice();
        var msgs = document.getElementById('aiosChatMessages');
        var html = typeof htmlOverride === 'string' ? htmlOverride : (msgs ? msgs.innerHTML : '');
        if (!agentId || (historyToPersist.length === 0 && !html)) return;
        try {
            var toSave = historyToPersist.slice(-MEMORY_MAX_MESSAGES);
            localStorage.setItem(MEMORY_KEY_PREFIX + agentId, JSON.stringify(toSave));
            // Salvar HTML do chat tambem para restaurar visual
            if (html) {
                localStorage.setItem(MEMORY_KEY_PREFIX + agentId + '_html', html);
            } else {
                localStorage.removeItem(MEMORY_KEY_PREFIX + agentId + '_html');
            }
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

    async function _loadServerSession(agentId) {
        if (!agentId) return { history: [], updated_at: '' };
        for (var attempt = 0; attempt < 3; attempt++) {
            try {
                var res = await fetchAPI('/api/aios/session/' + agentId);
                if (res && typeof res === 'object') {
                    return {
                        history: Array.isArray(res.history) ? res.history : [],
                        updated_at: res.updated_at || ''
                    };
                }
            } catch(e) {
                console.warn('[AIOS] Erro ao carregar sessao persistida:', e);
            }
            if (attempt < 2) {
                await _sleep(250 * (attempt + 1));
            }
        }
        return { history: [], updated_at: '' };
    }

    function _pickBestMemory(localMemory, serverSession) {
        var localHistory = Array.isArray(localMemory && localMemory.history) ? localMemory.history : [];
        var serverHistory = Array.isArray(serverSession && serverSession.history) ? serverSession.history : [];
        var localHtml = (localMemory && localMemory.html) || '';

        if (localHistory.length > serverHistory.length) {
            return { history: localHistory, html: localHtml };
        }
        if (serverHistory.length > localHistory.length) {
            return { history: serverHistory, html: '' };
        }
        if (localHistory.length > 0) {
            return { history: localHistory, html: localHtml };
        }
        if (localHtml) {
            return { history: localHistory, html: localHtml };
        }
        return { history: serverHistory, html: '' };
    }

    function _getStoredCursorProject() {
        try {
            return localStorage.getItem(CURSOR_PROJECT_KEY) || '';
        } catch(e) {
            return '';
        }
    }

    function _setStoredCursorProject(path) {
        try {
            if (path) {
                localStorage.setItem(CURSOR_PROJECT_KEY, path);
            } else {
                localStorage.removeItem(CURSOR_PROJECT_KEY);
            }
        } catch(e) {}
    }

    function _getDefaultCursorProject() {
        return (_cursorProjects[0] && _cursorProjects[0].path) ? _cursorProjects[0].path : '';
    }

    function _getSelectedCursorProject() {
        var select = document.getElementById('aiosCursorProjectSelect');
        var selected = select ? String(select.value || '').trim() : '';
        if (selected) return selected;

        selected = _getStoredCursorProject();
        if (selected) return selected;

        return _getDefaultCursorProject();
    }

    function _updateCursorProjectHint(selectedPath) {
        var hint = document.getElementById('aiosCursorProjectHint');
        if (!hint) return;

        var selected = (_cursorProjects || []).find(function(project) {
            return project.path === selectedPath;
        });
        if (!selected) {
            hint.textContent = 'Nenhum projeto configurado para o Codex local.';
            return;
        }

        hint.innerHTML = 'O agente Cursor/Dev vai usar <b>' + escapeHtml(selected.name || selected.path) +
            '</b> no Codex local.';
    }

    function _renderCursorProjectSelect() {
        var select = document.getElementById('aiosCursorProjectSelect');
        if (!select) return;

        if (!_cursorProjects.length) {
            select.innerHTML = '<option value="">Nenhum projeto disponivel</option>';
            select.disabled = true;
            _updateCursorProjectHint('');
            return;
        }

        var selected = _getSelectedCursorProject();
        if (!_cursorProjects.some(function(project) { return project.path === selected; })) {
            selected = _getDefaultCursorProject();
        }

        select.disabled = false;
        select.innerHTML = _cursorProjects.map(function(project) {
            var label = project.name || project.path;
            return '<option value="' + escapeHtml(project.path) + '">' + escapeHtml(label) + '</option>';
        }).join('');

        if (selected) {
            select.value = selected;
            _setStoredCursorProject(selected);
        }
        _updateCursorProjectHint(selected);
    }

    function _syncCursorProjectUI(agentId) {
        var row = document.getElementById('aiosCursorProjectRow');
        if (!row) return;

        var isCursorAgent = agentId === 'cursor';
        row.style.display = isCursorAgent ? 'block' : 'none';
        if (isCursorAgent) {
            _renderCursorProjectSelect();
        }
    }

    function handleCursorProjectChange(value) {
        var selected = String(value || '').trim();
        if (!selected) {
            selected = _getDefaultCursorProject();
        }
        _setStoredCursorProject(selected);
        _updateCursorProjectHint(selected);
    }

    function _syncMotorSelectors(selectedValue) {
        var source = document.getElementById('aiosModeloPrincipal');
        var quick = document.getElementById('aiosMotorSelectQuick');
        if (!source || !quick) return;

        quick.innerHTML = source.innerHTML;
        if (selectedValue) {
            source.value = selectedValue;
            quick.value = selectedValue;
        } else if (source.value) {
            quick.value = source.value;
        }
        _updateMotorHint(quick.value || source.value || '');
    }

    function _updateMotorHint(modelValue) {
        var hint = document.getElementById('aiosMotorHint');
        var source = document.getElementById('aiosModeloPrincipal');
        if (!hint || !source) return;

        var label = '';
        for (var i = 0; i < source.options.length; i++) {
            if (source.options[i].value === modelValue) {
                label = source.options[i].text;
                break;
            }
        }

        if (!label) {
            hint.textContent = 'Controla os agentes gerais. Cursor/Dev e Venice +18 mantem motores proprios.';
            return;
        }

        hint.innerHTML = 'Motor atual dos agentes gerais: <b>' + escapeHtml(label) +
            '</b>. Cursor/Dev e Venice +18 mantem motores proprios.';
    }

    async function handleMotorChange(value) {
        var selected = String(value || '').trim();
        if (!selected) return;

        var source = document.getElementById('aiosModeloPrincipal');
        var quick = document.getElementById('aiosMotorSelectQuick');
        if (source) source.value = selected;
        if (quick) quick.value = selected;
        _updateMotorHint(selected);

        try {
            var res = await fetchAPI('/api/aios/config', {
                method: 'PUT',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ modelo_principal: selected })
            });
            if (res && res.sucesso) {
                toast('Motor principal da AIOS atualizado', 'success');
                await carregarAIOS();
            } else {
                toast('Erro ao trocar motor da AIOS', 'error');
            }
        } catch (e) {
            toast('Erro ao trocar motor da AIOS: ' + e.message, 'error');
        }
    }

    // ========== Compactacao automatica de memoria ==========
    var _isCompacting = false;

    async function _compactIfNeeded(agentId) {
        if (!agentId || _isCompacting) return;
        if (_chatHistory.length <= MEMORY_MAX_MESSAGES) return;

        var originalHistory = _chatHistory.slice();
        var originalAgentId = agentId;
        var excess = originalHistory.length - MEMORY_MAX_MESSAGES;
        var toCompact = originalHistory.slice(0, excess);
        var trimmedHistory = originalHistory.slice(excess);

        _isCompacting = true;

        // Mostrar indicador no chat
        var msgs = document.getElementById('aiosChatMessages');
        var compactId = 'aios-compact-' + Date.now();
        if (msgs) {
            msgs.innerHTML +=
                '<div id="' + compactId + '" style="text-align:center;padding:8px;margin:8px 0;">' +
                '<span style="background:var(--bg-tertiary);color:var(--text-muted);padding:4px 12px;border-radius:12px;font-size:11px;">' +
                '<i class="fas fa-compress-alt fa-spin"></i> Compactando ' + toCompact.length + ' mensagens para memoria de longo prazo...</span></div>';
        }

        try {
            var res = await fetchAPI('/api/aios/memory/' + agentId + '/compact', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ mensagens: toCompact })
            });

            var compactEl = document.getElementById(compactId);
            if (!res || !res.sucesso) {
                throw new Error((res && (res.erro || res.mensagem)) || 'Erro ao compactar memoria');
            }

            if (_currentAgent === originalAgentId) {
                _chatHistory = trimmedHistory;
            }
            // Salvar estado atualizado (com historico reduzido)
            _saveMemory(originalAgentId, trimmedHistory);
            if (compactEl) {
                compactEl.innerHTML =
                    '<span style="background:#25D36622;color:#25D366;padding:4px 12px;border-radius:12px;font-size:11px;">' +
                    '<i class="fas fa-check-circle"></i> ' + res.mensagens_compactadas + ' mensagens compactadas para memoria de longo prazo</span>';
            }
        } catch(e) {
            console.warn('[AIOS] Erro na compactacao:', e);
            if (_currentAgent === originalAgentId) {
                _chatHistory = originalHistory;
                _saveMemory(originalAgentId, originalHistory);
            }
            var compactEl2 = document.getElementById(compactId);
            if (compactEl2) {
                compactEl2.innerHTML =
                    '<span style="background:#e74c3c22;color:#e74c3c;padding:4px 12px;border-radius:12px;font-size:11px;">' +
                    '<i class="fas fa-exclamation-triangle"></i> Memoria longa indisponivel; historico local preservado</span>';
            }
        }
        _isCompacting = false;
    }

    // ========== Persistencia automatica ==========
    // Salvar ao fechar aba/browser
    window.addEventListener('beforeunload', function() {
        if (_currentAgent && _chatHistory.length > 0) {
            _saveMemory(_currentAgent);
        }
    });
    // Salvar ao mudar de aba no browser
    document.addEventListener('visibilitychange', function() {
        if (document.hidden && _currentAgent && _chatHistory.length > 0) {
            _saveMemory(_currentAgent);
        }
    });
    // Salvar periodicamente a cada 30 segundos
    setInterval(function() {
        if (_currentAgent && _chatHistory.length > 0) {
            _saveMemory(_currentAgent);
        }
    }, 30000);
    // Salvar ao clicar em qualquer outra aba do Beka MKT
    document.addEventListener('click', function(e) {
        var tab = e.target.closest('[data-tab]');
        if (tab && tab.getAttribute('data-tab') !== 'aios') {
            if (_currentAgent && _chatHistory.length > 0) {
                _saveMemory(_currentAgent);
            }
        }
    });

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
    window._aiosRemovePendingMedia = removePendingMedia;
    window._aiosHandleCursorProjectChange = handleCursorProjectChange;
    window._aiosHandleMotorChange = handleMotorChange;
    window._aiosClearLongMemory = async function() {
        if (_isCompacting) {
            toast('Aguarde a compactacao terminar antes de limpar a memoria longa', 'warning');
            return;
        }
        if (!_currentAgent) {
            toast('Selecione um agente primeiro', 'warning');
            return;
        }
        if (!confirm('Limpar TODA a memoria de longo prazo do agente? Isso nao pode ser desfeito.')) return;
        try {
            await fetchAPI('/api/aios/memory/' + _currentAgent, { method: 'DELETE' });
            toast('Memoria de longo prazo limpa com sucesso', 'success');
        } catch(e) {
            toast('Erro ao limpar memoria longa: ' + e.message, 'error');
        }
    };
    window.carregarAIOS = carregarAIOS;

    // ========== Carregar AIOS ==========
    async function carregarAIOS() {
        try {
            var data = null;
            for (var attempt = 0; attempt < 3; attempt++) {
                data = await fetchAPI('/api/aios/status');
                if (data) break;
                if (attempt < 2) {
                    await _sleep(250 * (attempt + 1));
                }
            }
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
                var statusMeta = {
                    online: { color: '#25D366', label: 'Online' },
                    configurado: { color: '#25D366', label: 'Configurado' },
                    offline: { color: '#e74c3c', label: 'Offline' },
                    nao_configurado: { color: 'var(--text-muted)', label: 'Nao configurado' }
                };
                backendsEl.innerHTML = data.backends.map(function(b) {
                    var meta = statusMeta[b.status] || statusMeta.offline;
                    return '<div style="background:var(--bg-tertiary);border:1px solid var(--border);border-radius:6px;padding:6px 12px;font-size:11px;display:flex;gap:8px;align-items:center;">' +
                        '<span style="color:' + meta.color + ';"><i class="fas fa-circle" style="font-size:7px;"></i></span>' +
                        '<span><b>' + b.nome + '</b> - ' + b.modelo + '</span>' +
                        '<span style="padding:2px 8px;border-radius:999px;background:' + meta.color + '22;color:' + meta.color + ';font-size:10px;font-weight:700;">' + meta.label + '</span>' +
                        '</div>';
                }).join('');
                if (data.backends.length === 0) {
                    backendsEl.innerHTML = '<div style="font-size:11px;color:var(--text-muted);padding:8px;">Nenhum backend configurado.</div>';
                }
            }

            // Agents grid
            _agents = data.agentes || [];
            _cursorProjects = Array.isArray(data.cursor_projects) ? data.cursor_projects : [];
            _renderCursorProjectSelect();
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
            _syncMotorSelectors((mp && mp.value) || cfg.modelo_principal || '');

            // Registrar paste handler para Ctrl+V imagem
            var chatInput = document.getElementById('aiosChatInput');
            if (chatInput && !chatInput._aiosPasteAdded) {
                chatInput.addEventListener('paste', handlePaste);
                chatInput._aiosPasteAdded = true;
            }

            // Restaurar ultimo agente e conversa automaticamente
            var lastAgent = null;
            try { lastAgent = localStorage.getItem('aios_last_agent'); } catch(e) {}
            if (lastAgent && _agents.some(function(a) { return a.id === lastAgent; })) {
                await selectAgent(lastAgent);
            }
            _syncCursorProjectUI(_currentAgent);

        } catch (e) {
            console.error('[AIOS] Erro ao carregar:', e);
        }
    }

    // ========== Selecionar agente ==========
    async function selectAgent(agentId) {
        if (!agentId) return;
        if (_isSending && agentId !== _currentAgent) {
            toast('Aguarde a resposta atual terminar antes de trocar de agente', 'warning');
            return;
        }
        if (_isCompacting && agentId !== _currentAgent) {
            toast('Aguarde a compactacao da memoria terminar antes de trocar de agente', 'warning');
            return;
        }

        // Salvar memoria do agente anterior antes de trocar
        if (_currentAgent && _chatHistory.length > 0) {
            var previousMsgs = document.getElementById('aiosChatMessages');
            _saveMemory(_currentAgent, _chatHistory.slice(), previousMsgs ? previousMsgs.innerHTML : '');
        }

        _currentAgent = agentId;
        // Persistir ultimo agente selecionado para restaurar ao reabrir
        try { localStorage.setItem('aios_last_agent', agentId); } catch(e) {}

        var localMemory = _loadMemory(agentId);

        var agent = _agents.find(function(a) { return a.id === agentId; });
        if (!agent) return;
        _syncCursorProjectUI(agentId);

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

        var msgs = document.getElementById('aiosChatMessages');
        if (msgs) {
            if (localMemory.html && localMemory.history.length > 0) {
                _chatHistory = localMemory.history;
                msgs.innerHTML = localMemory.html;
                msgs.scrollTop = msgs.scrollHeight;
            } else if (localMemory.history.length > 0) {
                _chatHistory = localMemory.history;
                msgs.innerHTML = renderHistory(agent, _chatHistory);
                msgs.scrollTop = msgs.scrollHeight;
            } else {
                _chatHistory = [];
                msgs.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:24px 20px;font-size:12px;">' +
                    '<i class="fas fa-clock" style="margin-right:6px;"></i> Carregando conversa salva...</div>';
            }
        }

        var serverSession = await _loadServerSession(agentId);
        if (_currentAgent !== agentId) return;

        if (!serverSession.history.length && !localMemory.history.length) {
            await _sleep(300);
            serverSession = await _loadServerSession(agentId);
        }
        if (_currentAgent !== agentId) return;

        var memory = _pickBestMemory(localMemory, serverSession);
        _chatHistory = memory.history;

        if (msgs) {
            if (memory.html && _chatHistory.length > 0) {
                msgs.innerHTML = memory.html;
            } else if (_chatHistory.length > 0) {
                msgs.innerHTML = renderHistory(agent, _chatHistory);
            } else {
                msgs.innerHTML = renderAgentBubble(agent, 'Ola! Sou o <b>' + agent.nome + '</b>. ' + agent.descricao + '. Como posso ajudar?');
            }
            msgs.scrollTop = msgs.scrollHeight;
            _saveMemory(agentId, _chatHistory, msgs.innerHTML);
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
            if (arg === 'input_files') placeholder = 'video1.mp4 | video2.mp4';
            if (arg === 'output_name') placeholder = 'video_final.mp4';
            if (arg === 'output_format') placeholder = 'png / mp3';
            if (arg === 'quality') placeholder = 'media';
            if (arg === 'direction') placeholder = 'right';
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

    function _getMediaLabel(file) {
        var mime = String((file && file.type) || '').toLowerCase();
        if (mime.indexOf('video/') === 0) return 'Video';
        if (mime.indexOf('audio/') === 0) return 'Audio';
        if (mime.indexOf('image/') === 0) return 'Imagem';
        return 'Arquivo';
    }

    function _isSupportedChatMediaFile(file) {
        if (!file) return false;
        var mime = String(file.type || '').toLowerCase();
        if (mime.indexOf('image/') === 0 || mime.indexOf('video/') === 0 || mime.indexOf('audio/') === 0) {
            return true;
        }
        var name = String(file.name || '').toLowerCase();
        return /\.(png|jpe?g|webp|gif|bmp|mp4|mov|avi|mkv|webm|m4v|mp3|wav|m4a|aac|ogg)$/.test(name);
    }

    function _renderPendingMediaFiles() {
        var preview = document.getElementById('aiosMediaUploadsPreview');
        var list = document.getElementById('aiosMediaUploadsList');
        if (!preview || !list) return;

        if (!_pendingMediaFiles.length) {
            list.innerHTML = '';
            preview.style.display = 'none';
            return;
        }

        list.innerHTML = _pendingMediaFiles.map(function(file, index) {
            var sizeMb = (Number(file.size || 0) / (1024 * 1024)).toFixed(1);
            return '<div style="display:flex;align-items:center;gap:8px;padding:8px 10px;border-radius:8px;background:var(--bg-secondary);border:1px solid var(--border);max-width:280px;">' +
                '<div style="width:28px;height:28px;border-radius:50%;display:flex;align-items:center;justify-content:center;background:var(--bg-tertiary);color:var(--accent);flex-shrink:0;">' +
                '<i class="fas ' + (_getMediaLabel(file) === 'Video' ? 'fa-video' : (_getMediaLabel(file) === 'Audio' ? 'fa-music' : 'fa-file')) + '"></i></div>' +
                '<div style="min-width:0;flex:1;">' +
                '<div style="font-size:11px;font-weight:600;color:var(--text-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + escapeHtml(file.name || 'arquivo') + '</div>' +
                '<div style="font-size:10px;color:var(--text-muted);">' + _getMediaLabel(file) + ' • ' + sizeMb + ' MB</div>' +
                '</div>' +
                '<button onclick="_aiosRemovePendingMedia(' + index + ')" style="width:20px;height:20px;border-radius:50%;background:#e74c3c;color:#fff;border:none;cursor:pointer;font-size:11px;display:flex;align-items:center;justify-content:center;flex-shrink:0;">&times;</button>' +
                '</div>';
        }).join('');
        preview.style.display = 'block';
    }

    function _addPendingMediaFiles(files) {
        var added = 0;
        (files || []).forEach(function(file) {
            if (!file || !_isSupportedChatMediaFile(file)) return;
            if (Number(file.size || 0) > 250 * 1024 * 1024) {
                toast('Arquivo muito grande: ' + (file.name || 'arquivo') + ' (max 250MB)', 'warning');
                return;
            }

            var duplicate = _pendingMediaFiles.some(function(existing) {
                return existing.name === file.name &&
                    Number(existing.size || 0) === Number(file.size || 0) &&
                    Number(existing.lastModified || 0) === Number(file.lastModified || 0);
            });
            if (!duplicate) {
                _pendingMediaFiles.push(file);
                added += 1;
            }
        });

        if (added > 0) {
            _renderPendingMediaFiles();
        }
    }

    function removePendingMedia(index) {
        if (index < 0 || index >= _pendingMediaFiles.length) return;
        _pendingMediaFiles.splice(index, 1);
        _renderPendingMediaFiles();
    }

    async function _uploadPendingMediaFiles(files) {
        if (!files || !files.length) return [];

        var formData = new FormData();
        files.forEach(function(file) {
            formData.append('files', file);
        });

        var res = await fetchAPI('/api/aios/upload', {
            method: 'POST',
            body: formData
        });

        if (!res || !res.sucesso) {
            throw new Error((res && (res.mensagem || res.error)) || 'Falha no upload das midias');
        }

        return Array.isArray(res.arquivos) ? res.arquivos : [];
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
        if (input.files && input.files.length > 0) {
            var files = Array.prototype.slice.call(input.files);
            if (files.length === 1 && String(files[0].type || '').indexOf('image/') === 0) {
                _processImageFile(files[0]);
            } else {
                _addPendingMediaFiles(files);
            }
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
        var hasMediaFiles = _pendingMediaFiles.length > 0;

        if (!mensagem && !hasImage && !hasMediaFiles) return;
        if (_isSending) return;
        if (_isCompacting) {
            toast('Aguarde a compactacao da memoria terminar para enviar a proxima mensagem', 'warning');
            return;
        }
        if (!_currentAgent) {
            toast('Selecione um agente primeiro', 'warning');
            return;
        }

        var agent = _agents.find(function(a) { return a.id === _currentAgent; });
        var msgs = document.getElementById('aiosChatMessages');
        var sendBtn = document.getElementById('aiosSendBtn');

        // Capturar imagem pendente e limpar
        var imageData = _pendingImage;
        var mediaFiles = _pendingMediaFiles.slice();
        _pendingImage = null;
        var preview = document.getElementById('aiosImagePreview');

        input.value = '';
        input.style.height = 'auto';
        input.disabled = true;
        if (sendBtn) { sendBtn.disabled = true; sendBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>'; }
        _isSending = true;

        // Typing
        var typingId = 'aios-typing-' + Date.now();
        msgs.innerHTML +=
            '<div id="' + typingId + '" style="display:flex;gap:10px;margin-bottom:12px;">' +
            '<div style="width:32px;height:32px;border-radius:50%;background:' + agent.color + '22;display:flex;align-items:center;justify-content:center;flex-shrink:0;">' +
            '<i class="fas ' + agent.icon + '" style="color:' + agent.color + ';font-size:14px;"></i></div>' +
            '<div style="background:var(--bg-tertiary);border-radius:12px;padding:10px 14px;font-size:13px;">' +
            '<i class="fas fa-ellipsis-h fa-fade" style="color:var(--text-muted);"></i> ' + (mediaFiles.length ? 'Enviando midias...' : 'Pensando...') + '</div></div>';
        msgs.scrollTop = msgs.scrollHeight;

        try {
            var uploadedFiles = [];
            if (mediaFiles.length) {
                uploadedFiles = await _uploadPendingMediaFiles(mediaFiles);
                _pendingMediaFiles = [];
                _renderPendingMediaFiles();
            }
            if (preview) preview.style.display = 'none';

            // User bubble (com imagem/arquivos se houver)
            var userBubbleContent = '';
            if (imageData) {
                userBubbleContent += '<img src="' + imageData.base64 + '" style="max-width:200px;max-height:150px;border-radius:6px;margin-bottom:6px;display:block;">';
                userBubbleContent += '<div style="font-size:9px;color:rgba(255,255,255,0.7);margin-bottom:4px;"><i class="fas fa-image"></i> ' + escapeHtml(imageData.name) + '</div>';
            }
            if (uploadedFiles.length) {
                userBubbleContent += uploadedFiles.map(function(file) {
                    var type = String((file && file.type) || '').toLowerCase();
                    var icon = type.indexOf('video/') === 0 ? 'fa-video' : (type.indexOf('audio/') === 0 ? 'fa-music' : 'fa-file');
                    return '<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px;font-size:11px;color:rgba(255,255,255,0.8);">' +
                        '<i class="fas ' + icon + '"></i> ' + escapeHtml(file.name || 'arquivo') + '</div>';
                }).join('');
            }
            if (mensagem) {
                userBubbleContent += escapeHtml(mensagem).replace(/\n/g, '<br>');
            }

            var userBubbleHtml =
                '<div style="display:flex;gap:10px;margin-bottom:12px;justify-content:flex-end;">' +
                '<div style="background:var(--primary);color:#fff;border-radius:12px;padding:10px 14px;max-width:80%;font-size:13px;line-height:1.5;">' +
                userBubbleContent + '</div>' +
                '<div style="width:32px;height:32px;border-radius:50%;background:var(--primary);display:flex;align-items:center;justify-content:center;flex-shrink:0;">' +
                '<i class="fas fa-user" style="color:#fff;font-size:14px;"></i></div></div>';
            var pendingTyping = document.getElementById(typingId);
            if (pendingTyping) {
                pendingTyping.insertAdjacentHTML('beforebegin', userBubbleHtml);
                pendingTyping.innerHTML =
                    '<div style="background:var(--bg-tertiary);border-radius:12px;padding:10px 14px;font-size:13px;">' +
                    '<i class="fas fa-ellipsis-h fa-fade" style="color:var(--text-muted);"></i> Pensando...</div>';
            } else {
                msgs.innerHTML += userBubbleHtml;
            }

            // Historico: incluir descricao dos anexos
            var historyLines = [];
            if (mensagem) historyLines.push(mensagem);
            if (imageData) historyLines.push('[Imagem anexada: ' + imageData.name + ']');
            uploadedFiles.forEach(function(file) {
                historyLines.push('[Arquivo anexado: ' + (file.name || 'arquivo') + ']');
            });
            var historyContent = historyLines.join('\n');
            _chatHistory.push({role: 'user', content: historyContent});
            // Salvar imediatamente apos cada mensagem do usuario (protege contra fechamento)
            _saveMemory(_currentAgent);

            var chatPayload = {
                agent_id: _currentAgent,
                mensagem: mensagem || (imageData ? 'Analise esta imagem.' : (uploadedFiles.length ? 'Considere os arquivos anexados.' : '')),
                historico: _chatHistory.slice(-MEMORY_MAX_MESSAGES)
            };
            if (_currentAgent === 'cursor') {
                chatPayload.cursor_project = _getSelectedCursorProject();
            }
            if (imageData) {
                chatPayload.imagem = {
                    base64: imageData.base64,
                    name: imageData.name,
                    type: imageData.type
                };
            }
            if (uploadedFiles.length) {
                chatPayload.arquivos = uploadedFiles;
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

                var formatted = formatAssistantMessage(res.resposta);

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
        _isSending = false;

        // Salvar memoria apos cada interacao
        _saveMemory(_currentAgent);
        // Compactar se passou de 50 mensagens
        _compactIfNeeded(_currentAgent);
    }

    // ========== Limpar chat ==========
    async function clearChat() {
        if (_isCompacting) {
            toast('Aguarde a compactacao da memoria terminar antes de limpar o chat', 'warning');
            return;
        }
        if (_isSending) {
            toast('Aguarde a resposta atual terminar antes de limpar o chat', 'warning');
            return;
        }
        if (!_currentAgent) return;

        var agentId = _currentAgent;
        _chatHistory = [];
        _pendingImage = null;
        _pendingMediaFiles = [];
        _clearMemory(agentId);
        var imagePreview = document.getElementById('aiosImagePreview');
        if (imagePreview) imagePreview.style.display = 'none';
        _renderPendingMediaFiles();
        // Nota: NAO apaga memoria de longo prazo (.md no servidor)
        // Para limpar memoria de longo prazo, usar _clearLongMemory()
        var agent = _agents.find(function(a) { return a.id === agentId; });
        var msgs = document.getElementById('aiosChatMessages');
        if (!msgs) return;
        if (agent) {
            msgs.innerHTML = renderAgentBubble(agent, 'Chat limpo! Como posso ajudar?');
        } else {
            msgs.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:60px 20px;font-size:13px;">' +
                '<i class="fas fa-brain" style="font-size:40px;opacity:0.3;display:block;margin-bottom:12px;"></i>' +
                'Selecione um especialista acima para iniciar o chat</div>';
        }

        try {
            await fetchAPI('/api/aios/session/' + agentId, { method: 'DELETE' });
        } catch (e) {
            toast('Erro ao limpar conversa persistida: ' + e.message, 'warning');
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
    function formatAssistantMessage(text) {
        return escapeHtml(text || '')
            .replace(/\*\*(.*?)\*\*/g, '<b>$1</b>')
            .replace(/```([\s\S]*?)```/g, '<pre style="background:var(--bg-input);padding:8px;border-radius:6px;overflow-x:auto;font-size:12px;margin:6px 0;">$1</pre>')
            .replace(/`([^`]+)`/g, '<code style="background:var(--bg-input);padding:1px 4px;border-radius:3px;font-size:12px;">$1</code>')
            .replace(/\n/g, '<br>');
    }

    function formatUserMessage(text) {
        return escapeHtml(text || '').replace(/\n/g, '<br>');
    }

    function renderUserBubble(html) {
        return '<div style="display:flex;gap:10px;margin-bottom:12px;justify-content:flex-end;">' +
            '<div style="background:var(--primary);color:#fff;border-radius:12px;padding:10px 14px;max-width:80%;font-size:13px;line-height:1.5;">' +
            html + '</div>' +
            '<div style="width:32px;height:32px;border-radius:50%;background:var(--primary);display:flex;align-items:center;justify-content:center;flex-shrink:0;">' +
            '<i class="fas fa-user" style="color:#fff;font-size:14px;"></i></div></div>';
    }

    function renderHistory(agent, history) {
        return (history || []).map(function(msg) {
            if (!msg || !msg.role) return '';
            if (msg.role === 'user') {
                return renderUserBubble(formatUserMessage(msg.content || ''));
            }
            return renderAgentBubble(agent, formatAssistantMessage(msg.content || ''));
        }).join('');
    }

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
