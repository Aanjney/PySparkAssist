const ERROR_MESSAGES = {
    rate_limit: 'The AI service hit a rate limit. Please wait and try again.',
    rate_limit_tokens_daily: 'The daily token allowance is used up. Try again later or use shorter questions.',
    rate_limit_tokens_minute: 'Too many tokens used this minute. Wait a moment and try again.',
    rate_limit_requests_daily: 'Daily request limit reached. Try again tomorrow.',
    rate_limit_requests_minute: 'Too many requests this minute. Wait a moment.',
    context_too_large: 'Your question produced too much context. Try something more specific.',
    auth_error: 'Configuration issue with the AI service. Contact the administrator.',
    service_error: 'Something went wrong. Please try again shortly.',
};

/** Chip + dot classes keyed by _usageSeverity() */
const USAGE_SEVERITY_STYLES = {
    loading: {
        chip: 'bg-surface-container dark:bg-surface-container-dark text-on-surface-variant dark:text-on-surface-variant-dark',
        dot: 'bg-gray-400',
    },
    critical: {
        chip: 'bg-red-50 dark:bg-red-900/30 text-red-700 dark:text-red-300',
        dot: 'bg-red-500',
    },
    warning: {
        chip: 'bg-yellow-50 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300',
        dot: 'bg-yellow-500',
    },
    ok: {
        chip: 'bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-300',
        dot: 'bg-green-500',
    },
};

function escapeHtmlForCode(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

/** Highlight fenced code during markdown parse so x-html receives real hljs spans (DOM post-pass was flaky with Alpine + streaming). */
(function configureMarkdownHighlighter() {
    if (typeof marked === 'undefined' || typeof hljs === 'undefined') return;

    marked.use({
        renderer: {
            code({ text, lang, escaped }) {
                const langToken = ((lang || '').match(/[^\s]+/) || [])[0] || '';
                const langNorm = langToken.toLowerCase();
                const langKey = langNorm && hljs.getLanguage(langNorm) ? langNorm : 'python';
                const source = (text || '').replace(/\n$/, '') + '\n';

                let highlighted;
                let outLang = langKey;
                try {
                    highlighted = hljs.highlight(source, { language: langKey, ignoreIllegals: true }).value;
                } catch (e1) {
                    try {
                        const r = hljs.highlightAuto(source, ['python', 'bash', 'sql', 'json', 'yaml', 'xml']);
                        highlighted = r.value;
                        outLang = (r.language || 'plaintext').toLowerCase();
                    } catch (e2) {
                        const safe = escaped ? source : escapeHtmlForCode(source);
                        return '<pre><code>' + safe + '</code></pre>\n';
                    }
                }

                const cls = String(outLang).replace(/[^a-z0-9_-]/g, '') || 'plaintext';
                return '<pre><code class="hljs language-' + cls + '">' + highlighted + '</code></pre>\n';
            },
        },
    });
})();

function chatApp() {
    return {
        messages: [],
        input: '',
        isStreaming: false,

        groqUsage: null,
        groqUsageUpdatedAt: null,
        showUsagePanel: false,
        resetTokensAt: null,
        resetRequestsAt: null,
        _tick: 0,

        darkMode: localStorage.getItem('darkMode') === 'true' ||
                  (!localStorage.getItem('darkMode') && window.matchMedia('(prefers-color-scheme: dark)').matches),

        init() {
            document.documentElement.classList.toggle('dark', this.darkMode);
            this.$watch('darkMode', (val) => {
                localStorage.setItem('darkMode', val);
                document.documentElement.classList.toggle('dark', val);
            });

            this._bindViewportKeyboardPad();
            this.fetchLimits();
            setInterval(() => { this._tick++; this.tickLimits(); }, 1000);
            setInterval(() => this.fetchLimits(), 60000);
            this.$nextTick(() => this.autoResize());
        },

        /** iOS/Android: space obscured by virtual keyboard → CSS var --keyboard-pad on .composer-pad */
        _bindViewportKeyboardPad() {
            const setPad = () => {
                const vv = window.visualViewport;
                const px = vv ? Math.max(0, window.innerHeight - vv.height - vv.offsetTop) : 0;
                document.documentElement.style.setProperty('--keyboard-pad', px + 'px');
            };
            setPad();
            if (window.visualViewport) {
                window.visualViewport.addEventListener('resize', setPad);
                window.visualViewport.addEventListener('scroll', setPad);
            }
            window.addEventListener('orientationchange', () => setTimeout(setPad, 250));
        },

        onComposerFocus() {
            requestAnimationFrame(() => {
                const el = this.$refs.inputArea;
                if (el) el.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
            });
        },

        toggleDarkMode() {
            this.darkMode = !this.darkMode;
        },

        // ── Limits ─────────────────────────────────────────────

        async fetchLimits() {
            try {
                const res = await fetch('/api/limits');
                if (!res.ok) return;
                const data = await res.json();
                if (data && data.fetched_at) this.updateLimits(data);
            } catch (e) {}
        },

        updateLimits(data) {
            const fetchedMs = data.fetched_at * 1000;
            const now = Date.now();
            const usage = { ...data };

            const tokSecs = this._parseResetSecs(data.reset_tokens);
            const tokResetAt = tokSecs != null ? fetchedMs + tokSecs * 1000 : null;
            if (tokResetAt && tokResetAt <= now && data.limit_tokens != null) {
                usage.remaining_tokens = data.limit_tokens;
            }
            this.resetTokensAt = (tokResetAt && tokResetAt > now) ? tokResetAt : null;

            const reqSecs = this._parseResetSecs(data.reset_requests);
            const reqResetAt = reqSecs != null ? fetchedMs + reqSecs * 1000 : null;
            if (reqResetAt && reqResetAt <= now && data.limit_requests != null) {
                usage.remaining_requests = data.limit_requests;
            }
            this.resetRequestsAt = (reqResetAt && reqResetAt > now) ? reqResetAt : null;

            this.groqUsage = usage;
            this.groqUsageUpdatedAt = new Date();
        },

        _applyExpiredLimitReset(now, deadlineKey, limitKey, remainingKey) {
            const deadline = this[deadlineKey];
            if (!deadline || now < deadline) return;
            if (this.groqUsage[limitKey] != null) {
                this.groqUsage = { ...this.groqUsage, [remainingKey]: this.groqUsage[limitKey] };
            }
            this[deadlineKey] = null;
        },

        tickLimits() {
            if (!this.groqUsage) return;
            const now = Date.now();
            this._applyExpiredLimitReset(now, 'resetTokensAt', 'limit_tokens', 'remaining_tokens');
            this._applyExpiredLimitReset(now, 'resetRequestsAt', 'limit_requests', 'remaining_requests');
        },

        _parseResetSecs(val) {
            if (!val) return null;
            const m = val.match(/(?:(\d+)h)?(?:(\d+)m)?(?:([\d.]+)s)?/);
            if (!m || (!m[1] && !m[2] && !m[3])) return null;
            return parseInt(m[1] || '0') * 3600 + parseInt(m[2] || '0') * 60 + parseFloat(m[3] || '0');
        },

        _fmtCountdown(targetMs) {
            if (!targetMs) return null;
            const s = Math.max(0, Math.round((targetMs - Date.now()) / 1000));
            if (s <= 0) return null;
            const h = Math.floor(s / 3600);
            const m = Math.floor((s % 3600) / 60);
            const sec = s % 60;
            const parts = [];
            if (h) parts.push(h + 'h');
            if (m) parts.push(m + 'm');
            if (sec || !parts.length) parts.push(sec + 's');
            return parts.join(' ');
        },

        countdownText() {
            void this._tick;
            const req = this._fmtCountdown(this.resetRequestsAt);
            const tok = this._fmtCountdown(this.resetTokensAt);
            if (req && tok) return req + ' req · ' + tok + ' tok';
            if (req) return req + ' req';
            if (tok) return tok + ' tok';
            return 'Up to date';
        },

        formatNumber(n) {
            if (n == null) return '—';
            if (n >= 10000) return Math.round(n / 1000) + 'k';
            if (n >= 1000) return (n / 1000).toFixed(1).replace(/\.0$/, '') + 'k';
            return String(n);
        },

        _usageBadgeFormattedPair() {
            void this._tick;
            if (!this.groqUsage) return null;
            return {
                req: this.formatNumber(this.groqUsage.remaining_requests),
                tok: this.formatNumber(this.groqUsage.remaining_tokens),
            };
        },

        usageBadgeText() {
            const p = this._usageBadgeFormattedPair();
            if (!p) return 'Loading...';
            return p.req + ' req/day · ' + p.tok + ' tok/min';
        },

        /** One-line pill for narrow nav (< sm); keeps the usage chip shrink-wrapped. */
        usageBadgeTextCompact() {
            const p = this._usageBadgeFormattedPair();
            if (!p) return '…';
            return p.req + ' · ' + p.tok;
        },

        _usageSeverity() {
            if (!this.groqUsage) return 'loading';
            const r = this.groqUsage.remaining_requests;
            const t = this.groqUsage.remaining_tokens;
            if ((r != null && r <= 5) || (t != null && t <= 1000)) return 'critical';
            if ((r != null && r <= 15) || (t != null && t <= 3000)) return 'warning';
            return 'ok';
        },

        usageColor() {
            return USAGE_SEVERITY_STYLES[this._usageSeverity()].chip;
        },

        usageDotColor() {
            return USAGE_SEVERITY_STYLES[this._usageSeverity()].dot;
        },

        // ── Textarea auto-resize ───────────────────────────────

        autoResize(event) {
            const el = event?.target || this.$refs.inputArea;
            if (!el) return;
            el.style.height = 'auto';
            el.style.height = Math.min(el.scrollHeight, 150) + 'px';
        },

        // ── Markdown rendering ─────────────────────────────────

        normalizeMarkdown(raw) {
            const lines = raw.split('\n');
            const out = [];
            let inCode = false;

            for (const line of lines) {
                const stripped = line.trimStart();
                if (stripped.startsWith('```')) {
                    if (inCode) {
                        out.push('```');
                        inCode = false;
                    } else {
                        out.push('```' + (stripped.slice(3).trim() || 'python'));
                        inCode = true;
                    }
                } else {
                    out.push(line);
                }
            }

            if (inCode) out.push('```');
            return out.join('\n');
        },

        renderMarkdown(content) {
            if (!content) return '';
            const normalized = this.normalizeMarkdown(content);
            const raw = marked.parse(normalized, { breaks: false, gfm: true });
            return DOMPurify.sanitize(raw, {
                ADD_TAGS: ['pre', 'code', 'span'],
                ADD_ATTR: ['class'],
            });
        },

        // ── Chat ───────────────────────────────────────────────

        /** Scroll so message `idx` is near the top of the chat pane (read from start of reply; no auto-scroll as tokens stream). */
        scrollToMessage(idx) {
            this.$nextTick(() => {
                const area = this.$refs.chatArea;
                if (!area) return;
                const el = area.querySelectorAll(':scope > div')[idx];
                if (el) {
                    const top = el.getBoundingClientRect().top - area.getBoundingClientRect().top + area.scrollTop;
                    area.scrollTo({ top: Math.max(0, top - 8), behavior: 'smooth' });
                } else {
                    area.scrollTo({ top: area.scrollHeight, behavior: 'smooth' });
                }
            });
        },

        getHistory() {
            const hist = this.messages.filter(m => !m.streaming);
            return hist.slice(-6).map(m => ({ role: m.role, content: m.content }));
        },

        async sendMessage() {
            const query = this.input.trim();
            if (!query || this.isStreaming) return;

            this.messages.push({ role: 'user', content: query });
            this.input = '';
            this.$nextTick(() => this.autoResize());
            this.isStreaming = true;

            const assistantMsg = { role: 'assistant', content: '', renderedHtml: '', sources: [], streaming: true };
            this.messages.push(assistantMsg);
            const msgIndex = this.messages.length - 1;
            this.scrollToMessage(msgIndex);

            let renderDirty = false;
            const renderTimer = setInterval(() => {
                if (renderDirty) {
                    this.messages[msgIndex].renderedHtml = this.renderMarkdown(this.messages[msgIndex].content);
                    renderDirty = false;
                }
            }, 300);

            const abort = (text) => {
                clearInterval(renderTimer);
                this.messages[msgIndex].content = text;
                this.messages[msgIndex].renderedHtml = this.renderMarkdown(text);
                this.messages[msgIndex].streaming = false;
                this.isStreaming = false;
            };

            try {
                const response = await fetch('/api/chat', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query, history: this.getHistory() }),
                });

                if (response.status === 429) {
                    abort('Please wait a moment before asking another question.');
                    return;
                }

                if (!response.ok) {
                    abort('Something went wrong. Please try again.');
                    return;
                }

                const reader = response.body.getReader();
                const decoder = new TextDecoder();
                let buffer = '';
                let currentEvent = 'token';

                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;

                    buffer += decoder.decode(value, { stream: true });
                    buffer = buffer.replace(/\r\n|\r/g, '\n');
                    const lines = buffer.split('\n');
                    buffer = lines.pop() || '';

                    let eventType = currentEvent;
                    let dataChunks = [];

                    for (const line of lines) {
                        if (line.startsWith('event:')) { eventType = line.slice(6).trim(); continue; }
                        if (line.startsWith('data:')) {
                            const raw = line.slice(5);
                            dataChunks.push(raw.startsWith(' ') ? raw.slice(1) : raw);
                            continue;
                        }
                        if (line === '' && dataChunks.length > 0) {
                            const data = dataChunks.join('\n');
                            dataChunks = [];

                            if (eventType === 'token') {
                                this.messages[msgIndex].content += data;
                                renderDirty = true;
                            } else if (eventType === 'done') {
                                try {
                                    const d = JSON.parse(data);
                                    this.messages[msgIndex].sources = d.sources || [];
                                    if (d.usage) this.updateLimits(d.usage);
                                } catch (e) {}
                                this.messages[msgIndex].streaming = false;
                            } else if (eventType === 'error') {
                                let code = 'service_error', retryHint = null;
                                try {
                                    const o = JSON.parse(data);
                                    if (o && typeof o === 'object') { code = o.code || 'service_error'; retryHint = o.retry_hint || null; }
                                } catch { code = data; }
                                let msg = ERROR_MESSAGES[code] || ERROR_MESSAGES.service_error;
                                if (retryHint) msg += ' Retry in ' + retryHint + '.';
                                this.messages[msgIndex].content = msg;
                                this.messages[msgIndex].renderedHtml = this.renderMarkdown(msg);
                                this.messages[msgIndex].streaming = false;
                            }
                            eventType = 'token';
                        }
                    }
                }

                this.messages[msgIndex].streaming = false;
            } catch (err) {
                abort('Connection error. Please check your network and try again.');
            } finally {
                clearInterval(renderTimer);
                this.messages[msgIndex].renderedHtml = this.renderMarkdown(this.messages[msgIndex].content);
                this.isStreaming = false;
            }
        },
    }
}
