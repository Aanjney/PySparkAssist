function chatApp() {
    return {
        messages: [],
        input: '',
        isStreaming: false,
        groqUsage: null,
        groqUsageUpdatedAt: null,
        showUsagePanel: false,
        darkMode: localStorage.getItem('darkMode') === 'true' ||
                  (!localStorage.getItem('darkMode') && window.matchMedia('(prefers-color-scheme: dark)').matches),

        init() {
            document.documentElement.classList.toggle('dark', this.darkMode);
            this.$watch('darkMode', (val) => {
                localStorage.setItem('darkMode', val);
                document.documentElement.classList.toggle('dark', val);
            });
        },

        toggleDarkMode() {
            this.darkMode = !this.darkMode;
        },

        usageColor() {
            if (!this.groqUsage) return 'bg-surface-container dark:bg-surface-container-dark text-on-surface-variant dark:text-on-surface-variant-dark';
            if (this.groqUsage.remaining_requests <= 5) return 'bg-red-50 dark:bg-red-900/30 text-red-700 dark:text-red-300';
            if (this.groqUsage.remaining_requests <= 15) return 'bg-yellow-50 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
            return 'bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-300';
        },

        usageDotColor() {
            if (!this.groqUsage) return 'bg-gray-400';
            if (this.groqUsage.remaining_requests <= 5) return 'bg-red-500';
            if (this.groqUsage.remaining_requests <= 15) return 'bg-yellow-500';
            return 'bg-green-500';
        },

        formatResetTime(val) {
            if (!val) return '—';
            const match = val.match(/(?:(\d+)h)?(?:(\d+)m)?(?:(\d+(?:\.\d+)?)s)?/);
            if (!match) return val;
            const h = parseInt(match[1] || '0');
            const m = parseInt(match[2] || '0');
            const s = Math.round(parseFloat(match[3] || '0'));
            const parts = [];
            if (h) parts.push(h + 'h');
            if (m) parts.push(m + 'm');
            if (s || parts.length === 0) parts.push(s + 's');
            return parts.join(' ');
        },

        renderMarkdown(content) {
            if (!content) return '';
            const html = marked.parse(content, { breaks: false, gfm: true });
            this.$nextTick(() => {
                this.$el.querySelectorAll('pre code:not(.hljs)').forEach(block => {
                    hljs.highlightElement(block);
                });
            });
            return html;
        },

        scrollToBottom() {
            this.$nextTick(() => {
                const area = this.$refs.chatArea;
                if (area) area.scrollTo({ top: area.scrollHeight, behavior: 'smooth' });
            });
        },

        getHistory() {
            const historyMessages = this.messages.filter(m => !m.streaming);
            const recent = historyMessages.slice(-6);
            return recent.map(m => ({ role: m.role, content: m.content }));
        },

        async sendMessage() {
            const query = this.input.trim();
            if (!query || this.isStreaming) return;

            this.messages.push({ role: 'user', content: query });
            this.input = '';
            this.isStreaming = true;

            const assistantMsg = { role: 'assistant', content: '', sources: [], streaming: true };
            this.messages.push(assistantMsg);
            const msgIndex = this.messages.length - 1;
            this.scrollToBottom();

            try {
                const response = await fetch('/api/chat', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query: query, history: this.getHistory() }),
                });

                if (response.status === 429) {
                    this.messages[msgIndex].content = 'Please wait a moment before asking another question.';
                    this.messages[msgIndex].streaming = false;
                    this.isStreaming = false;
                    return;
                }

                if (!response.ok) {
                    this.messages[msgIndex].content = 'Something went wrong. Please try again.';
                    this.messages[msgIndex].streaming = false;
                    this.isStreaming = false;
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
                    buffer = buffer.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
                    const lines = buffer.split('\n');
                    buffer = lines.pop() || '';

                    let eventType = currentEvent;
                    let dataChunks = [];

                    for (const line of lines) {
                        if (line.startsWith('event:')) {
                            eventType = line.slice(6).trim();
                            continue;
                        }
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
                                this.scrollToBottom();
                            } else if (eventType === 'done') {
                                try {
                                    const doneData = JSON.parse(data);
                                    this.messages[msgIndex].sources = doneData.sources || [];
                                    if (doneData.usage) {
                                        this.groqUsage = doneData.usage;
                                        this.groqUsageUpdatedAt = new Date();
                                    }
                                } catch (e) {}
                                this.messages[msgIndex].streaming = false;
                            } else if (eventType === 'error') {
                                this.messages[msgIndex].content += '\n\n*Error: ' + data + '*';
                                this.messages[msgIndex].streaming = false;
                            }
                            eventType = 'token';
                        }
                    }
                }

                this.messages[msgIndex].streaming = false;
            } catch (err) {
                this.messages[msgIndex].content = 'Connection error. Please check your network and try again.';
                this.messages[msgIndex].streaming = false;
            } finally {
                this.isStreaming = false;
                this.scrollToBottom();
            }
        },
    }
}
