function chatApp() {
    return {
        messages: [],
        input: '',
        isStreaming: false,
        groqUsage: null,
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

        renderMarkdown(content) {
            if (!content) return '';
            return marked.parse(content, { breaks: true });
        },

        scrollToBottom() {
            this.$nextTick(() => {
                const area = this.$refs.chatArea;
                if (area) area.scrollTop = area.scrollHeight;
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
            this.scrollToBottom();

            const assistantMsg = { role: 'assistant', content: '', sources: [], streaming: true };
            this.messages.push(assistantMsg);
            const msgIndex = this.messages.length - 1;

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
                    const lines = buffer.split('\n');
                    buffer = lines.pop() || '';

                    for (const line of lines) {
                        if (line.startsWith('event: ')) {
                            currentEvent = line.slice(7).trim();
                            continue;
                        }
                        if (!line.startsWith('data: ')) continue;
                        const data = line.slice(6);

                        if (currentEvent === 'token') {
                            this.messages[msgIndex].content += data;
                            this.scrollToBottom();
                        } else if (currentEvent === 'done') {
                            try {
                                const doneData = JSON.parse(data);
                                this.messages[msgIndex].sources = doneData.sources || [];
                                if (doneData.usage) {
                                    this.groqUsage = doneData.usage;
                                }
                            } catch (e) {}
                            this.messages[msgIndex].streaming = false;
                        } else if (currentEvent === 'error') {
                            this.messages[msgIndex].content += '\n\n*Error: ' + data + '*';
                            this.messages[msgIndex].streaming = false;
                        }
                        currentEvent = 'token';
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
