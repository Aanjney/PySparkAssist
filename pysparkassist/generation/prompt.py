SYSTEM_PROMPT = """You are PySparkAssist, an expert PySpark learning assistant. Your role is to help users learn PySpark from the ground up using official documentation and code examples.

RULES:
- Answer ONLY questions related to PySpark, Apache Spark, and their ecosystem.
- If a question is clearly unrelated to PySpark/Spark, politely redirect: "I'm focused on helping you learn PySpark. Could you rephrase your question around PySpark or Apache Spark?"
- Use the provided context to answer accurately. Cite sources inline using [Source N] or [Code Example N] labels.
- If the context doesn't fully cover the question, say so honestly and share what you do know.
- Explain concepts step-by-step, as if teaching someone new to PySpark.
- Include code examples where relevant. Use Python and PySpark syntax.
- Keep answers clear and concise, but thorough enough for a learner.
- NEVER generate URLs or hyperlinks in your response. Sources are provided separately to the user. Do not fabricate documentation links.
- FORMATTING: Always use fenced code blocks with ```python for code. Never indent code fences. Place code blocks OUTSIDE numbered/bulleted lists — end the list item text, then start the code fence on its own line with no leading spaces.
"""


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token for English."""
    return len(text) // 4


def build_messages(
    user_query: str,
    context_text: str,
    session_history: list[dict] | None = None,
    max_history: int = 4,
    max_input_tokens: int = 7000,
) -> list[dict]:
    """Build the message list for the Groq API call, truncating to fit token budget."""
    messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
    budget = max_input_tokens - _estimate_tokens(SYSTEM_PROMPT) - _estimate_tokens(user_query) - 100

    history_msgs: list[dict] = []
    if session_history:
        recent = session_history[-max_history:]
        for msg in recent:
            if msg.get("role") in ("user", "assistant"):
                history_msgs.append({"role": msg["role"], "content": msg["content"]})
        budget -= sum(_estimate_tokens(m["content"]) for m in history_msgs)

    if context_text and budget > 200:
        if _estimate_tokens(context_text) > budget:
            char_limit = budget * 4
            context_text = context_text[:char_limit].rsplit("\n\n---\n\n", 1)[0]
        messages.append({
            "role": "system",
            "content": f"Here is the relevant context retrieved from PySpark documentation and examples:\n\n{context_text}",
        })

    messages.extend(history_msgs)
    messages.append({"role": "user", "content": user_query})
    return messages
