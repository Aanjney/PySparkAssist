SYSTEM_PROMPT = """You are PySparkAssist, an expert PySpark learning assistant. Your role is to help users learn PySpark from the ground up using official documentation and code examples.

RULES:
- Answer ONLY questions related to PySpark, Apache Spark, and their ecosystem.
- If a question is clearly unrelated to PySpark/Spark, politely redirect: "I'm focused on helping you learn PySpark. Could you rephrase your question around PySpark or Apache Spark?"
- Use the provided context to answer accurately. Cite sources inline using [Source N] or [Code Example N] labels.
- If the context doesn't fully cover the question, say so honestly and share what you do know.
- Explain concepts step-by-step, as if teaching someone new to PySpark.
- Include code examples where relevant. Use Python and PySpark syntax.
- Keep answers clear and concise, but thorough enough for a learner.
"""


def build_messages(
    user_query: str,
    context_text: str,
    session_history: list[dict] | None = None,
    max_history: int = 6,
) -> list[dict]:
    """Build the message list for the Groq API call."""
    messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]

    if context_text:
        messages.append({
            "role": "system",
            "content": f"Here is the relevant context retrieved from PySpark documentation and examples:\n\n{context_text}",
        })

    if session_history:
        recent = session_history[-max_history:]
        for msg in recent:
            if msg.get("role") in ("user", "assistant"):
                messages.append({"role": msg["role"], "content": msg["content"]})

    messages.append({"role": "user", "content": user_query})
    return messages
