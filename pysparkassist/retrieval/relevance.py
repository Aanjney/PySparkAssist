from pydantic import BaseModel

from pysparkassist.chat.schemas import ChatHistoryMessage
from pysparkassist.config import Settings
from pysparkassist.retrieval.models import QueryAnalysis, RetrievalResult

_OFF_TOPIC_MESSAGE = (
    "I'm designed to help with PySpark — could you rephrase your question "
    "around PySpark or Apache Spark?"
)
_NO_CONTEXT_MESSAGE = (
    "I couldn't find relevant PySpark documentation for your question. "
    "Could you try rephrasing it?"
)


class RelevanceDecision(BaseModel):
    should_answer: bool
    reason: str
    user_message: str | None = None


def _has_pyspark_history(history: list[ChatHistoryMessage]) -> bool:
    for msg in history:
        lower = msg.content.lower()
        if "pyspark" in lower or "spark" in lower or "dataframe" in lower:
            return True
    return False


def classify_relevance(
    query_analysis: QueryAnalysis,
    retrieval_result: RetrievalResult,
    history: list[ChatHistoryMessage],
    settings: Settings,
) -> RelevanceDecision:
    top_score = retrieval_result.top_score
    has_entities = len(query_analysis.entities) > 0
    domain_relevant = query_analysis.domain_relevant
    pyspark_history = _has_pyspark_history(history)

    if not retrieval_result.chunks:
        return RelevanceDecision(
            should_answer=False,
            reason="no_context",
            user_message=_NO_CONTEXT_MESSAGE,
        )

    if top_score > 0 and top_score < settings.relevance_threshold:
        return RelevanceDecision(
            should_answer=False,
            reason="low_relevance",
            user_message=_OFF_TOPIC_MESSAGE,
        )

    if (
        not has_entities
        and not domain_relevant
        and not pyspark_history
        and top_score < settings.off_topic_score_threshold
    ):
        return RelevanceDecision(
            should_answer=False,
            reason="out_of_domain",
            user_message=_OFF_TOPIC_MESSAGE,
        )

    return RelevanceDecision(should_answer=True, reason="in_domain")
