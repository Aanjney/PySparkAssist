import pytest

from pysparkassist.chat.schemas import ChatHistoryMessage
from pysparkassist.config import Settings
from pysparkassist.retrieval.models import (
    QueryAnalysis,
    RetrievedChunk,
    RetrievalResult,
    SourceMetadata,
)
from pysparkassist.retrieval.relevance import classify_relevance


def _settings(**overrides) -> Settings:
    return Settings(
        groq_api_key="test-key",
        groq_model="test-model",
        embedding_model="test-embed",
        **overrides,
    )


def _analysis(**overrides) -> QueryAnalysis:
    defaults = {
        "query": "hello",
        "embedding": [0.1],
        "entities": [],
        "domain_relevant": False,
    }
    defaults.update(overrides)
    return QueryAnalysis(**defaults)


def _chunk(score: float) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id="c1",
        content="docs",
        score=score,
        source=SourceMetadata(),
    )


def _result(analysis: QueryAnalysis, chunks: list[RetrievedChunk]) -> RetrievalResult:
    return RetrievalResult(
        query_analysis=analysis,
        chunks=chunks,
        mode="dense_entity_boost",
        top_score=chunks[0].score if chunks else 0.0,
    )


def test_out_of_domain_abstains() -> None:
    decision = classify_relevance(
        _analysis(),
        _result(_analysis(), [_chunk(0.40)]),
        [],
        _settings(off_topic_score_threshold=0.55),
    )
    assert decision.should_answer is False
    assert decision.reason == "out_of_domain"
    assert decision.user_message


def test_low_relevance_abstains() -> None:
    decision = classify_relevance(
        _analysis(domain_relevant=True),
        _result(_analysis(), [_chunk(0.30)]),
        [],
        _settings(relevance_threshold=0.35),
    )
    assert decision.should_answer is False
    assert decision.reason == "low_relevance"


def test_in_domain_with_entities() -> None:
    decision = classify_relevance(
        _analysis(entities=["DataFrame"], domain_relevant=False),
        _result(_analysis(), [_chunk(0.40)]),
        [],
        _settings(),
    )
    assert decision.should_answer is True
    assert decision.reason == "in_domain"


def test_in_domain_with_pyspark_history() -> None:
    history = [ChatHistoryMessage(role="user", content="How do I use spark.sql?")]
    decision = classify_relevance(
        _analysis(),
        _result(_analysis(), [_chunk(0.40)]),
        history,
        _settings(),
    )
    assert decision.should_answer is True
    assert decision.reason == "in_domain"


def test_no_context_abstains() -> None:
    analysis = _analysis()
    decision = classify_relevance(
        analysis,
        _result(analysis, []),
        [],
        _settings(),
    )
    assert decision.should_answer is False
    assert decision.reason == "no_context"
