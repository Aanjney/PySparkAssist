from pysparkassist.evals.metrics import (
    abstention_accuracy,
    chunk_is_hit,
    entity_match_rate,
    first_hit_rank,
    hit_at_k,
    mrr,
)
from pysparkassist.retrieval.models import RetrievedChunk, SourceMetadata


def _chunk(
    chunk_id: str,
    content: str = "",
    section: str = "",
    entities: list[str] | None = None,
) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        content=content,
        score=1.0,
        source=SourceMetadata(section_path=section),
        entity_names=entities or [],
    )


def test_hit_at_k_finds_match_in_top_k() -> None:
    chunks = [
        _chunk("a", content="unrelated"),
        _chunk("b", content="DataFrame.select example"),
    ]
    assert hit_at_k(chunks, ["select"], ["DataFrame"], k=2) is True
    assert hit_at_k(chunks, ["select"], ["DataFrame"], k=1) is False


def test_hit_at_k_entity_in_chunk_names() -> None:
    chunks = [_chunk("a", entities=["join", "DataFrame"])]
    assert hit_at_k(chunks, [], ["join"], k=1) is True


def test_first_hit_rank_one_indexed() -> None:
    chunks = [
        _chunk("a", content="miss"),
        _chunk("b", content="parquet read"),
    ]
    assert first_hit_rank(chunks, ["parquet"], [], k=8) == 2


def test_first_hit_rank_none_when_no_match() -> None:
    chunks = [_chunk("a", content="nothing here")]
    assert first_hit_rank(chunks, ["orc"], ["RDD"], k=3) is None


def test_mrr_averages_reciprocal_ranks() -> None:
    assert mrr([1, 2, None]) == (1.0 + 0.5 + 0.0) / 3


def test_mrr_empty() -> None:
    assert mrr([]) == 0.0


def test_entity_match_rate_partial() -> None:
    rate = entity_match_rate(
        ["DataFrame", "select", "join"],
        query_entities=["DataFrame"],
        chunk_entities=["select"],
    )
    assert rate == 2 / 3


def test_entity_match_rate_empty_expected() -> None:
    assert entity_match_rate([], ["DataFrame"], []) == 1.0


def test_abstention_accuracy() -> None:
    assert abstention_accuracy(True, True) is True
    assert abstention_accuracy(False, True) is False
    assert abstention_accuracy(False, False) is True


def test_chunk_is_hit_section_path() -> None:
    chunk = _chunk("x", section="API Reference > DataFrameWriter.parquet")
    assert chunk_is_hit(chunk, ["DataFrameWriter"], []) is True
