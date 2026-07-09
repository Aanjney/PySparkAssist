import pytest

from pysparkassist.retrieval.models import RetrievedChunk, SourceMetadata
from pysparkassist.retrieval.retriever import merge_chunks, rrf_merge


def _chunk(chunk_id: str, score: float, matched_by: str = "dense") -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        content=f"content-{chunk_id}",
        score=score,
        source=SourceMetadata(retrieval_reason="matched via semantic similarity"),
        matched_by=matched_by,
    )


def test_entity_boost_changes_ranking() -> None:
    vector = [_chunk("a", 0.50), _chunk("b", 0.48)]
    entity = [_chunk("b", 0.45, matched_by="entity_boost")]

    merged = merge_chunks(vector, entity, boost=0.1)

    assert [c.chunk_id for c in merged] == ["b", "a"]
    assert merged[0].score == pytest.approx(0.58)
    assert merged[0].matched_by == "entity_boost"


def test_rrf_merge_prefers_overlap() -> None:
    dense = [_chunk("a", 0.9), _chunk("b", 0.8), _chunk("c", 0.7)]
    bm25 = [_chunk("b", 3.0, matched_by="bm25"), _chunk("d", 2.0, matched_by="bm25")]

    merged = rrf_merge([dense, bm25], top_n=3)

    assert merged[0].chunk_id == "b"
    assert merged[0].matched_by == "hybrid"
    assert {c.chunk_id for c in merged} == {"b", "a", "d"}
