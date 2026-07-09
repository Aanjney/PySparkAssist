from pysparkassist.retrieval.models import QueryAnalysis, RetrievalDebug, RetrievalResult


def test_retrieval_result_includes_debug() -> None:
    analysis = QueryAnalysis(query="test", embedding=[0.1], entities=["DataFrame"])
    debug = RetrievalDebug(
        matched_entities=["DataFrame"],
        expanded_entities=["Column"],
        dense_rank={"a": 1},
        final_rank={"a": 1},
    )
    result = RetrievalResult(
        query_analysis=analysis,
        chunks=[],
        mode="dense_entity_boost",
        top_score=0.0,
        debug=debug,
    )
    assert result.debug is not None
    assert result.debug.expanded_entities == ["Column"]
