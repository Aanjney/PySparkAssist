from pysparkassist.retrieval.searcher import SearchResult, merge_results


def test_merge_results_deduplicates():
    vector_results = [
        SearchResult(chunk_id="a", content="chunk a", score=0.9, metadata={}),
        SearchResult(chunk_id="b", content="chunk b", score=0.8, metadata={}),
    ]
    graph_results = [
        SearchResult(chunk_id="b", content="chunk b", score=0.7, metadata={}),
        SearchResult(chunk_id="c", content="chunk c", score=0.6, metadata={}),
    ]
    merged = merge_results(vector_results, graph_results, boost=0.1)
    ids = [r.chunk_id for r in merged]
    assert len(set(ids)) == len(ids)
    b_result = next(r for r in merged if r.chunk_id == "b")
    assert b_result.score > 0.8


def test_merge_results_respects_limit():
    vector_results = [SearchResult(chunk_id=f"v{i}", content=f"v{i}", score=0.9 - i * 0.1, metadata={}) for i in range(5)]
    merged = merge_results(vector_results, [], top_n=3)
    assert len(merged) == 3
