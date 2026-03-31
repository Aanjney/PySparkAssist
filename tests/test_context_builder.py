from pysparkassist.retrieval.context_builder import ContextResult, build_context
from pysparkassist.retrieval.searcher import SearchResult


def test_build_context_formats_chunks():
    results = [
        SearchResult(
            chunk_id="1",
            content="groupBy groups a DataFrame.",
            score=0.9,
            metadata={"source_url": "https://example.com/df", "content_type": "documentation"},
            retrieval_reason="semantic similarity",
        ),
        SearchResult(
            chunk_id="2",
            content="df.groupBy('col').count()",
            score=0.8,
            metadata={"file_path": "example.py", "content_type": "code_example"},
            retrieval_reason="knowledge graph (groupBy)",
        ),
    ]
    ctx = build_context(results)
    assert isinstance(ctx, ContextResult)
    assert "groupBy groups a DataFrame" in ctx.context_text
    assert len(ctx.sources) == 2
    assert ctx.sources[0]["match_type"] == "semantic"
    assert ctx.sources[0]["content_type"] == "documentation"
    assert ctx.sources[1]["match_type"] == "knowledge_graph"
    assert ctx.sources[1]["content_type"] == "code_example"


def test_build_context_respects_max_chunks():
    results = [
        SearchResult(chunk_id="1", content="first", score=0.9, metadata={}, retrieval_reason="match"),
        SearchResult(chunk_id="2", content="second", score=0.8, metadata={}, retrieval_reason="match"),
        SearchResult(chunk_id="3", content="third", score=0.7, metadata={}, retrieval_reason="match"),
    ]
    ctx = build_context(results, max_chunks=2)
    assert len(ctx.sources) == 2
