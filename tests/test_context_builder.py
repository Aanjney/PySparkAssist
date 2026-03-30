from pysparkassist.retrieval.context_builder import ContextResult, build_context
from pysparkassist.retrieval.searcher import SearchResult


def test_build_context_formats_chunks():
    results = [
        SearchResult(
            chunk_id="1",
            content="groupBy groups a DataFrame.",
            score=0.9,
            metadata={"source_url": "https://example.com/df", "content_type": "documentation"},
            retrieval_reason="Direct match",
        ),
        SearchResult(
            chunk_id="2",
            content="df.groupBy('col').count()",
            score=0.8,
            metadata={"file_path": "example.py", "content_type": "code_example"},
            retrieval_reason="Example — demonstrates groupBy",
        ),
    ]
    ctx = build_context(results)
    assert isinstance(ctx, ContextResult)
    assert "groupBy groups a DataFrame" in ctx.context_text
    assert len(ctx.sources) == 2
    assert ctx.sources[0]["reason"] == "Direct match"


def test_build_context_respects_threshold():
    results = [
        SearchResult(chunk_id="1", content="relevant", score=0.9, metadata={}, retrieval_reason="match"),
        SearchResult(chunk_id="2", content="irrelevant", score=0.1, metadata={}, retrieval_reason="match"),
    ]
    ctx = build_context(results, relevance_threshold=0.3)
    assert len(ctx.sources) == 1
