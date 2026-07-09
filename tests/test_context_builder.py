from pysparkassist.retrieval.context_builder import build_context
from pysparkassist.retrieval.models import RetrievedChunk, SourceMetadata


def test_entity_boost_source_match_type() -> None:
    chunks = [
        RetrievedChunk(
            chunk_id="c1",
            content="example code",
            score=0.6,
            source=SourceMetadata(
                content_type="documentation",
                section_path="DataFrame",
                retrieval_reason="matched via entity-aware boost (DataFrame)",
            ),
            matched_by="entity_boost",
        ),
    ]
    result = build_context(chunks)
    assert result.sources[0]["match_type"] == "knowledge_graph"
