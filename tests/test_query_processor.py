from unittest.mock import MagicMock

import pytest

from pysparkassist.ingest.entities import EntityGraph
from pysparkassist.retrieval.query_processor import QueryProcessor


@pytest.fixture
def processor() -> QueryProcessor:
    graph = EntityGraph(":memory:")
    graph.initialize()
    graph.conn.execute(
        "INSERT INTO entities (name, entity_type, module) VALUES ('writeStream', 'method', '')"
    )
    graph.conn.execute(
        "INSERT INTO entities (name, entity_type, module) VALUES ('watermark', 'method', '')"
    )
    graph.commit()
    model = MagicMock()
    model.encode.return_value = [0.1, 0.2, 0.3]
    return QueryProcessor(model=model, graph=graph)


def test_row_not_matched_in_tomorrow(processor: QueryProcessor) -> None:
    entities = processor.extract_query_entities("schedule the job for tomorrow")
    assert "Row" not in entities


def test_row_matched_as_word(processor: QueryProcessor) -> None:
    entities = processor.extract_query_entities("How do I access a Row from a DataFrame?")
    assert "Row" in entities
    assert "DataFrame" in entities


def test_streaming_write_alias(processor: QueryProcessor) -> None:
    entities = processor.extract_query_entities("How do I write streaming results to parquet?")
    assert "writeStream" in entities


def test_streaming_sink_alias(processor: QueryProcessor) -> None:
    entities = processor.extract_query_entities("configure a streaming sink in PySpark")
    assert "writeStream" in entities


def test_watermark_alias(processor: QueryProcessor) -> None:
    entities = processor.extract_query_entities("What is a watermark in Structured Streaming?")
    assert "watermark" in entities
