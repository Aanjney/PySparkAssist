from pysparkassist.ingest.chunker import Chunk
from pysparkassist.ingest.entities import extract_entities_from_chunk


def test_entity_extraction_filters_print() -> None:
    chunk = Chunk(
        content="print('hello')\ndf.select('x').printSchema()\nlen([1, 2])",
        metadata={"source_url": "https://example.com/example.py"},
    )
    names = {e.name for e in extract_entities_from_chunk(chunk)}
    assert "print" not in names
    assert "len" not in names
    assert "select" in names


def test_entity_extraction_keeps_pyspark_class() -> None:
    chunk = Chunk(
        content="spark = SparkSession.builder.getOrCreate()\ndf = spark.createDataFrame([])",
        metadata={"source_url": "https://example.com/doc"},
    )
    names = {e.name for e in extract_entities_from_chunk(chunk)}
    assert "SparkSession" in names
    assert "DataFrame" in names
