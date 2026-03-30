from pysparkassist.ingest.entities import (
    extract_entities_from_chunk,
    EntityGraph,
    Entity,
)
from pysparkassist.ingest.chunker import Chunk


def test_extract_entities_from_doc_chunk():
    chunk = Chunk(
        content="## pyspark.sql.DataFrame.groupBy\nGroups the DataFrame using the specified columns.\nReturns a GroupedData object.",
        metadata={"content_type": "documentation", "section_path": "pyspark.sql.DataFrame.groupBy"},
    )
    entities = extract_entities_from_chunk(chunk)
    names = {e.name for e in entities}
    assert "groupBy" in names
    assert "DataFrame" in names
    assert "GroupedData" in names


def test_extract_entities_from_code_chunk():
    chunk = Chunk(
        content='spark = SparkSession.builder.getOrCreate()\ndf = spark.read.csv("data.csv")\ndf.groupBy("col").count().show()',
        metadata={"content_type": "code_example", "file_path": "example.py"},
    )
    entities = extract_entities_from_chunk(chunk)
    names = {e.name for e in entities}
    assert "SparkSession" in names


def test_entity_graph_stores_and_retrieves(tmp_path):
    db_path = tmp_path / "test_graph.db"
    graph = EntityGraph(str(db_path))
    graph.initialize()

    graph.add_entity(Entity(name="DataFrame", entity_type="class", module="pyspark.sql"))
    graph.add_entity(Entity(name="groupBy", entity_type="method", module="pyspark.sql.DataFrame"))
    graph.add_relationship("groupBy", "DataFrame", "belongs_to")

    related = graph.get_related_entities("groupBy")
    related_names = {e.name for e in related}
    assert "DataFrame" in related_names


def test_entity_graph_link_chunk(tmp_path):
    db_path = tmp_path / "test_graph.db"
    graph = EntityGraph(str(db_path))
    graph.initialize()

    graph.add_entity(Entity(name="DataFrame", entity_type="class", module="pyspark.sql"))
    graph.link_chunk_entities("chunk_001", ["DataFrame"])
    entity_ids = graph.get_entity_ids_for_chunk("chunk_001")
    assert len(entity_ids) >= 1
