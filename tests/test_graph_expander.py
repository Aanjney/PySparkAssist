from pysparkassist.ingest.entities import Entity, EntityGraph
from pysparkassist.retrieval.graph_expander import expand_entities


def test_expand_entities_finds_related(tmp_path):
    db_path = tmp_path / "graph.db"
    graph = EntityGraph(str(db_path))
    graph.initialize()

    graph.add_entity(Entity(name="DataFrame", entity_type="class", module="pyspark.sql"))
    graph.add_entity(Entity(name="groupBy", entity_type="method", module="pyspark.sql.DataFrame"))
    graph.add_entity(Entity(name="GroupedData", entity_type="class", module="pyspark.sql"))
    graph.add_relationship("groupBy", "DataFrame", "belongs_to")
    graph.add_relationship("groupBy", "GroupedData", "returns")

    expanded = expand_entities(["groupBy"], graph)
    assert "DataFrame" in expanded
    assert "GroupedData" in expanded
    assert "groupBy" in expanded


def test_expand_entities_empty_input(tmp_path):
    db_path = tmp_path / "graph.db"
    graph = EntityGraph(str(db_path))
    graph.initialize()
    expanded = expand_entities([], graph)
    assert expanded == set()
