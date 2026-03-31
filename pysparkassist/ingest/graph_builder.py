import logging

from pysparkassist.ingest.entities import EntityGraph

logger = logging.getLogger(__name__)

_STOPLIST = {
    "pyspark", "pyspark.sql", "pyspark.ml", "show", "stop",
    "getOrCreate", "appName", "builder", "print", "len",
    "append", "map", "set", "get", "run", "add", "format",
    "option", "mode", "save", "load", "write", "read",
}


def build_co_occurrence_relationships(
    graph: EntityGraph,
    min_cooccurrences: int = 3,
    max_entity_freq: float = 0.5,
) -> int:
    """Infer relationships from entity co-occurrence in chunks.

    Pairs that appear together in >= min_cooccurrences chunks get a
    'co_occurs_with' edge. Entities appearing in more than max_entity_freq
    fraction of all chunks are excluded as too generic.
    """
    total_chunks = graph.conn.execute(
        "SELECT COUNT(DISTINCT chunk_id) FROM chunk_entities"
    ).fetchone()[0]
    if total_chunks == 0:
        return 0

    freq_cap = int(total_chunks * max_entity_freq)

    frequent = {
        r[0]
        for r in graph.conn.execute(
            "SELECT entity_name FROM chunk_entities GROUP BY entity_name HAVING COUNT(*) > ?",
            (freq_cap,),
        ).fetchall()
    }
    skip = _STOPLIST | frequent

    rows = graph.conn.execute(
        """
        SELECT ce1.entity_name AS src, ce2.entity_name AS tgt, COUNT(*) AS cnt
        FROM chunk_entities ce1
        JOIN chunk_entities ce2
            ON ce1.chunk_id = ce2.chunk_id AND ce1.entity_name < ce2.entity_name
        GROUP BY ce1.entity_name, ce2.entity_name
        HAVING cnt >= ?
        ORDER BY cnt DESC
        """,
        (min_cooccurrences,),
    ).fetchall()

    added = 0
    for src, tgt, cnt in rows:
        if src in skip or tgt in skip:
            continue
        graph.add_relationship(src, tgt, "co_occurs_with")
        added += 1

    logger.info("Added %d co-occurrence relationships (from %d candidate pairs)", added, len(rows))
    return added


CURATED_RELATIONSHIPS: list[tuple[str, str, str]] = [
    ("SparkSession", "DataFrame", "creates"),
    ("SparkSession", "DataFrameReader", "provides"),
    ("SparkSession", "Catalog", "provides"),
    ("SparkSession", "SparkContext", "wraps"),
    ("SparkSession", "UDFRegistration", "provides"),
    ("DataFrame", "Column", "contains"),
    ("DataFrame", "Row", "contains"),
    ("DataFrame", "GroupedData", "produces_via_groupBy"),
    ("DataFrame", "DataFrameWriter", "provides"),
    ("DataFrame", "DataStreamReader", "related_to"),
    ("DataFrame", "select", "has_method"),
    ("DataFrame", "filter", "has_method"),
    ("DataFrame", "join", "has_method"),
    ("DataFrame", "groupBy", "has_method"),
    ("DataFrame", "orderBy", "has_method"),
    ("DataFrame", "withColumn", "has_method"),
    ("DataFrame", "drop", "has_method"),
    ("DataFrame", "cache", "has_method"),
    ("DataFrame", "persist", "has_method"),
    ("DataFrame", "unpersist", "has_method"),
    ("DataFrame", "explain", "has_method"),
    ("DataFrame", "createDataFrame", "created_by"),
    ("DataFrameReader", "parquet", "reads_format"),
    ("DataFrameReader", "csv", "reads_format"),
    ("DataFrameReader", "json", "reads_format"),
    ("DataFrameReader", "orc", "reads_format"),
    ("DataFrameReader", "jdbc", "reads_format"),
    ("DataFrameWriter", "parquet", "writes_format"),
    ("DataFrameWriter", "csv", "writes_format"),
    ("DataFrameWriter", "json", "writes_format"),
    ("cache", "persist", "related_to"),
    ("persist", "unpersist", "inverse_of"),
    ("cache", "unpersist", "inverse_of"),
    ("Pipeline", "Estimator", "contains"),
    ("Pipeline", "Transformer", "contains"),
    ("Estimator", "fit", "has_method"),
    ("Transformer", "transform", "has_method"),
    ("Evaluator", "evaluate", "has_method"),
    ("CrossValidator", "Pipeline", "uses"),
    ("TrainValidationSplit", "Pipeline", "uses"),
    ("Window", "WindowSpec", "creates"),
    ("RDD", "SparkContext", "created_by"),
    ("SparkContext", "RDD", "creates"),
    ("DataStreamReader", "StreamingQuery", "produces"),
    ("DataStreamWriter", "StreamingQuery", "produces"),
]


def seed_curated_relationships(graph: EntityGraph) -> int:
    """Insert hand-picked PySpark entity relationships."""
    added = 0
    for src, tgt, rel_type in CURATED_RELATIONSHIPS:
        graph.add_relationship(src, tgt, rel_type)
        added += 1
    logger.info("Seeded %d curated relationships", added)
    return added


def build_graph(graph: EntityGraph) -> dict:
    """Run the full graph-building pipeline: curated seed + co-occurrence."""
    graph.clear_relationships()
    curated = seed_curated_relationships(graph)
    cooccur = build_co_occurrence_relationships(graph)
    total = graph.relationship_count()
    logger.info("Graph complete: %d total relationships (%d curated + %d co-occurrence)", total, curated, cooccur)
    return {"curated": curated, "co_occurrence": cooccur, "total": total}
