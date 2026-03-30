import logging
import re
import sqlite3
from dataclasses import dataclass

from pysparkassist.ingest.chunker import Chunk

logger = logging.getLogger(__name__)

PYSPARK_CLASSES = {
    "SparkSession", "DataFrame", "Column", "Row", "GroupedData",
    "DataFrameReader", "DataFrameWriter", "SparkContext", "RDD",
    "StreamingQuery", "Window", "WindowSpec", "DataStreamReader",
    "DataStreamWriter", "Catalog", "UDFRegistration",
    "Pipeline", "Estimator", "Transformer", "Evaluator",
    "CrossValidator", "TrainValidationSplit",
}

PYSPARK_MODULES = {
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.window", "pyspark.ml", "pyspark.ml.feature",
    "pyspark.ml.classification", "pyspark.ml.regression",
    "pyspark.ml.clustering", "pyspark.ml.pipeline",
    "pyspark.streaming", "pyspark.pandas", "pyspark.sql.streaming",
}

DOMAIN_TERMS = {
    "ingestion", "ingest", "etl", "pipeline", "partition", "partitioning",
    "repartition", "coalesce", "broadcast", "shuffle", "cache", "persist",
    "unpersist", "schema", "parquet", "avro", "orc", "csv", "json",
    "jdbc", "hive", "delta", "iceberg", "catalyst", "tungsten",
    "udf", "udaf", "udtf", "aggregate", "aggregation", "groupby",
    "join", "crossjoin", "filter", "select", "withcolumn",
    "mappartitions", "foreachpartition", "collect", "take", "show",
    "explain", "checkpoint", "bucketing", "skew", "spill",
    "executor", "driver", "cluster", "yarn", "mesos", "kubernetes",
    "spark submit", "sparksubmit", "spark-submit", "sparksession",
    "dataframe", "dataset", "rdd", "resilient distributed",
    "lazy evaluation", "transformation", "action", "dag",
    "serialization", "deserialization", "kryo", "arrow",
    "vectorized", "pandas udf", "window function",
    "structured streaming", "dstream", "watermark", "trigger",
    "read", "write", "load", "save", "format", "option",
    "sql", "createtempview", "createglobaltempview",
    "ml", "mllib", "feature engineering", "model", "fit", "transform",
}


@dataclass
class Entity:
    name: str
    entity_type: str  # module, class, method, concept
    module: str = ""


def extract_entities_from_chunk(chunk: Chunk) -> list[Entity]:
    """Extract PySpark entities from a chunk using pattern matching."""
    entities: list[Entity] = []
    seen: set[str] = set()
    content = chunk.content

    for cls_name in PYSPARK_CLASSES:
        if cls_name in content and cls_name not in seen:
            entities.append(Entity(name=cls_name, entity_type="class", module="pyspark"))
            seen.add(cls_name)

    for mod in PYSPARK_MODULES:
        if mod in content and mod not in seen:
            entities.append(Entity(name=mod, entity_type="module", module=mod))
            seen.add(mod)

    method_pattern = re.compile(r"\.(\w+)\s*\(")
    for match in method_pattern.finditer(content):
        method_name = match.group(1)
        if method_name not in seen and not method_name.startswith("_") and len(method_name) > 2:
            entities.append(Entity(name=method_name, entity_type="method"))
            seen.add(method_name)

    section_path = chunk.metadata.get("section_path", "")
    fqn_pattern = re.compile(r"pyspark\.\w+(?:\.\w+)*\.(\w+)")
    for match in fqn_pattern.finditer(section_path):
        name = match.group(1)
        if name not in seen:
            entities.append(Entity(name=name, entity_type="method", module=section_path))
            seen.add(name)

    return entities


class EntityGraph:
    """SQLite-backed entity relationship graph."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def initialize(self) -> None:
        cur = self.conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                module TEXT DEFAULT '',
                UNIQUE(name, entity_type)
            );
            CREATE TABLE IF NOT EXISTS relationships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                target_name TEXT NOT NULL,
                rel_type TEXT NOT NULL,
                UNIQUE(source_name, target_name, rel_type)
            );
            CREATE TABLE IF NOT EXISTS chunk_entities (
                chunk_id TEXT NOT NULL,
                entity_name TEXT NOT NULL,
                PRIMARY KEY (chunk_id, entity_name)
            );
            CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name);
            CREATE INDEX IF NOT EXISTS idx_rel_source ON relationships(source_name);
            CREATE INDEX IF NOT EXISTS idx_rel_target ON relationships(target_name);
            CREATE INDEX IF NOT EXISTS idx_chunk_ent ON chunk_entities(entity_name);
        """)
        self.conn.commit()

    def add_entity(self, entity: Entity) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO entities (name, entity_type, module) VALUES (?, ?, ?)",
            (entity.name, entity.entity_type, entity.module),
        )
        self.conn.commit()

    def add_relationship(self, source: str, target: str, rel_type: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO relationships (source_name, target_name, rel_type) VALUES (?, ?, ?)",
            (source, target, rel_type),
        )
        self.conn.commit()

    def link_chunk_entities(self, chunk_id: str, entity_names: list[str]) -> None:
        for name in entity_names:
            self.conn.execute(
                "INSERT OR IGNORE INTO chunk_entities (chunk_id, entity_name) VALUES (?, ?)",
                (chunk_id, name),
            )
        self.conn.commit()

    def get_related_entities(self, name: str) -> list[Entity]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT e.name, e.entity_type, e.module FROM entities e
            WHERE e.name IN (
                SELECT target_name FROM relationships WHERE source_name = ?
                UNION
                SELECT source_name FROM relationships WHERE target_name = ?
            )
            """,
            (name, name),
        ).fetchall()
        return [Entity(name=r["name"], entity_type=r["entity_type"], module=r["module"]) for r in rows]

    def get_entity_ids_for_chunk(self, chunk_id: str) -> list[str]:
        rows = self.conn.execute(
            "SELECT entity_name FROM chunk_entities WHERE chunk_id = ?", (chunk_id,)
        ).fetchall()
        return [r["entity_name"] for r in rows]

    def find_entities_by_names(self, names: list[str]) -> list[Entity]:
        if not names:
            return []
        placeholders = ",".join("?" for _ in names)
        rows = self.conn.execute(
            f"SELECT name, entity_type, module FROM entities WHERE name IN ({placeholders})", names
        ).fetchall()
        return [Entity(name=r["name"], entity_type=r["entity_type"], module=r["module"]) for r in rows]

    def clear_relationships(self) -> None:
        self.conn.execute("DELETE FROM relationships")
        self.conn.commit()

    def relationship_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM relationships").fetchone()[0]

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


# Entities that are too generic to form meaningful relationships
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
    # SparkSession creation
    ("SparkSession", "DataFrame", "creates"),
    ("SparkSession", "DataFrameReader", "provides"),
    ("SparkSession", "Catalog", "provides"),
    ("SparkSession", "SparkContext", "wraps"),
    ("SparkSession", "UDFRegistration", "provides"),
    # DataFrame core
    ("DataFrame", "Column", "contains"),
    ("DataFrame", "Row", "contains"),
    ("DataFrame", "GroupedData", "produces_via_groupBy"),
    ("DataFrame", "DataFrameWriter", "provides"),
    ("DataFrame", "DataStreamReader", "related_to"),
    # DataFrame ↔ methods
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
    # I/O
    ("DataFrameReader", "parquet", "reads_format"),
    ("DataFrameReader", "csv", "reads_format"),
    ("DataFrameReader", "json", "reads_format"),
    ("DataFrameReader", "orc", "reads_format"),
    ("DataFrameReader", "jdbc", "reads_format"),
    ("DataFrameWriter", "parquet", "writes_format"),
    ("DataFrameWriter", "csv", "writes_format"),
    ("DataFrameWriter", "json", "writes_format"),
    # Caching
    ("cache", "persist", "related_to"),
    ("persist", "unpersist", "inverse_of"),
    ("cache", "unpersist", "inverse_of"),
    # ML pipeline
    ("Pipeline", "Estimator", "contains"),
    ("Pipeline", "Transformer", "contains"),
    ("Estimator", "fit", "has_method"),
    ("Transformer", "transform", "has_method"),
    ("Evaluator", "evaluate", "has_method"),
    ("CrossValidator", "Pipeline", "uses"),
    ("TrainValidationSplit", "Pipeline", "uses"),
    # Window
    ("Window", "WindowSpec", "creates"),
    # RDD
    ("RDD", "SparkContext", "created_by"),
    ("SparkContext", "RDD", "creates"),
    # Streaming
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
