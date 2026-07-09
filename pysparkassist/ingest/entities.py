import re
import sqlite3
from dataclasses import dataclass

from pysparkassist.ingest.chunker import Chunk
from pysparkassist.ingest.constants import (
    PYSPARK_CLASSES,
    PYSPARK_METHODS,
    PYSPARK_MODULES,
    PYTHON_BUILTINS,
)


@dataclass
class Entity:
    name: str
    entity_type: str
    module: str = ""


def extract_entities_from_chunk(chunk: Chunk) -> list[Entity]:
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
        if method_name in seen or method_name.startswith("_") or len(method_name) <= 2:
            continue
        if method_name in PYTHON_BUILTINS or method_name not in PYSPARK_METHODS:
            continue
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

    def clear_all(self) -> None:
        self.conn.executescript("""
            DELETE FROM chunk_entities;
            DELETE FROM relationships;
            DELETE FROM entities;
        """)
        self.conn.commit()

    def commit(self) -> None:
        self.conn.commit()

    def add_entity(self, entity: Entity) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO entities (name, entity_type, module) VALUES (?, ?, ?)",
            (entity.name, entity.entity_type, entity.module),
        )

    def add_relationship(self, source: str, target: str, rel_type: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO relationships (source_name, target_name, rel_type) VALUES (?, ?, ?)",
            (source, target, rel_type),
        )

    def link_chunk_entities_batch(self, links: list[tuple[str, str]]) -> None:
        self.conn.executemany(
            "INSERT OR IGNORE INTO chunk_entities (chunk_id, entity_name) VALUES (?, ?)",
            links,
        )

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

    def clear_relationships(self) -> None:
        self.conn.execute("DELETE FROM relationships")

    def entity_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    def relationship_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM relationships").fetchone()[0]

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
