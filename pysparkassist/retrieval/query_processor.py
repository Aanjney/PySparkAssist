import re

from sentence_transformers import SentenceTransformer

from pysparkassist.ingest.entities import EntityGraph, PYSPARK_CLASSES


class QueryProcessor:
    def __init__(self, model: SentenceTransformer, graph: EntityGraph):
        self.model = model
        self.graph = graph
        self._entity_names: set[str] | None = None

    @property
    def entity_names(self) -> set[str]:
        if self._entity_names is None:
            rows = self.graph.conn.execute("SELECT name FROM entities").fetchall()
            self._entity_names = {r["name"] for r in rows}
        return self._entity_names

    def embed_query(self, query: str) -> list[float]:
        return self.model.encode(query, normalize_embeddings=True).tolist()

    def extract_query_entities(self, query: str) -> list[str]:
        """Match known entity names in the user's query."""
        found: list[str] = []

        for cls_name in PYSPARK_CLASSES:
            if cls_name.lower() in query.lower():
                found.append(cls_name)

        words = re.findall(r"\b\w+\b", query)
        for word in words:
            if word in self.entity_names and word not in found:
                found.append(word)

        return found

    def process(self, query: str) -> dict:
        embedding = self.embed_query(query)
        entities = self.extract_query_entities(query)
        return {"embedding": embedding, "entities": entities}
