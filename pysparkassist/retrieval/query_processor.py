import re

from sentence_transformers import SentenceTransformer

from pysparkassist.ingest.constants import PYSPARK_CLASSES, DOMAIN_TERMS
from pysparkassist.ingest.entities import EntityGraph
from pysparkassist.retrieval.models import QueryAnalysis

# ponytail: phrase -> entity name; expand when evals show missed aliases
STREAMING_ALIASES: list[tuple[str, str]] = [
    (r"write\s+stream(?:ing)?", "writeStream"),
    (r"streaming\s+sink", "writeStream"),
    (r"read\s+stream(?:ing)?", "readStream"),
    (r"streaming\s+source", "readStream"),
    (r"\bwatermark(?:ing)?\b", "watermark"),
    (r"\boutput\s+mode\b", "outputMode"),
    (r"\bawait\s*termination\b", "awaitTermination"),
    (r"\bwith\s*watermark\b", "withWatermark"),
]


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
        found: list[str] = []
        q_lower = query.lower()

        for cls_name in PYSPARK_CLASSES:
            if re.search(rf"\b{re.escape(cls_name)}\b", query, re.I):
                found.append(cls_name)

        words = re.findall(r"\b\w+\b", query)
        for word in words:
            if word in self.entity_names and word not in found:
                found.append(word)

        for pattern, entity in STREAMING_ALIASES:
            if re.search(pattern, q_lower) and entity not in found:
                found.append(entity)

        return found

    def has_domain_relevance(self, query: str) -> bool:
        q_lower = query.lower()
        return any(term in q_lower for term in DOMAIN_TERMS)

    def process(self, query: str) -> QueryAnalysis:
        return QueryAnalysis(
            query=query,
            embedding=self.embed_query(query),
            entities=self.extract_query_entities(query),
            domain_relevant=self.has_domain_relevance(query),
        )
