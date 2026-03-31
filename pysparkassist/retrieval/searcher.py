import re
from dataclasses import dataclass

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchAny

from pysparkassist.config import COLLECTION_NAME
from pysparkassist.ingest.entities import EntityGraph
from pysparkassist.retrieval.graph_expander import expand_entities


@dataclass
class SearchResult:
    chunk_id: str
    content: str
    score: float
    metadata: dict
    retrieval_reason: str = ""


def merge_results(
    vector_results: list[SearchResult],
    graph_results: list[SearchResult],
    boost: float = 0.1,
    top_n: int = 8,
) -> list[SearchResult]:
    """Merge vector and graph-boosted results, boost overlaps."""
    by_id: dict[str, SearchResult] = {}

    for r in vector_results:
        by_id[r.chunk_id] = r

    for r in graph_results:
        if r.chunk_id in by_id:
            by_id[r.chunk_id].score += boost
            existing = by_id[r.chunk_id]
            if "knowledge graph" not in existing.retrieval_reason:
                existing.retrieval_reason = existing.retrieval_reason.replace(
                    "semantic similarity", "semantic similarity + knowledge graph"
                )
        else:
            by_id[r.chunk_id] = r

    ranked = sorted(by_id.values(), key=lambda r: r.score, reverse=True)
    return ranked[:top_n]


_SECTION_LINK_RE = re.compile(r'\[#?\]\([^)]*\)')


class Searcher:
    def __init__(self, client: QdrantClient, graph: EntityGraph, collection_name: str = COLLECTION_NAME):
        self.client = client
        self.graph = graph
        self.collection_name = collection_name

    @staticmethod
    def _clean_section(raw: str) -> str:
        return _SECTION_LINK_RE.sub('', raw).strip()

    def _build_reason(self, payload: dict, via: str = "semantic similarity") -> str:
        content_type = payload.get("content_type", "documentation")
        section = self._clean_section(payload.get("section_path", ""))
        version = payload.get("doc_version", "")
        file_path = payload.get("file_path", "")

        if content_type == "code_example" and file_path:
            return f"Python example from {file_path} — matched via {via}"
        parts = []
        if section:
            parts.append(section)
        if version:
            parts.append(f"PySpark {version}")
        location = " — ".join(parts) if parts else "PySpark documentation"
        return f"From {location} — matched via {via}"

    def vector_search(self, embedding: list[float], top_k: int = 10) -> list[SearchResult]:
        hits = self.client.query_points(
            collection_name=self.collection_name,
            query=embedding,
            limit=top_k,
        ).points

        return [
            SearchResult(
                chunk_id=h.payload.get("chunk_id", str(h.id)),
                content=h.payload.get("content", ""),
                score=h.score,
                metadata={k: v for k, v in h.payload.items() if k not in ("content", "chunk_id")},
                retrieval_reason=self._build_reason(h.payload, "semantic similarity"),
            )
            for h in hits
        ]

    def graph_boosted_search(self, embedding: list[float], entity_names: list[str], top_m: int = 5) -> list[SearchResult]:
        expanded = expand_entities(entity_names, self.graph)
        if not expanded:
            return []

        query_filter = Filter(
            must=[FieldCondition(key="entity_names", match=MatchAny(any=list(expanded)))]
        )

        hits = self.client.query_points(
            collection_name=self.collection_name,
            query=embedding,
            query_filter=query_filter,
            limit=top_m,
        ).points

        return [
            SearchResult(
                chunk_id=h.payload.get("chunk_id", str(h.id)),
                content=h.payload.get("content", ""),
                score=h.score,
                metadata={k: v for k, v in h.payload.items() if k not in ("content", "chunk_id")},
                retrieval_reason=self._build_reason(h.payload, f"knowledge graph ({', '.join(entity_names)})"),
            )
            for h in hits
        ]

    def search(self, embedding: list[float], entity_names: list[str]) -> list[SearchResult]:
        vector_results = self.vector_search(embedding)
        graph_results = self.graph_boosted_search(embedding, entity_names)
        return merge_results(vector_results, graph_results)
