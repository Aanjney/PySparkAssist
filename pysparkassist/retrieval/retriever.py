import re

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchAny
from rank_bm25 import BM25Okapi

from pysparkassist.config import COLLECTION_NAME, Settings
from pysparkassist.ingest.entities import EntityGraph
from pysparkassist.retrieval.graph_expander import expand_entities
from pysparkassist.retrieval.models import (
    QueryAnalysis,
    RetrievedChunk,
    RetrievalDebug,
    RetrievalResult,
    SourceMetadata,
)

_SECTION_LINK_RE = re.compile(r'\[#?\]\([^)]*\)')
_INDEX_SCORE_PENALTY = 0.5
_HYBRID_POOL = 20
_RRF_K = 60


def merge_chunks(
    vector_chunks: list[RetrievedChunk],
    entity_chunks: list[RetrievedChunk],
    boost: float = 0.1,
    top_n: int = 8,
) -> list[RetrievedChunk]:
    by_id: dict[str, RetrievedChunk] = {}

    for chunk in vector_chunks:
        by_id[chunk.chunk_id] = chunk.model_copy(deep=True)

    for chunk in entity_chunks:
        if chunk.chunk_id in by_id:
            existing = by_id[chunk.chunk_id]
            existing.score += boost
            if existing.matched_by == "dense":
                existing.matched_by = "entity_boost"
                existing.source.retrieval_reason = existing.source.retrieval_reason.replace(
                    "semantic similarity", "semantic similarity + entity-aware boost"
                )
        else:
            by_id[chunk.chunk_id] = chunk.model_copy(deep=True)

    return sorted(by_id.values(), key=lambda c: c.score, reverse=True)[:top_n]


def rrf_merge(
    ranked_lists: list[list[RetrievedChunk]],
    top_n: int = 8,
    k: int = _RRF_K,
) -> list[RetrievedChunk]:
    scores: dict[str, float] = {}
    chunks: dict[str, RetrievedChunk] = {}

    for chunk_list in ranked_lists:
        for rank, chunk in enumerate(chunk_list):
            cid = chunk.chunk_id
            scores[cid] = scores.get(cid, 0.0) + 1.0 / (k + rank + 1)
            if cid not in chunks:
                chunks[cid] = chunk.model_copy(deep=True)

    merged = sorted(chunks.values(), key=lambda c: scores[c.chunk_id], reverse=True)
    for chunk in merged:
        chunk.score = scores[chunk.chunk_id]
        if chunk.matched_by == "dense":
            chunk.matched_by = "hybrid"
    return merged[:top_n]


def _tokenize(text: str) -> list[str]:
    return re.findall(r"\b\w+\b", text.lower())


class Retriever:
    def __init__(
        self,
        client: QdrantClient,
        graph: EntityGraph,
        settings: Settings,
        collection_name: str = COLLECTION_NAME,
    ):
        self.client = client
        self.graph = graph
        self.settings = settings
        self.collection_name = collection_name
        self._bm25: BM25Okapi | None = None
        self._bm25_chunks: list[RetrievedChunk] = []

    @staticmethod
    def _clean_section(raw: str) -> str:
        return _SECTION_LINK_RE.sub("", raw).strip()

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

    def _payload_to_chunk(self, payload: dict, score: float, matched_by: str, via: str) -> RetrievedChunk:
        metadata = {k: v for k, v in payload.items() if k not in ("content", "chunk_id")}
        if metadata.get("content_type") == "index":
            score *= _INDEX_SCORE_PENALTY
        return RetrievedChunk(
            chunk_id=payload.get("chunk_id", ""),
            content=payload.get("content", ""),
            score=score,
            source=SourceMetadata(
                content_type=metadata.get("content_type", "documentation"),
                section_path=metadata.get("section_path", ""),
                source_url=metadata.get("source_url"),
                file_path=metadata.get("file_path"),
                doc_version=metadata.get("doc_version"),
                retrieval_reason=self._build_reason(payload, via),
            ),
            matched_by=matched_by,
            entity_names=list(metadata.get("entity_names") or []),
        )

    def _hit_to_chunk(self, hit, matched_by: str, via: str) -> RetrievedChunk:
        payload = hit.payload or {}
        return self._payload_to_chunk(payload, hit.score, matched_by, via)

    def _ensure_bm25_index(self) -> None:
        if self._bm25 is not None:
            return

        corpus_chunks: list[RetrievedChunk] = []
        offset = None
        while True:
            points, offset = self.client.scroll(
                collection_name=self.collection_name,
                limit=256,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            for point in points:
                payload = point.payload or {}
                if payload.get("content_type") == "index":
                    continue
                corpus_chunks.append(
                    self._payload_to_chunk(payload, 0.0, "bm25", "BM25 keyword match")
                )
            if offset is None:
                break

        tokenized = [_tokenize(c.content) for c in corpus_chunks]
        if not tokenized:
            self._bm25 = None
            self._bm25_chunks = []
            return
        self._bm25 = BM25Okapi(tokenized)
        self._bm25_chunks = corpus_chunks

    def _bm25_search(self, query: str, top_k: int = _HYBRID_POOL) -> list[RetrievedChunk]:
        self._ensure_bm25_index()
        if not self._bm25 or not self._bm25_chunks:
            return []

        scores = self._bm25.get_scores(_tokenize(query))
        ranked = sorted(
            zip(scores, self._bm25_chunks),
            key=lambda pair: pair[0],
            reverse=True,
        )[:top_k]

        results: list[RetrievedChunk] = []
        for score, chunk in ranked:
            if score <= 0:
                continue
            hit = chunk.model_copy(deep=True)
            hit.score = float(score)
            hit.matched_by = "bm25"
            hit.source.retrieval_reason = hit.source.retrieval_reason.replace(
                "semantic similarity", "BM25 keyword match"
            )
            results.append(hit)
        return results

    def _vector_search(self, embedding: list[float], top_k: int = _HYBRID_POOL) -> list[RetrievedChunk]:
        hits = self.client.query_points(
            collection_name=self.collection_name,
            query=embedding,
            limit=top_k,
        ).points
        return [self._hit_to_chunk(h, "dense", "semantic similarity") for h in hits]

    def _entity_boosted_search(
        self,
        embedding: list[float],
        entity_names: list[str],
        top_m: int = 5,
    ) -> list[RetrievedChunk]:
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
        via = f"entity-aware boost ({', '.join(entity_names)})"
        return [self._hit_to_chunk(h, "entity_boost", via) for h in hits]

    def retrieve(self, analysis: QueryAnalysis, mode: str | None = None) -> RetrievalResult:
        mode = mode or self.settings.retrieval_mode
        dense_chunks = self._vector_search(analysis.embedding, top_k=_HYBRID_POOL)
        bm25_chunks = self._bm25_search(analysis.query, top_k=_HYBRID_POOL)
        hybrid_chunks = rrf_merge([dense_chunks, bm25_chunks], top_n=8)
        dense_rank = {c.chunk_id: i + 1 for i, c in enumerate(hybrid_chunks)}
        dense_top_score = dense_chunks[0].score if dense_chunks else 0.0

        expanded: list[str] = []
        entity_chunks: list[RetrievedChunk] = []
        if mode == "dense_entity_boost" and analysis.entities:
            expanded = sorted(expand_entities(analysis.entities, self.graph))
            entity_chunks = self._entity_boosted_search(analysis.embedding, analysis.entities)

        if mode == "dense_only":
            chunks = hybrid_chunks
        elif mode == "dense_entity_boost":
            chunks = merge_chunks(hybrid_chunks, entity_chunks)
        else:
            raise ValueError(f"Unknown retrieval mode: {mode}")

        final_rank = {c.chunk_id: i + 1 for i, c in enumerate(chunks)}
        return RetrievalResult(
            query_analysis=analysis,
            chunks=chunks,
            mode=mode,
            top_score=dense_top_score,
            debug=RetrievalDebug(
                matched_entities=list(analysis.entities),
                expanded_entities=expanded,
                dense_rank=dense_rank,
                final_rank=final_rank,
            ),
        )
