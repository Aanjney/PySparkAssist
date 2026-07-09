import hashlib
import logging
from collections import defaultdict

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer

from pysparkassist.config import COLLECTION_NAME
from pysparkassist.ingest.chunker import Chunk, is_index_chunk
from pysparkassist.ingest.entities import Entity, EntityGraph, extract_entities_from_chunk

logger = logging.getLogger(__name__)


def generate_chunk_id(chunk: Chunk) -> str:
    content_hash = hashlib.md5(chunk.content.encode()).hexdigest()[:12]
    source = chunk.metadata.get("source_url", chunk.metadata.get("file_path", "unknown"))
    return f"{source}_{content_hash}"


def chunk_id_to_point_id(chunk_id: str) -> int:
    # ponytail: md5 first 8 bytes -> positive int64; collision unlikely at this scale
    digest = hashlib.md5(chunk_id.encode()).digest()[:8]
    return int.from_bytes(digest, "big") & 0x7FFFFFFFFFFFFFFF


def _collect_source_versions(chunks: list[Chunk]) -> dict[str, str]:
    versions: dict[str, set[str]] = defaultdict(set)
    for chunk in chunks:
        if v := chunk.metadata.get("doc_version"):
            versions["docs"].add(v)
        if cat := chunk.metadata.get("example_category"):
            versions["examples"].add(cat)
    return {k: ",".join(sorted(v)) for k, v in versions.items()}


def _recreate_collection(client: QdrantClient, collection_name: str, vector_size: int) -> None:
    names = {c.name for c in client.get_collections().collections}
    if collection_name in names:
        client.delete_collection(collection_name)
    client.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
    )


def embed_and_store(
    chunks: list[Chunk],
    qdrant_url: str,
    sqlite_path: str,
    model_name: str = "BAAI/bge-base-en-v1.5",
    collection_name: str = COLLECTION_NAME,
    batch_size: int = 64,
) -> dict:
    model = SentenceTransformer(model_name)
    vector_size = model.get_sentence_embedding_dimension()
    client = QdrantClient(url=qdrant_url)
    graph = EntityGraph(sqlite_path)
    graph.initialize()
    graph.clear_all()

    _recreate_collection(client, collection_name, vector_size)

    chunk_ids: list[str] = []
    entity_rows: list[Entity] = []
    chunk_entity_links: list[tuple[str, str]] = []
    texts: list[str] = []
    payloads: list[dict] = []

    for chunk in chunks:
        if is_index_chunk(chunk.content):
            continue
        chunk_id = generate_chunk_id(chunk)
        entities = extract_entities_from_chunk(chunk)
        entity_names = [e.name for e in entities]

        chunk_ids.append(chunk_id)
        entity_rows.extend(entities)
        chunk_entity_links.extend((chunk_id, name) for name in entity_names)
        texts.append(chunk.content)
        payloads.append(
            {
                **chunk.metadata,
                "content": chunk.content,
                "chunk_id": chunk_id,
                "entity_names": entity_names,
            }
        )

    for entity in entity_rows:
        graph.add_entity(entity)
    graph.link_chunk_entities_batch(chunk_entity_links)
    graph.commit()

    total_stored = 0
    for start in range(0, len(texts), batch_size):
        batch_texts = texts[start : start + batch_size]
        batch_payloads = payloads[start : start + batch_size]
        batch_ids = chunk_ids[start : start + batch_size]

        embeddings = model.encode(batch_texts, normalize_embeddings=True)
        points = [
            PointStruct(
                id=chunk_id_to_point_id(cid),
                vector=emb.tolist(),
                payload=payload,
            )
            for cid, emb, payload in zip(batch_ids, embeddings, batch_payloads)
        ]
        client.upsert(collection_name=collection_name, points=points)
        total_stored += len(points)
        logger.info("Stored batch of %d chunks", len(points))

    entity_count = graph.conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    graph.close()
    logger.info("Embedded and stored %d total chunks", total_stored)

    return {
        "chunk_count": total_stored,
        "entity_count": entity_count,
        "embedding_dimension": vector_size,
        "source_versions": _collect_source_versions(chunks),
    }
