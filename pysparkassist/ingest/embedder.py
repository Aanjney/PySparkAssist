import hashlib
import logging

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer

from pysparkassist.config import COLLECTION_NAME
from pysparkassist.ingest.chunker import Chunk
from pysparkassist.ingest.entities import EntityGraph, extract_entities_from_chunk

logger = logging.getLogger(__name__)


def generate_chunk_id(chunk: Chunk) -> str:
    content_hash = hashlib.md5(chunk.content.encode()).hexdigest()[:12]
    source = chunk.metadata.get("source_url", chunk.metadata.get("file_path", "unknown"))
    return f"{source}_{content_hash}"


def embed_and_store(
    chunks: list[Chunk],
    qdrant_path: str,
    sqlite_path: str,
    model_name: str = "BAAI/bge-base-en-v1.5",
    collection_name: str = COLLECTION_NAME,
    batch_size: int = 64,
) -> int:
    """Embed chunks, extract entities, store in Qdrant + SQLite."""
    model = SentenceTransformer(model_name)
    client = QdrantClient(path=qdrant_path)
    graph = EntityGraph(sqlite_path)
    graph.initialize()

    collections = [c.name for c in client.get_collections().collections]
    if collection_name not in collections:
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=768, distance=Distance.COSINE),
        )

    points: list[PointStruct] = []
    total_stored = 0
    point_id = 0

    for chunk in chunks:
        chunk_id = generate_chunk_id(chunk)

        entities = extract_entities_from_chunk(chunk)
        entity_names = [e.name for e in entities]
        for entity in entities:
            graph.add_entity(entity)
        graph.link_chunk_entities(chunk_id, entity_names)

        embedding = model.encode(chunk.content, normalize_embeddings=True).tolist()

        payload = {
            **chunk.metadata,
            "content": chunk.content,
            "chunk_id": chunk_id,
            "entity_names": entity_names,
        }

        points.append(
            PointStruct(
                id=point_id,
                vector=embedding,
                payload=payload,
            )
        )
        point_id += 1

        if len(points) >= batch_size:
            client.upsert(collection_name=collection_name, points=points)
            logger.info("Stored batch of %d chunks", len(points))
            total_stored += len(points)
            points = []

    if points:
        client.upsert(collection_name=collection_name, points=points)
        total_stored += len(points)

    graph.close()
    logger.info("Embedded and stored %d total chunks", total_stored)
    return total_stored
