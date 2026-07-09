from pysparkassist.ingest.chunker import Chunk
from pysparkassist.ingest.indexer import chunk_id_to_point_id, generate_chunk_id


def test_generate_chunk_id_stable() -> None:
    chunk = Chunk(
        content="df.select('a').filter('b')",
        metadata={"source_url": "https://example.com/doc", "doc_version": "3.5"},
    )
    assert generate_chunk_id(chunk) == generate_chunk_id(chunk)


def test_chunk_id_to_point_id_stable() -> None:
    chunk_id = "https://example.com/doc_abc123def456"
    assert chunk_id_to_point_id(chunk_id) == chunk_id_to_point_id(chunk_id)


def test_different_chunks_different_point_ids() -> None:
    c1 = Chunk(content="alpha", metadata={"source_url": "https://a"})
    c2 = Chunk(content="beta", metadata={"source_url": "https://a"})
    assert chunk_id_to_point_id(generate_chunk_id(c1)) != chunk_id_to_point_id(generate_chunk_id(c2))
