from pysparkassist.ingest.chunker import is_index_chunk


def test_is_index_chunk_nav_markers() -> None:
    content = "Site Navigation\n[Home](#home)\nSkip to main content\n## API"
    assert is_index_chunk(content) is True


def test_is_index_chunk_normal_doc() -> None:
    content = "## writeStream\n\nWrite streaming query results to external storage.\n\n```python\ndf.writeStream.format('parquet').start()\n```"
    assert is_index_chunk(content) is False


def test_is_index_chunk_high_anchor_density() -> None:
    links = " ".join(f"[Section {i}](#sec{i})" for i in range(12))
    content = f"## Table of Contents\n{links}"
    assert is_index_chunk(content) is True
