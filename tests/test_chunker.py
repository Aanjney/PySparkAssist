from pysparkassist.ingest.chunker import chunk_markdown, chunk_python_file, Chunk


def test_chunk_markdown_splits_on_headings():
    md = "## Overview\nSome intro text.\n## groupBy\nGroups the DataFrame.\n### Parameters\nparam info."
    chunks = chunk_markdown(md, source_url="https://example.com", doc_version="4.0.0")
    assert len(chunks) >= 2
    assert all(isinstance(c, Chunk) for c in chunks)
    assert any("groupBy" in c.content for c in chunks)


def test_chunk_markdown_preserves_metadata():
    md = "## DataFrame\nA distributed collection of data."
    chunks = chunk_markdown(md, source_url="https://example.com/df", doc_version="4.0.1")
    assert chunks[0].metadata["source_url"] == "https://example.com/df"
    assert chunks[0].metadata["doc_version"] == "4.0.1"


def test_chunk_python_file_splits_on_functions():
    code = """\"\"\"Example: word count.\"\"\"

def word_count(spark):
    \"\"\"Count words in a text file.\"\"\"
    lines = spark.read.text("data.txt")
    return lines.count()

def another_example(spark):
    \"\"\"Another example.\"\"\"
    return spark.range(10)
"""
    chunks = chunk_python_file(code, file_path="wordcount.py", category="sql")
    assert len(chunks) >= 2
    assert chunks[0].metadata["content_type"] == "code_example"


def test_chunk_python_file_single_script():
    code = """\"\"\"A single script example.\"\"\"
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.range(100)
df.show()
"""
    chunks = chunk_python_file(code, file_path="simple.py", category="sql")
    assert len(chunks) >= 1
    assert "SparkSession" in chunks[0].content
