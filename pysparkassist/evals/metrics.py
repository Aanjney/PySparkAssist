# ponytail: no LLM-as-judge.

from __future__ import annotations

from pysparkassist.retrieval.models import RetrievedChunk


def _chunk_haystack(chunk: RetrievedChunk) -> str:
    parts = [
        chunk.content,
        chunk.source.section_path,
        chunk.source.file_path or "",
        chunk.source.retrieval_reason,
        " ".join(chunk.entity_names),
    ]
    return " ".join(parts).lower()


def chunk_is_hit(
    chunk: RetrievedChunk,
    expected_source_contains: list[str],
    expected_entities: list[str],
) -> bool:
    haystack = _chunk_haystack(chunk)
    source_hit = any(s.lower() in haystack for s in expected_source_contains)
    entity_hit = any(e.lower() in haystack for e in expected_entities)
    return source_hit or entity_hit


def hit_at_k(
    chunks: list[RetrievedChunk],
    expected_source_contains: list[str],
    expected_entities: list[str],
    k: int,
) -> bool:
    for chunk in chunks[:k]:
        if chunk_is_hit(chunk, expected_source_contains, expected_entities):
            return True
    return False


def first_hit_rank(
    chunks: list[RetrievedChunk],
    expected_source_contains: list[str],
    expected_entities: list[str],
    k: int,
) -> int | None:
    for i, chunk in enumerate(chunks[:k], start=1):
        if chunk_is_hit(chunk, expected_source_contains, expected_entities):
            return i
    return None


def mrr(ranks: list[int | None]) -> float:
    if not ranks:
        return 0.0
    return sum(1.0 / r if r else 0.0 for r in ranks) / len(ranks)


def entity_match_rate(
    expected_entities: list[str],
    query_entities: list[str],
    chunk_entities: list[str],
) -> float:
    if not expected_entities:
        return 1.0
    found = {e.lower() for e in query_entities} | {e.lower() for e in chunk_entities}
    matched = sum(1 for e in expected_entities if e.lower() in found)
    return matched / len(expected_entities)


def abstention_accuracy(predicted_should_answer: bool, expected_should_answer: bool) -> bool:
    return predicted_should_answer == expected_should_answer
