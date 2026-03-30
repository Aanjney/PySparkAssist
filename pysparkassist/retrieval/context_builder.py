from dataclasses import dataclass, field

from pysparkassist.retrieval.searcher import SearchResult


@dataclass
class ContextResult:
    context_text: str
    sources: list[dict] = field(default_factory=list)
    top_score: float = 0.0


def build_context(
    results: list[SearchResult],
    relevance_threshold: float = 0.0,
    max_chunks: int = 8,
) -> ContextResult:
    """Assemble retrieved chunks into a context string with source metadata."""
    filtered = [r for r in results if r.score >= relevance_threshold][:max_chunks]

    if not filtered:
        return ContextResult(context_text="", sources=[], top_score=0.0)

    sections: list[str] = []
    sources: list[dict] = []

    for i, result in enumerate(filtered, 1):
        content_type = result.metadata.get("content_type", "documentation")
        source_url = result.metadata.get("source_url")
        file_path = result.metadata.get("file_path")

        label = f"[Source {i}]"
        if content_type == "code_example":
            label = f"[Code Example {i}: {file_path or 'unknown'}]"
        elif source_url:
            label = f"[Doc {i}: {result.metadata.get('section_path', source_url)}]"

        sections.append(f"{label}\n{result.content}")

        sources.append({
            "title": result.metadata.get("section_path", file_path or f"Source {i}"),
            "url": source_url,
            "reason": result.retrieval_reason,
            "content_type": content_type,
        })

    context_text = "\n\n---\n\n".join(sections)
    return ContextResult(
        context_text=context_text,
        sources=sources,
        top_score=filtered[0].score if filtered else 0.0,
    )
