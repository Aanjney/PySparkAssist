import re
from dataclasses import dataclass, field

from pysparkassist.retrieval.searcher import SearchResult

_ANCHOR_URL_RE = re.compile(r'\[#?\]\((https?://[^\s"]+)')
_LINK_ARTIFACTS_RE = re.compile(r'\[#?\]\([^)]*\)')

_GITHUB_EXAMPLES_BASE = (
    "https://github.com/apache/spark/blob/master/examples/src/main/python/"
)

_NAV_PATTERNS = [
    re.compile(r'\[Skip to main content\]\([^)]*\)'),
    re.compile(r'`Ctrl`\+`K`'),
    re.compile(r'\[ !\[Logo image\]\([^)]*\)[^\]]*\]\([^)]*\)'),
    re.compile(r'Site Navigation\s*\n(?:\s*\*[^\n]*\n)*', re.MULTILINE),
    re.compile(r'Section Navigation\s*\n(?:\s*\*[^\n]*\n)*', re.MULTILINE),
    re.compile(r'^More\s*$', re.MULTILINE),
    re.compile(r'^\d+\.\d+\.\d+\s*$', re.MULTILINE),
    re.compile(r'(?:\[[\d.]+\]\(https?://[^)]+\))+'),
    re.compile(r'Copy to clipboard', re.IGNORECASE),
    re.compile(r'^\[\d+\]:\s*$', re.MULTILINE),
    re.compile(r'\[Permalink to this headline\]', re.IGNORECASE),
]


def _clean_content(text: str) -> str:
    """Strip navigation boilerplate and page chrome from scraped markdown."""
    for pattern in _NAV_PATTERNS:
        text = pattern.sub('', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _dedup_sources(sources: list[dict]) -> list[dict]:
    """Merge sources with the same title + content_type, combining versions."""
    seen: dict[tuple, dict] = {}
    order: list[tuple] = []

    for src in sources:
        key = (src["title"], src["content_type"])
        if key in seen:
            existing = seen[key]
            for v in src["versions"]:
                if v and v not in existing["versions"]:
                    existing["versions"].append(v)
            if not existing["url"] and src["url"]:
                existing["url"] = src["url"]
            if src["match_type"] == "knowledge_graph" and existing["match_type"] != "knowledge_graph":
                existing["match_type"] = "knowledge_graph"
        else:
            seen[key] = src
            order.append(key)

    return [seen[k] for k in order]


@dataclass
class ContextResult:
    context_text: str
    sources: list[dict] = field(default_factory=list)
    top_score: float = 0.0


def build_context(
    results: list[SearchResult],
    max_chunks: int = 5,
) -> ContextResult:
    """Assemble retrieved chunks into a context string with source metadata."""
    filtered = results[:max_chunks]

    if not filtered:
        return ContextResult(context_text="", sources=[], top_score=0.0)

    sections: list[str] = []
    raw_sources: list[dict] = []

    for i, result in enumerate(filtered, 1):
        content_type = result.metadata.get("content_type", "documentation")
        raw_section = result.metadata.get("section_path", "")
        source_url = result.metadata.get("source_url")
        file_path = result.metadata.get("file_path")
        doc_version = result.metadata.get("doc_version")

        anchor_match = _ANCHOR_URL_RE.search(raw_section)
        if anchor_match:
            source_url = anchor_match.group(1)

        if content_type == "code_example" and file_path and not source_url:
            source_url = _GITHUB_EXAMPLES_BASE + file_path

        label = f"[Source {i}]"
        if content_type == "code_example":
            label = f"[Code Example {i}: {file_path or 'unknown'}]"
        elif source_url:
            label = f"[Doc {i}: {raw_section}]"

        cleaned = _clean_content(result.content) if content_type == "documentation" else result.content
        sections.append(f"{label}\n{cleaned}")

        title = _LINK_ARTIFACTS_RE.sub('', raw_section).strip()
        if not title and file_path:
            title = file_path
        if not title:
            title = f"Source {i}"

        reason = result.retrieval_reason
        match_type = "knowledge_graph" if "knowledge graph" in reason else "semantic"

        raw_sources.append({
            "title": title,
            "url": source_url,
            "content_type": content_type,
            "versions": [doc_version] if doc_version else [],
            "match_type": match_type,
        })

    sources = _dedup_sources(raw_sources)
    context_text = "\n\n---\n\n".join(sections)
    return ContextResult(
        context_text=context_text,
        sources=sources,
        top_score=filtered[0].score if filtered else 0.0,
    )
