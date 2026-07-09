import ast
import re
from dataclasses import dataclass, field

_NAV_MARKERS = ("Site Navigation", "Skip to main content")
_ANCHOR_LINK_RE = re.compile(r"\[[^\]]*\]\(#")


def is_index_chunk(content: str) -> bool:
    if any(marker in content for marker in _NAV_MARKERS):
        return True
    words = content.split()
    if not words:
        return False
    anchor_links = len(_ANCHOR_LINK_RE.findall(content))
    if anchor_links >= 5 and anchor_links / len(words) > 0.08:
        return True
    hash_links = content.count("](#")
    if hash_links >= 8 and hash_links / len(words) > 0.12:
        return True
    return False


@dataclass
class Chunk:
    content: str
    metadata: dict = field(default_factory=dict)


def _expr_is_string_literal(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, str)


def chunk_markdown(
    markdown: str,
    source_url: str,
    doc_version: str,
    max_tokens: int = 800,
    min_tokens: int = 100,
) -> list[Chunk]:
    sections = re.split(r"(?=^#{2,3}\s)", markdown, flags=re.MULTILINE)
    chunks: list[Chunk] = []

    def meta(section_path: str) -> dict:
        return {
            "source_url": source_url,
            "doc_version": doc_version,
            "section_path": section_path,
            "content_type": "documentation",
        }

    for section in sections:
        section = section.strip()
        if not section or is_index_chunk(section):
            continue

        heading_match = re.match(r"^(#{2,3})\s+(.+)", section)
        section_path = heading_match.group(2).strip() if heading_match else "intro"

        approx_tokens = len(section.split())
        if approx_tokens <= max_tokens:
            chunks.append(Chunk(content=section, metadata=meta(section_path)))
        else:
            paragraphs = section.split("\n\n")
            current = ""
            for para in paragraphs:
                if len((current + "\n\n" + para).split()) > max_tokens and len(current.split()) >= min_tokens:
                    chunks.append(Chunk(content=current.strip(), metadata=meta(section_path)))
                    current = para
                else:
                    current = current + "\n\n" + para if current else para
            if current.strip():
                chunks.append(Chunk(content=current.strip(), metadata=meta(section_path)))

    return chunks


def chunk_python_file(
    source: str,
    file_path: str,
    category: str,
) -> list[Chunk]:
    chunks: list[Chunk] = []
    module_docstring = ""

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return [Chunk(
            content=source,
            metadata={"file_path": file_path, "example_category": category, "content_type": "code_example"},
        )]

    if tree.body and isinstance(tree.body[0], ast.Expr) and _expr_is_string_literal(tree.body[0].value):
        module_docstring = ast.get_docstring(tree) or ""

    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]

    if not functions:
        return [Chunk(
            content=source,
            metadata={"file_path": file_path, "example_category": category, "content_type": "code_example"},
        )]

    source_lines = source.splitlines()
    for func in functions:
        start = func.lineno - 1
        end = func.end_lineno or (start + 1)
        func_source = "\n".join(source_lines[start:end])

        header = f'\"\"\"File: {file_path}\"\"\"\n' if not module_docstring else f'\"\"\"{module_docstring}\"\"\"\n'
        content = header + func_source

        chunks.append(Chunk(
            content=content,
            metadata={
                "file_path": file_path,
                "example_category": category,
                "content_type": "code_example",
                "function_name": func.name,
            },
        ))

    return chunks
