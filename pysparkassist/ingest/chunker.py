import ast
import re
from dataclasses import dataclass, field


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
    """Split markdown by section headings (H2/H3), keeping API signatures with their docs."""
    sections = re.split(r"(?=^#{2,3}\s)", markdown, flags=re.MULTILINE)
    chunks: list[Chunk] = []

    for section in sections:
        section = section.strip()
        if not section:
            continue

        heading_match = re.match(r"^(#{2,3})\s+(.+)", section)
        section_path = heading_match.group(2).strip() if heading_match else "intro"

        approx_tokens = len(section.split())
        if approx_tokens <= max_tokens:
            chunks.append(Chunk(
                content=section,
                metadata={
                    "source_url": source_url,
                    "doc_version": doc_version,
                    "section_path": section_path,
                    "content_type": "documentation",
                },
            ))
        else:
            paragraphs = section.split("\n\n")
            current = ""
            for para in paragraphs:
                if len((current + "\n\n" + para).split()) > max_tokens and len(current.split()) >= min_tokens:
                    chunks.append(Chunk(
                        content=current.strip(),
                        metadata={
                            "source_url": source_url,
                            "doc_version": doc_version,
                            "section_path": section_path,
                            "content_type": "documentation",
                        },
                    ))
                    current = para
                else:
                    current = current + "\n\n" + para if current else para
            if current.strip():
                chunks.append(Chunk(
                    content=current.strip(),
                    metadata={
                        "source_url": source_url,
                        "doc_version": doc_version,
                        "section_path": section_path,
                        "content_type": "documentation",
                    },
                ))

    return chunks


def chunk_python_file(
    source: str,
    file_path: str,
    category: str,
) -> list[Chunk]:
    """Split Python file by function definitions. Falls back to whole-file if no functions."""
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
