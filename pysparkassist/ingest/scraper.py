import asyncio
import json
import logging
import subprocess
from collections import deque
from pathlib import Path
from urllib.parse import urldefrag

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

logger = logging.getLogger(__name__)

PYSPARK_DOC_VERSIONS = ("4.0.0", "4.0.1")
MAX_CRAWL_PAGES = 200
MAX_CRAWL_DEPTH = 3

SPARK_EXAMPLES_REPO = "https://github.com/apache/spark.git"
SPARK_EXAMPLES_PATH = "examples/src/main/python"


def doc_seeds(version: str) -> list[str]:
    base = f"https://spark.apache.org/docs/{version}"
    return [
        f"{base}/api/python/",
        f"{base}/structured-streaming-programming-guide.html",
        f"{base}/api/python/reference/pyspark.ss.html",
        f"{base}/api/python/reference/pyspark.ss/io.html",
        f"{base}/api/python/reference/pyspark.ss/DataStreamWriter.html",
        f"{base}/api/python/reference/pyspark.ss/DataStreamReader.html",
    ]


def _same_doc_scope(url: str, version: str) -> bool:
    prefix = f"https://spark.apache.org/docs/{version}/"
    return url.startswith(prefix)


def _normalize_url(url: str) -> str:
    url, _ = urldefrag(url)
    return url.rstrip("/")


def _safe_filename(url: str, version: str) -> str:
    prefix = f"https://spark.apache.org/docs/{version}/"
    name = url.replace(prefix, "").strip("/").replace("/", "_") or "page"
    return name


async def _save_page(
    crawler: AsyncWebCrawler,
    url: str,
    version: str,
    version_dir: Path,
    crawl_config: CrawlerRunConfig,
    saved_urls: set[str],
) -> tuple[Path | None, list[str]]:
    if url in saved_urls:
        return None, []

    result = await crawler.arun(url=url, config=crawl_config)
    if not (result.success and result.markdown):
        return None, []

    md_path = version_dir / f"{_safe_filename(url, version)}.md"
    md_path.write_text(result.markdown, encoding="utf-8")
    meta = {"url": url, "version": version, "title": result.metadata.get("title", "")}
    md_path.with_suffix(".json").write_text(json.dumps(meta), encoding="utf-8")
    saved_urls.add(url)

    internal: list[str] = []
    for link in result.links.get("internal", []):
        href = link.get("href", "")
        if href and _same_doc_scope(href, version):
            internal.append(_normalize_url(href))

    return md_path, internal


async def scrape_pyspark_docs(output_dir: Path, version: str) -> list[Path]:
    version_dir = output_dir / "docs" / version
    version_dir.mkdir(parents=True, exist_ok=True)

    saved_files: list[Path] = []
    saved_urls: set[str] = set()
    browser_config = BrowserConfig(headless=True)
    crawl_config = CrawlerRunConfig(
        exclude_external_links=True,
        process_iframes=False,
    )

    queue: deque[tuple[str, int]] = deque()
    seen: set[str] = set()
    for seed in doc_seeds(version):
        norm = _normalize_url(seed)
        if norm not in seen:
            seen.add(norm)
            queue.append((norm, 0))

    async with AsyncWebCrawler(config=browser_config) as crawler:
        while queue and len(saved_urls) < MAX_CRAWL_PAGES:
            url, depth = queue.popleft()
            md_path, links = await _save_page(
                crawler, url, version, version_dir, crawl_config, saved_urls
            )
            if md_path:
                saved_files.append(md_path)

            if depth >= MAX_CRAWL_DEPTH:
                continue
            for link in links:
                if link not in seen:
                    seen.add(link)
                    queue.append((link, depth + 1))

    logger.info("Scraped %d pages for PySpark %s", len(saved_files), version)
    return saved_files


def clone_spark_examples(output_dir: Path) -> Path:
    examples_dir = output_dir / "examples"
    if examples_dir.exists():
        logger.info("Examples directory already exists, skipping clone")
        return examples_dir

    clone_dir = output_dir / "spark_repo"
    clone_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        ["git", "clone", "--depth", "1", "--no-checkout", SPARK_EXAMPLES_REPO, str(clone_dir)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "sparse-checkout", "init", "--cone"],
        cwd=str(clone_dir),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "sparse-checkout", "set", SPARK_EXAMPLES_PATH],
        cwd=str(clone_dir),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "checkout"],
        cwd=str(clone_dir),
        check=True,
        capture_output=True,
    )

    src = clone_dir / SPARK_EXAMPLES_PATH
    examples_dir.mkdir(parents=True, exist_ok=True)
    for py_file in src.rglob("*.py"):
        dest = examples_dir / py_file.relative_to(src)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(py_file.read_text(encoding="utf-8"), encoding="utf-8")

    logger.info("Cloned %d Python example files", len(list(examples_dir.rglob("*.py"))))
    return examples_dir


async def scrape_all(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    for version in PYSPARK_DOC_VERSIONS:
        await scrape_pyspark_docs(output_dir, version)

    clone_spark_examples(output_dir)
