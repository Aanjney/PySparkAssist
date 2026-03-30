import asyncio
import json
import logging
import subprocess
from pathlib import Path

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

logger = logging.getLogger(__name__)

PYSPARK_DOC_ROOTS = {
    "4.0.0": "https://spark.apache.org/docs/4.0.0/api/python/",
    "4.0.1": "https://spark.apache.org/docs/4.0.1/api/python/",
}

SPARK_EXAMPLES_REPO = "https://github.com/apache/spark.git"
SPARK_EXAMPLES_PATH = "examples/src/main/python"


async def scrape_pyspark_docs(output_dir: Path, version: str, root_url: str) -> list[Path]:
    """Crawl PySpark documentation pages and save as markdown files."""
    version_dir = output_dir / "docs" / version
    version_dir.mkdir(parents=True, exist_ok=True)

    saved_files: list[Path] = []
    browser_config = BrowserConfig(headless=True)
    crawl_config = CrawlerRunConfig(
        exclude_external_links=True,
        process_iframes=False,
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(url=root_url, config=crawl_config)

        if result.success and result.markdown:
            file_path = version_dir / "index.md"
            file_path.write_text(result.markdown, encoding="utf-8")
            saved_files.append(file_path)

            metadata = {
                "url": root_url,
                "version": version,
                "title": result.metadata.get("title", ""),
            }
            meta_path = file_path.with_suffix(".json")
            meta_path.write_text(json.dumps(metadata), encoding="utf-8")

            for link in result.links.get("internal", []):
                href = link.get("href", "")
                if not href or not href.startswith(root_url):
                    continue

                sub_result = await crawler.arun(url=href, config=crawl_config)
                if sub_result.success and sub_result.markdown:
                    safe_name = href.replace(root_url, "").strip("/").replace("/", "_") or "page"
                    md_path = version_dir / f"{safe_name}.md"
                    md_path.write_text(sub_result.markdown, encoding="utf-8")
                    saved_files.append(md_path)

                    meta = {"url": href, "version": version, "title": sub_result.metadata.get("title", "")}
                    md_path.with_suffix(".json").write_text(json.dumps(meta), encoding="utf-8")

    logger.info("Scraped %d pages for PySpark %s", len(saved_files), version)
    return saved_files


def clone_spark_examples(output_dir: Path) -> Path:
    """Sparse-clone only the Python examples from the Spark repo."""
    examples_dir = output_dir / "examples"
    if examples_dir.exists():
        logger.info("Examples directory already exists, skipping clone")
        return examples_dir

    clone_dir = output_dir / "spark_repo"
    subprocess.run(
        ["git", "clone", "--depth", "1", "--filter=blob:none", "--sparse", SPARK_EXAMPLES_REPO, str(clone_dir)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "sparse-checkout", "set", SPARK_EXAMPLES_PATH],
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
    """Run the full scraping pipeline."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for version, url in PYSPARK_DOC_ROOTS.items():
        await scrape_pyspark_docs(output_dir, version, url)

    clone_spark_examples(output_dir)
