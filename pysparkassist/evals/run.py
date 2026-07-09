from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

from pysparkassist.config import Settings, get_settings
from pysparkassist.evals.metrics import (
    abstention_accuracy,
    entity_match_rate,
    first_hit_rank,
    hit_at_k,
    mrr,
)
from pysparkassist.evals.report import format_markdown_report
from pysparkassist.ingest.entities import EntityGraph
from pysparkassist.ingest.manifest import load_manifest
from pysparkassist.retrieval.query_processor import QueryProcessor
from pysparkassist.retrieval.relevance import classify_relevance
from pysparkassist.retrieval.retriever import Retriever

logger = logging.getLogger(__name__)

DEFAULT_MODES = ("dense_only", "dense_entity_boost")
GOLDEN_PATH = Path(__file__).parent / "data" / "golden_questions.jsonl"
REPORT_DIR = Path("eval_reports")


class GoldenQuestion(BaseModel):
    id: str
    question: str
    category: str = "general"
    expected_entities: list[str] = Field(default_factory=list)
    expected_source_contains: list[str] = Field(default_factory=list)
    answer_notes: str = ""
    should_answer: bool = True


def load_golden(path: Path = GOLDEN_PATH) -> list[GoldenQuestion]:
    rows: list[GoldenQuestion] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(GoldenQuestion.model_validate_json(line))
    return rows


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def _load_embedding_model(model_name: str) -> SentenceTransformer:
    local_path = Path("data/models") / model_name.replace("/", "_")
    if local_path.exists():
        return SentenceTransformer(str(local_path))
    return SentenceTransformer(model_name)


def _eval_question(
    golden: GoldenQuestion,
    processor: QueryProcessor,
    retriever: Retriever,
    settings: Settings,
    mode: str,
    k: int,
) -> dict[str, Any]:
    analysis = processor.process(golden.question)
    result = retriever.retrieve(analysis, mode=mode)
    decision = classify_relevance(analysis, result, [], settings)
    top = result.chunks[:k]
    chunk_entities = [e for c in top for e in c.entity_names]

    return {
        "hit_at_k": hit_at_k(
            result.chunks,
            golden.expected_source_contains,
            golden.expected_entities,
            k,
        ),
        "mrr_rank": first_hit_rank(
            result.chunks,
            golden.expected_source_contains,
            golden.expected_entities,
            k,
        ),
        "entity_match_rate": entity_match_rate(
            golden.expected_entities,
            analysis.entities,
            chunk_entities,
        ),
        "abstention_correct": abstention_accuracy(
            decision.should_answer,
            golden.should_answer,
        ),
        "predicted_should_answer": decision.should_answer,
        "relevance_reason": decision.reason,
        "query_entities": analysis.entities,
        "top_chunk_ids": [c.chunk_id for c in top],
        "top_score": result.top_score,
        "debug": result.debug.model_dump() if result.debug else None,
    }


def run_eval(
    modes: list[str],
    k: int,
    golden_path: Path = GOLDEN_PATH,
) -> dict[str, Any]:
    settings = get_settings()
    model = _load_embedding_model(settings.embedding_model)
    qdrant = QdrantClient(url=settings.qdrant_url)
    graph = EntityGraph(settings.sqlite_path)
    processor = QueryProcessor(model=model, graph=graph)
    retriever = Retriever(client=qdrant, graph=graph, settings=settings)

    manifest_meta: dict[str, Any] | None = None
    manifest_path = Path(settings.data_dir) / "manifest.json"
    if manifest_path.is_file():
        m = load_manifest(manifest_path)
        manifest_meta = {
            "embedding_model": m.embedding_model,
            "chunk_count": m.chunk_count,
            "entity_count": m.entity_count,
            "git_commit": m.git_commit,
        }

    golden = load_golden(golden_path)
    per_question = [
        {
            "id": g.id,
            "category": g.category,
            "question": g.question,
            "should_answer": g.should_answer,
            "modes": {
                mode: _eval_question(g, processor, retriever, settings, mode, k)
                for mode in modes
            },
        }
        for g in golden
    ]

    graph.close()
    qdrant.close()

    n = len(per_question)
    summary = {
        mode: {
            "hit_at_k": sum(q["modes"][mode]["hit_at_k"] for q in per_question) / n if n else 0.0,
            "mrr": mrr([q["modes"][mode]["mrr_rank"] for q in per_question]),
            "entity_match_rate": sum(q["modes"][mode]["entity_match_rate"] for q in per_question) / n
            if n
            else 0.0,
            "abstention_accuracy": sum(q["modes"][mode]["abstention_correct"] for q in per_question) / n
            if n
            else 0.0,
        }
        for mode in modes
    }

    return {
        "meta": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "git_commit": _git_commit(),
            "k": k,
            "modes": modes,
            "question_count": len(golden),
            "manifest": manifest_meta,
        },
        "summary": summary,
        "per_question": per_question,
    }


def write_report(report: dict[str, Any], out_dir: Path = REPORT_DIR) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = out_dir / f"retrieval_eval_{ts}.json"
    md_path = out_dir / f"retrieval_eval_{ts}.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    md_path.write_text(format_markdown_report(report), encoding="utf-8")
    return json_path, md_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run retrieval evals against golden set")
    parser.add_argument(
        "--modes",
        default=",".join(DEFAULT_MODES),
        help="Comma-separated retrieval modes (default: dense_only,dense_entity_boost)",
    )
    parser.add_argument("--k", type=int, default=8, help="Top-k for hit@k (default: 8)")
    parser.add_argument(
        "--golden",
        type=Path,
        default=GOLDEN_PATH,
        help="Path to golden_questions.jsonl",
    )
    args = parser.parse_args(argv)
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logger.info("Running retrieval eval: modes=%s k=%d", modes, args.k)

    report = run_eval(modes=modes, k=args.k, golden_path=args.golden)
    json_path, md_path = write_report(report)
    logger.info("Wrote %s", json_path)
    logger.info("Wrote %s", md_path)

    for mode, summary in report["summary"].items():
        logger.info(
            "%s: hit@k=%.3f mrr=%.3f entity=%.3f abstention=%.3f",
            mode,
            summary["hit_at_k"],
            summary["mrr"],
            summary["entity_match_rate"],
            summary["abstention_accuracy"],
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
