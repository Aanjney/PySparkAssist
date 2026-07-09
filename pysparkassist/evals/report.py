from __future__ import annotations

from typing import Any


def format_summary_table(mode_summaries: dict[str, dict[str, float]]) -> str:
    metrics = ["hit_at_k", "mrr", "entity_match_rate", "abstention_accuracy"]
    header = "| Mode | " + " | ".join(metrics) + " |"
    sep = "| --- | " + " | ".join("---" for _ in metrics) + " |"
    rows = []
    for mode, summary in mode_summaries.items():
        cells = [f"{summary[m]:.3f}" for m in metrics]
        rows.append(f"| {mode} | " + " | ".join(cells) + " |")
    return "\n".join([header, sep, *rows])


def format_mode_diffs(
    per_question: list[dict[str, Any]],
    mode_a: str,
    mode_b: str,
) -> str:
    helped: list[str] = []
    hurt: list[str] = []
    for row in per_question:
        qid = row["id"]
        a_hit = row["modes"][mode_a]["hit_at_k"]
        b_hit = row["modes"][mode_b]["hit_at_k"]
        if not a_hit and b_hit:
            helped.append(qid)
        elif a_hit and not b_hit:
            hurt.append(qid)

    lines = ["## Mode diffs", ""]
    lines.append(f"**{mode_b} helped** (hit only in {mode_b}): " + (", ".join(helped) or "none"))
    lines.append("")
    lines.append(f"**{mode_b} hurt** (hit only in {mode_a}): " + (", ".join(hurt) or "none"))
    return "\n".join(lines)


def format_markdown_report(report: dict[str, Any]) -> str:
    meta = report["meta"]
    lines = [
        "# Retrieval Eval Report",
        "",
        f"- **Timestamp:** {meta['timestamp']}",
        f"- **Git commit:** {meta['git_commit']}",
        f"- **k:** {meta['k']}",
        f"- **Modes:** {', '.join(meta['modes'])}",
        f"- **Questions:** {meta['question_count']}",
        "",
    ]
    if manifest := meta.get("manifest"):
        lines.extend(
            [
                "## Manifest",
                "",
                f"- embedding_model: {manifest.get('embedding_model', 'n/a')}",
                f"- chunk_count: {manifest.get('chunk_count', 'n/a')}",
                f"- entity_count: {manifest.get('entity_count', 'n/a')}",
                "",
            ]
        )

    lines.extend(["## Summary", "", format_summary_table(report["summary"]), ""])
    if len(meta["modes"]) >= 2:
        lines.append(format_mode_diffs(report["per_question"], meta["modes"][0], meta["modes"][1]))
        lines.append("")

    failures = [
        q
        for q in report["per_question"]
        if q["should_answer"] and not all(q["modes"][m]["hit_at_k"] for m in meta["modes"])
    ]
    if failures:
        lines.extend(["## Failing in-domain questions", ""])
        for q in failures:
            lines.append(f"### {q['id']}")
            lines.append(f"- question: {q['question']}")
            for mode in meta["modes"]:
                chunks = q["modes"][mode].get("top_chunk_ids", [])
                lines.append(f"- {mode} top chunks: {', '.join(chunks[:5]) or 'none'}")
            lines.append("")

    return "\n".join(lines)
