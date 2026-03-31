"""Persist last Groq rate-limit snapshot to disk so UI survives process restarts."""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def load_groq_limits(path: str) -> dict | None:
    """Return cached limits dict, or None if missing/unreadable."""
    p = Path(path)
    if not p.is_file():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not load Groq limits from %s: %s", path, e)
        return None
    if not isinstance(data, dict):
        return None
    return data


def save_groq_limits(path: str, data: dict) -> None:
    """Atomically write limits JSON (small file, sync is fine)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, separators=(",", ":"))
    tmp = p.with_suffix(p.suffix + ".tmp")
    try:
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(p)
    except OSError as e:
        logger.warning("Could not save Groq limits to %s: %s", path, e)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
