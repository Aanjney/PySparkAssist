"""Optional first-boot: run full ingestion when Qdrant has no points (Docker / fresh volume)."""

import os
import subprocess
import sys

from qdrant_client import QdrantClient

from pysparkassist.config import COLLECTION_NAME


def _need_ingest(qdrant_path: str) -> bool:
    if os.environ.get("SKIP_DATA_BOOTSTRAP", "").lower() in ("1", "true", "yes"):
        return False
    client = QdrantClient(path=qdrant_path)
    names = {c.name for c in client.get_collections().collections}
    if COLLECTION_NAME not in names:
        return True
    try:
        points, _ = client.scroll(
            collection_name=COLLECTION_NAME,
            limit=1,
            with_payload=False,
            with_vectors=False,
        )
    except Exception:
        return True
    return len(points) < 1


def main() -> int:
    qdrant_path = os.environ.get("QDRANT_PATH", "./data/qdrant")
    if not _need_ingest(qdrant_path):
        print("docker_bootstrap: vector index OK, starting app.", flush=True)
        return 0
    print(
        "docker_bootstrap: empty or missing index — running ingestion (long one-time step; needs network).",
        flush=True,
    )
    r = subprocess.run(
        [sys.executable, "-m", "pysparkassist.ingest", "run"],
        cwd=os.environ.get("WORKDIR", "/app"),
    )
    return r.returncode


if __name__ == "__main__":
    raise SystemExit(main())
