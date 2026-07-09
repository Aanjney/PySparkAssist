import logging
import os

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

from pysparkassist.chat.schemas import ChatRequest

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
async def health(request: Request):
    qdrant = getattr(request.app.state, "qdrant", None)
    if qdrant is None:
        return {"status": "ok"}

    try:
        qdrant.get_collections()
    except Exception as exc:
        logger.warning("Qdrant health check failed: %s", exc)
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "qdrant": "unreachable"},
        )

    manifest_ok = getattr(request.app.state, "manifest_ok", False)
    if not manifest_ok and os.environ.get("SKIP_MANIFEST_CHECK") != "1":
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "manifest": "missing_or_invalid"},
        )

    return {"status": "ok", "qdrant": "connected", "manifest": "ok" if manifest_ok else "skipped"}


@router.get("/limits")
async def limits(request: Request):
    return request.app.state.groq_limits or {}


@router.post("/chat")
async def chat(request: Request, body: ChatRequest):
    if not body.query.strip():
        return JSONResponse(status_code=400, content={"error": "Query cannot be empty."})

    client_ip = request.client.host if request.client else "unknown"
    if not request.app.state.limiter.is_allowed(client_ip):
        return JSONResponse(
            status_code=429,
            content={"error": "Please wait a moment before asking another question."},
        )

    return EventSourceResponse(
        request.app.state.chat_service.stream_chat(body.query, body.history)
    )
