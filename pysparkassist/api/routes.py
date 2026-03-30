import json
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from pysparkassist.api.rate_limiter import RateLimiter
from pysparkassist.retrieval.context_builder import build_context
from pysparkassist.generation.prompt import build_messages
from pysparkassist.generation.groq_client import stream_completion_generator

logger = logging.getLogger(__name__)
router = APIRouter()
limiter = RateLimiter()


class ChatRequest(BaseModel):
    query: str
    history: list[dict] = []


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/chat")
async def chat(request: Request, body: ChatRequest):
    client_ip = request.client.host if request.client else "unknown"

    if not limiter.is_allowed(client_ip):
        return JSONResponse(
            status_code=429,
            content={"error": "Please wait a moment before asking another question."},
        )

    settings = request.app.state.settings
    qp = request.app.state.query_processor
    searcher = request.app.state.searcher

    processed = qp.process(body.query)
    results = searcher.search(processed["embedding"], processed["entities"])
    context = build_context(results)

    if context.top_score > 0 and context.top_score < settings.relevance_threshold:
        async def off_topic_stream():
            yield {"event": "token", "data": "I'm designed to help with PySpark — could you rephrase your question around PySpark or Apache Spark?"}
            yield {"event": "done", "data": json.dumps({"sources": [], "usage": None})}
        return EventSourceResponse(off_topic_stream())

    if not context.sources:
        async def no_results_stream():
            yield {"event": "token", "data": "I couldn't find relevant PySpark documentation for your question. Could you try rephrasing it?"}
            yield {"event": "done", "data": json.dumps({"sources": [], "usage": None})}
        return EventSourceResponse(no_results_stream())

    messages = build_messages(body.query, context.context_text, body.history)

    async def event_generator():
        for event in stream_completion_generator(messages, settings.groq_api_key, settings.groq_model):
            if event.event_type == "token":
                yield {"event": "token", "data": event.data}
            elif event.event_type == "done":
                done_data = {
                    "sources": context.sources,
                    "usage": {
                        "remaining_requests": event.usage.remaining_requests if event.usage else None,
                        "remaining_tokens": event.usage.remaining_tokens if event.usage else None,
                        "reset_time": event.usage.reset_time if event.usage else None,
                    } if event.usage else None,
                }
                yield {"event": "done", "data": json.dumps(done_data)}
            elif event.event_type == "error":
                yield {"event": "error", "data": event.data}

    return EventSourceResponse(event_generator())
