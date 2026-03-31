import json
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from pysparkassist.retrieval.context_builder import build_context
from pysparkassist.generation.prompt import build_messages
from pysparkassist.generation.groq_client import stream_completion
from pysparkassist.api.groq_limits_store import save_groq_limits

logger = logging.getLogger(__name__)
router = APIRouter()


MAX_QUERY_LENGTH = 2000
MAX_HISTORY_TURNS = 20


class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=MAX_QUERY_LENGTH)
    history: list[dict] = Field(default=[], max_length=MAX_HISTORY_TURNS)


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/limits")
async def limits(request: Request):
    data = request.app.state.groq_limits
    return data if data else {}


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

    settings = request.app.state.settings
    qp = request.app.state.query_processor
    searcher = request.app.state.searcher

    processed = qp.process(body.query)
    has_entities = len(processed["entities"]) > 0
    domain_relevant = processed.get("domain_relevant", False)
    results = searcher.search(processed["embedding"], processed["entities"])
    context = build_context(results)

    has_pyspark_history = any(
        "pyspark" in m.get("content", "").lower()
        or "spark" in m.get("content", "").lower()
        or "dataframe" in m.get("content", "").lower()
        for m in body.history
        if m.get("role") in ("user", "assistant")
    ) if body.history else False

    is_off_topic = (
        (context.top_score > 0 and context.top_score < settings.relevance_threshold)
        or (
            not has_entities
            and not domain_relevant
            and not has_pyspark_history
            and context.top_score < 0.55
        )
    )

    if is_off_topic:
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
        async for event in stream_completion(
            request.app.state.groq_client, messages, settings.groq_model,
            temperature=settings.groq_temperature, max_tokens=settings.groq_max_tokens,
        ):
            if event.event_type == "token":
                yield {"event": "token", "data": event.data}
            elif event.event_type == "done":
                usage_dict = event.usage.to_dict() if event.usage else None
                if usage_dict:
                    request.app.state.groq_limits = usage_dict
                    save_groq_limits(settings.groq_limits_path, usage_dict)
                done_data = {"sources": context.sources, "usage": usage_dict}
                yield {"event": "done", "data": json.dumps(done_data)}
            elif event.event_type == "error":
                yield {"event": "error", "data": event.data}

    return EventSourceResponse(event_generator())
