import json
from collections.abc import AsyncGenerator, Callable

from groq import AsyncGroq

from pysparkassist.chat.schemas import ChatHistoryMessage
from pysparkassist.config import Settings
from pysparkassist.generation.groq_client import stream_completion
from pysparkassist.generation.prompt import build_messages
from pysparkassist.retrieval.context_builder import build_context
from pysparkassist.retrieval.query_processor import QueryProcessor
from pysparkassist.retrieval.relevance import classify_relevance
from pysparkassist.retrieval.retriever import Retriever


class ChatService:
    def __init__(
        self,
        settings: Settings,
        query_processor: QueryProcessor,
        retriever: Retriever,
        groq_client: AsyncGroq,
        on_usage: Callable[[dict], None],
    ):
        self.settings = settings
        self.query_processor = query_processor
        self.retriever = retriever
        self.groq_client = groq_client
        self.on_usage = on_usage

    async def stream_chat(
        self,
        query: str,
        history: list[ChatHistoryMessage],
    ) -> AsyncGenerator[dict, None]:
        analysis = self.query_processor.process(query)
        retrieval = self.retriever.retrieve(analysis)
        decision = classify_relevance(analysis, retrieval, history, self.settings)

        if not decision.should_answer:
            yield {"event": "token", "data": decision.user_message or ""}
            yield {"event": "done", "data": json.dumps({"sources": [], "usage": None})}
            return

        context = build_context(retrieval.chunks)
        history_dicts = [m.model_dump() for m in history]
        messages = build_messages(query, context.context_text, history_dicts)

        async for event in stream_completion(
            self.groq_client,
            messages,
            self.settings.groq_model,
            temperature=self.settings.groq_temperature,
            max_tokens=self.settings.groq_max_tokens,
        ):
            match event.event_type:
                case "token":
                    yield {"event": "token", "data": event.data}
                case "done":
                    usage_dict = event.usage.to_dict() if event.usage else None
                    if usage_dict:
                        self.on_usage(usage_dict)
                    yield {
                        "event": "done",
                        "data": json.dumps({"sources": context.sources, "usage": usage_dict}),
                    }
                case "error":
                    yield {"event": "error", "data": event.data}
