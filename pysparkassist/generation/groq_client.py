import logging
from dataclasses import dataclass
from typing import Generator

from groq import Groq

logger = logging.getLogger(__name__)


@dataclass
class UsageStats:
    remaining_requests: int | None = None
    remaining_tokens: int | None = None
    reset_time: str | None = None


@dataclass
class StreamEvent:
    event_type: str  # "token", "done", "error"
    data: str = ""
    usage: UsageStats | None = None


def stream_completion_generator(
    messages: list[dict],
    api_key: str,
    model: str = "llama-3.3-70b-versatile",
) -> Generator[StreamEvent, None, None]:
    """Generator that yields StreamEvents for SSE consumption."""
    client = Groq(api_key=api_key)

    try:
        response = client.chat.completions.with_raw_response.create(
            model=model,
            messages=messages,
            stream=True,
            temperature=0.3,
            max_tokens=2048,
        )

        usage = UsageStats(
            remaining_requests=_parse_int(response.headers.get("x-ratelimit-remaining-requests")),
            remaining_tokens=_parse_int(response.headers.get("x-ratelimit-remaining-tokens")),
            reset_time=response.headers.get("x-ratelimit-reset-requests"),
        )

        stream = response.parse()
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield StreamEvent(event_type="token", data=chunk.choices[0].delta.content)

        yield StreamEvent(event_type="done", usage=usage)

    except Exception as e:
        logger.error("Groq streaming error: %s", e)
        err = str(e).lower()
        if "rate_limit" in err or "rate limit" in err or "429" in err:
            yield StreamEvent(event_type="error", data="rate_limit")
        elif "413" in err or "too large" in err:
            yield StreamEvent(event_type="error", data="context_too_large")
        elif "401" in err or "authentication" in err:
            yield StreamEvent(event_type="error", data="auth_error")
        else:
            yield StreamEvent(event_type="error", data="service_error")


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None
