import json
import logging
import re
import time
from collections.abc import AsyncGenerator
from dataclasses import dataclass, asdict

from groq import AsyncGroq

logger = logging.getLogger(__name__)

# Groq header semantics (https://console.groq.com/docs/rate-limits#rate-limit-headers):
#   x-ratelimit-*-requests → RPD (requests per day)
#   x-ratelimit-*-tokens   → TPM (tokens per minute)
# TPD (tokens per day) is NOT in headers; it surfaces only in 429 error bodies.


@dataclass
class UsageStats:
    remaining_requests: int | None = None
    limit_requests: int | None = None
    remaining_tokens: int | None = None
    limit_tokens: int | None = None
    reset_requests: str | None = None
    reset_tokens: str | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["fetched_at"] = time.time()
        return d


@dataclass
class StreamEvent:
    event_type: str  # "token", "done", "error"
    data: str = ""
    usage: UsageStats | None = None


async def fetch_rate_limits_from_groq(client: AsyncGroq) -> UsageStats | None:
    """Refresh quota snapshot from Groq using GET /openai/v1/models (same rate-limit headers as chat).

    Costs one API request against your daily request budget; no completion tokens.
    """
    try:
        response = await client.models.with_raw_response.list()
        return _usage_from_headers(response.headers)
    except Exception as e:
        logger.warning("Groq rate-limit probe (models.list) failed: %s", e)
        return None


async def stream_completion(
    client: AsyncGroq,
    messages: list[dict],
    model: str,
    temperature: float = 0.3,
    max_tokens: int = 2048,
) -> AsyncGenerator[StreamEvent, None]:
    """Async generator that yields StreamEvents for SSE consumption."""
    try:
        response = await client.chat.completions.with_raw_response.create(
            model=model,
            messages=messages,
            stream=True,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        usage = _usage_from_headers(response.headers)

        stream = await response.parse()
        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield StreamEvent(event_type="token", data=chunk.choices[0].delta.content)

        yield StreamEvent(event_type="done", usage=usage)

    except Exception as e:
        logger.error("Groq streaming error: %s", e)
        yield StreamEvent(event_type="error", data=_classify_error(e))


def _classify_error(exc: Exception) -> str:
    err = str(exc).lower()
    if "rate_limit" in err or "rate limit" in err or "429" in err:
        return _rate_limit_error_payload(str(exc))
    if "413" in err or "too large" in err:
        return json.dumps({"code": "context_too_large"})
    if "401" in err or "authentication" in err:
        return json.dumps({"code": "auth_error"})
    return json.dumps({"code": "service_error"})


def _rate_limit_error_payload(message: str) -> str:
    """Classify Groq 429 without leaking org IDs; TPD is only in the error body."""
    m = message.lower()
    code = "rate_limit"
    retry_hint: str | None = None

    match = re.search(r"try again in (\d+)m([\d.]+)s", m)
    if match:
        mins = int(match.group(1))
        secs = float(match.group(2))
        retry_hint = f"~{mins} min" if mins >= 1 else f"~{max(1, int(secs))}s"

    if "tokens per day" in m or "tpd" in m:
        code = "rate_limit_tokens_daily"
    elif "tokens per minute" in m or "tpm" in m:
        code = "rate_limit_tokens_minute"
    elif "requests per day" in m or "rpd" in m:
        code = "rate_limit_requests_daily"
    elif "requests per minute" in m or "rpm" in m:
        code = "rate_limit_requests_minute"

    payload: dict = {"code": code}
    if retry_hint:
        payload["retry_hint"] = retry_hint
    return json.dumps(payload)



def _usage_from_headers(headers) -> UsageStats:
    return UsageStats(
        remaining_requests=_parse_int(headers.get("x-ratelimit-remaining-requests")),
        limit_requests=_parse_int(headers.get("x-ratelimit-limit-requests")),
        remaining_tokens=_parse_int(headers.get("x-ratelimit-remaining-tokens")),
        limit_tokens=_parse_int(headers.get("x-ratelimit-limit-tokens")),
        reset_requests=headers.get("x-ratelimit-reset-requests"),
        reset_tokens=headers.get("x-ratelimit-reset-tokens"),
    )


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None
