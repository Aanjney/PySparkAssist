from typing import Literal

from pydantic import BaseModel, Field

MAX_QUERY_LENGTH = 2000
MAX_HISTORY_TURNS = 20


class ChatHistoryMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(..., min_length=1)


class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=MAX_QUERY_LENGTH)
    history: list[ChatHistoryMessage] = Field(default_factory=list, max_length=MAX_HISTORY_TURNS)
