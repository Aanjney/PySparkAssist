from pydantic import BaseModel, Field


class QueryAnalysis(BaseModel):
    query: str
    embedding: list[float]
    entities: list[str] = Field(default_factory=list)
    domain_relevant: bool = False


class SourceMetadata(BaseModel):
    content_type: str = "documentation"
    section_path: str = ""
    source_url: str | None = None
    file_path: str | None = None
    doc_version: str | None = None
    retrieval_reason: str = ""


class RetrievedChunk(BaseModel):
    chunk_id: str
    content: str
    score: float
    source: SourceMetadata
    matched_by: str = "dense"
    entity_names: list[str] = Field(default_factory=list)


class RetrievalDebug(BaseModel):
    matched_entities: list[str] = Field(default_factory=list)
    expanded_entities: list[str] = Field(default_factory=list)
    dense_rank: dict[str, int] = Field(default_factory=dict)
    final_rank: dict[str, int] = Field(default_factory=dict)


class RetrievalResult(BaseModel):
    query_analysis: QueryAnalysis
    chunks: list[RetrievedChunk] = Field(default_factory=list)
    mode: str
    top_score: float = 0.0
    debug: RetrievalDebug | None = None
