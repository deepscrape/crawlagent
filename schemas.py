from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import NotRequired, TypedDict

from enums import FilterType


class CrawlRequest(BaseModel):
    urls: List[str] = Field(min_length=1, max_length=100)
    operation_data: Optional[Dict] = Field(default_factory=dict)
    browser_config: Optional[Dict] = Field(default_factory=dict)
    crawler_config: Optional[Dict] = Field(default_factory=dict)

class CrawlConfigValidRequest(BaseModel):
    browser_config: Optional[Dict] = Field(default_factory=dict)
    crawler_config: Optional[Dict] = Field(default_factory=dict)
    seeder_config: Optional[Dict] = Field(default_factory=dict)

class CrawlConfigValidResponse(BaseModel):
    is_valid: bool
    errors: Optional[Dict] = None

class MarkdownRequest(BaseModel):
    """Request body for the /md endpoint."""
    urls: List[str]    = Field(...,  min_length=1, max_length=100, description="Absolute http/https URLs to fetch")
    f:   FilterType    = Field(FilterType.FIT,
                                        description="Content‑filter strategy: FIT, RAW, BM25, or LLM")
    q:   Optional[str] = Field(None,  description="Query string used by BM25/LLM filters")
    c:   Optional[str] = Field("0",   description="Cache‑bust / revision counter")
    browser_config: Optional[Dict] = Field(default_factory=dict, description="Browser configuration for the crawler")


class RawCode(BaseModel):
    code: str

class HTMLRequest(BaseModel):
    url: str
    
class ScreenshotRequest(BaseModel):
    url: str
    screenshot_wait_for: Optional[float] = 2
    output_path: Optional[str] = None

class PDFRequest(BaseModel):
    url: str
    output_path: Optional[str] = None


class JSEndpointRequest(BaseModel):
    url: str
    scripts: List[str] = Field(
        ...,
        description="List of separated JavaScript snippets to execute"
    )


class OpenAIModelFee(BaseModel):
    model_name: str = Field(..., description="Name of the OpenAI model.")
    input_fee: str = Field(..., description="Fee for input token for the OpenAI model.")
    output_fee: str = Field(
        ..., description="Fee for output token for the OpenAI model."
    )


class CrawlStorageMetadata(BaseModel):
    created_At: int
    updated_At: Optional[int] = None
    file_compressed_size: int
    file_size: int
    file_name: str
    key_name: str

class CrawlStorage(BaseModel):
    error: Optional[str] = None
    metadata: CrawlStorageMetadata
    url: str
    
class AIModel(BaseModel):
    name: str
    code: str

class Author(BaseModel):
    uid: str
    displayName: str

class CrawlJobType(str, Enum):
    PLAYGROUND = "playground"
    OPERATION = "operation"

class CrawlOperationMetrics(BaseModel):
    id: Optional[str] = None
    duration: float
    machine_id: str
    peak_memory: float
    memory_used: float
    timestamp: float
    urls_processed: int

# ONE OPERATION DEFINED AS MULTIPLE TASKS
class OperationResult(BaseModel):
    operation_id: str
    machine_id: str
    duration: Optional[float] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    peak_memory: Optional[float] = None
    memory_used: Optional[float] = None
    status: str
    urls_processed: Optional[int] = None
    error: Optional[str] = None

class CrawlOperation(BaseModel):
    id: Optional[str] = None
    task_id: Optional[str] = None
    urls: List[str]
    author: Author
    name: str
    type: CrawlJobType
    urlPath: Optional[str] = None
    color: str
    modelAI: Optional[AIModel] = None
    created_At: int
    updated_At: Optional[int] = None
    scheduled_At: Optional[int] = None
    prompt: Optional[str] = None
    status: str # Replace with Enum if you have a taskStatus enum
    metadataId: Optional[str] = None
    metrics: Optional[OperationResult] = None
    error: Optional[str] = None
    storage: Optional[List[CrawlStorage]] = None


class SystemStats(BaseModel):
    total_operations: Optional[int]
    machine_id: Optional[str]
    cpu_usage: float
    memory_usage: float
    queue_length: int
    error_rate: float

class SystemTaskStats(BaseModel):
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    start_mem_mb: Optional[float] = None
    end_mem_mb: Optional[float] = None
    mem_delta_mb: Optional[float] = None
    peak_mem_mb: Optional[float] = None
    duration: Optional[float] = None
class ResourceStatus(BaseModel):
    memory_ok: bool
    cpu_ok: bool
    memory_usage: float
    cpu_usage: float
    per_core_usage: list[float]

class CoreMetrics(BaseModel):
    core_id: int
    cpu_usage: float
    memory_usage: float

# FIXME: This class likely represents a product entity and inherits from a base model class.
class Product(BaseModel):
    name: str
    price: str

class UserAbortException(Exception):
    """Raised when a user aborts the operation."""
    pass


# Key remapping
KEY_MAP = {
    "og:title": "og_title",
    "og:description": "og_description",
    "og:image": "og_image",
    "og:type": "og_type",
    "og:url": "og_url",
    "og:site_name": "og_site_name",
    "twitter:card": "twitter_card",
    "twitter:title": "twitter_title",
    "twitter:description": "twitter_description",
    "twitter:image": "twitter_image",
    "dc.creator": "dc_creator",
    "dc.date": "dc_date",
    "@context": "context",
    "@type": "type",
    "as": "as_",
}


def normalize_keys(data):
    """Recursively remap keys according to KEY_MAP"""
    if isinstance(data, dict):
        return {
            KEY_MAP.get(k, k): normalize_keys(v)
            for k, v in data.items()
        }
    elif isinstance(data, list):
        return [normalize_keys(i) for i in data]
    return data

class MetaData(BaseModel):
    author: Optional[str]
    description: Optional[str]
    keywords: Optional[str]
    viewport: Optional[str]
    robots: Optional[str]

    # Open Graph
    og_title: Optional[str]
    og_description: Optional[str]
    og_image: Optional[str]
    og_type: Optional[str]
    og_url: Optional[str]
    og_site_name: Optional[str]

    # Twitter
    twitter_card: Optional[str]
    twitter_title: Optional[str]
    twitter_description: Optional[str]
    twitter_image: Optional[str]

    # Dublin Core
    dc_creator: Optional[str]
    dc_date: Optional[str]

    # 👇 allow extra keys beyond what is defined
    model_config = ConfigDict(extra="allow")


class LinkItem(TypedDict, ):
    href: str
    type: NotRequired[str]
    as_: NotRequired[str]


class LinkData(TypedDict, ):
    preconnect: NotRequired[List[LinkItem]]
    stylesheet: NotRequired[List[LinkItem]]
    preload: NotRequired[List[LinkItem]]
    canonical: NotRequired[List[LinkItem]]
    icon: NotRequired[List[LinkItem]]
    alternate: NotRequired[List[LinkItem]]
    shortcut: NotRequired[List[LinkItem]]
    manifest: NotRequired[List[LinkItem]]


class JsonLD(BaseModel, ):
    context: Optional[str]   # was @context
    type: Optional[str]      # was @type
    headline: Optional[str]
    datePublished: Optional[str]
    author: Optional[Dict[str, Any]]
    url: Optional[str]
    name: Optional[str]

    # 👇 allow extra keys beyond what is defined
    model_config = ConfigDict(extra="allow")



class HeadData(TypedDict):
    title: str
    charset: str
    lang: NotRequired[str]
    meta: MetaData
    link: LinkData
    jsonld: List[JsonLD]


class SeederResultType(TypedDict):
    url: str
    status: str
    head_data: HeadData
    relevance_score: float
    domain: str
    query: str

class SeederResult(BaseModel):
    url: str
    status: str
    head_data: HeadData
    relevance_score: float
    domain: str
    query: str

    # 👇 auto-normalize at model creation
    @model_validator(mode="before")
    @classmethod
    def normalize_input(cls, data):
        return normalize_keys(data)
