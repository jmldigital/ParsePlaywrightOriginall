from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

Text = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)
]


Identifier = Annotated[
    str, StringConstraints(min_length=1, max_length=128, pattern=r"\S")
]


class Part(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: Identifier
    oem: Text
    brand: Text


class JobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    batch_id: Identifier
    max_delivery_days: int = Field(default=2, ge=0, le=365, strict=True)
    parts: list[Part] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def unique_ids(self):
        if len({p.id for p in self.parts}) != len(self.parts):
            raise ValueError("Position ids must be unique within a batch")
        return self


class ItemError(BaseModel):
    code: str
    message: str


class SearchResult(BaseModel):
    status: Literal["found", "not_found", "error"]
    price: str | None = None
    delivery_days: int | None = None
    error: ItemError | None = None


class Accepted(BaseModel):
    job_id: str
    batch_id: str
    status: Literal["queued", "running", "completed", "failed"]
    total: int


class Progress(Accepted):
    processed: int
    found: int
    not_found: int
    errors: int


class PartResult(Part, SearchResult):
    currency: Literal["RUB"] = "RUB"
    parsed_at: str


class Results(BaseModel):
    job_id: str
    batch_id: str
    status: Literal["queued", "running", "completed", "failed"]
    max_delivery_days: int
    expires_at: str | None
    parts: list[PartResult]
