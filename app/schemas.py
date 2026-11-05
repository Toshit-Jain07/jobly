from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

JobType = Literal["webhook_delivery", "url_check", "report_generate"]
JobStatus = Literal["queued", "running", "succeeded", "dead"]


class JobCreate(BaseModel):
    type: JobType
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=5, ge=1, le=20)

    @model_validator(mode="after")
    def check_payload(self):
        """Reject bad payloads at submission, not after N pointless retries."""
        p = self.payload
        if self.type in ("webhook_delivery", "url_check"):
            url = p.get("url")
            if not isinstance(url, str) or not url.startswith(("http://", "https://")):
                raise ValueError("payload.url must be an http(s) URL")
        if self.type == "report_generate":
            rows, seconds = p.get("rows", 1000), p.get("seconds", 0)
            if not isinstance(rows, int) or not 1 <= rows <= 1_000_000:
                raise ValueError("payload.rows must be an integer between 1 and 1,000,000")
            if not isinstance(seconds, (int, float)) or not 0 <= seconds <= 60:
                raise ValueError("payload.seconds must be a number between 0 and 60")
        return self


class JobOut(BaseModel):
    id: int
    type: str
    payload: dict[str, Any]
    idempotency_key: str
    status: str
    attempts: int
    max_attempts: int
    run_at: float
    locked_at: float | None
    last_error: str | None
    result: Any | None
    duration_ms: float | None
    created_at: float
    updated_at: float
