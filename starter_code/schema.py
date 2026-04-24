from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Any
from datetime import datetime
import uuid

# ==========================================
# ROLE 1: LEAD DATA ARCHITECT
# ==========================================
# v1 Schema — Defines the data contract for all sources.
# v2 Migration: 'author' -> 'created_by', 'content' -> 'body_text'
# The to_v2() method allows seamless migration without data loss.

class UnifiedDocument(BaseModel):
    # --- Core identity fields ---
    document_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_type: str  # 'PDF', 'Transcript', 'HTML', 'CSV', 'Code'

    # --- v1 field names ---
    content: str = ""
    author: Optional[str] = "Unknown"

    # --- Temporal ---
    timestamp: Optional[datetime] = None

    # --- Quality / validation ---
    is_valid: bool = True
    quality_flags: List[str] = Field(default_factory=list)

    # --- Source-specific flexible metadata ---
    source_metadata: dict = Field(default_factory=dict)

    @field_validator("content")
    @classmethod
    def content_must_not_be_empty(cls, v):
        return v.strip() if isinstance(v, str) else v

    def to_v2(self) -> dict:
        """
        Schema v2 migration: rename fields without data loss.
        - 'content' -> 'body_text'
        - 'author'  -> 'created_by'
        All other fields remain unchanged.
        """
        data = self.model_dump()
        data["body_text"] = data.pop("content")
        data["created_by"] = data.pop("author")
        data["schema_version"] = "v2"
        return data
