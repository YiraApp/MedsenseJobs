"""
Parser job logs model for tracking job execution history and errors.
Stores detailed logs of parsing job events in MongoDB.
"""

from enum import Enum
from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class LogEventType(str, Enum):
    """Type of log event."""
    JOB_STARTED = "job_started"
    PARSING_SUCCESS = "parsing_success"
    PARSING_FAILED = "parsing_failed"
    FILE_DOWNLOAD_ERROR = "file_download_error"
    FILE_PARSE_ERROR = "file_parse_error"
    WEBHOOK_SUCCESS = "webhook_success"
    WEBHOOK_FAILED = "webhook_failed"
    JOB_RETRY = "job_retry"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"


class ParserJobLog(BaseModel):
    """MongoDB schema for parser job logs."""
    
    # Job reference
    job_id: str = Field(
        description="Reference to the parsing job (MongoDB ObjectId as string)",
        index=True
    )
    tenant_id: str = Field(
        description="Tenant identifier",
        index=True
    )
    project_id: str = Field(
        description="Project identifier",
        index=True
    )
    
    # Event information
    event_type: LogEventType = Field(
        description="Type of event that occurred"
    )
    timestamp: float = Field(
        default_factory=lambda: datetime.utcnow().timestamp(),
        description="Unix timestamp of when the event occurred"
    )
    
    # Message and error details
    message: str = Field(
        description="Event message describing what happened"
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Error message if the event was a failure"
    )
    
    # Additional context
    details: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Additional contextual information about the event"
    )
    
    # File-specific information
    filename: Optional[str] = Field(
        default=None,
        description="Filename related to this event (if applicable)"
    )
    
    # Retry information
    retry_count: Optional[int] = Field(
        default=None,
        description="Retry attempt number (if applicable)"
    )
    
    class Config:
        extra = "allow"  # Allow additional fields
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ParserJobLogCreate(BaseModel):
    """Model for creating parser job logs."""
    job_id: str
    tenant_id: str
    project_id: str
    event_type: LogEventType
    message: str
    error_message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    filename: Optional[str] = None
    retry_count: Optional[int] = None
