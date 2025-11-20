"""
Project and AI Model database models.
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from bson import ObjectId

logger = logging.getLogger(__name__)


class AIModel(BaseModel):
    """AI Model configuration"""
    model_id: str = Field(..., description="Unique model identifier")
    tenant_id: str = Field(..., description="Tenant ID that owns this model")
    model_name: str = Field(..., description="Display name of the AI model")
    cost_per_page: float = Field(..., gt=0, description="Cost per page in USD")
    description: Optional[str] = Field(None, description="Model description")
    provider: str = Field(default="gemini", description="AI provider (gemini, etc)")
    status: str = Field(default="active", description="Model status: active, deprecated, archived")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }


class Project(BaseModel):
    """Project model representing a hospital/clinic project within a tenant"""
    project_id: str = Field(..., description="Unique project identifier")
    tenant_id: str = Field(..., description="Parent tenant ID")
    project_name: str = Field(..., description="Display name of the project")
    description: Optional[str] = Field(None, description="Project description")
    ai_model_id: Optional[str] = Field(None, description="Associated AI Model ID")
    is_active: bool = Field(default=True, description="Project status")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional project metadata")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }


class ProjectAnalytics(BaseModel):
    """Project-level analytics with per-document metrics"""
    project_id: str = Field(..., description="Project ID")
    tenant_id: str = Field(..., description="Tenant ID")
    project_name: str = Field(default="", description="Project display name")
    
    # Upload metrics
    total_uploads: int = Field(default=0, description="Number of upload sessions")
    
    # Page metrics
    total_pages: int = Field(default=0, description="Total pages across all documents")
    pages_per_doc: List[int] = Field(default_factory=list, description="Page count for each document")
    
    # Parse time metrics
    avg_parse_time_seconds: float = Field(default=0.0, description="Average parse time across all documents")
    avg_parse_time_per_doc_seconds: float = Field(default=0.0, description="Average parse time per document")
    parse_times: List[float] = Field(default_factory=list, description="Parse time for each document")
    
    # Success rate metrics
    average_success_rate: float = Field(default=100.0, description="Average success rate percentage")
    success_rates: List[float] = Field(default_factory=list, description="Success rate for each document")
    
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }


class TenantAnalytics(BaseModel):
    """Tenant-level aggregated analytics"""
    tenant_id: str = Field(..., description="Tenant ID")
    total_projects: int = Field(default=0, description="Total number of projects")
    total_uploads: int = Field(default=0, description="Total uploads across all projects")
    total_pages: int = Field(default=0, description="Total pages across all projects")
    average_success_rate: float = Field(default=100.0, description="Average success rate across all projects")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }
