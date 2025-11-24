"""
Utility for logging parser job events to MongoDB.
"""

import logging
import time
from typing import Optional, Dict, Any

from server.integrations.mongodb import MongoDBClient
from server.models.parser_job_logs import LogEventType

logger = logging.getLogger(__name__)


class ParserJobLogger:
    """Utility class for logging parser job events."""
    
    COLLECTION_NAME = "parser_job_logs"
    
    @staticmethod
    async def log_event(
        job_id: str,
        tenant_id: str,
        project_id: str,
        event_type: LogEventType,
        message: str,
        error_message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        filename: Optional[str] = None,
        retry_count: Optional[int] = None,
    ) -> bool:
        """
        Log a parser job event to MongoDB.
        
        Args:
            job_id: Job identifier
            tenant_id: Tenant identifier
            project_id: Project identifier
            event_type: Type of event
            message: Event message
            error_message: Error message (if applicable)
            details: Additional context
            filename: Related filename (if applicable)
            retry_count: Retry attempt number (if applicable)
        
        Returns:
            True if log was created successfully, False otherwise
        """
        try:
            db = await MongoDBClient.get_database()
            logs_collection = db[ParserJobLogger.COLLECTION_NAME]
            
            log_entry = {
                "job_id": job_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "event_type": event_type.value,
                "timestamp": time.time(),
                "message": message,
                "error_message": error_message,
                "details": details or {},
                "filename": filename,
                "retry_count": retry_count,
            }
            
            result = await logs_collection.insert_one(log_entry)
            logger.debug("Parser job log created for job %s: %s", job_id, event_type.value)
            return bool(result.inserted_id)
            
        except Exception as exc:
            logger.exception("Failed to log parser job event for job %s: %s", job_id, exc)
            return False
    
    @staticmethod
    async def log_job_started(
        job_id: str,
        tenant_id: str,
        project_id: str,
        files_count: int,
    ) -> bool:
        """Log when a job starts processing."""
        return await ParserJobLogger.log_event(
            job_id=job_id,
            tenant_id=tenant_id,
            project_id=project_id,
            event_type=LogEventType.JOB_STARTED,
            message=f"Job started processing {files_count} file(s)",
            details={"files_count": files_count}
        )
    
    @staticmethod
    async def log_parsing_success(
        job_id: str,
        tenant_id: str,
        project_id: str,
        filename: str,
        parsing_time_seconds: float,
    ) -> bool:
        """Log successful file parsing."""
        return await ParserJobLogger.log_event(
            job_id=job_id,
            tenant_id=tenant_id,
            project_id=project_id,
            event_type=LogEventType.PARSING_SUCCESS,
            message=f"Successfully parsed {filename}",
            filename=filename,
            details={"parsing_time_seconds": parsing_time_seconds}
        )
    
    @staticmethod
    async def log_parsing_failed(
        job_id: str,
        tenant_id: str,
        project_id: str,
        filename: str,
        error_message: str,
        error_type: Optional[str] = None,
    ) -> bool:
        """Log failed file parsing."""
        return await ParserJobLogger.log_event(
            job_id=job_id,
            tenant_id=tenant_id,
            project_id=project_id,
            event_type=LogEventType.PARSING_FAILED,
            message=f"Failed to parse {filename}",
            error_message=error_message,
            filename=filename,
            details={"error_type": error_type}
        )
    
    @staticmethod
    async def log_file_download_error(
        job_id: str,
        tenant_id: str,
        project_id: str,
        filename: str,
        error_message: str,
    ) -> bool:
        """Log file download error."""
        return await ParserJobLogger.log_event(
            job_id=job_id,
            tenant_id=tenant_id,
            project_id=project_id,
            event_type=LogEventType.FILE_DOWNLOAD_ERROR,
            message=f"Failed to download {filename} from blob storage",
            error_message=error_message,
            filename=filename
        )
    
    @staticmethod
    async def log_job_retry(
        job_id: str,
        tenant_id: str,
        project_id: str,
        retry_count: int,
        error_message: str,
    ) -> bool:
        """Log job retry attempt."""
        return await ParserJobLogger.log_event(
            job_id=job_id,
            tenant_id=tenant_id,
            project_id=project_id,
            event_type=LogEventType.JOB_RETRY,
            message=f"Retrying job (attempt {retry_count})",
            error_message=error_message,
            retry_count=retry_count
        )
    
    @staticmethod
    async def log_job_completed(
        job_id: str,
        tenant_id: str,
        project_id: str,
        files_processed: int,
        parsing_time_seconds: float,
    ) -> bool:
        """Log job completion."""
        return await ParserJobLogger.log_event(
            job_id=job_id,
            tenant_id=tenant_id,
            project_id=project_id,
            event_type=LogEventType.JOB_COMPLETED,
            message=f"Job completed successfully, processed {files_processed} file(s)",
            details={
                "files_processed": files_processed,
                "parsing_time_seconds": parsing_time_seconds
            }
        )
    
    @staticmethod
    async def log_job_failed(
        job_id: str,
        tenant_id: str,
        project_id: str,
        error_message: str,
        retry_count: Optional[int] = None,
    ) -> bool:
        """Log final job failure."""
        return await ParserJobLogger.log_event(
            job_id=job_id,
            tenant_id=tenant_id,
            project_id=project_id,
            event_type=LogEventType.JOB_FAILED,
            message="Job failed after all retry attempts",
            error_message=error_message,
            retry_count=retry_count
        )
    
    @staticmethod
    async def log_webhook_success(
        job_id: str,
        tenant_id: str,
        project_id: str,
        webhook_url: str,
        status_code: int,
    ) -> bool:
        """Log successful webhook delivery."""
        return await ParserJobLogger.log_event(
            job_id=job_id,
            tenant_id=tenant_id,
            project_id=project_id,
            event_type=LogEventType.WEBHOOK_SUCCESS,
            message=f"Webhook delivered successfully to {webhook_url}",
            details={"webhook_url": webhook_url, "status_code": status_code}
        )
    
    @staticmethod
    async def log_webhook_failed(
        job_id: str,
        tenant_id: str,
        project_id: str,
        webhook_url: str,
        error_message: str,
        status_code: Optional[int] = None,
    ) -> bool:
        """Log failed webhook delivery."""
        return await ParserJobLogger.log_event(
            job_id=job_id,
            tenant_id=tenant_id,
            project_id=project_id,
            event_type=LogEventType.WEBHOOK_FAILED,
            message=f"Webhook delivery failed to {webhook_url}",
            error_message=error_message,
            details={"webhook_url": webhook_url, "status_code": status_code}
        )
    
    @staticmethod
    async def get_job_logs(job_id: str) -> list:
        """
        Retrieve all logs for a specific job.
        
        Args:
            job_id: Job identifier
        
        Returns:
            List of log entries sorted by timestamp
        """
        try:
            db = await MongoDBClient.get_database()
            logs_collection = db[ParserJobLogger.COLLECTION_NAME]
            
            cursor = logs_collection.find({"job_id": job_id}).sort("timestamp", 1)
            logs = await cursor.to_list(length=None)
            
            return logs
            
        except Exception as exc:
            logger.exception("Failed to retrieve logs for job %s: %s", job_id, exc)
            return []
    
    @staticmethod
    async def get_job_logs_by_tenant(tenant_id: str, limit: int = 100) -> list:
        """
        Retrieve recent logs for a tenant.
        
        Args:
            tenant_id: Tenant identifier
            limit: Maximum number of logs to return
        
        Returns:
            List of log entries sorted by timestamp (newest first)
        """
        try:
            db = await MongoDBClient.get_database()
            logs_collection = db[ParserJobLogger.COLLECTION_NAME]
            
            cursor = logs_collection.find({"tenant_id": tenant_id}).sort("timestamp", -1).limit(limit)
            logs = await cursor.to_list(length=limit)
            
            return logs
            
        except Exception as exc:
            logger.exception("Failed to retrieve logs for tenant %s: %s", tenant_id, exc)
            return []
