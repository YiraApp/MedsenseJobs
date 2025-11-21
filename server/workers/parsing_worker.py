"""
Background worker for parsing medical reports asynchronously.
Runs every 2 minutes to process pending jobs from the database.
"""

import asyncio
import time
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import httpx

from bson import ObjectId
from server.integrations.mongodb import MongoDBClient
from server.integrations.azure_multitenant import MultiTenantAzureBlobClient
from server.integrations.gemini import GeminiParser
from server.config.settings import get_settings
from server.models.parsing_job import JobStatus, ParsingJob, BlobFileInfo
from server.utils.confidence_calculator import calculate_confidence

logger = logging.getLogger(__name__)


class ParsingWorker:
    """Background worker for processing parsing jobs."""
    
    # Configuration
    POLLING_INTERVAL_SECONDS = 120  # Run every 2 minutes
    BATCH_SIZE = 10  # Process max 10 jobs per cycle
    MAX_RETRIES = 3
    WEBHOOK_TIMEOUT_SECONDS = 60.0
    PARSE_TIMEOUT_SECONDS = 300
    
    def __init__(self):
        self.is_running = False
        self._gemini_client: Optional[GeminiParser] = None
        self._blob_client: Optional[MultiTenantAzureBlobClient] = None
        self._settings = get_settings()
    
    def _get_gemini_client(self) -> GeminiParser:
        """Get or create Gemini client instance."""
        if self._gemini_client is None:
            if not self._settings.gemini_api_key:
                raise RuntimeError("Gemini API key not configured")
            self._gemini_client = GeminiParser(
                api_key=self._settings.gemini_api_key,
                model=self._settings.gemini_model,
            )
        return self._gemini_client
    
    def _get_blob_client(self) -> MultiTenantAzureBlobClient:
        """Get or create blob storage client instance."""
        if self._blob_client is None:
            if not self._settings.azure_connection_string:
                raise RuntimeError("Azure storage not configured")
            self._blob_client = MultiTenantAzureBlobClient(
                connection_string=self._settings.azure_connection_string,
                container_name=self._settings.azure_container_name,
            )
        return self._blob_client
    
    async def start(self):
        """Start the background worker loop."""
        if self.is_running:
            logger.warning("Worker already running")
            return
        
        self.is_running = True
        logger.info("Starting parsing worker (polling interval: %d seconds)", self.POLLING_INTERVAL_SECONDS)
        
        try:
            while self.is_running:
                try:
                    await self._process_batch()
                except Exception as exc:
                    logger.exception("Error in parsing worker cycle: %s", exc)
                
                # Wait before next cycle
                await asyncio.sleep(self.POLLING_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            logger.info("Parsing worker cancelled")
        finally:
            self.is_running = False
            logger.info("Parsing worker stopped")
    
    def stop(self):
        """Stop the background worker."""
        self.is_running = False
        logger.info("Parsing worker stop requested")
    
    async def _process_batch(self):
        """Fetch and process a batch of pending jobs."""
        db = await MongoDBClient.get_database()
        jobs_collection = db["parsing_jobs"]
        
        try:
            # Fetch pending jobs, ordered by creation time (FIFO)
            cursor = jobs_collection.find(
                {"status": JobStatus.PENDING}
            ).sort("created_at", 1).limit(self.BATCH_SIZE)
            
            pending_jobs = await cursor.to_list(length=self.BATCH_SIZE)
            
            if not pending_jobs:
                logger.debug("No pending jobs to process")
                return
            
            logger.info("Processing %d pending job(s)", len(pending_jobs))
            
            for job_doc in pending_jobs:
                try:
                    job_id = str(job_doc["_id"])
                    await self._process_job(job_id, job_doc)
                except Exception as exc:
                    logger.exception("Error processing job %s: %s", job_doc.get("_id"), exc)
                    # Continue with next job
                    continue
        
        except Exception as exc:
            logger.exception("Error fetching batch of jobs: %s", exc)
    
    async def _process_job(self, job_id: str, job_doc: Dict[str, Any]):
        """
        Process a single parsing job.
        
        Args:
            job_id: Job identifier (MongoDB ObjectId as string)
            job_doc: Job document from MongoDB
        """
        db = await MongoDBClient.get_database()
        jobs_collection = db["parsing_jobs"]
        
        try:
            # Mark job as processing
            update_time = time.time()
            await jobs_collection.update_one(
                {"_id": ObjectId(job_id)},
                {
                    "$set": {
                        "status": JobStatus.PROCESSING,
                        "started_at": update_time,
                        "message": "Parsing in progress"
                    }
                }
            )
            
            # Extract job details
            tenant_id = job_doc["tenant_id"]
            project_id = job_doc["project_id"]
            report_id = job_doc["report_id"]
            files = job_doc.get("files", [])
            model_name = job_doc.get("model_name")
            webhook_url = job_doc.get("webhook_meta", {}).get("webhook_url")
            
            # Validate configuration
            if not model_name:
                raise ValueError("Model name not specified in job")
            
            # Download files from blob storage
            logger.info("Downloading %d file(s) from blob storage for job %s", len(files), job_id)
            pdf_files = []
            
            blob_client = self._get_blob_client()
            for file_info in files:
                try:
                    blob_url = file_info.get("blob_url")
                    filename = file_info.get("filename", "document.pdf")
                    
                    if not blob_url:
                        logger.warning("File info missing blob_url: %s", file_info)
                        continue
                    
                    file_bytes = await blob_client.download_file_bytes(blob_url, tenant_id)
                    pdf_files.append({
                        "filename": filename,
                        "bytes": file_bytes,
                        "size_mb": len(file_bytes) / (1024 * 1024)
                    })
                    logger.debug("Downloaded file from blob: %s", filename)
                    
                except Exception as exc:
                    logger.exception("Failed to download file from blob %s: %s", file_info.get("blob_url"), exc)
                    continue
            
            if not pdf_files:
                raise ValueError("Failed to download any PDF files from blob storage")
            
            # Parse files using AI model
            parse_start_time = time.time()
            parsed_data = await self._parse_files(pdf_files, model_name)
            parse_end_time = time.time()
            parsing_time_seconds = round(parse_end_time - parse_start_time, 2)
            
            # Calculate confidence scores
            gemini_confidence = parsed_data.pop("confidence_score", None) if isinstance(parsed_data, dict) else None
            gemini_confidence_summary = parsed_data.pop("confidence_summary", None) if isinstance(parsed_data, dict) else None
            
            validated_confidence, validated_summary, validation_details = calculate_confidence(
                parsed_data=parsed_data or {},
                gemini_confidence=gemini_confidence
            )
            
            # Update job with completion status
            update_data = {
                "status": JobStatus.COMPLETED,
                "completed_at": time.time(),
                "message": f"Successfully processed {len(pdf_files)} file(s) in {parsing_time_seconds}s",
                "files_processed": len(files),
                "successful_parses": len(pdf_files),
                "failed_parses": len(files) - len(pdf_files),
                "parsing_time_seconds": parsing_time_seconds,
                "parsed_data": parsed_data,
                "confidence_score": validated_confidence,
                "confidence_summary": validated_summary,
            }
            
            await jobs_collection.update_one(
                {"_id": ObjectId(job_id)},
                {"$set": update_data}
            )
            
            logger.info("Job %s completed successfully (parsed %d files in %fs)", 
                       job_id, len(pdf_files), parsing_time_seconds)
            
            # Send webhook callback if configured
            if webhook_url:
                await self._send_webhook(
                    job_id=job_id,
                    tenant_id=tenant_id,
                    project_id=project_id,
                    report_id=report_id,
                    webhook_url=webhook_url,
                    status="completed",
                    parsed_data=parsed_data
                )
        
        except Exception as exc:
            logger.exception("Error processing job %s: %s", job_id, exc)
            await self._handle_job_failure(job_id, job_doc, str(exc))
    
    async def _parse_files(self, pdf_files: List[Dict[str, Any]], model_name: str) -> Dict[str, Any]:
        """
        Parse PDF files using the specified AI model.
        
        Args:
            pdf_files: List of dicts with 'filename' and 'bytes' keys
            model_name: Name of the AI model to use
        
        Returns:
            Parsed data dictionary
        """
        gemini_client = self._get_gemini_client()
        
        try:
            # Try consolidated parsing for multiple files
            if len(pdf_files) > 1:
                try:
                    logger.info("Attempting consolidated parsing for %d files", len(pdf_files))
                    consolidated_data = gemini_client.parse_multiple_pdfs(pdf_files)
                    if consolidated_data:
                        logger.info("Consolidated parsing successful")
                        return consolidated_data
                except Exception as exc:
                    logger.warning("Consolidated parsing failed: %s, falling back to individual parsing", exc)
            
            # Fall back to individual file parsing
            parsed_results = []
            for pdf_file in pdf_files:
                try:
                    logger.info("Parsing individual file: %s", pdf_file["filename"])
                    parsed_data = gemini_client.parse_pdf(
                        pdf_file["bytes"],
                        pdf_file["filename"]
                    )
                    if parsed_data:
                        parsed_results.append({
                            "filename": pdf_file["filename"],
                            "data": parsed_data
                        })
                except Exception as exc:
                    logger.exception("Failed to parse file %s: %s", pdf_file["filename"], exc)
                    continue
            
            if not parsed_results:
                raise ValueError("Failed to parse any files")
            
            # Merge individual results if needed
            if len(parsed_results) == 1:
                return parsed_results[0]["data"]
            else:
                return self._merge_parsed_results(parsed_results)
        
        except Exception as exc:
            logger.exception("Error parsing files: %s", exc)
            raise
    
    def _merge_parsed_results(self, parsed_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Merge multiple parsed results into a consolidated report.
        
        Args:
            parsed_results: List of dicts with 'filename' and 'data' keys
        
        Returns:
            Consolidated parsed data
        """
        if not parsed_results:
            return {}
        
        if len(parsed_results) == 1:
            result = parsed_results[0]["data"].copy()
            result.pop("batch_info", None)
            return result
        
        # Start with first report
        consolidated = parsed_results[0]["data"].copy()
        consolidated.pop("batch_info", None)
        
        # Array fields to merge
        array_fields = [
            "medications", "procedures", "lab_results", 
            "imaging_findings", "recommendations"
        ]
        
        # Merge data from remaining reports
        for result in parsed_results[1:]:
            data = result["data"]
            filename = result["filename"]
            
            # Merge array fields
            for field in array_fields:
                if field in data and data[field]:
                    if field not in consolidated or not consolidated[field]:
                        consolidated[field] = []
                    
                    if not isinstance(consolidated[field], list):
                        consolidated[field] = [consolidated[field]] if consolidated[field] else []
                    
                    source_data = data[field] if isinstance(data[field], list) else [data[field]]
                    
                    for item in source_data:
                        if isinstance(item, dict):
                            item["source_file"] = filename
                    
                    consolidated[field].extend(source_data)
            
            # Merge diagnosis
            if "diagnosis" in data and data["diagnosis"]:
                if "diagnosis" not in consolidated or not consolidated["diagnosis"]:
                    consolidated["diagnosis"] = []
                
                if isinstance(consolidated["diagnosis"], str):
                    consolidated["diagnosis"] = [consolidated["diagnosis"]]
                elif not isinstance(consolidated["diagnosis"], list):
                    consolidated["diagnosis"] = []
                
                source_diagnosis = data["diagnosis"]
                if isinstance(source_diagnosis, str):
                    source_diagnosis = [source_diagnosis]
                elif not isinstance(source_diagnosis, list):
                    source_diagnosis = [str(source_diagnosis)] if source_diagnosis else []
                
                for diag in source_diagnosis:
                    if diag and diag not in consolidated["diagnosis"]:
                        consolidated["diagnosis"].append(diag)
        
        return consolidated
    
    async def _send_webhook(
        self,
        job_id: str,
        tenant_id: str,
        project_id: str,
        report_id: str,
        webhook_url: str,
        status: str,
        parsed_data: Optional[Dict[str, Any]] = None
    ):
        """
        Send webhook callback with parsing results.
        
        Args:
            job_id: Job identifier
            tenant_id: Tenant identifier
            project_id: Project identifier
            report_id: Report identifier
            webhook_url: Webhook URL to POST to
            status: Job status (completed or failed)
            parsed_data: Parsed medical data (if successful)
        """
        db = await MongoDBClient.get_database()
        jobs_collection = db["parsing_jobs"]
        
        try:
            payload = {
                "job_id": job_id,
                "tenant_id": tenant_id,
                "project_id": project_id,
                "report_id": report_id,
                "status": status,
                "timestamp": datetime.utcnow().isoformat(),
            }
            
            if status == "completed" and parsed_data:
                payload["parsed_data"] = parsed_data
            elif status == "failed":
                # Get error message from job
                job = await jobs_collection.find_one({"_id": ObjectId(job_id)})
                if job and job.get("last_error"):
                    payload["error"] = job["last_error"]
            
            logger.info("Sending webhook to %s for job %s (status: %s)", 
                       webhook_url, job_id, status)
            
            async with httpx.AsyncClient(timeout=self.WEBHOOK_TIMEOUT_SECONDS) as client:
                response = await client.post(webhook_url, json=payload)
                
                if response.status_code >= 200 and response.status_code < 300:
                    logger.info("Webhook delivered successfully to %s (status: %d)", 
                               webhook_url, response.status_code)
                    
                    await jobs_collection.update_one(
                        {"_id": ObjectId(job_id)},
                        {
                            "$set": {
                                "webhook_meta.delivered": True,
                                "webhook_meta.status": "success",
                                "webhook_meta.last_attempt_at": time.time(),
                            },
                            "$inc": {"webhook_meta.attempts": 1}
                        }
                    )
                else:
                    logger.warning("Webhook delivery failed to %s (status: %d)", 
                                  webhook_url, response.status_code)
                    logger.warning("Webhook delivery failed to %s (status: %d)", 
                                  webhook_url, response)

                    
                    await jobs_collection.update_one(
                        {"_id": ObjectId(job_id)},
                        {
                            "$set": {
                                "webhook_meta.delivered": False,
                                "webhook_meta.status": f"fail ({response.status_code})",
                                "webhook_meta.last_attempt_at": time.time(),
                            },
                            "$inc": {"webhook_meta.attempts": 1}
                        }
                    )
        
        except Exception as exc:
            logger.exception("Failed to send webhook to %s: %s", webhook_url, exc)
            
            try:
                await jobs_collection.update_one(
                    {"_id": ObjectId(job_id)},
                    {
                        "$set": {
                            "webhook_meta.delivered": False,
                            "webhook_meta.status": f"fail ({str(exc)[:100]})",
                            "webhook_meta.last_attempt_at": time.time(),
                        },
                        "$inc": {"webhook_meta.attempts": 1}
                    }
                )
            except Exception as update_exc:
                logger.exception("Failed to update webhook metadata: %s", update_exc)
    
    async def _handle_job_failure(self, job_id: str, job_doc: Dict[str, Any], error_message: str):
        """
        Handle job failure with retry logic.
        
        Args:
            job_id: Job identifier
            job_doc: Job document from MongoDB
            error_message: Error description
        """
        db = await MongoDBClient.get_database()
        jobs_collection = db["parsing_jobs"]
        
        try:
            retry_count = job_doc.get("retry_count", 0)
            max_retries = job_doc.get("max_retries", self.MAX_RETRIES)
            webhook_url = job_doc.get("webhook_meta", {}).get("webhook_url")
            
            # Determine if we should retry
            if retry_count < max_retries:
                # Mark for retry
                retry_count += 1
                logger.info("Job %s failed, retrying (attempt %d/%d)", 
                           job_id, retry_count, max_retries)
                
                await jobs_collection.update_one(
                    {"_id": ObjectId(job_id)},
                    {
                        "$set": {
                            "status": JobStatus.PENDING,
                            "retry_count": retry_count,
                            "last_error": error_message,
                            "message": f"Retry {retry_count}/{max_retries}: {error_message[:100]}"
                        }
                    }
                )
            else:
                # Max retries exhausted
                logger.error("Job %s failed after %d retries", job_id, retry_count)
                
                await jobs_collection.update_one(
                    {"_id": ObjectId(job_id)},
                    {
                        "$set": {
                            "status": JobStatus.FAILED,
                            "completed_at": time.time(),
                            "last_error": error_message,
                            "message": f"Failed after {max_retries} retries: {error_message[:200]}"
                        }
                    }
                )
                
                # Send failure webhook if configured
                if webhook_url:
                    try:
                        await self._send_webhook(
                            job_id=job_id,
                            tenant_id=job_doc["tenant_id"],
                            project_id=job_doc["project_id"],
                            report_id=job_doc["report_id"],
                            webhook_url=webhook_url,
                            status="failed"
                        )
                    except Exception as exc:
                        logger.exception("Failed to send failure webhook: %s", exc)
        
        except Exception as exc:
            logger.exception("Error handling job failure: %s", exc)


# Global worker instance
_worker_instance: Optional[ParsingWorker] = None


def get_parsing_worker() -> ParsingWorker:
    """Get or create the global parsing worker instance."""
    global _worker_instance
    if _worker_instance is None:
        _worker_instance = ParsingWorker()
    return _worker_instance
