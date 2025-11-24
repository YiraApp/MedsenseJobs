"""
Background worker for parsing medical reports asynchronously.
Two variants below:
- Version A: 3 parallel batches, jobs inside each batch sequential.
- Version B: 3 parallel batches, jobs inside each batch fully parallel.
Pick ONE version and remove the other.
"""

import asyncio
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Any
from datetime import datetime

import httpx
from bson import ObjectId

from server.integrations.mongodb import MongoDBClient
from server.integrations.azure_multitenant import MultiTenantAzureBlobClient
from server.integrations.gemini import GeminiParser
from server.config.settings import get_settings
from server.models.parsing_job import JobStatus
from server.utils.confidence_calculator import calculate_confidence
from server.utils.parser_job_logger import ParserJobLogger

logger = logging.getLogger(__name__)

class ParsingWorker:
    """Version A: 3 parallel chunks, jobs processed sequentially inside each chunk."""

    # Configuration
    POLLING_INTERVAL_SECONDS = 180        # Run every 2 minutes
    MAX_JOBS_PER_CYCLE = 9               # Fetch up to 9 jobs per cycle
    WORKER_BATCHES = 3                   # Split into 3 chunks
    MAX_RETRIES = 3
    # Backoff for job retries (1m, 10m, 30m)
    RETRY_BACKOFF_SECONDS = [60, 600, 1800]

    WEBHOOK_TIMEOUT_SECONDS = 60.0
    PARSE_TIMEOUT_SECONDS = 300

    def __init__(self):
        self.is_running = False
        self._gemini_client: Optional[GeminiParser] = None
        self._blob_client: Optional[MultiTenantAzureBlobClient] = None
        self._settings = get_settings()

        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None

        # Thread pool for blocking Gemini calls
        self._thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=5, thread_name_prefix="ParsingWorker"
        )

    # ---------- Clients ----------

    def _get_gemini_client(self) -> GeminiParser:
        if self._gemini_client is None:
            if not self._settings.gemini_api_key:
                raise RuntimeError("Gemini API key not configured")
            self._gemini_client = GeminiParser(
                api_key=self._settings.gemini_api_key,
                model=self._settings.gemini_model,
            )
        return self._gemini_client

    def _get_blob_client(self) -> MultiTenantAzureBlobClient:
        if self._blob_client is None:
            if not self._settings.azure_connection_string:
                raise RuntimeError("Azure storage not configured")
            self._blob_client = MultiTenantAzureBlobClient(
                connection_string=self._settings.azure_connection_string,
                container_name=self._settings.azure_container_name,
            )
        return self._blob_client

    # ---------- Lifecycle ----------

    async def start(self):
        """Start the background worker as an asyncio task."""
        if self.is_running:
            logger.warning("Parsing worker already running")
            return

        self._stop_event = asyncio.Event()
        self.is_running = True

        self._task = asyncio.create_task(self._worker_loop(), name="ParsingWorkerLoop")
        logger.info(
            "Parsing worker started (polling interval: %d seconds)",
            self.POLLING_INTERVAL_SECONDS,
        )

    def stop(self):
        """Request the worker to stop (non-blocking)."""
        if not self.is_running:
            logger.warning("Parsing worker is not running")
            return

        logger.info("Parsing worker stop requested")
        self.is_running = False
        if self._stop_event:
            self._stop_event.set()

    async def _worker_loop(self):
        """Main async worker loop."""
        logger.info("Parsing worker loop started")
        try:
            while self.is_running and not self._stop_event.is_set():
                try:
                    await self._process_batch()
                except Exception as exc:
                    logger.exception("Error in parsing worker cycle: %s", exc)

                # Sleep until next cycle or stop requested
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self.POLLING_INTERVAL_SECONDS
                    )
                except asyncio.TimeoutError:
                    # Normal wake-up for next cycle
                    continue
        except asyncio.CancelledError:
            logger.info("Parsing worker loop cancelled")
        finally:
            self.is_running = False
            logger.info("Parsing worker loop stopped")

    # ---------- Batch helpers ----------

    def _split_into_chunks(self, jobs: List[Dict[str, Any]], worker_count: Optional[int] = None):
        """Split jobs into N roughly equal chunks."""
        if not jobs:
            return []
        if worker_count is None:
            worker_count = self.WORKER_BATCHES
        size = (len(jobs) + worker_count - 1) // worker_count
        return [jobs[i:i + size] for i in range(0, len(jobs), size)]

    async def _process_batch(self):
        """
        Fetch up to MAX_JOBS_PER_CYCLE jobs once,
        split into WORKER_BATCHES chunks,
        process the chunks in parallel.
        """
        db = await MongoDBClient.get_database()
        jobs_collection = db["parsing_jobs"]

        cursor = (
            jobs_collection.find({"status": JobStatus.PENDING})
            .sort("created_at", 1)
            .limit(self.MAX_JOBS_PER_CYCLE)
        )
        pending_jobs = await cursor.to_list(length=self.MAX_JOBS_PER_CYCLE)

        if not pending_jobs:
            logger.debug("No pending jobs to process")
            return

        logger.info("Fetched %d pending job(s)", len(pending_jobs))

        chunks = self._split_into_chunks(pending_jobs, self.WORKER_BATCHES)
        logger.info("Split into %d chunk(s)", len(chunks))

        # Process each chunk in parallel, jobs inside each chunk sequential
        tasks = [
            asyncio.create_task(self._process_batch_chunk(chunk))
            for chunk in chunks
        ]
        await asyncio.gather(*tasks)

    async def _process_batch_chunk(self, job_list: List[Dict[str, Any]]):
        """Process a fixed subset of jobs sequentially."""
        logger.info("Processing chunk of %d job(s) sequentially", len(job_list))

        for job_doc in job_list:
            job_id = str(job_doc["_id"])
            try:
                await self._process_job(job_id, job_doc)
            except Exception as exc:
                logger.exception("Error processing job %s in chunk: %s", job_id, exc)
                await self._handle_job_failure(job_id, job_doc, str(exc))

    # ---------- Single job processing ----------

    async def _process_job(self, job_id: str, job_doc: Dict[str, Any]):
        """
        Process a single parsing job:
        - Atomically claim job
        - Download files
        - Parse via Gemini (with timeout)
        - Store result
        - Send webhook if configured
        """
        db = await MongoDBClient.get_database()
        jobs_collection = db["parsing_jobs"]

        tenant_id = job_doc["tenant_id"]
        project_id = job_doc["project_id"]
        report_id = job_doc["report_id"]
        files = job_doc.get("files", [])

        # Atomic claim (avoid duplicate processing)
        claim_result = await jobs_collection.update_one(
            {"_id": ObjectId(job_id), "status": JobStatus.PENDING},
            {
                "$set": {
                    "status": JobStatus.PROCESSING,
                    "started_at": time.time(),
                    "message": "Parsing in progress",
                }
            },
        )
        if getattr(claim_result, "modified_count", 0) == 0:
            logger.info("Job %s was already claimed by another worker, skipping", job_id)
            return

        try:
            await ParserJobLogger.log_job_started(
                job_id=job_id,
                tenant_id=tenant_id,
                project_id=project_id,
                files_count=len(files),
            )

            model_name = job_doc.get("model_name")
            webhook_url = job_doc.get("webhook_meta", {}).get("webhook_url")

            if not model_name:
                raise ValueError("Model name not specified in job")

            # Download files from blob
            logger.info(
                "Downloading %d file(s) from blob storage for job %s",
                len(files),
                job_id,
            )
            pdf_files = []
            blob_client = self._get_blob_client()

            for file_info in files:
                try:
                    blob_url = file_info.get("blob_url")
                    filename = file_info.get("filename", "document.pdf")

                    if not blob_url:
                        logger.warning("File info missing blob_url: %s", file_info)
                        await ParserJobLogger.log_file_download_error(
                            job_id=job_id,
                            tenant_id=tenant_id,
                            project_id=project_id,
                            filename=filename,
                            error_message="Missing blob_url in file info",
                        )
                        continue

                    file_bytes = await blob_client.download_file_bytes(blob_url, tenant_id)
                    pdf_files.append(
                        {
                            "filename": filename,
                            "bytes": file_bytes,
                            "size_mb": len(file_bytes) / (1024 * 1024),
                        }
                    )
                    logger.debug("Downloaded file from blob: %s", filename)
                except Exception as exc:
                    error_msg = f"{type(exc).__name__}: {str(exc)}"
                    logger.exception(
                        "Failed to download file from blob %s: %s",
                        file_info.get("blob_url"),
                        exc,
                    )
                    await ParserJobLogger.log_file_download_error(
                        job_id=job_id,
                        tenant_id=tenant_id,
                        project_id=project_id,
                        filename=file_info.get("filename", "unknown"),
                        error_message=error_msg,
                    )
                    continue

            if not pdf_files:
                raise ValueError("Failed to download any PDF files from blob storage")

            # Parse files using AI model with timeout
            parse_start_time = time.time()
            try:
                parsed_data = await asyncio.wait_for(
                    self._parse_files(pdf_files, model_name, job_id, tenant_id, project_id),
                    timeout=self.PARSE_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"Parsing timed out after {self.PARSE_TIMEOUT_SECONDS} seconds"
                )

            parse_end_time = time.time()
            parsing_time_seconds = round(parse_end_time - parse_start_time, 2)

            # Confidence scores
            gemini_confidence = (
                parsed_data.pop("confidence_score", None)
                if isinstance(parsed_data, dict)
                else None
            )
            gemini_confidence_summary = (
                parsed_data.pop("confidence_summary", None)
                if isinstance(parsed_data, dict)
                else None
            )

            validated_confidence, validated_summary, validation_details = (
                calculate_confidence(
                    parsed_data=parsed_data or {},
                    gemini_confidence=gemini_confidence,
                )
            )

            # Update job as completed
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
                "next_run_at": None,
            }

            await jobs_collection.update_one(
                {"_id": ObjectId(job_id)}, {"$set": update_data}
            )

            await ParserJobLogger.log_job_completed(
                job_id=job_id,
                tenant_id=tenant_id,
                project_id=project_id,
                files_processed=len(pdf_files),
                parsing_time_seconds=parsing_time_seconds,
            )

            logger.info(
                "Job %s completed successfully (parsed %d files in %fs)",
                job_id,
                len(pdf_files),
                parsing_time_seconds,
            )

            # Send webhook callback if configured
            if webhook_url:
                await self._send_webhook(
                    job_id=job_id,
                    tenant_id=tenant_id,
                    project_id=project_id,
                    report_id=report_id,
                    webhook_url=webhook_url,
                    status="completed",
                    parsed_data=parsed_data,
                )

        except Exception as exc:
            logger.exception("Error processing job %s: %s", job_id, exc)
            # Let caller handle retry logic
            raise

    # ---------- Parsing logic (Gemini) ----------

    async def _parse_files(
        self,
        pdf_files: List[Dict[str, Any]],
        model_name: str,
        job_id: str,
        tenant_id: str,
        project_id: str,
    ) -> Dict[str, Any]:
        """
        Parse PDF files using the specified AI model.
        Uses consolidated parsing if possible, falls back to per-file parsing.
        """
        gemini_client = self._get_gemini_client()
        loop = asyncio.get_running_loop()

        try:
            # Try consolidated parsing first if multiple files
            if len(pdf_files) > 1:
                try:
                    logger.info(
                        "Attempting consolidated parsing for %d files",
                        len(pdf_files),
                    )
                    consolidated_data = await loop.run_in_executor(
                        self._thread_pool,
                        lambda: gemini_client.parse_multiple_pdfs(pdf_files),
                    )
                    if consolidated_data:
                        logger.info("Consolidated parsing successful")
                        return consolidated_data
                except Exception as exc:
                    logger.warning(
                        "Consolidated parsing failed: %s, falling back to individual parsing",
                        exc,
                    )

            # Fall back to per-file parsing
            parsed_results = []
            parsing_errors: List[str] = []

            for pdf_file in pdf_files:
                try:
                    logger.info("Parsing individual file: %s", pdf_file["filename"])
                    parsed_data = await loop.run_in_executor(
                        self._thread_pool,
                        lambda f=pdf_file: gemini_client.parse_pdf(
                            f["bytes"], f["filename"]
                        ),
                    )
                    if parsed_data:
                        parsed_results.append(
                            {"filename": pdf_file["filename"], "data": parsed_data}
                        )
                        await ParserJobLogger.log_parsing_success(
                            job_id=job_id,
                            tenant_id=tenant_id,
                            project_id=project_id,
                            filename=pdf_file["filename"],
                            parsing_time_seconds=0.0,
                        )
                    else:
                        error_msg = (
                            f"Gemini returned null/empty response for {pdf_file['filename']}"
                        )
                        logger.warning(error_msg)
                        parsing_errors.append(error_msg)
                        await ParserJobLogger.log_parsing_failed(
                            job_id=job_id,
                            tenant_id=tenant_id,
                            project_id=project_id,
                            filename=pdf_file["filename"],
                            error_message="Empty response from Gemini",
                            error_type="EMPTY_RESPONSE",
                        )
                except Exception as exc:
                    error_msg = (
                        f"Exception parsing {pdf_file['filename']}: "
                        f"{type(exc).__name__}: {str(exc)}"
                    )
                    logger.exception(error_msg)
                    parsing_errors.append(error_msg)
                    await ParserJobLogger.log_parsing_failed(
                        job_id=job_id,
                        tenant_id=tenant_id,
                        project_id=project_id,
                        filename=pdf_file["filename"],
                        error_message=str(exc),
                        error_type=type(exc).__name__,
                    )
                    continue

            if not parsed_results:
                error_details = (
                    "; ".join(parsing_errors) if parsing_errors else "Unknown error"
                )
                raise ValueError(
                    f"Failed to parse any files. Details: {error_details}"
                )

            if len(parsed_results) == 1:
                return parsed_results[0]["data"]
            else:
                return self._merge_parsed_results(parsed_results)

        except Exception as exc:
            logger.exception("Error parsing files: %s", exc)
            raise

    def _merge_parsed_results(
        self, parsed_results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Merge multiple parsed results into a consolidated report."""
        if not parsed_results:
            return {}

        if len(parsed_results) == 1:
            result = parsed_results[0]["data"].copy()
            result.pop("batch_info", None)
            return result

        consolidated = parsed_results[0]["data"].copy()
        consolidated.pop("batch_info", None)

        array_fields = [
            "medications",
            "procedures",
            "lab_results",
            "imaging_findings",
            "recommendations",
        ]

        for result in parsed_results[1:]:
            data = result["data"]
            filename = result["filename"]

            # Merge array fields
            for field in array_fields:
                if field in data and data[field]:
                    if field not in consolidated or not consolidated[field]:
                        consolidated[field] = []

                    if not isinstance(consolidated[field], list):
                        consolidated[field] = (
                            [consolidated[field]] if consolidated[field] else []
                        )

                    source_data = (
                        data[field] if isinstance(data[field], list) else [data[field]]
                    )

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
                    source_diagnosis = (
                        [str(source_diagnosis)] if source_diagnosis else []
                    )

                for diag in source_diagnosis:
                    if diag and diag not in consolidated["diagnosis"]:
                        consolidated["diagnosis"].append(diag)

        return consolidated

    # ---------- Webhook sending ----------

    async def _send_webhook(
        self,
        job_id: str,
        tenant_id: str,
        project_id: str,
        report_id: str,
        webhook_url: str,
        status: str,
        parsed_data: Optional[Dict[str, Any]] = None,
    ):
        """Send webhook callback with parsing results."""
        db = await MongoDBClient.get_database()
        jobs_collection = db["parsing_jobs"]

        try:
            payload: Dict[str, Any] = {
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
                job = await jobs_collection.find_one({"_id": ObjectId(job_id)})
                if job and job.get("last_error"):
                    payload["error"] = job["last_error"]

            logger.warning("Sending Payload : %s", payload)

            async with httpx.AsyncClient(
                timeout=self.WEBHOOK_TIMEOUT_SECONDS
            ) as client:
                response = await client.post(webhook_url, json=payload)
                logger.warning("Response status: %s", response.status_code)
                logger.warning("Response headers: %s", response.headers)
                logger.warning("Response body: %s", response.text)

                if 200 <= response.status_code < 300:
                    logger.info(
                        "Webhook delivered successfully to %s (status: %d)",
                        webhook_url,
                        response.status_code,
                    )

                    await jobs_collection.update_one(
                        {"_id": ObjectId(job_id)},
                        {
                            "$set": {
                                "webhook_meta.delivered": True,
                                "webhook_meta.status": "success",
                                "webhook_meta.last_attempt_at": time.time(),
                            },
                            "$inc": {"webhook_meta.attempts": 1},
                        },
                    )

                    await ParserJobLogger.log_webhook_success(
                        job_id=job_id,
                        tenant_id=tenant_id,
                        project_id=project_id,
                        webhook_url=webhook_url,
                        status_code=response.status_code,
                    )
                else:
                    logger.warning(
                        "Webhook delivery failed to %s (status: %d)",
                        webhook_url,
                        response.status_code,
                    )

                    await jobs_collection.update_one(
                        {"_id": ObjectId(job_id)},
                        {
                            "$set": {
                                "webhook_meta.delivered": False,
                                "webhook_meta.status": f"fail ({response.status_code})",
                                "webhook_meta.last_attempt_at": time.time(),
                            },
                            "$inc": {"webhook_meta.attempts": 1},
                        },
                    )

                    await ParserJobLogger.log_webhook_failed(
                        job_id=job_id,
                        tenant_id=tenant_id,
                        project_id=project_id,
                        webhook_url=webhook_url,
                        error_message=f"HTTP {response.status_code}",
                        status_code=response.status_code,
                    )

        except Exception as exc:
            logger.exception("Failed to send webhook to %s: %s", webhook_url, exc)

            await ParserJobLogger.log_webhook_failed(
                job_id=job_id,
                tenant_id=tenant_id,
                project_id=project_id,
                webhook_url=webhook_url,
                error_message=str(exc),
            )

            try:
                await jobs_collection.update_one(
                    {"_id": ObjectId(job_id)},
                    {
                        "$set": {
                            "webhook_meta.delivered": False,
                            "webhook_meta.status": f"fail ({str(exc)[:100]})",
                            "webhook_meta.last_attempt_at": time.time(),
                        },
                        "$inc": {"webhook_meta.attempts": 1},
                    },
                )
            except Exception as update_exc:
                logger.exception(
                    "Failed to update webhook metadata: %s", update_exc
                )

    # ---------- Failure & retry handling ----------

    async def _handle_job_failure(
        self, job_id: str, job_doc: Dict[str, Any], error_message: str
    ):
        """Handle job failure with retry backoff or final failure + webhook."""
        db = await MongoDBClient.get_database()
        jobs_collection = db["parsing_jobs"]

        tenant_id = job_doc.get("tenant_id")
        project_id = job_doc.get("project_id")
        webhook_url = job_doc.get("webhook_meta", {}).get("webhook_url")

        try:
            retry_count = job_doc.get("retry_count", 0)
            max_retries = job_doc.get("max_retries", self.MAX_RETRIES)

            if retry_count < max_retries:
                retry_count += 1
                backoff_idx = min(
                    retry_count - 1, len(self.RETRY_BACKOFF_SECONDS) - 1
                )
                delay_seconds = self.RETRY_BACKOFF_SECONDS[backoff_idx]
                next_run_at = time.time() + delay_seconds

                logger.info(
                    "Job %s failed, retrying (attempt %d/%d) in %ds",
                    job_id,
                    retry_count,
                    max_retries,
                    delay_seconds,
                )

                await jobs_collection.update_one(
                    {"_id": ObjectId(job_id)},
                    {
                        "$set": {
                            "status": JobStatus.PENDING,
                            "retry_count": retry_count,
                            "next_run_at": next_run_at,
                            "last_error": error_message,
                            "message": f"Retry {retry_count}/{max_retries} in {delay_seconds}s: {error_message[:100]}",
                        }
                    },
                )

                if tenant_id and project_id:
                    await ParserJobLogger.log_job_retry(
                        job_id=job_id,
                        tenant_id=tenant_id,
                        project_id=project_id,
                        retry_count=retry_count,
                        error_message=error_message,
                    )
            else:
                logger.error(
                    "Job %s failed after %d retries", job_id, retry_count
                )

                await jobs_collection.update_one(
                    {"_id": ObjectId(job_id)},
                    {
                        "$set": {
                            "status": JobStatus.FAILED,
                            "completed_at": time.time(),
                            "last_error": error_message,
                            "message": f"Failed after {max_retries} retries: {error_message[:200]}",
                            "next_run_at": None,
                        }
                    },
                )

                if tenant_id and project_id:
                    await ParserJobLogger.log_job_failed(
                        job_id=job_id,
                        tenant_id=tenant_id,
                        project_id=project_id,
                        error_message=error_message,
                        retry_count=retry_count,
                    )

                # if webhook_url:
                #     try:
                #         await self._send_webhook(
                #             job_id=job_id,
                #             tenant_id=job_doc["tenant_id"],
                #             project_id=job_doc["project_id"],
                #             report_id=job_doc["report_id"],
                #             webhook_url=webhook_url,
                #             status="failed",
                #         )
                #     except Exception as exc:
                #         logger.exception(
                #             "Failed to send failure webhook: %s", exc
                #         )

        except Exception as exc:
            logger.exception("Error handling job failure: %s", exc)


# Global worker instance (Version A)
_worker_instance: Optional[ParsingWorker] = None


def get_parsing_worker() -> ParsingWorker:
    """Get or create the global parsing worker instance (Version A)."""
    global _worker_instance
    if _worker_instance is None:
        _worker_instance = ParsingWorker()
    return _worker_instance
