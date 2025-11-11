"""
Tenant-scoped medical report endpoints.
"""

from __future__ import annotations
from fastapi import (
    APIRouter, UploadFile, File, Depends, HTTPException, status, Query, BackgroundTasks
)
import time
import io
import logging
import zipfile
from typing import List, Optional
import asyncio

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from bson import ObjectId
from pydantic import BaseModel
from server.config.settings import get_settings
from server.integrations.azure_multitenant import MultiTenantAzureBlobClient
from server.integrations.mongodb import MongoDBClient
from server.integrations.gemini import GeminiParser
from server.middleware.auth import (
    AuthenticatedTenant,
    resolve_tenant,
)
from server.models.tenant import tenant_manager
from server.utils.usage_tracker import usage_tracker
from server.utils.confidence_calculator import calculate_confidence

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Medical Reports"])


def _get_gemini_client() -> GeminiParser:
    """Return a cached Gemini client instance."""
    if not hasattr(_get_gemini_client, "_client"):
        settings = get_settings()
        if not settings.gemini_api_key:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Gemini integration is not configured.",
            )
        _get_gemini_client._client = GeminiParser(
            api_key=settings.gemini_api_key,
            model=settings.gemini_model,
        )
    return _get_gemini_client._client  # type: ignore[attr-defined]


def _get_storage_client() -> MultiTenantAzureBlobClient:
    """Return a cached storage client instance."""
    if not hasattr(_get_storage_client, "_client"):
        settings = get_settings()
        if not settings.azure_connection_string:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Azure storage is not configured.",
            )
        _get_storage_client._client = MultiTenantAzureBlobClient(
            connection_string=settings.azure_connection_string,
            container_name=settings.azure_container_name,
        )
    return _get_storage_client._client  # type: ignore[attr-defined]

class ReportStatus(BaseModel):
    report_id: str
    status: str  # "pending", "completed", "failed"
    message: Optional[str] = None
    parsed_data: Optional[dict] = None

@router.post(
    "aync/tenants/{tenant_id}/projects/{project_id}/reports",
    summary="Upload medical report(s) for project",
    description="Upload one or more PDF medical reports for a specific project. Files are parsed using the project's assigned AI model.",
)
async def upload_report(
    tenant_id: str,
    project_id: str,
    file: List[UploadFile] = File(..., description="PDF medical report(s) or ZIP file containing PDFs"),
    waitForParsedResponse: bool = Query(True, description="Wait for parsing (synchronous) or queue and poll by reportId"),
    background_tasks: BackgroundTasks = None,
    auth: AuthenticatedTenant = Depends(resolve_tenant),
):
    db = await MongoDBClient.get_database()
    projects_collection = db["projects"]
    ai_models_collection = db["ai_models"]
    parsed_reports_collection = db["parsed_reports"]

    # Verify project
    project = await projects_collection.find_one({"project_id": project_id, "tenant_id": tenant_id})
    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Project '{project_id}' not found for tenant '{tenant_id}'")

    ai_model_id = project.get("ai_model_id")
    if not ai_model_id:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Project '{project_id}' has not activeted please contact contact@yira.ai")

    ai_model = await ai_models_collection.find_one({"model_id": ai_model_id, "tenant_id": tenant_id})
    if not ai_model:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"AI model '{ai_model_id}' not found")

    settings = get_settings()
    selected_model = ai_model.get("model_name")
    print("Using Gemini model:", selected_model)
    gemini_client = GeminiParser(api_key=settings.gemini_api_key, model=selected_model)

    # File handling (same as before)
    pdf_files = []
    total_size_mb = 0.0
    for uploaded_file in file:
        file_bytes = await uploaded_file.read()
        file_size_mb = len(file_bytes) / (1024 * 1024)
        total_size_mb += file_size_mb
        is_zip = (uploaded_file.filename and uploaded_file.filename.lower().endswith('.zip')) or (
            uploaded_file.content_type and 'zip' in uploaded_file.content_type.lower()
        )
        if is_zip:
            try:
                with zipfile.ZipFile(io.BytesIO(file_bytes)) as zip_ref:
                    for zip_info in zip_ref.namelist():
                        if '__MACOSX' in zip_info or zip_info.endswith('/'):
                            continue
                        if zip_info.lower().endswith('.pdf'):
                            pdf_bytes = zip_ref.read(zip_info)
                            pdf_size_mb = len(pdf_bytes) / (1024 * 1024)
                            pdf_files.append({'filename': zip_info, 'bytes': pdf_bytes, 'size_mb': pdf_size_mb})
            except zipfile.BadZipFile:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"File '{uploaded_file.filename}' is not a valid ZIP file.")
        else:
            if uploaded_file.content_type and uploaded_file.content_type.lower() != "application/pdf":
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Only PDF and ZIP files are supported. Got: {uploaded_file.content_type}")
            pdf_files.append({'filename': uploaded_file.filename or 'document.pdf', 'bytes': file_bytes, 'size_mb': file_size_mb})

    if not pdf_files:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No PDF files found in the uploaded file(s).")

    # Generate unique reportId
    report_object_id = ObjectId()
    report_id = str(report_object_id)
    usage_tracker.track_upload(tenant_id, total_size_mb)

    async def do_parsing_and_save(update_record=False):
        parse_start_time = time.time()
        consolidated_data = None
        parsed_results = []
        # Parse files (same as before)
        if len(pdf_files) > 1:
            try:
                consolidated_data = gemini_client.parse_multiple_pdfs(pdf_files)
                if consolidated_data:
                    parsed_results = [{'filename': pdf['filename'], 'data': consolidated_data} for pdf in pdf_files]
            except Exception as exc:
                consolidated_data = None
        if not consolidated_data:
            parsed_results = []
            for pdf_file in pdf_files:
                try:
                    parsed_data = gemini_client.parse_pdf(pdf_file['bytes'], pdf_file['filename'])
                    if parsed_data:
                        parsed_results.append({'filename': pdf_file['filename'], 'data': parsed_data})
                except Exception as exc:
                    continue
            if not parsed_results:
                await parsed_reports_collection.update_one(
                    {"report_id": report_id},
                    {"$set": {"status": "failed", "message": "Failed to parse any of the uploaded PDF files."}}
                )
                return
            consolidated_data = _merge_parsed_reports(parsed_results)
        parse_end_time = time.time()
        parsing_time_seconds = round(parse_end_time - parse_start_time, 2)
        first_file = file[0]
        blob_url = None

        clean_parsed_data = consolidated_data.copy() if isinstance(consolidated_data, dict) else {}
        gemini_confidence = clean_parsed_data.pop("confidence_score", None)
        gemini_confidence_summary = clean_parsed_data.pop("confidence_summary", None)
        validated_confidence, validated_summary, validation_details = calculate_confidence(
            parsed_data=clean_parsed_data or consolidated_data,
            gemini_confidence=gemini_confidence
        )
        parsed_report_doc = {
            "tenant_id": tenant_id,
            "project_id": project_id,
            "report_id": report_id,
            "blob_url": blob_url,
            "parsing_time_seconds": parsing_time_seconds,
            "confidence_score": validated_confidence,
            "confidence_summary": validated_summary,
            "total_size_mb": round(total_size_mb, 2),
            "files_processed": len(pdf_files),
            "successful_parses": len(parsed_results),
            "failed_parses": len(pdf_files) - len(parsed_results),
            "parsed_data": clean_parsed_data or consolidated_data,
            "status": "completed",
            "message": f"Successfully processed {len(parsed_results)} of {len(pdf_files)} PDF file(s) in {parsing_time_seconds}s.",
            "created_at": time.time(),
        }
        if update_record:
            await parsed_reports_collection.update_one(
                {"report_id": report_id},
                {"$set": parsed_report_doc}
            )
        else:
            await parsed_reports_collection.insert_one(parsed_report_doc)


    # For async: Immediately persist with status "pending", then queue parsing
    if not waitForParsedResponse:
        await parsed_reports_collection.insert_one({
            "tenant_id": tenant_id,
            "project_id": project_id,
            "report_id": report_id,
            "status": "pending",
            "message": "Parsing queued",
            "created_at": time.time(),
        })
        if background_tasks:
            asyncio.create_task(do_parsing_and_save(update_record=True))
        return {"success": True, "report_id": report_id, "status": "pending", "message": "Parsing queued"}

    # For sync: Do parsing synchronously, then return data
    await do_parsing_and_save(update_record=False)
    doc = await parsed_reports_collection.find_one({"report_id": report_id})
    return _serialize_mongodb_doc(doc) if doc else {"success": False, "report_id": report_id, "status": "failed", "message": "Parsing failed"}



@router.post(
    "/tenants/{tenant_id}/projects/{project_id}/reports",
    summary="Upload medical report(s) for project",
    description="Upload one or more PDF medical reports for a specific project. Files are parsed using the project's assigned AI model.",
)
async def upload_report(
    tenant_id: str,
    project_id: str,
    file: List[UploadFile] = File(..., description="PDF medical report(s) or ZIP file containing PDFs"),
    auth: AuthenticatedTenant = Depends(resolve_tenant),
):
    """
    Upload PDF report(s) for a project and kick off parsing.
    
    Updated to use projectId instead of model parameter.
    Retrieves AI model from project configuration.
    
    Supports:
    - Single PDF file
    - Multiple PDF files (consolidated into one report)
    - ZIP file containing PDFs (auto-extracted and consolidated)
    - Mix of PDFs and ZIP files
    """
    try:
        db = await MongoDBClient.get_database()
        projects_collection = db["projects"]
        ai_models_collection = db["ai_models"]
        parsed_reports_collection = db["parsed_reports"]  # <-- Add this line

        # Verify project exists and belongs to tenant
        project = await projects_collection.find_one({
            "project_id": project_id,
            "tenant_id": tenant_id
        })
        
        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Project '{project_id}' not found for tenant '{tenant_id}'",
            )
        
        ai_model_id = project.get("ai_model_id")
        if not ai_model_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Project '{project_id}' has no assigned AI model",
            )
        
        ai_model = await ai_models_collection.find_one({
            "model_id": ai_model_id,
            "tenant_id": tenant_id
        })
        
        if not ai_model:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"AI model '{ai_model_id}' not found",
            )
        
        # Use AI model's provider to determine which model to use
        settings = get_settings()
        selected_model = settings.gemini_model  # Default to settings
        
        logger.info(f"Processing report for project '{project_id}' using AI model '{ai_model_id}' with Gemini model '{selected_model}'")
        
        # Create Gemini client with selected model
        gemini_client = GeminiParser(
            api_key=settings.gemini_api_key,
            model=selected_model,
        )

        storage_client = _get_storage_client()
        
        try:
            # Collect all PDF files (from direct uploads and ZIP extraction)
            pdf_files = []
            total_size_mb = 0.0
            
            for uploaded_file in file:
                file_bytes = await uploaded_file.read()
                file_size_mb = len(file_bytes) / (1024 * 1024)
                total_size_mb += file_size_mb
                
                # Check if this is a ZIP file
                is_zip = (
                    uploaded_file.filename and uploaded_file.filename.lower().endswith('.zip')
                ) or (
                    uploaded_file.content_type and 'zip' in uploaded_file.content_type.lower()
                )
                
                if is_zip:
                    logger.info("Extracting ZIP file '%s' for tenant '%s'", uploaded_file.filename, tenant_id)
                    try:
                        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zip_ref:
                            for zip_info in zip_ref.namelist():
                                # Skip macOS metadata files and directories
                                if '__MACOSX' in zip_info or zip_info.endswith('/'):
                                    continue
                                
                                # Only process PDF files
                                if zip_info.lower().endswith('.pdf'):
                                    pdf_bytes = zip_ref.read(zip_info)
                                    pdf_size_mb = len(pdf_bytes) / (1024 * 1024)
                                    pdf_files.append({
                                        'filename': zip_info,
                                        'bytes': pdf_bytes,
                                        'size_mb': pdf_size_mb,
                                    })
                                    logger.info("Extracted PDF '%s' (%.2f MB) from ZIP", zip_info, pdf_size_mb)
                    except zipfile.BadZipFile:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"File '{uploaded_file.filename}' is not a valid ZIP file.",
                        )
                else:
                    # Regular PDF file
                    if uploaded_file.content_type and uploaded_file.content_type.lower() != "application/pdf":
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Only PDF and ZIP files are supported. Got: {uploaded_file.content_type}",
                        )
                    pdf_files.append({
                        'filename': uploaded_file.filename or 'document.pdf',
                        'bytes': file_bytes,
                        'size_mb': file_size_mb,
                    })
            
            if not pdf_files:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="No PDF files found in the uploaded file(s).",
                )
            
            # Log all received files
            logger.info("=" * 60)
            logger.info("FILES RECEIVED FOR PROJECT '%s' IN TENANT '%s':", project_id, tenant_id)
            for idx, pdf_file in enumerate(pdf_files, 1):
                logger.info("  [%d] %s (%.2f MB)", idx, pdf_file['filename'], pdf_file['size_mb'])
            logger.info("=" * 60)
            
            logger.info("Processing %d PDF file(s) for project '%s'", len(pdf_files), project_id)
            
            # Start timing
            parse_start_time = time.time()
            
            # For multiple PDFs, parse them together to enable accurate cross-document photo comparison
            consolidated_data = None
            parsed_results = []
            
            if len(pdf_files) > 1:
                logger.info("Parsing %d PDFs together for accurate cross-document photo comparison", len(pdf_files))
                try:
                    consolidated_data = gemini_client.parse_multiple_pdfs(pdf_files)
                    if consolidated_data:
                        logger.info("Successfully parsed %d PDFs together", len(pdf_files))
                        parsed_results = [{'filename': pdf['filename'], 'data': consolidated_data} for pdf in pdf_files]
                except Exception as exc:
                    logger.error("Batch parsing failed: %s. Falling back to individual parsing.", exc)
                    consolidated_data = None
            
            # Fallback: Parse each PDF individually if batch failed or single file
            if not consolidated_data:
                parsed_results = []
                for pdf_file in pdf_files:
                    logger.info("Parsing PDF '%s' for project '%s'", pdf_file['filename'], project_id)
                    try:
                        parsed_data = gemini_client.parse_pdf(pdf_file['bytes'], pdf_file['filename'])
                        if parsed_data:
                            parsed_results.append({
                                'filename': pdf_file['filename'],
                                'data': parsed_data,
                            })
                    except Exception as exc:
                        logger.warning("Failed to parse PDF '%s': %s", pdf_file['filename'], exc)
                        continue
                
                if not parsed_results:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Failed to parse any of the uploaded PDF files.",
                    )
                
                # Consolidate all parsed data into a single report
                consolidated_data = _merge_parsed_reports(parsed_results)
            
            # Calculate parsing time
            parse_end_time = time.time()
            parsing_time_seconds = round(parse_end_time - parse_start_time, 2)
            logger.info("⏱️  PARSING TIME: %.2f seconds using model '%s'", parsing_time_seconds, selected_model)
            
            # Try to store the consolidated report (optional - continue even if storage fails)
            report_id = None
            storage_error = None
            first_file = file[0]
            blob_url = None

            try:
                await first_file.seek(0)
                # Ensure we have a report_id for a stable blob name
                if not report_id:
                    # Create a concise unique id for the blob name
                    report_object_id = ObjectId()
                    report_id = str(report_object_id)

                # Include UTC timestamp in blob name to avoid collisions and aid debugging
                from datetime import datetime
                timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                azure_blob_name = f"{tenant_id}/{report_id}_{timestamp}.pdf"

                blob_url = await storage_client.upload_pdf_and_get_url(
                    tenant_id=tenant_id,
                    blob_name=azure_blob_name,
                    file=first_file,
                )
            except Exception as storage_exc:
                storage_error = str(storage_exc)
                logger.warning("Failed to store report in Azure (continuing anyway): %s", storage_exc)
                blob_url = None

            usage_tracker.track_upload(tenant_id, total_size_mb)

            # Split confidence information from the parsed payload
            clean_parsed_data = consolidated_data.copy() if isinstance(consolidated_data, dict) else {}
            gemini_confidence = None
            gemini_confidence_summary = None
            if clean_parsed_data:
                gemini_confidence = clean_parsed_data.pop("confidence_score", None)
                gemini_confidence_summary = clean_parsed_data.pop("confidence_summary", None)
            
            # Calculate REAL confidence score based on data quality validation
            logger.info("🔍 Calculating validated confidence score...")
            validated_confidence, validated_summary, validation_details = calculate_confidence(
                parsed_data=clean_parsed_data or consolidated_data,
                gemini_confidence=gemini_confidence
            )
            logger.info("✅ Validated Confidence: %d/100 - %s", validated_confidence, validated_summary)

            # response_data = {
            #     "success": True,
            #     "tenant_id": tenant_id,
            #     "project_id": project_id,
            #     "report_id": report_id,
            #     "ai_model_id": ai_model_id,
            #     "model_used": selected_model,
            #     "parsing_time_seconds": parsing_time_seconds,
            #     "confidence_score": validated_confidence,
            #     "confidence_summary": validated_summary,
            #     "total_size_mb": round(total_size_mb, 2),
            #     "files_processed": len(pdf_files),
            #     "successful_parses": len(parsed_results),
            #     "failed_parses": len(pdf_files) - len(parsed_results),
            #     "parsed_data": clean_parsed_data or consolidated_data,
            #     "message": f"Successfully processed {len(parsed_results)} of {len(pdf_files)} PDF file(s) in {parsing_time_seconds}s.",
            # }
            
            # if storage_error:
            #     response_data["storage_warning"] = (
            #         "Note: Azure Blob Storage is not available. Report parsed successfully but not persisted. "
            #         f"Error: {storage_error[:100]}"
            #     )
            
            # Store parsed data and blob URL in MongoDB
            try:
                # Generate unique report_id if not already set (we may have created it before upload)
                if not report_id:
                    report_object_id = ObjectId()
                    report_id = str(report_object_id)

                parsed_report_doc = {
                    "tenant_id": tenant_id,
                    "project_id": project_id,
                    "report_id": report_id,  # <-- always unique ObjectId string
                    "blob_url": blob_url,
                    "parsing_time_seconds": parsing_time_seconds,
                    "confidence_score": validated_confidence,
                    "confidence_summary": validated_summary,
                    "total_size_mb": round(total_size_mb, 2),
                    "files_processed": len(pdf_files),
                    "successful_parses": len(parsed_results),
                    "failed_parses": len(pdf_files) - len(parsed_results),
                    "parsed_data": clean_parsed_data or consolidated_data,
                    "created_at": time.time(),
                }
                result = await parsed_reports_collection.insert_one(parsed_report_doc)
                parsed_report_doc["_id"] = str(result.inserted_id)
                logger.info("Parsed report stored in DB for report_id: %s", report_id)
            except Exception as db_exc:
                logger.warning("Failed to store parsed report in DB: %s", db_exc)

            return parsed_report_doc
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Failed to upload report for project %s: %s", project_id, exc)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process report. Please retry or contact support.",
            ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to process upload request: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process report. Please retry or contact support.",
        ) from exc


@router.post(
    "/tenants/{tenant_id}/reports",
    summary="Upload medical report(s) for tenant",
    description="Upload one or more PDF medical reports, or a ZIP file containing PDFs. All files will be consolidated into a single report.",
)
async def upload_report_legacy(
    tenant_id: str,
    file: List[UploadFile] = File(..., description="PDF medical report(s) or ZIP file containing PDFs"),
    model: Optional[str] = Query(
        None, 
        description="Gemini model to use: 'gemini-2.5-pro' (accurate, slower) or 'gemini-2.5-flash' (fast, efficient). Defaults to settings.",
        regex="^(gemini-2\\.5-pro|gemini-2\\.5-flash)$"
    ),
    auth: AuthenticatedTenant = Depends(resolve_tenant),
):
    """
    Upload PDF report(s) for the tenant and kick off parsing.
    Supports:
    - Single PDF file
    - Multiple PDF files (consolidated into one report)
    - ZIP file containing PDFs (auto-extracted and consolidated)
    - Mix of PDFs and ZIP files
    """
    tenant = tenant_manager.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tenant '{tenant_id}' not found.",
        )

    storage_client = _get_storage_client()
    db = await MongoDBClient.get_database()
    parsed_reports_collection = db["parsed_reports"]  # <-- Add this line
    
    # Use specified model or fallback to settings default
    settings = get_settings()
    selected_model = model or settings.gemini_model
    logger.info("Using Gemini model: %s (requested: %s, default: %s)", 
                selected_model, model, settings.gemini_model)
    
    # Create Gemini client with selected model
    gemini_client = GeminiParser(
        api_key=settings.gemini_api_key,
        model=selected_model,
    )

    try:
        # Collect all PDF files (from direct uploads and ZIP extraction)
        pdf_files = []
        total_size_mb = 0.0
        
        for uploaded_file in file:
            file_bytes = await uploaded_file.read()
            file_size_mb = len(file_bytes) / (1024 * 1024)
            total_size_mb += file_size_mb
            
            # Check if this is a ZIP file
            is_zip = (
                uploaded_file.filename and uploaded_file.filename.lower().endswith('.zip')
            ) or (
                uploaded_file.content_type and 'zip' in uploaded_file.content_type.lower()
            )
            
            if is_zip:
                logger.info("Extracting ZIP file '%s' for tenant '%s'", uploaded_file.filename, tenant_id)
                try:
                    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zip_ref:
                        for zip_info in zip_ref.namelist():
                            # Skip macOS metadata files and directories
                            if '__MACOSX' in zip_info or zip_info.endswith('/'):
                                continue
                            
                            # Only process PDF files
                            if zip_info.lower().endswith('.pdf'):
                                pdf_bytes = zip_ref.read(zip_info)
                                pdf_size_mb = len(pdf_bytes) / (1024 * 1024)
                                pdf_files.append({
                                    'filename': zip_info,
                                    'bytes': pdf_bytes,
                                    'size_mb': pdf_size_mb,
                                })
                                logger.info("Extracted PDF '%s' (%.2f MB) from ZIP", zip_info, pdf_size_mb)
                except zipfile.BadZipFile:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"File '{uploaded_file.filename}' is not a valid ZIP file.",
                    )
            else:
                # Regular PDF file
                if uploaded_file.content_type and uploaded_file.content_type.lower() != "application/pdf":
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Only PDF and ZIP files are supported. Got: {uploaded_file.content_type}",
                    )
                pdf_files.append({
                    'filename': uploaded_file.filename or 'document.pdf',
                    'bytes': file_bytes,
                    'size_mb': file_size_mb,
                })
        
        if not pdf_files:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No PDF files found in the uploaded file(s).",
            )
        
        # Log all received files
        logger.info("=" * 60)
        logger.info("FILES RECEIVED FOR TENANT '%s':", tenant_id)
        for idx, pdf_file in enumerate(pdf_files, 1):
            logger.info("  [%d] %s (%.2f MB)", idx, pdf_file['filename'], pdf_file['size_mb'])
        logger.info("=" * 60)
        
        logger.info("Processing %d PDF file(s) for tenant '%s'", len(pdf_files), tenant_id)
        
        # Start timing
        import time
        parse_start_time = time.time()
        
        # For multiple PDFs, parse them together to enable accurate cross-document photo comparison
        consolidated_data = None
        parsed_results = []  # Initialize to track parsing results
        
        if len(pdf_files) > 1:
            logger.info("Parsing %d PDFs together for accurate cross-document photo comparison", len(pdf_files))
            try:
                consolidated_data = gemini_client.parse_multiple_pdfs(pdf_files)
                if consolidated_data:
                    logger.info("Successfully parsed %d PDFs together", len(pdf_files))
                    # Mark all as successfully parsed for batch mode
                    parsed_results = [{'filename': pdf['filename'], 'data': consolidated_data} for pdf in pdf_files]
            except Exception as exc:
                logger.error("Batch parsing failed: %s. Falling back to individual parsing.", exc)
                consolidated_data = None
        
        # Fallback: Parse each PDF individually if batch failed or single file
        if not consolidated_data:
            parsed_results = []
            for pdf_file in pdf_files:
                logger.info("Parsing PDF '%s' for tenant '%s'", pdf_file['filename'], tenant_id)
                try:
                    parsed_data = gemini_client.parse_pdf(pdf_file['bytes'], pdf_file['filename'])
                    if parsed_data:
                        parsed_results.append({
                            'filename': pdf_file['filename'],
                            'data': parsed_data,
                        })
                except Exception as exc:
                    logger.warning("Failed to parse PDF '%s': %s", pdf_file['filename'], exc)
                    continue
            
            if not parsed_results:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to parse any of the uploaded PDF files.",
                )
            
            # Consolidate all parsed data into a single report
            consolidated_data = _merge_parsed_reports(parsed_results)
        
        # Calculate parsing time
        parse_end_time = time.time()
        parsing_time_seconds = round(parse_end_time - parse_start_time, 2)
        logger.info("⏱️  PARSING TIME: %.2f seconds using model '%s'", parsing_time_seconds, selected_model)
        
        # Try to store the consolidated report (optional - continue even if storage fails)
        report_id = None
        storage_error = None
        first_file = file[0]
        blob_url = None

        try:
            await first_file.seek(0)
            # Ensure we have a report_id for a stable blob name
            if not report_id:
                # Create a concise unique id for the blob name
                report_object_id = ObjectId()
                report_id = str(report_object_id)

            # Include UTC timestamp in blob name to avoid collisions and aid debugging
            from datetime import datetime
            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            azure_blob_name = f"{tenant_id}/{report_id}_{timestamp}.pdf"

            blob_url = await storage_client.upload_pdf_and_get_url(
                tenant_id=tenant_id,
                blob_name=azure_blob_name,
                file=first_file,
            )
        except Exception as storage_exc:
            storage_error = str(storage_exc)
            logger.warning("Failed to store report in Azure (continuing anyway): %s", storage_exc)
            blob_url = None

        usage_tracker.track_upload(tenant_id, total_size_mb)

        # Split confidence information from the parsed payload so we can surface it cleanly
        clean_parsed_data = consolidated_data.copy() if isinstance(consolidated_data, dict) else {}
        gemini_confidence = None
        gemini_confidence_summary = None
        if clean_parsed_data:
            gemini_confidence = clean_parsed_data.pop("confidence_score", None)
            gemini_confidence_summary = clean_parsed_data.pop("confidence_summary", None)
        
        # Calculate REAL confidence score based on data quality validation
        logger.info("🔍 Calculating validated confidence score...")
        validated_confidence, validated_summary, validation_details = calculate_confidence(
            parsed_data=clean_parsed_data or consolidated_data,
            gemini_confidence=gemini_confidence
        )
        logger.info("✅ Validated Confidence: %d/100 - %s", validated_confidence, validated_summary)
        logger.info("   Gemini Self-Assessment: %s", gemini_confidence or "N/A")

        # response_data = {
        #     "success": True,
        #     "parsing_time_seconds": parsing_time_seconds,
        #     "confidence_score": validated_confidence,
        #     "confidence_summary": validated_summary,
        #     "total_size_mb": round(total_size_mb, 2),
        #     "files_processed": len(pdf_files),
        #     "successful_parses": len(parsed_results),
        #     "failed_parses": len(pdf_files) - len(parsed_results),
        #     "parsed_data": clean_parsed_data or consolidated_data,
        #     "message": f"Successfully processed {len(parsed_results)} of {len(pdf_files)} PDF file(s) in {parsing_time_seconds}s using {selected_model}.",
        # }
        
        # Store parsed data and blob URL in MongoDB
        try:
            # Generate unique report_id if not already set (we may have created it before upload)
            if not report_id:
                report_object_id = ObjectId()
                report_id = str(report_object_id)

            parsed_report_doc = {
                "tenant_id": tenant_id,
                "project_id": None,
                "report_id": report_id,  # <-- always unique ObjectId string
                "blob_url": blob_url,
                "parsing_time_seconds": parsing_time_seconds,
                "confidence_score": validated_confidence,
                "confidence_summary": validated_summary,
                "total_size_mb": round(total_size_mb, 2),
                "files_processed": len(pdf_files),
                "successful_parses": len(parsed_results),
                "failed_parses": len(pdf_files) - len(parsed_results),
                "parsed_data": clean_parsed_data or consolidated_data,
                "created_at": time.time(),
            }
            result = await parsed_reports_collection.insert_one(parsed_report_doc)
            parsed_report_doc["_id"] = str(result.inserted_id)
            logger.info("Parsed report stored in DB for report_id: %s", report_id)
        except Exception as db_exc:
            logger.warning("Failed to store parsed report in DB: %s", db_exc)

        return parsed_report_doc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to upload report for tenant %s: %s", tenant_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process report. Please retry or contact support.",
        ) from exc


@router.get(
    "/tenants/{tenant_id}/reports",
    summary="List reports for tenant",
    description="Retrieve metadata for reports that belong to the tenant.",
)
async def list_reports(
    tenant_id: str,
    limit: int = Query(10, ge=1, le=100, description="Maximum number of reports to return."),
    offset: int = Query(0, ge=0, description="Number of reports to skip from the start."),
    projectid: Optional[str] = Query(None, description="Filter reports by project id (query param: projectid)"),
    auth: AuthenticatedTenant = Depends(resolve_tenant),
):
    """List reports that belong to the authenticated tenant."""
    logger.debug("Tenant %s authenticated via %s", auth.tenant_id, auth.method)
    usage_tracker.track_api_call(tenant_id, "list_reports")

    db = await MongoDBClient.get_database()
    parsed_reports_collection = db["parsed_reports"]

    try:
        # Build base filter scoped to tenant
        filter_query: dict = {"tenant_id": tenant_id}
        # Optional project filter
        if projectid:
            filter_query["project_id"] = projectid

        cursor = parsed_reports_collection.find(filter_query).skip(offset).limit(limit)
        reports = await cursor.to_list(length=limit)
        reports = [_serialize_mongodb_doc(report) for report in reports]
        total_reports = await parsed_reports_collection.count_documents(filter_query)
        return {
            "success": True,
            "tenant_id": tenant_id,
            "project_id": projectid,
            "limit": limit,
            "offset": offset,
            "total_reports": total_reports,
            "reports": reports,
        }
    except Exception as exc:
        logger.exception("Failed to list reports for tenant %s: %s", tenant_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to fetch reports at this time.",
        ) from exc


@router.get(
    "/tenants/{tenant_id}/reports/{report_id:path}",
    summary="Get report details",
    description="Retrieve a specific report and its parsed data for the tenant.",
)
async def get_report(
    tenant_id: str,
    report_id: str,
    auth: AuthenticatedTenant = Depends(resolve_tenant),
):
    """Fetch report data for the authenticated tenant."""
    logger.debug("Tenant %s authenticated via %s", auth.tenant_id, auth.method)
    usage_tracker.track_api_call(tenant_id, "get_report")

    db = await MongoDBClient.get_database()
    parsed_reports_collection = db["parsed_reports"]

    try:
        report = await parsed_reports_collection.find_one({"tenant_id": tenant_id, "report_id": report_id})
        if not report:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Report not found or access denied.",
            )
        return {
            "success": True, 
            "tenant_id": tenant_id, 
            "report": _serialize_mongodb_doc(report)
        }
    except Exception as exc:
        logger.exception("Failed to fetch report '%s' for tenant %s: %s", report_id, tenant_id, exc)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Report not found or access denied.",
        ) from exc


@router.delete(
    "/tenants/{tenant_id}/reports/{report_id:path}",
    summary="Delete report",
    description="Delete a report and its parsed data for the tenant.",
)
async def delete_report(
    tenant_id: str,
    report_id: str,
    auth: AuthenticatedTenant = Depends(resolve_tenant),
):
    """Delete a report belonging to the tenant."""
    logger.debug("Tenant %s authenticated via %s", auth.tenant_id, auth.method)

    db = await MongoDBClient.get_database()
    parsed_reports_collection = db["parsed_reports"]
    storage_client = _get_storage_client()

    try:
        report = await parsed_reports_collection.find_one({"tenant_id": tenant_id, "report_id": report_id})
        if not report:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Report not found or already deleted.",
            )
        # Delete blob from Azure if blob_url exists
        blob_url = report.get("blob_url")
        if blob_url and hasattr(storage_client, "delete_blob_by_url"):
            try:
                await storage_client.delete_blob_by_url(blob_url)
            except Exception as exc:
                logger.warning("Failed to delete blob from Azure: %s", exc)
        # Delete metadata from MongoDB
        await parsed_reports_collection.delete_one({"tenant_id": tenant_id, "report_id": report_id})
        return {
            "success": True,
            "tenant_id": tenant_id,
            "message": f"Report '{report_id}' deleted successfully.",
        }
    except Exception as exc:
        logger.exception("Failed to delete report '%s' for tenant %s: %s", report_id, tenant_id, exc)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Report not found or already deleted.",
        ) from exc


@router.post(
    "/tenants/{tenant_id}/reports/{report_id:path}/generate",
    summary="Trigger report generation",
    description="Re-run parsing or generate a derived report.",
)
async def generate_report(
    tenant_id: str,
    report_id: str,
    auth: AuthenticatedTenant = Depends(resolve_tenant),
):
    """Trigger report regeneration for the tenant."""
    logger.debug("Tenant %s authenticated via %s", auth.tenant_id, auth.method)
    usage_tracker.track_report_generation(tenant_id)

    db = await MongoDBClient.get_database()
    parsed_reports_collection = db["parsed_reports"]

    try:
        report = await parsed_reports_collection.find_one({"tenant_id": tenant_id, "report_id": report_id})
        if not report:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Report not found or access denied.",
            )
        return {
            "success": True,
            "tenant_id": tenant_id,
            "report_id": report_id,
            "status": "processing",
            "message": "Report generation triggered.",
            "report": _serialize_mongodb_doc(report),
        }
    except Exception as exc:
        logger.exception(
            "Failed to trigger generation for report '%s' tenant %s: %s",
            report_id,
            tenant_id,
            exc,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Report not found or access denied.",
        ) from exc


@router.get(
    "/tenants/{tenant_id}/usage",
    summary="Get usage summary",
    description="Retrieve aggregated usage statistics for billing and analytics.",
)
async def get_usage(
    tenant_id: str,
    auth: AuthenticatedTenant = Depends(resolve_tenant),
):
    """Return aggregate usage metrics for the tenant."""
    logger.debug("Tenant %s authenticated via %s", auth.tenant_id, auth.method)
    usage = usage_tracker.get_usage(tenant_id)
    return {"success": True, "tenant_id": tenant_id, "usage": usage}


@router.get(
    "/tenants/{tenant_id}/usage/monthly",
    summary="Get monthly usage",
    description="Retrieve usage for a specific month with derived billing totals.",
)
async def get_monthly_usage(
    tenant_id: str,
    month: Optional[str] = Query(None, description="Month in YYYY-MM format."),
    auth: AuthenticatedTenant = Depends(resolve_tenant),
):
    """Return monthly usage metrics and computed billing totals."""
    logger.debug("Tenant %s authenticated via %s", auth.tenant_id, auth.method)
    usage = usage_tracker.get_monthly_usage(tenant_id, month)

    # Example pricing model for demos
    price_per_upload = 0.50
    price_per_gb = 5.00
    price_per_generation = 0.25

    uploads = usage.get("uploads", 0)
    storage_gb = usage.get("storage_mb", 0.0) / 1024
    generations = usage.get("reports_generated", 0)

    total_cost = (uploads * price_per_upload) + (storage_gb * price_per_gb) + (generations * price_per_generation)

    return {
        "success": True,
        "tenant_id": tenant_id,
        "month": month or "current",
        "usage": usage,
        "billing": {
            "uploads": round(uploads * price_per_upload, 2),
            "storage": round(storage_gb * price_per_gb, 2),
            "generations": round(generations * price_per_generation, 2),
            "total_usd": round(total_cost, 2),
        },
    }


def _merge_parsed_reports(parsed_results: List[dict]) -> dict:
    """
    Merge multiple parsed report results into a single consolidated report.
    
    Args:
        parsed_results: List of dicts with 'filename' and 'data' keys
        
    Returns:
        Consolidated report data with merged fields and batch_info
    """
    if not parsed_results:
        return {}
    
    if len(parsed_results) == 1:
        # Single report, no batch info needed
        result = parsed_results[0]['data'].copy()
        # Remove batch_info if it exists (added by parse_multiple_pdfs)
        result.pop('batch_info', None)
        return result
    
    # Start with first report as base
    consolidated = parsed_results[0]['data'].copy()
    # Remove batch_info - will be added at root level in response
    consolidated.pop('batch_info', None)
    
    # Array fields that should be merged by extending
    array_fields = ['medications', 'procedures', 'lab_results', 'imaging_findings', 'recommendations']
    
    # Merge data from remaining reports
    for result in parsed_results[1:]:
        data = result['data']
        filename = result['filename']
        
        # Merge array fields
        for field in array_fields:
            if field in data and data[field]:
                # Initialize field if not present or empty
                if field not in consolidated or not consolidated[field]:
                    consolidated[field] = []
                
                # Ensure it's a list before extending
                if not isinstance(consolidated[field], list):
                    consolidated[field] = [consolidated[field]] if consolidated[field] else []
                
                # Ensure source data is a list
                source_data = data[field] if isinstance(data[field], list) else [data[field]]
                
                # Add source_file attribution to each item
                for item in source_data:
                    if isinstance(item, dict):
                        item['source_file'] = filename
                
                consolidated[field].extend(source_data)
        
        # Merge diagnosis (can be string or list)
        if 'diagnosis' in data and data['diagnosis']:
            if 'diagnosis' not in consolidated or not consolidated['diagnosis']:
                consolidated['diagnosis'] = []
            
            # Normalize consolidated diagnosis to list
            if isinstance(consolidated['diagnosis'], str):
                consolidated['diagnosis'] = [consolidated['diagnosis']]
            elif not isinstance(consolidated['diagnosis'], list):
                consolidated['diagnosis'] = []
            
            # Normalize source diagnosis to list
            source_diagnosis = data['diagnosis']
            if isinstance(source_diagnosis, str):
                source_diagnosis = [source_diagnosis]
            elif not isinstance(source_diagnosis, list):
                source_diagnosis = [str(source_diagnosis)] if source_diagnosis else []
            
            # Merge without duplicates
            for diag in source_diagnosis:
                if diag and diag not in consolidated['diagnosis']:
                    consolidated['diagnosis'].append(diag)
        
        # Merge photo_comparison data
        if 'photo_comparison' in data and data['photo_comparison']:
            pc_data = data['photo_comparison']
            
            if 'photo_comparison' not in consolidated or not consolidated['photo_comparison']:
                consolidated['photo_comparison'] = {
                    'images_found': [],
                    'comparison_performed': False,
                    'similarity_percentage': None,
                    'comparison_details': None,
                    'notes': '',
                }
            
            # Merge images_found with offset for image numbers
            if 'images_found' in pc_data and pc_data['images_found']:
                existing_images = consolidated['photo_comparison'].get('images_found', [])
                offset = len(existing_images)
                
                for img in pc_data['images_found']:
                    new_img = img.copy()
                    new_img['image_number'] = img.get('image_number', 0) + offset
                    new_img['source_file'] = filename
                    existing_images.append(new_img)
                
                consolidated['photo_comparison']['images_found'] = existing_images
            
            # Update comparison fields if not already set
            if pc_data.get('comparison_performed') and not consolidated['photo_comparison'].get('comparison_performed'):
                consolidated['photo_comparison']['comparison_performed'] = True
            
            if pc_data.get('similarity_percentage') is not None and consolidated['photo_comparison'].get('similarity_percentage') is None:
                consolidated['photo_comparison']['similarity_percentage'] = pc_data['similarity_percentage']
            
            if pc_data.get('comparison_details') and not consolidated['photo_comparison'].get('comparison_details'):
                consolidated['photo_comparison']['comparison_details'] = pc_data['comparison_details']
            
            # Merge notes
            if pc_data.get('notes'):
                existing_notes = consolidated['photo_comparison'].get('notes', '')
                if existing_notes:
                    consolidated['photo_comparison']['notes'] = f"{existing_notes} | {pc_data['notes']}"
                else:
                    consolidated['photo_comparison']['notes'] = pc_data['notes']
    
    # After merging all photo comparisons, check if we have 2+ images but no comparison
    if consolidated.get('photo_comparison'):
        pc = consolidated['photo_comparison']
        images_count = len(pc.get('images_found', []))
        
        # If we have 2+ images but comparison wasn't performed, log warning
        if images_count >= 2 and not pc.get('comparison_performed'):
            logger.warning(
                "Photo comparison has %d images but comparison_performed=False. "
                "This may indicate Gemini didn't follow instructions.",
                images_count
            )
            # Force it to show that comparison should have been performed
            pc['notes'] = (pc.get('notes', '') + 
                          f" | WARNING: {images_count} images found but comparison not performed by AI.").strip()
    
    return consolidated
    
def _serialize_mongodb_doc(doc: dict) -> dict:
    """Convert MongoDB document to JSON-serializable format."""
    if not doc:
        return doc
        
    result = {}
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            result[key] = str(value)
        elif isinstance(value, dict):
            result[key] = _serialize_mongodb_doc(value)
        elif isinstance(value, list):
            result[key] = [
                _serialize_mongodb_doc(item) if isinstance(item, dict) else 
                str(item) if isinstance(item, ObjectId) else item 
                for item in value
            ]
        else:
            result[key] = value
    return result
