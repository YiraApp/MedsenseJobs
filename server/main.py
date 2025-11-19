"""
Google API standards compliant FastAPI application
Main entry point for Medical Report Parser API
"""

import logging
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from fastapi import Request
from fastapi.responses import Response
from datetime import datetime

from server.config.settings import get_settings
from server.core.exceptions import APIException
from server.core.logging_config import configure_logging
from server.integrations.mongodb import MongoDBClient
from server.workers.parsing_worker import get_parsing_worker
from server.api.v1.handlers.log_repository import LogRepository


# Configure logging
logger = configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    logger.info("Medical Report Parser API starting...")
    
    # Initialize MongoDB on startup
    try:
        await MongoDBClient.get_database()
        logger.info("MongoDB connection established")
    except Exception as e:
        logger.warning(f"MongoDB connection warning: {e}")
    
    # Start background parsing worker
    try:
        worker = get_parsing_worker()
        worker_task = asyncio.create_task(worker.start())
        logger.info("Parsing worker started")
        app.state.worker_task = worker_task
        app.state.worker = worker
    except Exception as e:
        logger.error(f"Failed to start parsing worker: {e}")
    
    yield
    
    # Cleanup on shutdown
    logger.info("Medical Report Parser API shutting down...")
    
    # Stop background worker
    try:
        if hasattr(app.state, "worker"):
            worker = app.state.worker
            worker.stop()
            logger.info("Parsing worker stopped")
        
        if hasattr(app.state, "worker_task"):
            worker_task = app.state.worker_task
            # Give worker time to gracefully shut down
            await asyncio.sleep(1)
            if not worker_task.done():
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
    except Exception as e:
        logger.warning(f"Error stopping worker: {e}")
    
    await MongoDBClient.close()


def create_app() -> FastAPI:
    """
    Create and configure FastAPI application

    Returns:
        FastAPI application instance
    """
    settings = get_settings()

    # Create FastAPI app with Google API metadata
    app = FastAPI(
        title=settings.app_name,
        description=settings.app_description,
        version=settings.app_version,
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def log_request_response(request: Request, call_next):
        start_time = datetime.utcnow()

        # Read request body
        try:
            request_body = await request.body()
        except Exception:
            request_body = b""

        # Process request and capture response
        response = await call_next(request)

        # Clone or read response body
        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk

        # Rebuild response object since ASGI only consumes body once
        response = Response(
            content=response_body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )

        # Compute processing time
        process_time = (datetime.utcnow() - start_time).total_seconds()

        # Log to MongoDB or Console (via configured logger)
        log_data = {
        "method": request.method,
        "path": request.url.path,
        "query": str(request.query_params),
        "request_body": request_body.decode("utf-8", errors="ignore")[:10000],
        "status": response.status_code,
        "response_body": response_body.decode("utf-8", errors="ignore")[:10000],
        "process_time": (datetime.utcnow() - start_time).total_seconds(),
        "timestamp": start_time,
        }

        try:
          await LogRepository.insert_log(log_data)
        except Exception as e:
          logger.error(f"Failed to store logs in MongoDB: {e}")

        return response

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add exception handlers
    @app.exception_handler(APIException)
    async def api_exception_handler(request, exc: APIException):
        """Handle custom API exceptions"""
        return JSONResponse(status_code=exc.status_code, content=exc.to_dict())

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request, exc: HTTPException):
        """Handle HTTP exceptions"""
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": "HTTP_ERROR",
                    "message": exc.detail,
                }
            },
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request, exc: Exception):
        """Handle general exceptions"""
        logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "An unexpected error occurred",
                }
            },
        )

    # Root endpoint
    @app.get("/", tags=["Root"])
    async def root():
        """Root endpoint - API information"""
        return {
            "service": settings.app_name,
            "version": settings.app_version,
            "status": "operational",
            "docs": "/docs",
            "health": "/api/v1/health",
            "api": f"/api/{settings.api_version}",
        }

    # Include API v1 routes (health is inside /api/v1)
    from server.api.v1.routes import router as v1_router
    app.include_router(v1_router, prefix=settings.api_prefix)

    logger.info(f"Application configured: {settings.app_name} v{settings.app_version}")
    logger.info(f"API endpoint: {settings.api_prefix}")

    return app


# Create application instance
app = create_app()


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "server.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )
