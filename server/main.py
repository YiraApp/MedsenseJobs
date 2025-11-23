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

from server.config.settings import get_settings
from server.core.exceptions import APIException
from server.core.logging_config import configure_logging
from server.integrations.mongodb import MongoDBClient
from server.workers.parsing_worker import get_parsing_worker


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
    # DISABLED: Worker is not running for now
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
