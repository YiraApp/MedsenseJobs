"""
Analytics Tracking Service
Captures and stores metrics for project and tenant-level analytics.
Tracks usage, costs, parsing performance, and success rates.
"""

import logging
from datetime import datetime
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorDatabase

from server.models.project import ProjectAnalytics, TenantAnalytics
from server.integrations.mongodb import MongoDBClient

logger = logging.getLogger(__name__)


class AnalyticsTracker:
    """Tracks and persists analytics for projects and tenants"""
    
    def __init__(self):
        """Initialize analytics tracker"""
        self.db: Optional[AsyncIOMotorDatabase] = None
    
    async def _get_database(self) -> AsyncIOMotorDatabase:
        """Get MongoDB database instance"""
        if self.db is None:
            self.db = await MongoDBClient.get_database()
        return self.db
    
    async def track_report_parse(
        self,
        tenant_id: str,
        project_id: str,
        pages_processed: int,
        parsing_time_seconds: float,
        cost_per_page: float,
        success: bool = True,
        error_message: Optional[str] = None,
    ) -> bool:
        """
        Track a single report parsing event.
        
        This creates/updates a ProjectAnalytics record in MongoDB with the metrics
        from a single report parse.
        
        Args:
            tenant_id: Tenant identifier
            project_id: Project identifier
            pages_processed: Number of pages in the report
            parsing_time_seconds: How long parsing took (seconds)
            cost_per_page: Cost per page from the AI model
            success: Whether parsing was successful
            error_message: Error message if parsing failed (optional)
        
        Returns:
            True if successfully tracked, False otherwise
        """
        try:
            db = await self._get_database()
            analytics_collection = db["analytics"]
            
            # Success rate is either 100% (success) or 0% (failure)
            success_rate = 100.0 if success else 0.0
            
            # Create analytics document
            analytics_doc = {
                "tenant_id": tenant_id,
                "project_id": project_id,
                "timestamp": datetime.utcnow(),
                "uploads_count": 1,  # One report parsed
                "total_pages_processed": pages_processed,
                "cost_per_page": cost_per_page,  # Store cost_per_page without calculating total
                "average_parsing_time_seconds": parsing_time_seconds,
                "success_rate": success_rate,
                "status": "success" if success else "failed",
                "error_message": error_message,
            }
            
            # Insert the analytics record
            result = await analytics_collection.insert_one(analytics_doc)
            
            logger.info(
                f"✅ Analytics tracked for project {project_id}: "
                f"{pages_processed} pages, cost_per_page=${cost_per_page:.4f}, "
                f"{parsing_time_seconds:.2f}s, success={success}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to track analytics: {str(e)}")
            logger.exception("Full traceback:")
            return False
    
    async def get_project_analytics_summary(
        self,
        tenant_id: str,
        project_id: str,
    ) -> dict:
        """
        Get aggregated analytics for a specific project.
        
        Args:
            tenant_id: Tenant identifier
            project_id: Project identifier
        
        Returns:
            Dictionary with aggregated metrics
        """
        try:
            db = await self._get_database()
            analytics_collection = db["analytics"]
            
            # Aggregate all analytics records for this project
            pipeline = [
                {
                    "$match": {
                        "project_id": project_id,
                        "tenant_id": tenant_id,
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total_uploads": {"$sum": "$uploads_count"},
                        "total_pages": {"$sum": "$total_pages_processed"},
                        "avg_cost_per_page": {"$avg": "$cost_per_page"},
                        "avg_parsing_time": {"$avg": "$average_parsing_time_seconds"},
                        "avg_success_rate": {"$avg": "$success_rate"},
                        "latest_timestamp": {"$max": "$timestamp"},
                    }
                }
            ]
            
            result = await analytics_collection.aggregate(pipeline).to_list(None)
            
            if result:
                data = result[0]
                return {
                    "project_id": project_id,
                    "tenant_id": tenant_id,
                    "total_uploads": data.get("total_uploads", 0),
                    "total_pages_processed": data.get("total_pages", 0),
                    "average_cost_per_page": round(data.get("avg_cost_per_page", 0.0), 4),
                    "average_parsing_time_seconds": round(data.get("avg_parsing_time", 0.0), 2),
                    "average_success_rate_percent": round(data.get("avg_success_rate", 100.0), 2),
                    "last_activity": data.get("latest_timestamp"),
                }
            else:
                return {
                    "project_id": project_id,
                    "tenant_id": tenant_id,
                    "total_uploads": 0,
                    "total_pages_processed": 0,
                    "average_cost_per_page": 0.0,
                    "average_parsing_time_seconds": 0.0,
                    "average_success_rate_percent": 100.0,
                    "last_activity": None,
                }
            
        except Exception as e:
            logger.error(f"❌ Failed to get project analytics: {str(e)}")
            return {}
    
    async def get_tenant_analytics_summary(
        self,
        tenant_id: str,
    ) -> dict:
        """
        Get aggregated analytics for entire tenant.
        
        Args:
            tenant_id: Tenant identifier
        
        Returns:
            Dictionary with aggregated metrics across all projects
        """
        try:
            db = await self._get_database()
            analytics_collection = db["analytics"]
            
            # Aggregate all analytics records for this tenant
            pipeline = [
                {
                    "$match": {
                        "tenant_id": tenant_id,
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total_uploads": {"$sum": "$uploads_count"},
                        "total_pages": {"$sum": "$total_pages_processed"},
                        "avg_cost_per_page": {"$avg": "$cost_per_page"},
                        "avg_parsing_time": {"$avg": "$average_parsing_time_seconds"},
                        "avg_success_rate": {"$avg": "$success_rate"},
                        "latest_timestamp": {"$max": "$timestamp"},
                    }
                }
            ]
            
            result = await analytics_collection.aggregate(pipeline).to_list(None)
            
            if result:
                data = result[0]
                return {
                    "tenant_id": tenant_id,
                    "total_uploads": data.get("total_uploads", 0),
                    "total_pages_processed": data.get("total_pages", 0),
                    "average_cost_per_page": round(data.get("avg_cost_per_page", 0.0), 4),
                    "average_parsing_time_seconds": round(data.get("avg_parsing_time", 0.0), 2),
                    "average_success_rate_percent": round(data.get("avg_success_rate", 100.0), 2),
                    "last_activity": data.get("latest_timestamp"),
                }
            else:
                return {
                    "tenant_id": tenant_id,
                    "total_uploads": 0,
                    "total_pages_processed": 0,
                    "average_cost_per_page": 0.0,
                    "average_parsing_time_seconds": 0.0,
                    "average_success_rate_percent": 100.0,
                    "last_activity": None,
                }
            
        except Exception as e:
            logger.error(f"❌ Failed to get tenant analytics: {str(e)}")
            return {}
    
    async def sync_project_analytics(
        self,
        tenant_id: str,
        project_id: str,
    ) -> bool:
        """
        Synchronize aggregated analytics to ProjectAnalytics collection.
        
        This reads from the analytics collection and creates/updates a document
        in the ProjectAnalytics collection with aggregated metrics.
        
        Args:
            tenant_id: Tenant identifier
            project_id: Project identifier
        
        Returns:
            True if successfully synced, False otherwise
        """
        try:
            db = await self._get_database()
            analytics_collection = db["analytics"]
            project_analytics_collection = db["ProjectAnalytics"]
            
            # Get aggregated data
            pipeline = [
                {
                    "$match": {
                        "project_id": project_id,
                        "tenant_id": tenant_id,
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total_uploads": {"$sum": "$uploads_count"},
                        "total_pages": {"$sum": "$total_pages_processed"},
                        "avg_cost_per_page": {"$avg": "$cost_per_page"},
                        "avg_parsing_time": {"$avg": "$average_parsing_time_seconds"},
                        "avg_success_rate": {"$avg": "$success_rate"},
                        "latest_timestamp": {"$max": "$timestamp"},
                    }
                }
            ]
            
            result = await analytics_collection.aggregate(pipeline).to_list(None)
            
            if result:
                data = result[0]
                project_analytics_doc = {
                    "project_id": project_id,
                    "tenant_id": tenant_id,
                    "timestamp": datetime.utcnow(),
                    "uploads_count": data.get("total_uploads", 0),
                    "total_pages_processed": data.get("total_pages", 0),
                    "average_cost_per_page": round(data.get("avg_cost_per_page", 0.0), 4),
                    "average_parsing_time_seconds": round(data.get("avg_parsing_time", 0.0), 2),
                    "success_rate": round(data.get("avg_success_rate", 100.0), 2),
                }
                
                # Upsert (create or update)
                await project_analytics_collection.update_one(
                    {"project_id": project_id, "tenant_id": tenant_id},
                    {"$set": project_analytics_doc},
                    upsert=True
                )
                
                logger.info(
                    f"✅ ProjectAnalytics synced for {project_id}: "
                    f"uploads={data.get('total_uploads', 0)}, "
                    f"pages={data.get('total_pages', 0)}"
                )
                
                return True
            else:
                logger.info(f"ℹ️ No analytics data found for project {project_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to sync ProjectAnalytics: {str(e)}")
            logger.exception("Full traceback:")
            return False
    
    async def sync_tenant_analytics(
        self,
        tenant_id: str,
    ) -> bool:
        """
        Synchronize aggregated analytics to TenantAnalytics collection.
        
        This reads from the analytics collection and creates/updates a document
        in the TenantAnalytics collection with aggregated metrics across all projects.
        
        Args:
            tenant_id: Tenant identifier
        
        Returns:
            True if successfully synced, False otherwise
        """
        try:
            db = await self._get_database()
            analytics_collection = db["analytics"]
            tenant_analytics_collection = db["TenantAnalytics"]
            projects_collection = db["projects"]
            
            # Get aggregated data for entire tenant
            pipeline = [
                {
                    "$match": {
                        "tenant_id": tenant_id,
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total_uploads": {"$sum": "$uploads_count"},
                        "total_pages": {"$sum": "$total_pages_processed"},
                        "avg_cost_per_page": {"$avg": "$cost_per_page"},
                        "avg_parsing_time": {"$avg": "$average_parsing_time_seconds"},
                        "avg_success_rate": {"$avg": "$success_rate"},
                        "latest_timestamp": {"$max": "$timestamp"},
                    }
                }
            ]
            
            result = await analytics_collection.aggregate(pipeline).to_list(None)
            
            # Count total projects
            total_projects = await projects_collection.count_documents({"tenant_id": tenant_id})
            
            if result:
                data = result[0]
                tenant_analytics_doc = {
                    "tenant_id": tenant_id,
                    "timestamp": datetime.utcnow(),
                    "total_projects": total_projects,
                    "total_uploads": data.get("total_uploads", 0),
                    "total_pages_processed": data.get("total_pages", 0),
                    "average_cost_per_page": round(data.get("avg_cost_per_page", 0.0), 4),
                    "average_parsing_time_seconds": round(data.get("avg_parsing_time", 0.0), 2),
                    "average_success_rate": round(data.get("avg_success_rate", 100.0), 2),
                }
                
                # Upsert (create or update)
                await tenant_analytics_collection.update_one(
                    {"tenant_id": tenant_id},
                    {"$set": tenant_analytics_doc},
                    upsert=True
                )
                
                logger.info(
                    f"✅ TenantAnalytics synced for {tenant_id}: "
                    f"uploads={data.get('total_uploads', 0)}, "
                    f"pages={data.get('total_pages', 0)}, "
                    f"projects={total_projects}"
                )
                
                return True
            else:
                logger.info(f"ℹ️ No analytics data found for tenant {tenant_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to sync TenantAnalytics: {str(e)}")
            logger.exception("Full traceback:")
            return False


# Global analytics tracker instance
analytics_tracker = AnalyticsTracker()
