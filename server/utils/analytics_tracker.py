"""
Analytics Tracking Service
Captures and stores metrics for project and tenant-level analytics.
Tracks parsing performance and success rates (optimized for minimal DB writes).
"""

import logging
from datetime import datetime
from typing import Optional, List
from motor.motor_asyncio import AsyncIOMotorDatabase

from server.models.project import ProjectAnalytics, TenantAnalytics
from server.integrations.mongodb import MongoDBClient

logger = logging.getLogger(__name__)


class AnalyticsTracker:
    """Tracks and persists analytics for projects and tenants with optimized writes"""
    
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
        success: bool = True,
        error_message: Optional[str] = None,
    ) -> bool:
        """
        Track a single report parsing event in audit trail.
        Stores event in analytics collection for historical tracking.
        
        Args:
            tenant_id: Tenant identifier
            project_id: Project identifier
            pages_processed: Number of pages in the report
            parsing_time_seconds: How long parsing took (seconds)
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
            
            # Create analytics document for audit trail
            analytics_doc = {
                "tenant_id": tenant_id,
                "project_id": project_id,
                "timestamp": datetime.utcnow(),
                "total_pages_processed": pages_processed,
                "average_parsing_time_seconds": parsing_time_seconds,
                "success_rate": success_rate,
                "status": "success" if success else "failed",
                "error_message": error_message,
            }
            
            # Insert the analytics record (audit trail)
            await analytics_collection.insert_one(analytics_doc)
            
            logger.info(
                f"✅ Analytics event tracked for project {project_id}: "
                f"{pages_processed} pages, {parsing_time_seconds:.2f}s, success={success}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to track analytics: {str(e)}")
            logger.exception("Full traceback:")
            return False
    
    async def track_batch_and_update_aggregates(
        self,
        tenant_id: str,
        project_id: str,
        project_name: str,
        parsed_results: List[dict],
    ) -> bool:
        """
        Track multiple PDFs and update ProjectAnalytics/TenantAnalytics in optimized manner.
        
        OPTIMIZED: 
        - Inserts individual analytics events (audit trail)
        - Directly updates aggregates WITHOUT reading back
        - Appends per-document metrics to arrays
        
        Args:
            tenant_id: Tenant identifier
            project_id: Project identifier
            project_name: Project display name
            parsed_results: List of dicts with 'pages', 'parse_time', and 'success_rate' keys
        
        Returns:
            True if successfully tracked, False otherwise
        """
        try:
            db = await self._get_database()
            analytics_collection = db["analytics"]
            project_analytics_collection = db["ProjectAnalytics"]
            tenant_analytics_collection = db["TenantAnalytics"]
            
            # Extract per-document metrics for this batch
            pages_list = [r.get('pages', 1) for r in parsed_results]
            parse_times_list = [r.get('parse_time', 0) for r in parsed_results]
            success_rates_list = [r.get('success_rate', 100.0) for r in parsed_results]
            
            # Calculate batch metrics
            total_pages = sum(pages_list)
            avg_parse_time_current_batch = sum(parse_times_list) / len(parse_times_list) if parse_times_list else 0
            avg_success_rate_current_batch = sum(success_rates_list) / len(success_rates_list) if success_rates_list else 100.0
            
            # ✅ WRITE 1: Insert analytics events (audit trail - one per PDF)
            analytics_docs = [
                {
                    "tenant_id": tenant_id,
                    "project_id": project_id,
                    "timestamp": datetime.utcnow(),
                    "total_pages_processed": result.get('pages', 1),
                    "average_parsing_time_seconds": result.get('parse_time', 0),
                    "success_rate": result.get('success_rate', 100.0),
                    "status": "success" if result.get('success_rate', 100.0) == 100.0 else "partial",
                    "error_message": None,
                }
                for result in parsed_results
            ]
            if analytics_docs:
                await analytics_collection.insert_many(analytics_docs)
                logger.info(f"✅ Inserted {len(analytics_docs)} audit trail documents")
            
            # ✅ WRITE 2: Update ProjectAnalytics (APPEND arrays, then recalculate averages)
            project_update = {
                "$inc": {
                    "total_uploads": 1,  # One upload session
                    "total_pages": total_pages,
                },
                "$push": {
                    "pages_per_doc": {"$each": pages_list},
                    "parse_times": {"$each": parse_times_list},
                    "success_rates": {"$each": success_rates_list},
                },
            }
            
            # After updating arrays, recalculate overall averages from ALL historical data
            # Get current document to calculate new averages including this batch
            current_project = await project_analytics_collection.find_one({
                "project_id": project_id,
                "tenant_id": tenant_id,
            })
            
            if current_project:
                # Calculate averages from existing arrays + new values
                all_parse_times = current_project.get('parse_times', []) + parse_times_list
                all_success_rates = current_project.get('success_rates', []) + success_rates_list
            else:
                # First update, just use current batch
                all_parse_times = parse_times_list
                all_success_rates = success_rates_list
            
            # Calculate overall averages
            overall_avg_parse_time = sum(all_parse_times) / len(all_parse_times) if all_parse_times else 0
            overall_avg_success_rate = sum(all_success_rates) / len(all_success_rates) if all_success_rates else 100.0
            
            # Add the calculated averages to the update
            project_update["$set"] = {
                "project_name": project_name,
                "avg_parse_time_seconds": round(overall_avg_parse_time, 2),
                "avg_parse_time_per_doc_seconds": round(overall_avg_parse_time, 2),
                "average_success_rate": round(overall_avg_success_rate, 2),
                "last_updated": datetime.utcnow(),
            }
            
            await project_analytics_collection.update_one(
                {"project_id": project_id, "tenant_id": tenant_id},
                project_update,
                upsert=True
            )
            logger.info(f"✅ Updated ProjectAnalytics for {project_id}")
            
            # ✅ WRITE 3: Update TenantAnalytics (recalculate average_success_rate from all projects)
            # Get all projects for this tenant to calculate tenant-level average success rate
            all_projects = await project_analytics_collection.find({
                "tenant_id": tenant_id,
            }).to_list(None)
            
            # Calculate tenant-level average success rate and total pages from all projects
            all_project_success_rates = []
            total_tenant_pages = 0
            for proj in all_projects:
                proj_rates = proj.get('success_rates', [])
                all_project_success_rates.extend(proj_rates)
                total_tenant_pages += proj.get('total_pages', 0)
            
            tenant_avg_success_rate = sum(all_project_success_rates) / len(all_project_success_rates) if all_project_success_rates else 100.0
            total_active_projects = len(all_projects)
            
            tenant_update = {
                "$inc": {
                    "total_uploads": 1,  # One upload session
                },
                "$set": {
                    "total_projects": total_active_projects,
                    "total_pages": total_tenant_pages,
                    "average_success_rate": round(tenant_avg_success_rate, 2),
                    "last_updated": datetime.utcnow(),
                }
            }
            
            await tenant_analytics_collection.update_one(
                {"tenant_id": tenant_id},
                tenant_update,
                upsert=True
            )
            logger.info(f"✅ Updated TenantAnalytics for {tenant_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to track batch analytics: {str(e)}")
            logger.exception("Full traceback:")
            return False
    
    async def get_project_analytics_summary(
        self,
        tenant_id: str,
        project_id: str,
    ) -> dict:
        """
        Get project analytics directly from ProjectAnalytics collection.
        
        Args:
            tenant_id: Tenant identifier
            project_id: Project identifier
        
        Returns:
            Dictionary with project metrics
        """
        try:
            db = await self._get_database()
            project_analytics_collection = db["ProjectAnalytics"]
            
            project_doc = await project_analytics_collection.find_one({
                "project_id": project_id,
                "tenant_id": tenant_id,
            })
            
            if project_doc:
                return {
                    "project_id": project_id,
                    "tenant_id": tenant_id,
                    "project_name": project_doc.get("project_name", ""),
                    "total_uploads": project_doc.get("total_uploads", 0),
                    "total_pages": project_doc.get("total_pages", 0),
                    "pages_per_doc": project_doc.get("pages_per_doc", []),
                    "avg_parse_time_seconds": round(project_doc.get("avg_parse_time_seconds", 0.0), 2),
                    "avg_parse_time_per_doc_seconds": round(project_doc.get("avg_parse_time_per_doc_seconds", 0.0), 2),
                    "parse_times": [round(t, 2) for t in project_doc.get("parse_times", [])],
                    "average_success_rate": round(project_doc.get("average_success_rate", 100.0), 2),
                    "success_rates": [round(s, 2) for s in project_doc.get("success_rates", [])],
                }
            else:
                return {
                    "project_id": project_id,
                    "tenant_id": tenant_id,
                    "project_name": "",
                    "total_uploads": 0,
                    "total_pages": 0,
                    "pages_per_doc": [],
                    "avg_parse_time_seconds": 0.0,
                    "avg_parse_time_per_doc_seconds": 0.0,
                    "parse_times": [],
                    "average_success_rate": 100.0,
                    "success_rates": [],
                }
            
        except Exception as e:
            logger.error(f"❌ Failed to get project analytics: {str(e)}")
            return {}
    
    async def get_tenant_analytics_summary(
        self,
        tenant_id: str,
    ) -> dict:
        """
        Get tenant analytics directly from TenantAnalytics collection.
        
        Args:
            tenant_id: Tenant identifier
        
        Returns:
            Dictionary with tenant metrics
        """
        try:
            db = await self._get_database()
            tenant_analytics_collection = db["TenantAnalytics"]
            projects_collection = db["projects"]
            
            tenant_doc = await tenant_analytics_collection.find_one({
                "tenant_id": tenant_id,
            })
            
            # Count active projects
            total_projects = await projects_collection.count_documents({
                "tenant_id": tenant_id,
                "is_active": True
            })
            
            if tenant_doc:
                return {
                    "tenant_id": tenant_id,
                    "total_projects": total_projects,
                    "total_uploads": tenant_doc.get("total_uploads", 0),
                    "total_pages": tenant_doc.get("total_pages", 0),
                    "average_success_rate": round(tenant_doc.get("average_success_rate", 100.0), 2),
                }
            else:
                return {
                    "tenant_id": tenant_id,
                    "total_projects": total_projects,
                    "total_uploads": 0,
                    "total_pages": 0,
                    "average_success_rate": 100.0,
                }
            
        except Exception as e:
            logger.error(f"❌ Failed to get tenant analytics: {str(e)}")
            return {}


# Global analytics tracker instance
analytics_tracker = AnalyticsTracker()
