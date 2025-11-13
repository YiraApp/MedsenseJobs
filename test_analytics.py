#!/usr/bin/env python3
"""
Quick test script to verify analytics tracking is working.
Run this to check if analytics documents are being written to MongoDB.
"""

import asyncio
from datetime import datetime
from server.integrations.mongodb import MongoDBClient
from server.utils.analytics_tracker import analytics_tracker

async def test_analytics():
    """Test analytics tracker functionality"""
    print("\n" + "="*60)
    print("ANALYTICS TRACKER TEST")
    print("="*60)
    
    # Get database
    db = await MongoDBClient.get_database()
    analytics_collection = db["analytics"]
    
    # Count existing documents
    existing_count = await analytics_collection.count_documents({})
    print(f"\n📊 Existing analytics documents: {existing_count}")
    
    # Show existing documents
    if existing_count > 0:
        print("\n📝 Recent analytics documents:")
        async for doc in analytics_collection.find({}).sort("_id", -1).limit(3):
            print(f"\n  Project: {doc.get('project_id')}")
            print(f"  Tenant: {doc.get('tenant_id')}")
            print(f"  Pages: {doc.get('total_pages_processed')}")
            print(f"  Time: {doc.get('average_parsing_time_seconds')}s")
            print(f"  Timestamp: {doc.get('timestamp')}")
    else:
        print("\n⚠️  No analytics documents found!")
    
    # Try to create a test analytics record
    print("\n" + "="*60)
    print("ATTEMPTING TO CREATE TEST ANALYTICS RECORD")
    print("="*60)
    
    success = await analytics_tracker.track_report_parse(
        tenant_id="test_tenant",
        project_id="test_project",
        pages_processed=10,
        parsing_time_seconds=2.5,
        cost_per_page=0.05,
        success=True,
    )
    
    if success:
        print("✅ Test analytics record created successfully!")
        
        # Check new count
        new_count = await analytics_collection.count_documents({})
        print(f"📊 Total analytics documents now: {new_count}")
        
        # Show the test record
        test_doc = await analytics_collection.find_one(
            {"tenant_id": "test_tenant", "project_id": "test_project"},
            sort=[("_id", -1)]
        )
        if test_doc:
            print("\n📄 Test document details:")
            print(f"  _id: {test_doc.get('_id')}")
            print(f"  Pages: {test_doc.get('total_pages_processed')}")
            print(f"  Time: {test_doc.get('average_parsing_time_seconds')}s")
            print(f"  Status: {test_doc.get('status')}")
    else:
        print("❌ Failed to create test analytics record!")
    
    print("\n" + "="*60 + "\n")

if __name__ == "__main__":
    asyncio.run(test_analytics())
