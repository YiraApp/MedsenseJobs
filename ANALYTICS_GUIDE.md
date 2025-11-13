# Complete Analytics Implementation Guide

## Executive Overview

The analytics system captures granular metrics from medical report parsing operations and stores them in MongoDB for aggregated analysis. It provides a complete audit trail of system activity while enabling flexible billing and reporting capabilities.

The system automatically synchronizes raw event data to aggregated ProjectAnalytics and TenantAnalytics collections, providing both detailed event logs and high-level summaries.

---

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Data Flow Process](#data-flow-process)
3. [Synchronization System](#synchronization-system)
4. [Implementation Details](#implementation-details)
5. [MongoDB Collections](#mongodb-collections)
6. [API Integration](#api-integration)
7. [Code Organization](#code-organization)
8. [Metrics Reference](#metrics-reference)
9. [Usage Examples](#usage-examples)

---

## System Architecture

### Overview Diagram

```
┌────────────────────────────────────────────────────────────────────────┐
│                        USER UPLOADS REPORT                              │
│                    POST /api/v1/tenants/{tenant_id}/                   │
│                    projects/{project_id}/reports                        │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
                     ├─ Validates API Key
                     ├─ Validates Tenant
                     ├─ Validates Project
                     └─ Retrieves AI Model Config
                     
                     │
                     ↓
┌────────────────────────────────────────────────────────────────────────┐
│              GEMINI PARSER PROCESSES PDF                                │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ GeminiParser.parse_pdf()                                         │  │
│  │ Returns structured JSON with:                                    │  │
│  │  - diagnosis, medications, procedures                            │  │
│  │  - total_pages: 15    ← CAPTURED HERE                            │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
                     ├─ Stop parsing timer
                     ├─ Extract: pages_processed (15)
                     ├─ Extract: parsing_time_seconds (2.34)
                     ├─ Get: cost_per_page from AI model (0.05)
                     └─ Set: success = True
                     
                     │
                     ↓
┌────────────────────────────────────────────────────────────────────────┐
│         STORE REPORT IN MONGODB (parsed_reports)                       │
│  Document contains:                                                    │
│  - tenant_id, project_id, report_id                                   │
│  - parsed_data (diagnosis, meds, etc.)                                │
│  - parsing_time_seconds: 2.34                                         │
│  - created_at: timestamp                                              │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
                     ↓
┌────────────────────────────────────────────────────────────────────────┐
│  ✅ ANALYTICS TRACKER CALLED                                            │
│  await analytics_tracker.track_report_parse(                          │
│    tenant_id="hosp_001",                                               │
│    project_id="proj_001",                                              │
│    pages_processed=15,                                                 │
│    parsing_time_seconds=2.34,                                          │
│    cost_per_page=0.05,                                                │
│    success=True                                                        │
│  )                                                                     │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
                     ↓
┌────────────────────────────────────────────────────────────────────────┐
│    INSERT ANALYTICS DOCUMENT INTO MONGODB (analytics collection)       │
│                                                                        │
│  {                                                                    │
│    "_id": ObjectId("..."),                                            │
│    "tenant_id": "hosp_001",                                           │
│    "project_id": "proj_001",                                          │
│    "timestamp": ISODate("2025-11-12T10:30:00Z"),                      │
│    "uploads_count": 1,                                                │
│    "total_pages_processed": 15,                                       │
│    "cost_per_page": 0.05,                                             │
│    "average_parsing_time_seconds": 2.34,                              │
│    "success_rate": 100.0,                                             │
│    "status": "success",                                               │
│    "error_message": null                                              │
│  }                                                                    │
│                                                                        │
│  ✅ DOCUMENT STORED IN DATABASE                                        │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
                     ├─────────────────────────┬──────────────────────────┐
                     ↓                         ↓                          ↓
┌───────────────────────────────┐ ┌──────────────────────┐ ┌──────────────┐
│ Aggregate from analytics      │ │ Count projects in    │ │ Return       │
│ for this project              │ │ projects collection  │ │ success to   │
│                               │ │                      │ │ user         │
│ Upsert into ProjectAnalytics  │ │ Aggregate all data   │ │              │
│ collection                    │ │ Upsert into          │ │ User gets    │
│                               │ │ TenantAnalytics      │ │ response     │
│ ✅ Project Summary Created    │ │ ✅ Tenant Summary    │ │ immediately  │
└───────────────────────────────┘ └──────────────────────┘ └──────────────┘
```

### Analytics Query Flow

```
═══════════════════════════════════════════════════════════════════════════
                    USER QUERIES ANALYTICS LATER
═══════════════════════════════════════════════════════════════════════════

                     │
                     ↓
┌────────────────────────────────────────────────────────────────────────┐
│              USER REQUESTS ANALYTICS                                    │
│  GET /api/v1/analytics/tenants/{tenant_id}/projects/{project_id}/      │
│      summary                                                            │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
                     ↓
┌────────────────────────────────────────────────────────────────────────┐
│    MONGODB AGGREGATION PIPELINE                                        │
│                                                                        │
│  get_project_analytics_summary(tenant_id, project_id)                 │
│                                                                        │
│  $match: { tenant_id, project_id }                                    │
│    ↓                                                                    │
│  $group:                                                               │
│    - total_uploads: { $sum: "$uploads_count" }                        │
│    - total_pages: { $sum: "$total_pages_processed" }                  │
│    - avg_cost_per_page: { $avg: "$cost_per_page" }                    │
│    - avg_parsing_time: { $avg: "$average_parsing_time_seconds" }      │
│    - avg_success_rate: { $avg: "$success_rate" }                      │
│    - latest_timestamp: { $max: "$timestamp" }                         │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
                     ↓
┌────────────────────────────────────────────────────────────────────────┐
│              RETURN AGGREGATED METRICS                                 │
│                                                                        │
│  {                                                                     │
│    "project_id": "proj_001",                                           │
│    "tenant_id": "hosp_001",                                            │
│    "total_uploads": 5,                                                 │
│    "total_pages_processed": 75,                                        │
│    "average_cost_per_page": 0.05,                                      │
│    "average_parsing_time_seconds": 2.18,                               │
│    "average_success_rate_percent": 100.0,                              │
│    "last_activity": "2025-11-12T10:35:00Z"                            │
│  }                                                                     │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow Process

### Step 1: Report Upload Triggers Parsing

The system receives a PDF file via the report upload endpoint:
```
POST /api/v1/tenants/{tenant_id}/projects/{project_id}/reports
```

**Handler Source:** `server/api/v1/handlers/medical_reports_multitenant.py`

The handler performs the following operations:
1. Validates the API key and tenant context
2. Retrieves project configuration
3. Fetches AI model settings (including cost_per_page)
4. Starts a parsing timer

### Step 2: Gemini Parses PDF and Extracts Metrics

The Gemini integration parses the PDF and returns structured data:

**Parser Source:** `server/integrations/gemini.py`

The parser extracts:
- Medical diagnosis and procedures
- Medication information
- **Total page count** (stored in response as `total_pages`)

**Code Reference:**
```python
# In medical_reports_multitenant.py
parsed_data = await GeminiParser.parse_pdf(...)
pages_processed = parsed_data.get("total_pages", 1)  # Default to 1 if missing
parsing_time_seconds = time.time() - start_time
```

### Step 3: Metrics Are Calculated and Gathered

After successful parsing, the handler extracts and prepares metrics:

**Metrics Captured:**
- `pages_processed` - Extracted from Gemini response's `total_pages` field
- `parsing_time_seconds` - Time elapsed from start to completion
- `cost_per_page` - Retrieved from AI model configuration
- `success` - Boolean flag (True if parsing succeeded)

**Code Reference:**
```python
# In medical_reports_multitenant.py (lines ~520-535)
pages_processed = parsed_data.get("total_pages", 1)
parsing_time_seconds = elapsed_time
cost_per_page = ai_model_config.get("cost_per_page")
success = True  # At this point, parsing succeeded
```

### Step 4: Report Stored in MongoDB

The parsed report document is stored in the `parsed_reports` collection with all parsed data and timing information:

**Collection:** `MEDPARSER.parsed_reports`

**Document Structure:**
```json
{
  "_id": ObjectId("..."),
  "tenant_id": "hosp_001",
  "project_id": "proj_001",
  "report_id": "507f1f77bcf86cd799439011",
  "parsed_data": { /* diagnosis, medications, etc. */ },
  "parsing_time_seconds": 2.34,
  "created_at": 1731405000
}
```

### Step 5: Analytics Tracker Records Event

The analytics tracker is called with the captured metrics:

**Source:** `server/utils/analytics_tracker.py` (AnalyticsTracker class)

**Method:** `async def track_report_parse(...)`

**Code Reference:**
```python
# In medical_reports_multitenant.py (lines ~535-542)
await analytics_tracker.track_report_parse(
    tenant_id=tenant_id,
    project_id=project_id,
    pages_processed=pages_processed,
    parsing_time_seconds=parsing_time_seconds,
    cost_per_page=cost_per_page,
    success=True,
)
```

### Step 6: Analytics Document Created and Stored

The analytics tracker creates a new analytics document with all captured metrics:

**Collection:** `MEDPARSER.analytics`

**Document Structure:**
```json
{
  "_id": ObjectId("..."),
  "tenant_id": "hosp_001",
  "project_id": "proj_001",
  "timestamp": ISODate("2025-11-12T10:30:00Z"),
  "uploads_count": 1,
  "total_pages_processed": 15,
  "cost_per_page": 0.05,
  "average_parsing_time_seconds": 2.34,
  "success_rate": 100.0,
  "status": "success",
  "error_message": null
}
```

**Code Reference (analytics_tracker.py lines ~60-75):**
```python
analytics_doc = {
    "tenant_id": tenant_id,
    "project_id": project_id,
    "timestamp": datetime.utcnow(),
    "uploads_count": 1,
    "total_pages_processed": pages_processed,
    "cost_per_page": cost_per_page,
    "average_parsing_time_seconds": parsing_time_seconds,
    "success_rate": success_rate,
    "status": "success" if success else "failed",
    "error_message": error_message,
}

result = await analytics_collection.insert_one(analytics_doc)
```

### Step 7: Synchronization to Aggregated Collections

After storing in the `analytics` collection, the system automatically syncs aggregated data to two additional collections:

#### 7a: ProjectAnalytics Synchronization

```python
await analytics_tracker.sync_project_analytics(tenant_id, project_id)
```

**What it does:**
1. Reads ALL analytics records for this project from `analytics` collection
2. Aggregates pages, costs, times, and success rates
3. Creates or updates a single document in `ProjectAnalytics` collection

**Result Document:**
```json
{
  "project_id": "proj_001",
  "tenant_id": "hosp_001",
  "timestamp": ISODate("2025-11-12T15:30:00Z"),
  "uploads_count": 5,
  "total_pages_processed": 75,
  "average_cost_per_page": 0.05,
  "average_parsing_time_seconds": 2.18,
  "success_rate": 100.0
}
```

#### 7b: TenantAnalytics Synchronization

```python
await analytics_tracker.sync_tenant_analytics(tenant_id)
```

**What it does:**
1. Reads ALL analytics records for this tenant (all projects) from `analytics` collection
2. Counts total projects in `projects` collection
3. Aggregates data across all projects
4. Creates or updates a single document in `TenantAnalytics` collection

**Result Document:**
```json
{
  "tenant_id": "hosp_001",
  "timestamp": ISODate("2025-11-12T15:30:00Z"),
  "total_projects": 3,
  "total_uploads": 12,
  "total_pages_processed": 187,
  "average_cost_per_page": 0.05,
  "average_parsing_time_seconds": 2.25,
  "average_success_rate": 99.8
}
```

### Step 8: Response Returned to User

The handler returns a success response to the user immediately:

```json
{
  "success": true,
  "report_id": "507f1f77bcf86cd799439011",
  "parsing_time_seconds": 2.34,
  "confidence_score": 95,
  "parsed_data": { /* diagnosis, medications, etc. */ }
}
```

**Note:** At this point, analytics documents are already persisted in all three collections.

### Step 9: User Queries Analytics Endpoints

Later, the user can request aggregated analytics:

**Endpoint:** `GET /api/v1/analytics/tenants/{tenant_id}/projects/{project_id}/summary`

**Handler Calls:** `analytics_tracker.get_project_analytics_summary(tenant_id, project_id)`

Or query the ProjectAnalytics/TenantAnalytics collections directly via MongoDB.

---

## Synchronization System

### Overview

The synchronization system automatically keeps ProjectAnalytics and TenantAnalytics collections in sync with the raw analytics data. This provides three levels of analytics access:

1. **Raw Events** (`analytics` collection) - One document per report
2. **Project Summary** (`ProjectAnalytics` collection) - Aggregated per project
3. **Tenant Summary** (`TenantAnalytics` collection) - Aggregated per tenant

### How Synchronization Works

#### Upsert Pattern (Create or Update)

Both sync methods use MongoDB's `upsert` operation:

```python
await collection.update_one(
    {"project_id": project_id, "tenant_id": tenant_id},  # Filter
    {"$set": document},                                    # Update
    upsert=True                                            # Create if not exists
)
```

**Benefits:**
- ✅ First upload creates the document
- ✅ Subsequent uploads update it with latest aggregates
- ✅ Always maintains current state
- ✅ No duplicates

#### Aggregation Pipeline in sync_project_analytics()

```python
pipeline = [
    # Step 1: Match records for this project
    {
        "$match": {
            "project_id": project_id,
            "tenant_id": tenant_id,
        }
    },
    # Step 2: Group and aggregate
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
```

#### Aggregation Pipeline in sync_tenant_analytics()

```python
pipeline = [
    # Step 1: Match records for this tenant (all projects)
    {
        "$match": {
            "tenant_id": tenant_id,
        }
    },
    # Step 2: Group and aggregate
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
```

### Error Handling in Synchronization

Both sync methods include error handling to prevent upload failures:

```python
try:
    await analytics_tracker.sync_project_analytics(tenant_id, project_id)
    await analytics_tracker.sync_tenant_analytics(tenant_id)
except Exception as sync_exc:
    logger.warning("Failed to sync analytics collections: %s", sync_exc)
    # Don't fail the request if sync fails
```

**Behavior:**
- ✅ Logs warning if sync fails
- ✅ Does NOT fail the upload request
- ✅ User still gets successful response
- ✅ Analytics continue to be tracked in `analytics` collection

### Complete Data Flow with Synchronization

```
User Uploads PDF
    ↓
Parse with Gemini (extract pages, time)
    ↓
Store in MEDPARSER.parsed_reports
    ↓
Call analytics_tracker.track_report_parse()
    ↓
Insert into MEDPARSER.analytics ✅
    ↓
Call analytics_tracker.sync_project_analytics() ✅
    ├─ Read from: MEDPARSER.analytics
    ├─ Aggregate: pages, cost_per_page, time, success_rate
    └─ Write to: ProjectAnalytics collection
    ↓
Call analytics_tracker.sync_tenant_analytics() ✅
    ├─ Read from: MEDPARSER.analytics
    ├─ Read from: MEDPARSER.projects
    ├─ Aggregate: pages, cost_per_page, time, success_rate, project_count
    └─ Write to: TenantAnalytics collection
    ↓
Return success response to user
```

---

## Implementation Details

### Architecture Components

#### 1. Analytics Tracker Service

**File:** `server/utils/analytics_tracker.py`

**Purpose:** Central service for capturing and querying analytics data

**Class:** `AnalyticsTracker`

**Methods:**

| Method | Purpose | Input | Output |
|--------|---------|-------|--------|
| `track_report_parse()` | Records a single parse event | tenant_id, project_id, pages, time, cost_per_page, success | Boolean (success/failure) |
| `get_project_analytics_summary()` | Aggregates project metrics | tenant_id, project_id | Dictionary with aggregated metrics |
| `get_tenant_analytics_summary()` | Aggregates tenant metrics | tenant_id | Dictionary with aggregated metrics |
| `sync_project_analytics()` | Syncs project aggregates to ProjectAnalytics | tenant_id, project_id | Boolean (success/failure) |
| `sync_tenant_analytics()` | Syncs tenant aggregates to TenantAnalytics | tenant_id | Boolean (success/failure) |

**Key Features:**
- Async/await for non-blocking I/O
- Graceful error handling (won't crash if tracking fails)
- Automatic MongoDB connection management
- Detailed logging for troubleshooting

#### 2. Report Upload Handler Integration

**File:** `server/api/v1/handlers/medical_reports_multitenant.py`

**Integration Points:**

1. **Import Statement (Line 30):**
```python
from server.utils.analytics_tracker import analytics_tracker
```

2. **Tracking and Sync Calls (Lines 535-549):**
```python
# After successful report parsing and storage
await analytics_tracker.track_report_parse(
    tenant_id=tenant_id,
    project_id=project_id,
    pages_processed=pages_processed,
    parsing_time_seconds=parsing_time_seconds,
    cost_per_page=cost_per_page,
    success=True,
)

# Sync aggregated analytics to ProjectAnalytics and TenantAnalytics collections
try:
    await analytics_tracker.sync_project_analytics(tenant_id, project_id)
    await analytics_tracker.sync_tenant_analytics(tenant_id)
except Exception as sync_exc:
    logger.warning("Failed to sync analytics collections: %s", sync_exc)
```

3. **Error Handling:**
```python
try:
    await analytics_tracker.track_report_parse(...)
except Exception as e:
    logger.warning(f"Analytics tracking failed: {str(e)}")
    # Continue - don't fail the upload
```

#### 3. AI Model Configuration

**File:** `server/config/settings.py` or database AI model collection

**Cost Per Page Source:**
```python
# AI model configuration
ai_model_config = {
    "name": "gemini-2.0-flash",
    "cost_per_page": 0.05,  # Used for billing/analytics
    "max_concurrent_requests": 10
}
```

---

## MongoDB Collections

### Analytics Collection (Raw Events)

**Collection Name:** `analytics`

**Database:** `MEDPARSER`

**Full Path:** `MEDPARSER.analytics`

**Purpose:** One document per report parse - complete event log

**Document Schema:**

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `_id` | ObjectId | MongoDB auto-generated ID | ObjectId("...") |
| `tenant_id` | String | Tenant identifier | "hosp_001" |
| `project_id` | String | Project identifier | "proj_001" |
| `timestamp` | ISODate | When the report was parsed | ISODate("2025-11-12T10:30:00Z") |
| `uploads_count` | Number | Reports in this batch (always 1) | 1 |
| `total_pages_processed` | Number | Pages in the report | 15 |
| `cost_per_page` | Number | Cost per page from AI model | 0.05 |
| `average_parsing_time_seconds` | Number | Time to parse in seconds | 2.34 |
| `success_rate` | Number | Success percentage (0 or 100) | 100.0 |
| `status` | String | "success" or "failed" | "success" |
| `error_message` | String/Null | Error details if failed | null |

**Sample Document:**
```json
{
  "_id": ObjectId("507f1f77bcf86cd799439011"),
  "tenant_id": "hosp_001",
  "project_id": "proj_2024_001",
  "timestamp": ISODate("2025-11-12T10:30:00Z"),
  "uploads_count": 1,
  "total_pages_processed": 15,
  "cost_per_page": 0.05,
  "average_parsing_time_seconds": 2.34,
  "success_rate": 100.0,
  "status": "success",
  "error_message": null
}
```

### ProjectAnalytics Collection (Project Aggregates)

**Collection Name:** `ProjectAnalytics`

**Database:** `MEDPARSER`

**Purpose:** One document per project - aggregated project-level summary

**Document Schema:**

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `_id` | ObjectId | MongoDB auto-generated ID | ObjectId("...") |
| `project_id` | String | Project identifier | "proj_001" |
| `tenant_id` | String | Tenant identifier | "hosp_001" |
| `timestamp` | ISODate | When last updated | ISODate("2025-11-12T15:30:00Z") |
| `uploads_count` | Number | Total uploads for project | 5 |
| `total_pages_processed` | Number | Total pages for project | 75 |
| `average_cost_per_page` | Number | Average cost across all uploads | 0.05 |
| `average_parsing_time_seconds` | Number | Average parsing time | 2.18 |
| `success_rate` | Number | Success rate percentage | 100.0 |

**Sample Document:**
```json
{
  "_id": ObjectId("507f1f77bcf86cd799439012"),
  "project_id": "proj_2024_001",
  "tenant_id": "hosp_001",
  "timestamp": ISODate("2025-11-12T15:30:00Z"),
  "uploads_count": 5,
  "total_pages_processed": 75,
  "average_cost_per_page": 0.05,
  "average_parsing_time_seconds": 2.18,
  "success_rate": 100.0
}
```

### TenantAnalytics Collection (Tenant Aggregates)

**Collection Name:** `TenantAnalytics`

**Database:** `MEDPARSER`

**Purpose:** One document per tenant - aggregated tenant-level summary across all projects

**Document Schema:**

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `_id` | ObjectId | MongoDB auto-generated ID | ObjectId("...") |
| `tenant_id` | String | Tenant identifier | "hosp_001" |
| `timestamp` | ISODate | When last updated | ISODate("2025-11-12T15:30:00Z") |
| `total_projects` | Number | Total active projects | 3 |
| `total_uploads` | Number | Total uploads across all projects | 12 |
| `total_pages_processed` | Number | Total pages across all projects | 187 |
| `average_cost_per_page` | Number | Average cost across all projects | 0.05 |
| `average_parsing_time_seconds` | Number | Average parsing time across all projects | 2.25 |
| `average_success_rate` | Number | Average success rate across all projects | 99.8 |

**Sample Document:**
```json
{
  "_id": ObjectId("507f1f77bcf86cd799439013"),
  "tenant_id": "hosp_001",
  "timestamp": ISODate("2025-11-12T15:30:00Z"),
  "total_projects": 3,
  "total_uploads": 12,
  "total_pages_processed": 187,
  "average_cost_per_page": 0.05,
  "average_parsing_time_seconds": 2.25,
  "average_success_rate": 99.8
}
```

---

## API Integration

### Analytics Query Endpoints

#### Endpoint 1: Project-Level Analytics

**Request:**
```http
GET /api/v1/analytics/tenants/{tenant_id}/projects/{project_id}/summary
Headers:
  X-API-Key: sk_<your-api-key>
```

**Handler:** Calls `analytics_tracker.get_project_analytics_summary(tenant_id, project_id)`

**Response:**
```json
{
  "success": true,
  "analytics": {
    "project_id": "proj_001",
    "tenant_id": "hosp_001",
    "total_uploads": 5,
    "total_pages_processed": 75,
    "average_cost_per_page": 0.05,
    "average_parsing_time_seconds": 2.18,
    "average_success_rate_percent": 100.0,
    "last_activity": "2025-11-12T10:35:00Z"
  }
}
```

#### Endpoint 2: Tenant-Level Analytics

**Request:**
```http
GET /api/v1/analytics/tenants/{tenant_id}/summary
Headers:
  X-API-Key: sk_<your-api-key>
```

**Handler:** Calls `analytics_tracker.get_tenant_analytics_summary(tenant_id)`

**Response:**
```json
{
  "success": true,
  "analytics": {
    "tenant_id": "hosp_001",
    "total_uploads": 12,
    "total_pages_processed": 187,
    "average_cost_per_page": 0.05,
    "average_parsing_time_seconds": 2.25,
    "average_success_rate_percent": 100.0,
    "last_activity": "2025-11-12T10:35:00Z"
  }
}
```

---

## Code Organization

### File Structure

```
server/
├── api/
│   └── v1/
│       └── handlers/
│           └── medical_reports_multitenant.py    [MODIFIED]
│               ├─ Imports: analytics_tracker
│               ├─ Calls: track_report_parse() after parsing
│               ├─ Calls: sync_project_analytics() and sync_tenant_analytics()
│               └─ Integration: Lines 535-549
│
├── utils/
│   └── analytics_tracker.py    [NEW FILE - 380+ lines]
│       ├─ Class: AnalyticsTracker
│       ├─ Method: track_report_parse()
│       ├─ Method: get_project_analytics_summary()
│       ├─ Method: get_tenant_analytics_summary()
│       ├─ Method: sync_project_analytics()
│       ├─ Method: sync_tenant_analytics()
│       └─ Global: analytics_tracker instance
│
├── integrations/
│   ├── gemini.py    [Referenced for parsing]
│   └── mongodb.py    [Referenced for database access]
│
└── models/
    └── project.py    [Contains ProjectAnalytics schema]
```

### Code Links and References

**Analytics Tracker Class Definition:**
- File: `server/utils/analytics_tracker.py`
- Lines: 18-380+
- Source: Global instance at end of file

**Integration in Handler:**
- File: `server/api/v1/handlers/medical_reports_multitenant.py`
- Import: Line 30
- Call Location: Lines 535-549
- Error Handling: Lines 543-549

**Database Connection:**
- Uses: `server/integrations/mongodb.py` (MongoDBClient)
- Method: `MongoDBClient.get_database()`

**Gemini Parser Integration:**
- Uses: `server/integrations/gemini.py` (GeminiParser)
- Method: `GeminiParser.parse_pdf()`
- Returns: JSON with `total_pages` field

---

## Metrics Reference

### Captured Metrics Explained

#### 1. Total Pages Processed

**Source:** Gemini parser response

**Field Name:** `total_pages` (in parsed response) → `total_pages_processed` (in analytics)

**Purpose:** Count of pages in the PDF for billing and capacity planning

**Example:** 15 pages

**Aggregation:** SUM across all reports

#### 2. Parsing Time

**Source:** Timer measurement (start before parsing, stop after)

**Field Name:** `average_parsing_time_seconds`

**Purpose:** Performance tracking and SLA monitoring

**Example:** 2.34 seconds

**Aggregation:** AVERAGE across all reports

#### 3. Cost Per Page

**Source:** AI model configuration

**Field Name:** `cost_per_page`

**Purpose:** Billing rate for each page parsed

**Example:** 0.05 (USD per page)

**Aggregation:** AVERAGE cost per page across all reports

**Note:** This is a stored rate, not a calculated total cost

#### 4. Success Rate

**Source:** Parse completion status

**Field Name:** `success_rate`

**Values:** 100.0 (success) or 0.0 (failure)

**Purpose:** Track system reliability and error rates

**Example:** 100.0 percent

**Aggregation:** AVERAGE success rate across all reports

#### 5. Upload Count

**Field Name:** `uploads_count`

**Value:** Always 1 (one analytics document per report)

**Purpose:** Count total number of reports processed

**Example:** 1 per document

**Aggregation:** SUM to get total uploads

#### 6. Timestamp

**Field Name:** `timestamp`

**Format:** ISO 8601 (ISODate in MongoDB)

**Purpose:** Audit trail and temporal analysis

**Example:** ISODate("2025-11-12T10:30:00Z")

**Aggregation:** MAX (latest activity)

### Calculating Costs

**To calculate total cost from analytics:**

```python
total_cost = total_pages_processed × average_cost_per_page

# Example:
# total_pages_processed = 75
# average_cost_per_page = 0.05
# total_cost = 75 × 0.05 = $3.75
```

**Flexible Billing Implementation:**

The system stores raw metrics, allowing a separate billing service to:
- Apply custom pricing logic
- Handle volume discounts
- Implement tiered pricing
- Calculate taxes and fees
- Generate invoices

---

## Usage Examples

### Example 1: Single Report Upload Flow

**Step 1: User Uploads PDF**
```
File: medical_record_2025.pdf (15 pages)
Endpoint: POST /api/v1/tenants/hosp_001/projects/proj_001/reports
```

**Step 2: Handler Processes Report**
- Timer starts
- Gemini parses: 2.34 seconds
- Response includes: `"total_pages": 15`

**Step 3: Analytics Captured**
```python
pages_processed = 15
parsing_time_seconds = 2.34
cost_per_page = 0.05
```

**Step 4: Analytics Document Created**
```json
{
  "tenant_id": "hosp_001",
  "project_id": "proj_001",
  "timestamp": ISODate("2025-11-12T10:30:00Z"),
  "uploads_count": 1,
  "total_pages_processed": 15,
  "cost_per_page": 0.05,
  "average_parsing_time_seconds": 2.34,
  "success_rate": 100.0,
  "status": "success"
}
```

**Step 5: ProjectAnalytics and TenantAnalytics Updated**
- ProjectAnalytics: Now shows 1 upload, 15 pages
- TenantAnalytics: Now shows 1 upload, 15 pages across tenant

**Step 6: User Gets Response**
```json
{
  "success": true,
  "report_id": "507f1f77bcf86cd799439011",
  "parsing_time_seconds": 2.34,
  "parsed_data": { /* ... */ }
}
```

---

### Example 2: Multiple Reports and Aggregation

**Scenario:** Hospital uploads 3 reports

**Report 1:**
- Pages: 10
- Time: 2.5 seconds
- Status: Success

**Report 2:**
- Pages: 5
- Time: 1.8 seconds
- Status: Success

**Report 3:**
- Pages: 8
- Time: 2.2 seconds
- Status: Success

**Analytics Collection (3 documents):**
```json
// Document 1
{ "uploads_count": 1, "total_pages_processed": 10, "average_parsing_time_seconds": 2.5, "success_rate": 100.0 }

// Document 2
{ "uploads_count": 1, "total_pages_processed": 5, "average_parsing_time_seconds": 1.8, "success_rate": 100.0 }

// Document 3
{ "uploads_count": 1, "total_pages_processed": 8, "average_parsing_time_seconds": 2.2, "success_rate": 100.0 }
```

**ProjectAnalytics Collection (1 updated document):**
```json
{
  "project_id": "proj_001",
  "tenant_id": "hosp_001",
  "uploads_count": 3,                          // Sum: 1+1+1
  "total_pages_processed": 23,                 // Sum: 10+5+8
  "average_cost_per_page": 0.05,               // Avg
  "average_parsing_time_seconds": 2.17,        // Avg: (2.5+1.8+2.2)/3
  "success_rate": 100.0,                       // Avg
  "timestamp": ISODate(...)                    // Latest
}
```

**TenantAnalytics Collection (1 updated document):**
```json
{
  "tenant_id": "hosp_001",
  "total_projects": 1,
  "total_uploads": 3,
  "total_pages_processed": 23,
  "average_cost_per_page": 0.05,
  "average_parsing_time_seconds": 2.17,
  "average_success_rate": 100.0,
  "timestamp": ISODate(...)
}
```

**Query:** `GET /api/v1/analytics/tenants/hosp_001/projects/proj_001/summary`

**Aggregation Result:**
```json
{
  "total_uploads": 3,                          // Sum
  "total_pages_processed": 23,                 // Sum
  "average_cost_per_page": 0.05,               // Average
  "average_parsing_time_seconds": 2.17,        // Average
  "average_success_rate_percent": 100.0,       // Average
  "last_activity": "2025-11-12T10:35:00Z"
}
```

**Cost Calculation:**
```
Total Cost = 23 pages × 0.05 = $1.15
```

---

### Example 3: Querying Tenant-Level Analytics

**Request:**
```http
GET /api/v1/analytics/tenants/hosp_001/summary
```

**Aggregation:** Across ALL projects for the tenant

**Sample Response:**
```json
{
  "tenant_id": "hosp_001",
  "total_uploads": 47,
  "total_pages_processed": 312,
  "average_cost_per_page": 0.05,
  "average_parsing_time_seconds": 2.31,
  "average_success_rate_percent": 99.8,
  "last_activity": "2025-11-12T15:45:00Z"
}
```

---

### Example 4: MongoDB Collection Queries

**Check raw analytics:**
```javascript
db.analytics.find({"tenant_id": "hosp_001", "project_id": "proj_001"}).pretty()
```

**Check project aggregates:**
```javascript
db.ProjectAnalytics.find({"project_id": "proj_001", "tenant_id": "hosp_001"}).pretty()
```

**Check tenant aggregates:**
```javascript
db.TenantAnalytics.find({"tenant_id": "hosp_001"}).pretty()
```

---

## Summary

The analytics system provides a complete, scalable solution for:
- ✅ **Capturing** granular metrics from every report parse
- ✅ **Storing** facts in MongoDB for historical analysis
- ✅ **Aggregating** data across projects and tenants automatically
- ✅ **Synchronizing** to summary collections for fast queries
- ✅ **Querying** via REST API endpoints or MongoDB
- ✅ **Flexible Billing** by separating metrics from calculations

The implementation maintains separation of concerns, allowing billing logic to be implemented independently from analytics tracking.

**Three-Level Architecture:**
1. **Event Level** (`analytics`) - Complete audit trail
2. **Project Level** (`ProjectAnalytics`) - Project summaries
3. **Tenant Level** (`TenantAnalytics`) - Organization-wide summaries

---

**Document Version:** 2.0  
**Last Updated:** November 13, 2025  
**Status:** Complete, Production Ready, and Fully Synchronized
