# Zenvoyer Dashboard

Web-based monitoring dashboard untuk Email Enhancer scraping system.

## Features

### 1. Real-time Monitoring
- Progress per country (total, pending, completed)
- Server status (online, offline, health)
- Processing rate (URLs/minute)
- Success/failure rate

### 2. CSV Export
- Export selected columns only
- Filter by country, status, date range
- Download langsung dari browser

### 3. Statistics
- Hourly/daily processing stats
- Contact discovery rate (emails, phones, WhatsApp)
- Performance trends (grafik)

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Browser                             │
│                  http://server:8080                      │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│                   Dashboard Server                       │
│                   (FastAPI/Flask)                        │
│                                                          │
│  Routes:                                                 │
│  ├── GET  /                    → Dashboard UI            │
│  ├── GET  /api/stats           → Overall statistics      │
│  ├── GET  /api/countries       → Country progress        │
│  ├── GET  /api/servers         → Server status           │
│  ├── GET  /api/recent          → Recent activity         │
│  ├── POST /api/export          → CSV export (filtered)   │
│  └── WS   /ws/live             → Live updates (optional) │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│                    PostgreSQL                            │
│                                                          │
│  Views (Materialized for performance):                   │
│  ├── zen_mv_dashboard      → Main stats (refresh 5min)  │
│  ├── zen_mv_country_stats  → Per-country (refresh 5min) │
│  ├── zen_mv_hourly         → Hourly trends (refresh 1h) │
│  └── zen_v_server_status   → Live server status         │
└─────────────────────────────────────────────────────────┘
```

---

## Quick Start

### 1. Install Dependencies
```bash
cd dashboard
pip install -r requirements.txt
```

### 2. Configure
```bash
cp .env.example .env
# Edit .env with database credentials
```

### 3. Run Dashboard
```bash
python app.py --port 8080
```

### 4. Access
```
http://localhost:8080
```

---

## File Structure

```
dashboard/
├── README.md              # This file
├── requirements.txt       # Python dependencies
├── .env.example          # Environment template
├── app.py                # Main FastAPI/Flask app
├── config.py             # Configuration loader
├── database.py           # Database connection
├── routes/
│   ├── __init__.py
│   ├── api.py            # REST API endpoints
│   └── export.py         # CSV export logic
├── services/
│   ├── __init__.py
│   ├── stats.py          # Statistics queries
│   └── exporter.py       # CSV generation
├── templates/
│   ├── base.html         # Base template
│   ├── index.html        # Main dashboard
│   └── export.html       # Export page
├── static/
│   ├── css/
│   │   └── style.css
│   └── js/
│       └── dashboard.js
└── sql/
    ├── materialized_views.sql    # MV definitions
    └── refresh_views.sql         # Refresh scripts
```

---

## CSV Export Feature

### Endpoint
```
POST /api/export
Content-Type: application/json

{
    "columns": ["business_name", "emails", "phones", "country_code"],
    "filters": {
        "country_code": ["ID", "SG"],
        "scrape_status": "success",
        "has_email": true,
        "date_from": "2025-01-01",
        "date_to": "2025-12-31"
    },
    "limit": 10000
}
```

### Available Columns for Export
```
Identity:
- id, source_link, source_id

Business Info:
- business_name, business_category, business_website

Location:
- country_code, country_name, city, state, address, postal_code
- latitude, longitude, timezone

Google Maps:
- gmaps_phone, gmaps_rating, gmaps_review_count, gmaps_price_range

Enriched Contacts:
- emails, emails_count
- phones, phones_count
- whatsapp, whatsapp_count
- social_facebook, social_instagram, social_tiktok, social_youtube

Metadata:
- scrape_status, scrape_error, scrape_time_seconds
- created_at, updated_at, scrape_count
```

### Export Filters
| Filter | Type | Example |
|--------|------|---------|
| country_code | string[] | ["ID", "SG", "MY"] |
| scrape_status | string | "success", "failed", "pending" |
| has_email | boolean | true |
| has_phone | boolean | true |
| has_whatsapp | boolean | true |
| date_from | date | "2025-01-01" |
| date_to | date | "2025-12-31" |
| category | string | "restaurant" |
| limit | integer | 10000 (max: 100000) |

---

## Database Views

### Materialized Views (Performance Optimized)

Dashboard menggunakan **Materialized Views** untuk menghindari slow queries di 1M+ rows.

```sql
-- Main dashboard stats (refresh every 5 minutes)
CREATE MATERIALIZED VIEW zen_mv_dashboard AS
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE scrape_status = 'success') AS success_count,
    COUNT(*) FILTER (WHERE scrape_status = 'failed') AS failed_count,
    SUM(emails_count) AS total_emails,
    SUM(phones_count) AS total_phones,
    SUM(whatsapp_count) AS total_whatsapp,
    COUNT(DISTINCT country_code) AS countries_count,
    NOW() AS refreshed_at
FROM zen_contacts;

-- Refresh command (run via cron or pg_cron)
REFRESH MATERIALIZED VIEW CONCURRENTLY zen_mv_dashboard;
```

### Refresh Schedule
| View | Refresh Interval | Command |
|------|------------------|---------|
| zen_mv_dashboard | 5 minutes | `REFRESH MATERIALIZED VIEW CONCURRENTLY zen_mv_dashboard;` |
| zen_mv_country_stats | 5 minutes | `REFRESH MATERIALIZED VIEW CONCURRENTLY zen_mv_country_stats;` |
| zen_mv_hourly | 1 hour | `REFRESH MATERIALIZED VIEW CONCURRENTLY zen_mv_hourly;` |

---

## Configuration

### Environment Variables (.env)
```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=zenvoyer_db
DB_USER=zenvoyer_db
DB_PASSWORD=your_password

# Dashboard Server
DASHBOARD_HOST=0.0.0.0
DASHBOARD_PORT=8080
DASHBOARD_DEBUG=false

# Export Limits
EXPORT_MAX_ROWS=100000
EXPORT_TIMEOUT=300  # seconds

# Security (optional)
DASHBOARD_USERNAME=admin
DASHBOARD_PASSWORD=secure_password
```

---

## API Reference

### GET /api/stats
Returns overall dashboard statistics.

**Response:**
```json
{
    "total_rows": 1250000,
    "success_count": 1100000,
    "failed_count": 150000,
    "pending_count": 50000,
    "total_emails": 890000,
    "total_phones": 650000,
    "total_whatsapp": 420000,
    "countries_count": 45,
    "refreshed_at": "2025-11-30T10:30:00Z"
}
```

### GET /api/countries
Returns progress per country.

**Response:**
```json
{
    "countries": [
        {
            "country_code": "ID",
            "country_name": "Indonesia",
            "total": 500000,
            "completed": 450000,
            "pending": 50000,
            "progress_percent": 90.0,
            "emails_found": 320000,
            "whatsapp_found": 180000
        },
        ...
    ]
}
```

### GET /api/servers
Returns server status.

**Response:**
```json
{
    "servers": [
        {
            "server_id": "sg-01",
            "status": "online",
            "health": "healthy",
            "urls_per_minute": 45.2,
            "last_heartbeat": "2025-11-30T10:29:55Z"
        },
        ...
    ]
}
```

### POST /api/export
Export data to CSV.

**Request:**
```json
{
    "columns": ["business_name", "emails", "country_code"],
    "filters": {"country_code": ["ID"], "has_email": true},
    "limit": 10000
}
```

**Response:** CSV file download

---

## Security Considerations

1. **Authentication** - Optional basic auth via env variables
2. **Rate Limiting** - Export endpoint limited to prevent abuse
3. **Export Limits** - Max 100K rows per export
4. **SQL Injection** - All queries parameterized
5. **Network** - Run behind reverse proxy (nginx) in production

---

## Development Roadmap

### Phase 1: MVP (Current)
- [ ] Basic dashboard UI
- [ ] Overall stats display
- [ ] Country progress table
- [ ] Simple CSV export

### Phase 2: Enhanced
- [ ] Server monitoring
- [ ] Hourly charts
- [ ] Advanced filters
- [ ] Column selection UI

### Phase 3: Advanced
- [ ] WebSocket live updates
- [ ] User authentication
- [ ] Export scheduling
- [ ] Email notifications

---

## Related Files

- `/migrations/schema_v3_complete.sql` - Database schema with views
- `/DATABASE_AUDIT_REPORT.md` - Performance analysis
- `/POSTGRESQL_INTEGRATION.md` - Database integration docs

---

**Status**: Planning  
**Priority**: Medium  
**Estimated Effort**: 3-5 days for MVP
