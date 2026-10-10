# Employee Attendance & Analytics API

FastAPI + MongoDB service for punch-in/out, regularization with an audit trail, and aggregation-based analytics.
All code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"   # or put them in a local .env
python sample_seed.py            # optional: load the nine sample documents
uvicorn app.main:app --port 8000
```
Open http://localhost:8000/docs for the interactive API. Requires MongoDB 6.0+ (tested on 7.0).

## Notes
- Indexes are created idempotently at startup (`create_indexes`). See `DECISIONS.md`.
- Instants are stored as BSON UTC dates and exposed as epoch milliseconds, truncated to whole seconds.
- Half-up rounding in pipelines is done with `Decimal128`, because MongoDB's `$round` rounds half to even.
- Race safety: unique indexes for creates and punch-in, conditional updates for punch-out, and optimistic concurrency for PATCH (the loser gets 409).
- `GET /admin/explain/{endpoint}` runs `explain("executionStats")` on the same query or pipeline the endpoint builds.
- All endpoints in the contract are implemented. Nothing known to be missing.
