"""
api.py — LeadGen FastAPI Backend
Production-grade REST API for the LeadGen dashboard.
All Supabase / DB logic stays server-side; the React frontend never holds credentials.

Column sources verified from database.py:
  • raw_scrape_events  — columns: id, batch_id, source, country_code, company_name_raw,
                                   website_raw, team_size_raw, is_hiring_raw, scraped_at (Supabase default)
  • token_status       — columns: company_id (PK), name_normalized, profile_needs_refresh,
                                   job_needs_refresh, ai_needs_refresh, last_verdict, last_score,
                                   profile_expires_at, job_expires_at, ai_expires_at
  • raw_job_events     — columns: id, batch_id, company_id, check_method, job_count, hr_role_found,
                                   hr_tech_role_found, ats_board_url, expires_at
  • raw_ai_results     — columns: id, batch_id, company_id, final_score, max_score, verdict,
                                   data_confidence, llm_model_used, llm_cost_usd, expires_at
  • pipeline_runs      — columns: id, started_at, completed_at, geographies_run, sources_run,
                                   companies_scraped, companies_filtered, companies_job_checked,
                                   companies_ai_evaluated, hot_count, warm_count, cold_count,
                                   total_llm_cost_usd, pipeline_version
  • active_leads view  — exposes: id, name_display, website, verdict, final_score, team_size,
                                   country_code, city, why_they_fit, outreach_opener
  • companies          — includes: id, name_display, website, team_size, country_code, city,
                                   industries, sources, first_seen_run

Start: uvicorn api:app --reload --port 8000
Docs:  http://localhost:8000/docs
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import Optional
import os
import sys
import queue
import asyncio
from dotenv import load_dotenv

load_dotenv()

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="LeadGen API",
    description="Backend API for the LeadGen lead intelligence dashboard.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "https://leadgenfrontend.vercel.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Standard paginated response envelope ────────────────────────────────────
def paged(data: list, total: int, page: int, page_size: int) -> dict:
    return {
        "data":        data,
        "total":       total,
        "page":        page,
        "page_size":   page_size,
        "total_pages": max(1, -(-total // page_size)),  # ceiling division
    }

# ─── Lazy DB singleton ───────────────────────────────────────────────────────
_db = None

def get_db():
    global _db
    if _db is None:
        from database import get_client
        _db = get_client()
    return _db

# ─── Supabase pagination helpers ─────────────────────────────────────────────
def supabase_range(page: int, page_size: int):
    """Convert 1-based page to Supabase [start, end] offsets."""
    start = (page - 1) * page_size
    end   = start + page_size - 1
    return start, end


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health", tags=["System"])
async def health_check():
    return {"status": "healthy", "version": "1.0.0"}


# ── Dashboard Stats ────────────────────────────────────────────────────────────
@app.get("/dashboard/stats", tags=["Dashboard"])
async def get_dashboard_stats():
    """Aggregate counts for dashboard stat cards — single DB round-trip per stat."""
    try:
        db = get_db()

        total_r = db.table("active_leads").select("id", count="exact").execute()
        hot_r   = db.table("active_leads").select("id", count="exact").eq("verdict", "HOT").execute()
        warm_r  = db.table("active_leads").select("id", count="exact").eq("verdict", "WARM").execute()

        from datetime import datetime, timedelta, timezone
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        runs_r = db.table("pipeline_runs").select("hot_count, warm_count").gte("started_at", yesterday).execute()
        daily_total = sum(
            (r.get("hot_count") or 0) + (r.get("warm_count") or 0)
            for r in (runs_r.data or [])
        )

        return {
            "total_leads":    total_r.count  or 0,
            "hot_leads":      hot_r.count    or 0,
            "warm_leads":     warm_r.count   or 0,
            "pipeline_speed": daily_total,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Active Leads ───────────────────────────────────────────────────────────────
@app.get("/leads", tags=["Leads"])
async def get_leads(
    page:      int           = Query(1,   ge=1),
    page_size: int           = Query(10,  ge=1, le=100),
    search:    Optional[str] = Query(None, description="Partial company name match"),
    verdict:   Optional[str] = Query(None, description="HOT | WARM | COLD"),
    country:   Optional[str] = Query(None, description="Country code e.g. US, GB"),
):
    """
    Paginated active leads from the `active_leads` view,
    enriched with industry and AI signal details.
    """
    try:
        db = get_db()
        from database import bulk_fetch_cached_ai_results

        start, end = supabase_range(page, page_size)

        q = db.table("active_leads").select("*", count="exact").order("final_score", desc=True)
        if verdict:
            q = q.eq("verdict", verdict.upper())
        if country:
            q = q.eq("country_code", country.upper())
        if search:
            q = q.ilike("name_display", f"%{search}%")

        result = q.range(start, end).execute()
        leads  = result.data or []
        total  = result.count or 0

        if not leads:
            return paged([], total, page, page_size)

        company_ids = [l["id"] for l in leads]

        comp_r       = db.table("companies").select("id, industries").in_("id", company_ids).execute()
        industry_map = {c["id"]: (c.get("industries") or []) for c in (comp_r.data or [])}
        ai_map       = bulk_fetch_cached_ai_results(db, company_ids)

        formatted = []
        for lead in leads:
            cid        = lead["id"]
            ai_data    = ai_map.get(cid, {})
            industries = industry_map.get(cid, [])
            formatted.append({
                "id":              cid,
                "name_display":    lead.get("name_display") or "Unknown",
                "website":         lead.get("website", ""),
                "verdict":         lead.get("verdict", "COLD"),
                "final_score":     lead.get("final_score", 0),
                "industry":        industries[0] if industries else "Unknown",
                "team_size":       lead.get("team_size"),
                "why_they_fit":    ai_data.get("why_they_fit")    or lead.get("why_they_fit", ""),
                "outreach_opener": ai_data.get("outreach_opener") or lead.get("outreach_opener", ""),
                "signal_results":  ai_data.get("signal_results", []),
                "proof_urls":      ai_data.get("proof_urls", {}),
                "country_code":    lead.get("country_code", ""),
                "city":            lead.get("city", ""),
                "data_confidence": ai_data.get("data_confidence", "LOW"),
            })

        return paged(formatted, total, page, page_size)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── All Companies ──────────────────────────────────────────────────────────────
@app.get("/companies", tags=["Pipeline"])
async def get_companies(
    page:      int           = Query(1,   ge=1),
    page_size: int           = Query(10,  ge=1, le=100),
    country:   Optional[str] = Query(None),
    search:    Optional[str] = Query(None, description="Partial company name match"),
):
    """Paginated companies from the `companies` table."""
    try:
        db = get_db()
        start, end = supabase_range(page, page_size)

        q = db.table("companies").select(
            "id, name_display, website, team_size, country_code, city, industries, sources, first_seen_run",
            count="exact"
        ).order("name_display")

        if country:
            q = q.eq("country_code", country.upper())
        if search:
            q = q.ilike("name_display", f"%{search}%")

        result = q.range(start, end).execute()
        return paged(result.data or [], result.count or 0, page, page_size)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Raw Scrape Events ──────────────────────────────────────────────────────────
@app.get("/scrapes", tags=["Pipeline"])
async def get_scrapes(
    page:      int           = Query(1,   ge=1),
    page_size: int           = Query(10,  ge=1, le=100),
    search:    Optional[str] = Query(None, description="Partial company name match"),
    source:    Optional[str] = Query(None, description="Scrape source e.g. YC, LinkedIn"),
    country:   Optional[str] = Query(None),
):
    """
    Paginated raw scrape events from `raw_scrape_events`.
    Note: this table has no `created_at` — uses Supabase's auto `id` ordering.
    """
    try:
        db = get_db()
        start, end = supabase_range(page, page_size)

        q = db.table("raw_scrape_events").select(
            "id, company_name_raw, source, country_code, team_size_raw, is_hiring_raw, batch_id",
            count="exact"
        ).order("id", desc=True)   # id is UUID with Supabase default ordering

        if source:
            q = q.eq("source", source)
        if country:
            q = q.eq("country_code", country.upper())
        if search:
            q = q.ilike("company_name_raw", f"%{search}%")

        result = q.range(start, end).execute()
        return paged(result.data or [], result.count or 0, page, page_size)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Job Events ─────────────────────────────────────────────────────────────────
@app.get("/job-events", tags=["Pipeline"])
async def get_job_events(
    page:      int  = Query(1,   ge=1),
    page_size: int  = Query(10,  ge=1, le=100),
    hr_only:   bool = Query(False, description="Only companies where hr_role_found=true"),
):
    """Paginated job check results from `raw_job_events`."""
    try:
        db = get_db()
        start, end = supabase_range(page, page_size)

        q = db.table("raw_job_events").select(
            "id, company_id, check_method, job_count, hr_role_found, hr_tech_role_found, "
            "ats_board_url, batch_id, expires_at, "
            "companies(name_display)",
            count="exact"
        ).order("expires_at", desc=True)

        if hr_only:
            q = q.eq("hr_role_found", True)

        result = q.range(start, end).execute()

        rows = []
        for r in (result.data or []):
            rows.append({
                "id":                 r.get("id"),
                "company":            (r.get("companies") or {}).get("name_display") or r.get("company_id", ""),
                "company_id":         r.get("company_id"),
                "check_method":       r.get("check_method"),
                "job_count":          r.get("job_count", 0),
                "hr_role_found":      r.get("hr_role_found", False),
                "hr_tech_role_found": r.get("hr_tech_role_found", False),
                "ats_board_url":      r.get("ats_board_url"),
                "batch_id":           r.get("batch_id"),
                "expires_at":         r.get("expires_at"),
            })

        return paged(rows, result.count or 0, page, page_size)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── AI Evaluations ─────────────────────────────────────────────────────────────
@app.get("/ai-results", tags=["AI"])
async def get_ai_results(
    page:      int           = Query(1,   ge=1),
    page_size: int           = Query(10,  ge=1, le=100),
    verdict:   Optional[str] = Query(None, description="HOT | WARM | COLD"),
):
    """Paginated AI evaluation results from `raw_ai_results`."""
    try:
        db = get_db()
        start, end = supabase_range(page, page_size)

        q = db.table("raw_ai_results").select(
            "id, company_id, final_score, max_score, verdict, data_confidence, "
            "llm_model_used, llm_cost_usd, batch_id, expires_at, "
            "companies(name_display)",
            count="exact"
        ).order("final_score", desc=True)

        if verdict:
            q = q.eq("verdict", verdict.upper())

        result = q.range(start, end).execute()

        rows = []
        for r in (result.data or []):
            rows.append({
                "id":              r.get("id"),
                "company":         (r.get("companies") or {}).get("name_display") or r.get("company_id", ""),
                "company_id":      r.get("company_id"),
                "verdict":         r.get("verdict"),
                "final_score":     r.get("final_score", 0),
                "max_score":       r.get("max_score", 18),
                "data_confidence": r.get("data_confidence"),
                "llm_model_used":  r.get("llm_model_used"),
                "llm_cost_usd":    r.get("llm_cost_usd"),
                "batch_id":        r.get("batch_id"),
                "expires_at":      r.get("expires_at"),
            })

        return paged(rows, result.count or 0, page, page_size)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Pipeline Runs ──────────────────────────────────────────────────────────────
@app.get("/pipeline-runs", tags=["System"])
async def get_pipeline_runs(
    page:      int = Query(1,  ge=1),
    page_size: int = Query(5,  ge=1, le=50),
):
    """Paginated pipeline run history from `pipeline_runs`."""
    try:
        db = get_db()
        start, end = supabase_range(page, page_size)

        result = db.table("pipeline_runs").select(
            "id, started_at, completed_at, geographies_run, sources_run, "
            "companies_scraped, companies_filtered, companies_job_checked, "
            "companies_ai_evaluated, hot_count, warm_count, cold_count, "
            "total_llm_cost_usd, pipeline_version",
            count="exact"
        ).order("started_at", desc=True).range(start, end).execute()

        return paged(result.data or [], result.count or 0, page, page_size)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Token Cache ────────────────────────────────────────────────────────────────
@app.get("/token-cache", tags=["System"])
async def get_token_cache(
    page:       int  = Query(1,   ge=1),
    page_size:  int  = Query(10,  ge=1, le=100),
    search:     Optional[str] = Query(None, description="Partial company name match"),
    stale_only: bool = Query(False, description="Only companies with ≥1 stale token"),
):
    """
    Paginated token freshness status from the `token_status` view.
    Primary key is company_id (UUID), not `id`.
    """
    try:
        db = get_db()
        start, end = supabase_range(page, page_size)

        # token_status PK = company_id — no `id` column
        q = db.table("token_status").select(
            "company_id, name_normalized, "
            "profile_needs_refresh, job_needs_refresh, ai_needs_refresh, "
            "last_verdict, last_score, profile_expires_at, job_expires_at, ai_expires_at",
            count="exact"
        ).order("name_normalized")

        if stale_only:
            q = q.or_("profile_needs_refresh.eq.true,job_needs_refresh.eq.true,ai_needs_refresh.eq.true")
        if search:
            q = q.ilike("name_normalized", f"%{search}%")

        result = q.range(start, end).execute()

        rows = []
        for r in (result.data or []):
            rows.append({
                "id":                   r.get("company_id"),  # use company_id as unique key for React
                "company_id":           r.get("company_id"),
                "name_normalized":      r.get("name_normalized", ""),
                "profile_needs_refresh":r.get("profile_needs_refresh", False),
                "job_needs_refresh":    r.get("job_needs_refresh", False),
                "ai_needs_refresh":     r.get("ai_needs_refresh", False),
                "last_verdict":         r.get("last_verdict"),
                "last_score":           r.get("last_score"),
                "profile_expires_at":   r.get("profile_expires_at"),
                "job_expires_at":       r.get("job_expires_at"),
                "ai_expires_at":        r.get("ai_expires_at"),
            })

        return paged(rows, result.count or 0, page, page_size)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Pipeline Runner Stream (SSE) ───────────────────────────────────────────────
class QueueWriter:
    def __init__(self, q: queue.Queue):
        self.q = q
    def write(self, text):
        if text.strip():
            self.q.put(text)
    def flush(self):
        pass

@app.get("/pipeline/stream", tags=["System"])
async def stream_pipeline(
    region: str = Query("US", description="Geography to run: US, GB, AU, VN")
):
    """
    Executes the main pipeline synchronously in a background thread, 
    but intercepts `print()` statements to stream standard output 
    as Server-Sent Events (SSE) back to the client.
    """
    q = queue.Queue()

    def run_wrapper():
        # Locally import to avoid circular dependencies if any
        from config import RunConfig
        from main import main as run_pipeline

        old_stdout = sys.stdout
        sys.stdout = QueueWriter(q)
        try:
            r = region.upper()
            config = RunConfig(
                geographies=[r],
                use_yc=True,
                use_techcrunch=True,
                use_google_news=True,
                use_linkedin=True,
                use_seek=(r == "AU"),
                use_reed=(r == "GB"),
                use_naukri=False,   # Explicitly disabled IN
                max_companies_for_ai=200,
                pipeline_version="v3",
            )
            run_pipeline(config)
        except Exception as e:
            print(f"ERROR: {e}")
        finally:
            q.put(None)  # Sentinel value signaling end of stream
            sys.stdout = old_stdout

    # Launch in a background thread so we don't block FastAPI's event loop
    asyncio.get_running_loop().run_in_executor(None, run_wrapper)

    async def log_generator():
        while True:
            try:
                # get_nowait is non-blocking
                msg = q.get_nowait()
                if msg is None:
                    yield "event: close\ndata: Pipeline completed.\n\n"
                    break
                # Replace actual newlines within messages so we don't break SSE protocol
                safe_msg = msg.replace("\n", " ")
                yield f"data: {safe_msg}\n\n"
            except queue.Empty:
                await asyncio.sleep(0.1)

    return StreamingResponse(log_generator(), media_type="text/event-stream")


# ─── Entry point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
