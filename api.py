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

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import Optional
import os
import sys
import queue
import asyncio
import logging
import time
from dotenv import load_dotenv

load_dotenv()

from logging_setup import configure_app_logging

configure_app_logging()
log = logging.getLogger("leadgen.outreach")

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


@app.on_event("startup")
async def _api_startup_log():
    log.info(
        "leadgen_api.startup | log_level_env=%s",
        os.getenv("LOG_LEVEL", "INFO"),
    )


class ApolloPasteRequest(BaseModel):
    raw_text: str
    company_name: str

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
pipeline_lock = asyncio.Lock()
PIPELINE_RUNNING = False

class QueueWriter:
    def __init__(self, q: queue.Queue):
        self.q = q
    def write(self, text):
        if text and text.strip():
            # Add a timestamp for better UX
            from datetime import datetime
            ts = datetime.now().strftime("%H:%M:%S")
            self.q.put(f"[{ts}] {text.strip()}")
    def flush(self):
        pass

@app.get("/pipeline/stream", tags=["System"])
async def stream_pipeline(
    region: str = Query("US", description="Geography to run: US, GB, AU, VN")
):
    """
    Executes the main pipeline. 
    Uses a global lock to prevent concurrent runs (resource safety).
    Intercepts prints to stream logs via SSE.
    """
    global PIPELINE_RUNNING
    
    if PIPELINE_RUNNING:
        raise HTTPException(
            status_code=409, 
            detail="A pipeline is already running. Please wait for it to complete."
        )

    q = queue.Queue()

    def run_wrapper():
        global PIPELINE_RUNNING
        from config import RunConfig
        from main import main as run_pipeline

        PIPELINE_RUNNING = True
        old_stdout = sys.stdout
        sys.stdout = QueueWriter(q)
        
        try:
            r = region.upper()
            print(f"🚀 Starting Priority Pipeline for region: {r}")
            config = RunConfig(
                geographies=[r],
                use_yc=True,
                use_techcrunch=True,
                use_google_news=True,
                use_linkedin=True,
                use_seek=(r == "AU"),
                use_reed=(r == "GB"),
                max_companies_for_ai=200,
                pipeline_version="v3_stream",
            )
            run_pipeline(config)
            print("✅ Pipeline execution finished successfully.")
        except Exception as e:
            print(f"❌ ERROR: {e}")
        finally:
            PIPELINE_RUNNING = False
            sys.stdout = old_stdout
            q.put(None) # Sentinel

    # Run in background via executor
    asyncio.get_running_loop().run_in_executor(None, run_wrapper)

    async def log_generator():
        import time
        last_heartbeat = time.time()
        while True:
            try:
                # get_nowait is non-blocking
                msg = q.get_nowait()
                if msg is None:
                    yield "data: [DONE] Pipeline finished.\n\n"
                    break
                yield f"data: {msg}\n\n"
                last_heartbeat = time.time()
            except queue.Empty:
                # If no logs for 15 seconds, send a keep-alive heartbeat
                # Render/Cloudflare might close the connection after 100s of inactivity
                if time.time() - last_heartbeat > 15:
                    yield ": heartbeat\n\n"
                    last_heartbeat = time.time()
                await asyncio.sleep(0.5)

    return StreamingResponse(log_generator(), media_type="text/event-stream")


# ═══════════════════════════════════════════════════════════════════════════════
# OUTREACH ENDPOINTS — Apollo.io + Email Drafting + Sending
# ═══════════════════════════════════════════════════════════════════════════════

# ── Top Leads for Outreach ────────────────────────────────────────────────────
@app.get("/outreach/top-leads", tags=["Outreach"])
async def get_outreach_top_leads(
    min_score: int = Query(10, ge=1, le=18, description="Minimum lead score"),
    limit:     int = Query(50, ge=1, le=200, description="Max leads to return"),
):
    """
    Get top active leads scored ≥ min_score, enriched with AI signal data.
    These are the leads you'd want to outreach to.
    """
    try:
        db = get_db()
        from database import bulk_fetch_cached_ai_results

        result = db.table("active_leads").select(
            "*", count="exact"
        ).gte(
            "final_score", min_score
        ).order(
            "final_score", desc=True
        ).limit(limit).execute()

        leads = result.data or []
        if not leads:
            return {"data": [], "total": 0}

        company_ids = [l["id"] for l in leads]

        # Enrich with industries
        comp_r = db.table("companies").select(
            "id, industries, description, funding_amount"
        ).in_("id", company_ids).execute()
        company_map = {c["id"]: c for c in (comp_r.data or [])}

        # Enrich with AI data
        ai_map = bulk_fetch_cached_ai_results(db, company_ids)

        # Check which already have contacts found
        contacts_r = db.table("outreach_contacts").select(
            "company_id", count="exact"
        ).in_("company_id", company_ids).execute()
        contacted_ids = set(c["company_id"] for c in (contacts_r.data or []))

        formatted = []
        for lead in leads:
            cid = lead["id"]
            ai_data = ai_map.get(cid, {})
            comp = company_map.get(cid, {})
            industries = comp.get("industries") or []

            formatted.append({
                "id":               cid,
                "name_display":     lead.get("name_display", "Unknown"),
                "website":          lead.get("website", ""),
                "verdict":          lead.get("verdict", "COLD"),
                "final_score":      lead.get("final_score", 0),
                "team_size":        lead.get("team_size"),
                "country_code":     lead.get("country_code", ""),
                "city":             lead.get("city", ""),
                "industry":         industries[0] if industries else "Unknown",
                "description":      comp.get("description", "")[:200],
                "funding_amount":   comp.get("funding_amount", ""),
                "why_they_fit":     ai_data.get("why_they_fit", ""),
                "outreach_opener":  ai_data.get("outreach_opener", ""),
                "recommended_plan": ai_data.get("recommended_plan", ""),
                "signal_results":   ai_data.get("signal_results", []),
                "has_contacts":     cid in contacted_ids,
            })

        return {"data": formatted, "total": len(formatted)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Manual Contact Input ──────────────────────────────────────────────────────
@app.post("/outreach/add-contacts", tags=["Outreach"])
async def add_contacts(body: dict):
    """
    Manually add contacts found from Apollo's web UI (or any source).
    User copies contact info from Apollo and pastes it here.

    Request body: {
      "company_id": "uuid",   // required — which lead this contact belongs to
      "contacts": [
        {
          "full_name": "Jane Smith",
          "title": "Head of Talent Acquisition",
          "email": "jane@company.com",
          "email_status": "verified",       // optional, default "verified"
          "linkedin_url": "https://...",    // optional
          "seniority": "director",          // optional
          "phone": "+1...",                 // optional
        },
        ...
      ]
    }
    """
    try:
        company_id = body.get("company_id")
        contacts_list = body.get("contacts", [])

        if not company_id:
            raise HTTPException(status_code=400, detail="company_id is required")
        if not contacts_list:
            raise HTTPException(status_code=400, detail="contacts list is required")
        if len(contacts_list) > 20:
            raise HTTPException(status_code=400, detail="Max 20 contacts per batch")

        db = get_db()

        # Verify company exists
        comp_r = db.table("companies").select("id, name_display").eq("id", company_id).execute()
        if not comp_r.data:
            raise HTTPException(status_code=404, detail="Company not found")

        saved_count = 0
        for contact in contacts_list:
            email = contact.get("email", "").strip()
            full_name = contact.get("full_name", "").strip()
            if not email or not full_name:
                continue  # skip entries without email or name

            db.table("outreach_contacts").insert({
                "company_id":       company_id,
                "apollo_person_id": "",
                "full_name":        full_name,
                "title":            contact.get("title", ""),
                "email":            email,
                "email_status":     contact.get("email_status", "verified"),
                "linkedin_url":     contact.get("linkedin_url", ""),
                "seniority":        contact.get("seniority", ""),
                "departments":      contact.get("departments", []),
                "phone":            contact.get("phone", ""),
            }).execute()
            saved_count += 1

        company_name = comp_r.data[0].get("name_display", "Unknown")
        return {
            "message": f"Added {saved_count} contacts for {company_name}",
            "contacts_saved": saved_count,
            "company_name": company_name,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── List Contacts ─────────────────────────────────────────────────────────────
@app.get("/outreach/contacts", tags=["Outreach"])
async def list_contacts(
    page:         int           = Query(1, ge=1),
    page_size:    int           = Query(25, ge=1, le=100),
    company_id:   Optional[str] = Query(None),
    email_status: Optional[str] = Query(None, description="verified | guessed"),
):
    """List all found contacts with their company info."""
    try:
        db = get_db()
        start, end = supabase_range(page, page_size)

        q = db.table("outreach_contacts").select(
            "*, companies(name_display, website)",
            count="exact"
        ).order("fetched_at", desc=True)

        if company_id:
            q = q.eq("company_id", company_id)
        if email_status:
            q = q.eq("email_status", email_status)

        result = q.range(start, end).execute()

        rows = []
        for r in (result.data or []):
            company = r.pop("companies", {}) or {}
            rows.append({
                **r,
                "company_name": company.get("name_display", ""),
                "company_website": company.get("website", ""),
            })

        return paged(rows, result.count or 0, page, page_size)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Draft Emails ──────────────────────────────────────────────────────────────
@app.post("/outreach/draft-emails", tags=["Outreach"])
async def draft_emails(body: dict):
    """
    Trigger AI email drafting for selected contacts.

    Request body: { "contact_ids": ["uuid1", "uuid2", ...] }
    """
    try:
        contact_ids = body.get("contact_ids", [])
        if not contact_ids:
            raise HTTPException(status_code=400, detail="contact_ids required")
        if len(contact_ids) > 50:
            raise HTTPException(status_code=400, detail="Max 50 contacts per batch")

        log.info("draft_emails.request | contact_ids=%s", len(contact_ids))

        db = get_db()
        from database import bulk_fetch_cached_ai_results
        from email_drafter import draft_batch

        # Fetch contacts
        contacts_r = db.table("outreach_contacts").select("*").in_("id", contact_ids).execute()
        contacts = contacts_r.data or []

        if not contacts:
            raise HTTPException(status_code=404, detail="No contacts found")

        # Gather company IDs
        company_ids = list(set(c["company_id"] for c in contacts))

        # Fetch companies
        comp_r = db.table("companies").select("*").in_("id", company_ids).execute()
        company_map = {c["id"]: c for c in (comp_r.data or [])}

        # Fetch AI data
        ai_map = bulk_fetch_cached_ai_results(db, company_ids)

        # Build draft input
        draft_input = []
        for contact in contacts:
            cid = contact["company_id"]
            draft_input.append({
                "contact": contact,
                "company": company_map.get(cid, {}),
                "ai_data": ai_map.get(cid, {}),
                "contact_id": contact["id"],
                "company_id": cid,
            })

        # Run drafting in background
        drafts = await asyncio.get_running_loop().run_in_executor(
            None, draft_batch, draft_input
        )

        # Save drafts to DB
        saved_count = 0
        for draft in drafts:
            if draft.get("subject") and draft.get("body"):
                db.table("outreach_emails").insert({
                    "contact_id": draft["contact_id"],
                    "company_id": draft["company_id"],
                    "subject":    draft["subject"],
                    "body":       draft["body"],
                    "status":     "draft",
                }).execute()
                saved_count += 1

        log.info(
            "draft_emails.complete | contacts_requested=%s drafts_saved=%s",
            len(contact_ids),
            saved_count,
        )
        return {
            "message": f"Drafted {saved_count} emails",
            "drafts_created": saved_count,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── List Emails ───────────────────────────────────────────────────────────────
@app.get("/outreach/emails", tags=["Outreach"])
async def list_emails(
    page:      int           = Query(1, ge=1),
    page_size: int           = Query(25, ge=1, le=100),
    status:    Optional[str] = Query(None, description="draft | approved | sent | rejected"),
):
    """List all email drafts with contact and company info."""
    try:
        db = get_db()
        start, end = supabase_range(page, page_size)

        q = db.table("outreach_emails").select(
            "*, outreach_contacts(full_name, title, email), companies(name_display)",
            count="exact"
        ).order("created_at", desc=True)

        if status:
            q = q.eq("status", status)

        result = q.range(start, end).execute()

        rows = []
        for r in (result.data or []):
            contact = r.pop("outreach_contacts", {}) or {}
            company = r.pop("companies", {}) or {}
            rows.append({
                **r,
                "contact_name":   contact.get("full_name", ""),
                "contact_title":  contact.get("title", ""),
                "contact_email":  contact.get("email", ""),
                "company_name":   company.get("name_display", ""),
            })

        return paged(rows, result.count or 0, page, page_size)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Approve / Reject / Edit Email ─────────────────────────────────────────────
@app.put("/outreach/emails/{email_id}", tags=["Outreach"])
async def update_email(email_id: str, body: dict):
    """
    Update an email draft: approve, reject, or edit.

    Request body:
      { "action": "approve" | "reject", "edited_subject": "...", "edited_body": "..." }
    """
    try:
        action = body.get("action", "")
        if action not in ("approve", "reject"):
            raise HTTPException(status_code=400, detail="action must be 'approve' or 'reject'")

        db = get_db()

        updates = {
            "status": "approved" if action == "approve" else "rejected",
        }

        # Allow inline editing when approving
        if action == "approve":
            if body.get("edited_subject"):
                updates["edited_subject"] = body["edited_subject"]
            if body.get("edited_body"):
                updates["edited_body"] = body["edited_body"]

        db.table("outreach_emails").update(updates).eq("id", email_id).execute()

        log.info("email.status_update | email_id=%s action=%s", email_id, action)
        return {"message": f"Email {action}d successfully", "email_id": email_id}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Send Single Approved Email ────────────────────────────────────────────────
def _run_send_single_email(email_id: str) -> dict:
    """
    Fetch outreach row, send via SMTP, update Supabase.
    Safe to call from a worker thread or FastAPI BackgroundTasks (sync → threadpool).
    """
    from email_sender import send_email, is_configured
    from datetime import datetime, timezone

    t0 = time.perf_counter()
    log.info("send_single.worker_start | email_id=%s", email_id)

    try:
        if not is_configured():
            log.warning("send_single.worker_abort | email_id=%s reason=smtp_not_configured", email_id)
            return {"ok": False, "error": "smtp_not_configured", "message": "SMTP not configured."}

        db = get_db()
        result = db.table("outreach_emails").select(
            "*, outreach_contacts(full_name, email)"
        ).eq("id", email_id).execute()

        if not result.data:
            log.warning("send_single.worker_abort | email_id=%s reason=not_found", email_id)
            return {"ok": False, "error": "not_found", "message": "Email not found"}

        email_row = result.data[0]
        if email_row["status"] == "sent":
            log.info(
                "send_single.worker_skip | email_id=%s reason=already_sent elapsed_ms=%.0f",
                email_id,
                (time.perf_counter() - t0) * 1000,
            )
            return {"ok": True, "already_sent": True, "message": "Email already sent", "success": True}

        if email_row["status"] != "approved":
            log.warning(
                "send_single.worker_abort | email_id=%s reason=not_approved status=%s",
                email_id,
                email_row.get("status"),
            )
            return {
                "ok": False,
                "error": "not_approved",
                "message": "Email must be approved before sending.",
            }

        contact = email_row.get("outreach_contacts", {}) or {}
        to_email = contact.get("email", "")
        full_name = contact.get("full_name", "")
        if not to_email:
            log.warning("send_single.worker_abort | email_id=%s reason=no_recipient", email_id)
            return {"ok": False, "error": "no_recipient", "message": "Contact email is missing"}

        subject = email_row.get("edited_subject") or email_row.get("subject", "")
        body = email_row.get("edited_body") or email_row.get("body", "")

        log.info(
            "send_single.smtp_invoke | email_id=%s to=%s contact=%s subject_preview=%s",
            email_id,
            to_email,
            full_name or "?",
            (subject[:80] + "…") if len(subject) > 80 else subject,
        )

        send_result = send_email(to_email, subject, body)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        if send_result["success"]:
            db.table("outreach_emails").update({
                "status": "sent",
                "sent_at": datetime.now(timezone.utc).isoformat(),
                "smtp_response": send_result["message"],
            }).eq("id", email_id).execute()
            log.info(
                "send_single.worker_success | email_id=%s to=%s elapsed_ms=%.0f smtp=%s",
                email_id,
                to_email,
                elapsed_ms,
                send_result.get("message", ""),
            )
            return {"ok": True, "success": True, "message": send_result["message"]}

        db.table("outreach_emails").update({
            "smtp_response": send_result["message"],
        }).eq("id", email_id).execute()
        log.warning(
            "send_single.worker_smtp_failed | email_id=%s to=%s elapsed_ms=%.0f detail=%s",
            email_id,
            to_email,
            elapsed_ms,
            send_result.get("message", ""),
        )
        return {"ok": False, "success": False, "message": send_result["message"]}

    except Exception as e:
        log.exception(
            "send_single.worker_exception | email_id=%s elapsed_ms=%.0f",
            email_id,
            (time.perf_counter() - t0) * 1000,
        )
        try:
            db = get_db()
            db.table("outreach_emails").update({
                "smtp_response": f"Server error: {str(e)[:500]}",
            }).eq("id", email_id).execute()
        except Exception:
            log.exception("send_single.db_update_failed | email_id=%s", email_id)
        return {"ok": False, "success": False, "message": str(e)}


@app.post("/outreach/send-single/{email_id}", tags=["Outreach"])
async def send_single_email(
    email_id: str,
    background_tasks: BackgroundTasks,
    wait: bool = Query(
        False,
        description=(
            "If false (default), SMTP runs after the HTTP response — avoids gateway timeouts on Render. "
            "Poll GET /outreach/emails or refresh the UI. If true, block until send completes (may timeout)."
        ),
    ),
):
    """
    Send a specific approved email via SMTP.

    Default **queued** mode returns immediately so proxies (Render ~30s, browsers, Vercel) do not time out
    while Gmail/SMTP connects. Use `?wait=true` only for local debugging.
    """
    try:
        from email_sender import is_configured

        log.info("send_single.http_request | email_id=%s wait=%s", email_id, wait)

        if not is_configured():
            log.warning("send_single.http_reject | email_id=%s reason=smtp_not_configured", email_id)
            raise HTTPException(
                status_code=400,
                detail="SMTP not configured.",
            )

        db = get_db()
        result = db.table("outreach_emails").select(
            "*, outreach_contacts(full_name, email)"
        ).eq("id", email_id).execute()

        if not result.data:
            log.warning("send_single.http_reject | email_id=%s reason=not_found", email_id)
            raise HTTPException(status_code=404, detail="Email not found")

        email_row = result.data[0]
        if email_row["status"] == "sent":
            log.info("send_single.http_skip | email_id=%s reason=already_sent", email_id)
            return {"message": "Email already sent", "success": True, "queued": False}

        if email_row["status"] != "approved":
            log.warning(
                "send_single.http_reject | email_id=%s reason=not_approved status=%s",
                email_id,
                email_row.get("status"),
            )
            raise HTTPException(
                status_code=400,
                detail="Email must be approved before sending.",
            )

        contact = email_row.get("outreach_contacts", {}) or {}
        if not contact.get("email"):
            log.warning("send_single.http_reject | email_id=%s reason=missing_contact_email", email_id)
            raise HTTPException(status_code=400, detail="Contact email is missing")

        if not wait:
            background_tasks.add_task(_run_send_single_email, email_id)
            log.info("send_single.http_queued | email_id=%s to=%s", email_id, contact.get("email", ""))
            return {
                "success": True,
                "queued": True,
                "email_id": email_id,
                "message": (
                    "Send started in the background. Wait a few seconds and refresh — "
                    "status will change to sent, or check smtp_response if it failed."
                ),
            }

        # Blocking path (local / short SMTP only)
        log.info("send_single.http_blocking | email_id=%s", email_id)
        out = await asyncio.to_thread(_run_send_single_email, email_id)
        if out.get("error") == "not_found":
            raise HTTPException(status_code=404, detail="Email not found")
        if out.get("already_sent"):
            return {"message": out["message"], "success": True, "queued": False}
        if not out.get("ok"):
            log.warning(
                "send_single.http_done | email_id=%s success=false message=%s",
                email_id,
                out.get("message", ""),
            )
            return {"message": out.get("message", "Send failed"), "success": False, "queued": False}
        log.info("send_single.http_done | email_id=%s success=true", email_id)
        return {"message": out.get("message", "Email sent successfully"), "success": True, "queued": False}

    except HTTPException:
        raise
    except Exception as e:
        log.exception("send_single.http_error | email_id=%s", email_id)
        raise HTTPException(status_code=500, detail=str(e))


# ── Send Approved Emails ──────────────────────────────────────────────────────
@app.post("/outreach/send", tags=["Outreach"])
async def send_approved_emails():
    """
    Send all emails with status='approved' via SMTP.
    Updates status to 'sent' and records SMTP response.
    """
    try:
        db = get_db()
        from email_sender import send_email, is_configured
        from datetime import datetime, timezone

        if not is_configured():
            log.warning("send_batch.http_reject | reason=smtp_not_configured")
            raise HTTPException(
                status_code=400,
                detail="SMTP not configured. Set SMTP_HOST, SMTP_USER, SMTP_PASSWORD, SMTP_FROM_EMAIL in .env"
            )

        # Fetch approved emails with contact info
        result = db.table("outreach_emails").select(
            "*, outreach_contacts(full_name, email)"
        ).eq("status", "approved").execute()

        emails = result.data or []
        if not emails:
            log.info("send_batch.skip | reason=no_approved_emails")
            return {"message": "No approved emails to send", "sent": 0, "failed": 0}

        log.info("send_batch.start | approved_count=%s", len(emails))
        sent = 0
        failed = 0

        for email_row in emails:
            eid = email_row["id"]
            contact = email_row.get("outreach_contacts", {}) or {}
            to_email = contact.get("email", "")

            if not to_email:
                log.warning("send_batch.skip_row | email_id=%s reason=no_recipient", eid)
                failed += 1
                continue

            # Use edited version if available, otherwise original
            subject = email_row.get("edited_subject") or email_row.get("subject", "")
            body = email_row.get("edited_body") or email_row.get("body", "")

            send_result = send_email(to_email, subject, body)

            if send_result["success"]:
                db.table("outreach_emails").update({
                    "status": "sent",
                    "sent_at": datetime.now(timezone.utc).isoformat(),
                    "smtp_response": send_result["message"],
                }).eq("id", eid).execute()
                sent += 1
                log.info("send_batch.row_ok | email_id=%s to=%s", eid, to_email)
            else:
                db.table("outreach_emails").update({
                    "smtp_response": send_result["message"],
                }).eq("id", eid).execute()
                failed += 1
                log.warning(
                    "send_batch.row_fail | email_id=%s to=%s detail=%s",
                    eid,
                    to_email,
                    send_result.get("message", ""),
                )

        log.info("send_batch.complete | sent=%s failed=%s", sent, failed)
        return {
            "message": f"Sent {sent} emails, {failed} failed",
            "sent": sent,
            "failed": failed,
        }

    except HTTPException:
        raise
    except Exception as e:
        log.exception("send_batch.error")
        raise HTTPException(status_code=500, detail=str(e))


# ── Parse Apollo Raw Text ─────────────────────────────────────────────────────
@app.post("/outreach/parse-apollo", tags=["Outreach"])
async def parse_apollo(req: ApolloPasteRequest):
    """
    Takes raw pasted text from Apollo UI, parses out contacts (name, title, location),
    and uses OpenAI to identify the best decision makers to target for InterviewScreener.com.
    """
    try:
        from openai import OpenAI
        import os
        import json
        
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        prompt = f"""
We have a raw list of contacts copied from Apollo.io for the company: {req.company_name}.
Our product is InterviewScreener.com (an AI-powered candidate screening platform replacing first-round phone screens). 
Our target buyers are Founders, CEOs, VP of HR, Head of Talent Acquisition, HR Directors, and Recruiters.

Here is the raw text:
{req.raw_text}

Task:
1. Parse the text to find all individuals. Apollo format usually shows:
   Name
   Job Title
   (Empty line or company name)
   Access email / Access Mobile / Click to run
   Location
   Employees
   Industry
   Connections
2. For each parsed individual, extract:
   - full_name
   - title
   - location
3. Evaluate if they are a good target for our product (Target buyers are TA, HR, Founders, C-Suite).
   - "is_target": true if they are a strong buyer persona, false otherwise.
   - "ai_reasoning": 1 short concise sentence explaining why.

Return ONLY a JSON array of objects. Make sure the JSON is valid.
Example:
[
  {{
    "full_name": "Alfredo O.",
    "title": "Account Executive",
    "location": "Chicago, Illinois",
    "is_target": false,
    "ai_reasoning": "Account executives are not the buyer for recruiting software."
  }}
]
"""
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=2000,
        )
        
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()
        
        contacts = json.loads(raw)
        return {"contacts": contacts, "success": True}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# ── Outreach Stats ────────────────────────────────────────────────────────────
@app.get("/outreach/stats", tags=["Outreach"])
async def get_outreach_stats():
    """Get summary stats for the outreach pipeline."""
    try:
        db = get_db()

        contacts_r = db.table("outreach_contacts").select("id", count="exact").execute()
        verified_r = db.table("outreach_contacts").select("id", count="exact").eq("email_status", "verified").execute()
        drafts_r   = db.table("outreach_emails").select("id", count="exact").eq("status", "draft").execute()
        approved_r = db.table("outreach_emails").select("id", count="exact").eq("status", "approved").execute()
        sent_r     = db.table("outreach_emails").select("id", count="exact").eq("status", "sent").execute()

        return {
            "total_contacts":    contacts_r.count or 0,
            "verified_contacts": verified_r.count or 0,
            "drafts":            drafts_r.count   or 0,
            "approved":          approved_r.count or 0,
            "sent":              sent_r.count     or 0,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Entry point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
