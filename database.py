"""
database.py — Supabase client, CRUD helpers, and token checker
All pipeline data flows through this module.
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from dotenv import load_dotenv
from supabase import create_client, Client, ClientOptions

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# TTL constants (in days) — change these to adjust freshness windows
TTL = {
    "profile":   180,   # Company description, industry, team size
    "job":       7,     # Job postings check
    "linkedin":  14,    # LinkedIn HR signal
    "funding":   None,  # Permanent — funding is a historical fact
    "ai":        7,     # AI Brain verdict (driven by job TTL)
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _expiry(ttl_days: Optional[int]) -> Optional[datetime]:
    if ttl_days is None:
        return None
    return _now() + timedelta(days=ttl_days)


def get_client() -> Client:
    """Returns authenticated Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env"
        )
    # timeout=30.0 prevents any single DB call from hanging the pipeline indefinitely.
    # Without this, a dropped Supabase connection silently blocks forever.
    options = ClientOptions(postgrest_client_timeout=30, storage_client_timeout=30)
    return create_client(SUPABASE_URL, SUPABASE_KEY, options=options)


# ─────────────────────────────────────────
# PIPELINE RUNS
# ─────────────────────────────────────────

def start_pipeline_run(db: Client, geographies: list[str], sources: list[str]) -> str:
    """Creates a new pipeline_run row. Returns the run UUID."""
    result = db.table("pipeline_runs").insert({
        "started_at": _now().isoformat(),
        "geographies_run": geographies,
        "sources_run": sources,
    }).execute()
    run_id = result.data[0]["id"]
    print(f"[DB] Pipeline run started: {run_id}")
    return run_id


def finish_pipeline_run(db: Client, run_id: str, stats: dict):
    """Updates the pipeline_run row with final stats."""
    db.table("pipeline_runs").update({
        "completed_at": _now().isoformat(),
        **stats,
    }).eq("id", run_id).execute()
    print(f"[DB] Pipeline run {run_id} completed.")


# ─────────────────────────────────────────
# COMPANY UPSERT
# ─────────────────────────────────────────

def normalize_name(name: str) -> str:
    return name.lower().strip()


def normalize_location(locations: list) -> dict:
    """
    Converts a raw locations list into structured country_code / city.
    Handles YC-style ["San Francisco, CA", "United States"] and
    LinkedIn-style ["New York, NY"] and international variants.
    """
    country_map = {
        "united states": "US", "usa": "US", "u.s.": "US", "us": "US",
        "united kingdom": "GB", "uk": "GB", "england": "GB", "britain": "GB",
        "australia": "AU", "aus": "AU",
        "vietnam": "VN", "viet nam": "VN",
    }

    country_code = None
    city = None
    state_region = None
    raw_location = ", ".join([str(l) for l in locations]) if locations else ""

    for loc in locations:
        loc_str = str(loc).strip()
        loc_lower = loc_str.lower()

        # Detect country
        for keyword, code in country_map.items():
            if keyword in loc_lower:
                country_code = code
                break

        # Try to extract city (first part before comma)
        if "," in loc_str:
            parts = [p.strip() for p in loc_str.split(",")]
            if parts[0] and len(parts[0]) > 1:
                city = parts[0]
            if len(parts) > 1:
                state_region = parts[1].strip()

    return {
        "country_code": country_code,
        "city": city,
        "state_region": state_region,
        "raw_location": raw_location,
    }


def bulk_upsert_companies(db: Client, companies: list[dict], run_id: str) -> dict[str, str]:
    """
    Bulk upsert companies. Returns name_normalized -> company_id map.
    Much faster than one-by-one: 1 bulk select + 1 bulk insert + few updates.
    """
    company_id_map: dict[str, str] = {}
    if not companies:
        return company_id_map

    names_norm = []
    for c in companies:
        n = normalize_name(c.get("company_name", ""))
        if n:
            names_norm.append(n)
    unique_names = list(dict.fromkeys(names_norm))
    if not unique_names:
        return company_id_map

    # Single bulk fetch of existing companies
    existing = db.table("companies").select(
        "id, name_normalized, sources, source_count, website, team_size, funding_amount, country_code, enrichment_evidence_url"
    ).in_("name_normalized", unique_names).execute()
    existing_map = {r["name_normalized"]: r for r in (existing.data or [])}

    # Separate new vs existing (dedupe by name_normalized — same company can appear from multiple sources)
    to_insert: list[dict] = []
    to_update_map: dict[str, dict] = {}  # company_id -> merged updates
    seen_new: set[str] = set()

    for company in companies:
        name_norm = normalize_name(company.get("company_name", ""))
        if not name_norm:
            continue
        loc = normalize_location(company.get("locations", []))
        source = company.get("source", "")

        if name_norm in existing_map:
            row = existing_map[name_norm]
            company_id_map[name_norm] = row["id"]
            updates = {}
            existing_sources = row.get("sources") or []
            if source and source not in existing_sources:
                updates["sources"] = existing_sources + [source]
                updates["source_count"] = len(updates["sources"])
            if not row.get("website") and company.get("website"):
                updates["website"] = company["website"]
            if not row.get("team_size") and company.get("team_size"):
                updates["team_size"] = company["team_size"]
            if not row.get("funding_amount") and company.get("funding_amount"):
                updates["funding_amount"] = company["funding_amount"]
            if not row.get("enrichment_evidence_url") and company.get("enrichment_evidence_url"):
                updates["enrichment_evidence_url"] = company["enrichment_evidence_url"]
            if not row.get("country_code") and loc.get("country_code"):
                updates.update(loc)
            if updates:
                cid = row["id"]
                if cid not in to_update_map:
                    to_update_map[cid] = {}
                cur = to_update_map[cid]
                # Merge sources (same company can appear from multiple sources in one batch)
                if "sources" in updates:
                    merged = list(dict.fromkeys((cur.get("sources") or row.get("sources") or []) + updates["sources"]))
                    cur["sources"] = merged
                    cur["source_count"] = len(merged)
                for k, v in updates.items():
                    if k not in ("sources", "source_count"):
                        cur[k] = v
        elif name_norm not in seen_new:
            seen_new.add(name_norm)
            to_insert.append({
                "name_normalized": name_norm,
                "name_display": company.get("company_name", "").strip(),
                "website": company.get("website", ""),
                "yc_batch": company.get("yc_batch", "") or None,
                "yc_url": company.get("yc_url", "") or None,
                "sources": [source] if source else [],
                "source_count": 1,
                "description": company.get("description", ""),
                "long_description": company.get("long_description", ""),
                "team_size": company.get("team_size") or None,
                "industries": company.get("industries", []),
                "tags": company.get("tags", []),
                "is_hiring": company.get("is_hiring", False),
                "funding_amount": company.get("funding_amount", "") or None,
                "enrichment_evidence_url": company.get("enrichment_evidence_url", "") or None,
                "first_seen_run": run_id,
                "profile_fetched_at": _now().isoformat(),
                "profile_expires_at": _expiry(TTL["profile"]).isoformat(),
                **loc,
            })

    # Batch insert new companies
    if to_insert:
        BATCH_SIZE = 100
        for i in range(0, len(to_insert), BATCH_SIZE):
            batch = to_insert[i : i + BATCH_SIZE]
            result = db.table("companies").insert(batch).execute()
            for row in result.data:
                company_id_map[row["name_normalized"]] = row["id"]

    # Updates (usually few; merged when same company from multiple sources)
    for cid, updates in to_update_map.items():
        db.table("companies").update(updates).eq("id", cid).execute()

    return company_id_map

def bulk_update_enrichment(db: Client, companies: list[dict]) -> int:
    """
    Writes back enriched data (URL, description, team_size, industries) to the companies table.
    Called AFTER the enricher step.
    Only updates fields that are currently empty/null in the DB — never overwrites existing data.
    Uses chunked UPDATE (not upsert) to avoid row-level lock deadlocks.
    Each chunk is wrapped with a thread timeout so a stalled DB call can never freeze the pipeline.
    Returns count of companies updated.
    """
    import time
    import concurrent.futures
    ids = [c["_company_id"] for c in companies if c.get("_company_id")]
    if not ids:
        return 0

    # Chunk the SELECT too — 200 UUIDs in one IN() is the safe maximum
    SELECT_CHUNK = 100
    db_state: dict = {}
    print(f"  [DB] Prefetching enrichment state for {len(ids)} companies ({SELECT_CHUNK}/chunk)...")
    for i in range(0, len(ids), SELECT_CHUNK):
        chunk_ids = ids[i:i + SELECT_CHUNK]
        t0 = time.time()
        chunk_num = i // SELECT_CHUNK + 1
        total_chunks = (len(ids) - 1) // SELECT_CHUNK + 1
        print(f"  [DB] SELECT chunk {chunk_num}/{total_chunks}...", flush=True)
        try:
            resp = db.table("companies").select(
                "id, website, description, team_size, industries, enrichment_evidence_url"
            ).in_("id", chunk_ids).execute()
            db_state.update({row["id"]: row for row in (resp.data or [])})
            print(f"  [DB] SELECT chunk done in {time.time()-t0:.1f}s ({len(resp.data or [])} rows)", flush=True)
        except Exception as e:
            print(f"  [DB] SELECT chunk error: {e}", flush=True)

    updates_to_write: list[dict] = []
    for company in companies:
        company_id = company.get("_company_id")
        if not company_id:
            continue

        enrichment_status = company.get("enrichment_status", "")
        # Skip companies where enrichment produced no useful data
        if enrichment_status in ("failed", "no_url", "", "no_valid_page"):
            continue

        current = db_state.get(company_id, {})
        fields: dict = {}  # only the fields to update — NOT including 'id'

        company_url = company.get("company_url", "")
        if company_url and not current.get("website"):
            fields["website"] = company_url

        description = company.get("description", "")
        if description and len(description) > 10 and not current.get("description"):
            fields["description"] = description[:2000]

        team_size = company.get("team_size")
        if team_size and isinstance(team_size, (int, float)) and team_size > 0 and not current.get("team_size"):
            fields["team_size"] = int(team_size)

        industries = company.get("industries", [])
        if industries and isinstance(industries, list) and len(industries) > 0 and not current.get("industries"):
            fields["industries"] = industries

        evidence_url = company.get("enrichment_evidence_url", "")
        if evidence_url and not current.get("enrichment_evidence_url"):
            fields["enrichment_evidence_url"] = evidence_url

        if fields:
            updates_to_write.append({"_id": company_id, **fields})

    if not updates_to_write:
        print(f"  [DB] Enrichment writeback: nothing new to write", flush=True)
        return 0

    print(f"  [DB] Writing enrichment updates for {len(updates_to_write)} companies (individual UPDATE calls)...", flush=True)
    # Use individual UPDATEs instead of batch UPSERT to avoid row-level lock deadlocks.
    # UPSERT with on_conflict='id' can deadlock when many rows exist — UPDATE by PK never does.
    CHUNK_TIMEOUT = 45  # seconds per individual UPDATE before we skip it
    updated = 0
    skipped_timeout = 0
    for i, row in enumerate(updates_to_write):
        cid = row["_id"]
        fields_only = {k: v for k, v in row.items() if k != "_id"}
        if (i + 1) % 25 == 0 or (i + 1) == len(updates_to_write):
            print(f"  [DB] UPDATE progress: {i+1}/{len(updates_to_write)}", flush=True)
        def _do_update(db=db, cid=cid, fields_only=fields_only):
            db.table("companies").update(fields_only).eq("id", cid).execute()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(_do_update)
                future.result(timeout=CHUNK_TIMEOUT)
            updated += 1
        except concurrent.futures.TimeoutError:
            skipped_timeout += 1
            print(f"  [DB] UPDATE timeout after {CHUNK_TIMEOUT}s for company {cid} — skipping", flush=True)
        except Exception as e:
            print(f"  [DB] UPDATE error for id={cid}: {e}", flush=True)

    if skipped_timeout:
        print(f"  [DB] {skipped_timeout} updates skipped due to timeout — pipeline continues", flush=True)
    return updated


def upsert_company(db: Client, company: dict, run_id: str) -> str:
    """
    Inserts a new company or updates an existing one.
    Returns the company UUID.
    Merges sources and enriches missing fields.
    """
    name_norm = normalize_name(company.get("company_name", ""))
    if not name_norm:
        return None

    loc = normalize_location(company.get("locations", []))

    # Check if company already exists
    existing = db.table("companies").select("id, sources, source_count, website, team_size, funding_amount, country_code, enrichment_evidence_url").eq("name_normalized", name_norm).execute()

    source = company.get("source", "")

    if existing.data:
        row = existing.data[0]
        company_id = row["id"]

        # Merge updates — only fill in missing fields
        updates = {}
        existing_sources = row.get("sources") or []
        if source and source not in existing_sources:
            updates["sources"] = existing_sources + [source]
            updates["source_count"] = len(updates["sources"])
        if not row.get("website") and company.get("website"):
            updates["website"] = company["website"]
        if not row.get("team_size") and company.get("team_size"):
            updates["team_size"] = company["team_size"]
        if not row.get("funding_amount") and company.get("funding_amount"):
            updates["funding_amount"] = company["funding_amount"]
        if not row.get("enrichment_evidence_url") and company.get("enrichment_evidence_url"):
            updates["enrichment_evidence_url"] = company["enrichment_evidence_url"]
        if not row.get("country_code") and loc.get("country_code"):
            updates.update(loc)

        if updates:
            db.table("companies").update(updates).eq("id", company_id).execute()

    else:
        # Insert new company
        insert_data = {
            "name_normalized": name_norm,
            "name_display": company.get("company_name", "").strip(),
            "website": company.get("website", ""),
            "yc_batch": company.get("yc_batch", "") or None,
            "yc_url": company.get("yc_url", "") or None,
            "sources": [source] if source else [],
            "source_count": 1,
            "description": company.get("description", ""),
            "long_description": company.get("long_description", ""),
            "team_size": company.get("team_size") or None,
            "industries": company.get("industries", []),
            "tags": company.get("tags", []),
            "is_hiring": company.get("is_hiring", False),
            "funding_amount": company.get("funding_amount", "") or None,
            "enrichment_evidence_url": company.get("enrichment_evidence_url", "") or None,
            "first_seen_run": run_id,
            "profile_fetched_at": _now().isoformat(),
            "profile_expires_at": _expiry(TTL["profile"]).isoformat(),
            **loc,
        }
        result = db.table("companies").insert(insert_data).execute()
        company_id = result.data[0]["id"]

    return company_id


# ─────────────────────────────────────────
# RAW SCRAPE EVENTS (append-only write)
# ─────────────────────────────────────────

def _raw_scrape_row(company: dict, run_id: str, country_code: str = None) -> dict:
    """Build a single raw_scrape_events row."""
    return {
        "batch_id": run_id,
        "source": company.get("source", ""),
        "country_code": country_code or normalize_location(company.get("locations", []))["country_code"],
        "company_name_raw": company.get("company_name"),
        "website_raw": company.get("website"),
        "description_raw": company.get("description"),
        "long_description_raw": company.get("long_description"),
        "team_size_raw": company.get("team_size"),
        "industries_raw": company.get("industries", []),
        "tags_raw": company.get("tags", []),
        "locations_raw": company.get("locations", []),
        "yc_batch_raw": company.get("yc_batch"),
        "yc_url_raw": company.get("yc_url"),
        "is_hiring_raw": company.get("is_hiring", False),
        "funding_amount_raw": company.get("funding_amount"),
        "article_title_raw": company.get("article_title"),
        "article_url_raw": company.get("article_url"),
        "hr_roles_found_raw": company.get("hr_roles_found", []),
        "days_ago_raw": company.get("days_ago"),
    }


def log_raw_scrape(db: Client, company: dict, run_id: str, country_code: str = None):
    """Saves raw scrape data exactly as received — no transformation."""
    db.table("raw_scrape_events").insert(_raw_scrape_row(company, run_id, country_code)).execute()


def log_raw_scrape_bulk(db: Client, companies: list[dict], run_id: str) -> None:
    """Bulk insert raw scrape events. Much faster than one-by-one."""
    BATCH_SIZE = 100
    rows = [_raw_scrape_row(c, run_id, c.get("country_code")) for c in companies]
    for i in range(0, len(rows), BATCH_SIZE):
        db.table("raw_scrape_events").insert(rows[i : i + BATCH_SIZE]).execute()


# ─────────────────────────────────────────
# TOKEN CHECKER
# ─────────────────────────────────────────

def get_token_status(db: Client, name_normalized: str) -> dict:
    """
    For a given normalized company name, returns its full token status.
    Used by the pipeline to decide what work is needed.
    """
    result = db.table("token_status").select("*").eq("company_id",
        db.table("companies").select("id").eq("name_normalized", name_normalized).execute().data[0]["id"]
        if db.table("companies").select("id").eq("name_normalized", name_normalized).execute().data
        else None
    ).execute()

    if not result.data:
        return {
            "exists": False,
            "company_id": None,
            "profile_needs_refresh": True,
            "job_needs_refresh": True,
            "ai_needs_refresh": True,
            "last_verdict": None,
            "last_score": None,
        }

    row = result.data[0]
    return {
        "exists": True,
        "company_id": row["company_id"],
        "profile_needs_refresh": row["profile_needs_refresh"],
        "job_needs_refresh": row["job_needs_refresh"],
        "ai_needs_refresh": row["ai_needs_refresh"],
        "last_verdict": row["last_verdict"],
        "last_score": row["last_score"],
    }


def bulk_token_lookup(db: Client, name_list: list[str]) -> dict:
    """
    Efficient bulk lookup — filtered query for only the companies in this batch.
    Returns dict: name_normalized -> token_status dict.
    Used at pipeline start to build the full work plan.
    """
    if not name_list:
        return {}

    # Fetch only the rows we care about (avoids a full-table scan as DB grows)
    result = db.table("token_status").select("*").in_("name_normalized", name_list).execute()

    # Build lookup map keyed by name_normalized
    existing = {row["name_normalized"]: row for row in result.data} if result.data else {}

    plan = {}
    for name in name_list:
        norm = normalize_name(name)
        if norm in existing:
            row = existing[norm]
            plan[norm] = {
                "exists": True,
                "company_id": row["company_id"],
                "profile_needs_refresh": row["profile_needs_refresh"],
                "job_needs_refresh": row["job_needs_refresh"],
                "ai_needs_refresh": row["ai_needs_refresh"],
                "last_verdict": row["last_verdict"],
                "last_score": row["last_score"],
            }
        else:
            plan[norm] = {
                "exists": False,
                "company_id": None,
                "profile_needs_refresh": True,
                "job_needs_refresh": True,
                "ai_needs_refresh": True,
                "last_verdict": None,
                "last_score": None,
            }
    return plan


# ─────────────────────────────────────────
# JOB CACHE
# ─────────────────────────────────────────

def log_job_check(db: Client, company_id: str, run_id: str, result: dict, country_code: str = None):
    """Stores a job check result. TTL: 7 days."""
    # The job checker uses 'source' key (e.g. "Greenhouse", "Lever"), not 'method'
    check_method = result.get("source", result.get("method", "unknown"))

    # Ensure ats_board_url is never null when we have it
    ats_board_url = result.get("ats_board_url") or result.get("careers_page_url") or result.get("google_search_url") or ""

    # job_urls is a list of {title, url} dicts
    job_urls = result.get("job_urls", [])

    db.table("raw_job_events").insert({
        "batch_id": run_id,
        "company_id": company_id,
        "check_method": check_method,
        "country_code": country_code,
        "job_count": result.get("total_jobs", 0),
        "job_titles_raw": result.get("all_titles", []),
        "hr_role_found": result.get("hr_role_found", False) or len(result.get("hr_roles", [])) > 0,
        "hr_tech_role_found": result.get("hr_tech_role_found", False),
        "hr_role_evidence": result.get("hr_role_evidence", ""),
        "raw_response_url": result.get("url", ""),
        "ats_board_url": ats_board_url if ats_board_url else None,
        "job_urls": job_urls if job_urls else None,
        "http_status": result.get("http_status"),
        "expires_at": _expiry(TTL["job"]).isoformat(),
    }).execute()


# ─────────────────────────────────────────
# SIGNAL CACHE
# ─────────────────────────────────────────

def log_signal(db: Client, company_id: str, run_id: str, signal_num: int,
               signal_name: str, passed: bool, score: int, max_score: int,
               evidence: str, raw_llm: dict = None, ttl_days: int = 7):
    """Stores a single signal check result."""
    db.table("raw_signal_events").insert({
        "batch_id": run_id,
        "company_id": company_id,
        "signal_number": signal_num,
        "signal_name": signal_name,
        "passed": passed,
        "score_awarded": score,
        "max_score": max_score,
        "evidence": evidence,
        "raw_llm_response": raw_llm,
        "expires_at": _expiry(ttl_days).isoformat(),
    }).execute()


# ─────────────────────────────────────────
# AI RESULTS
# ─────────────────────────────────────────

def log_ai_result(db: Client, company_id: str, run_id: str, result: dict):
    """Stores the full AI Brain evaluation result."""
    signals = result.get("signal_results", [])
    signal_map = {s["signal_id"]: s for s in signals}

    def sp(sid): return signal_map.get(sid, {}).get("passed", False)
    def se(sid): return signal_map.get(sid, {}).get("evidence", "")

    db.table("raw_ai_results").insert({
        "batch_id": run_id,
        "company_id": company_id,
        "final_score": result.get("final_score", 0),
        "max_score": result.get("max_score", 18),
        "verdict": result.get("verdict", "COLD"),
        "verdict_reason": result.get("verdict_reason", ""),
        "recommended_plan": result.get("recommended_plan", ""),
        "why_they_fit": result.get("why_they_fit", ""),
        "outreach_opener": result.get("outreach_opener", ""),
        "signal_1_passed": sp("active_hiring"),   "signal_1_evidence": se("active_hiring"),
        "signal_2_passed": sp("hr_ta_role"),      "signal_2_evidence": se("hr_ta_role"),
        "signal_3_passed": sp("company_size"),    "signal_3_evidence": se("company_size"),
        "signal_4_passed": sp("funded"),          "signal_4_evidence": se("funded"),
        "signal_5_passed": sp("jobs_open_long"),  "signal_5_evidence": se("jobs_open_long"),
        "signal_6_passed": sp("hr_tech_buyer"),   "signal_6_evidence": se("hr_tech_buyer"),
        "signal_7_passed": sp("icp_fit"),         "signal_7_evidence": se("icp_fit"),
        "signal_8_passed": sp("ats_confirmed"),   "signal_8_evidence": se("ats_confirmed"),
        "data_confidence": result.get("data_confidence", "LOW"),
        "llm_model_used": "gpt-4o-mini",
        "llm_tokens_used": result.get("llm_tokens_used"),
        "llm_cost_usd": result.get("llm_cost_usd", 0),
        "expires_at": _expiry(TTL["ai"]).isoformat(),
    }).execute()


# ─────────────────────────────────────────
# READ HELPERS
# ─────────────────────────────────────────

def get_active_leads(db: Client, country_code: str = None) -> list[dict]:
    """Returns current HOT/WARM leads from the active_leads view."""
    query = db.table("active_leads").select("*")
    if country_code:
        query = query.eq("country_code", country_code)
    return query.execute().data or []


def bulk_fetch_cached_ai_results(db: Client, company_ids: list[str]) -> dict:
    """
    Fetches the most recent raw_ai_results row for each company_id.
    Returns dict: company_id -> reconstructed AI result fields.
    Used to hydrate cached companies (fresh AI token) so their signal
    details, plan, why_they_fit, and outreach_opener show correctly
    in the terminal summary and CSV output.
    """
    if not company_ids:
        return {}

    # Fetch all recent AI results for these companies (still within TTL)
    result = db.table("raw_ai_results").select(
        "company_id, final_score, max_score, verdict, verdict_reason, "
        "recommended_plan, why_they_fit, outreach_opener, data_confidence, "
        "signal_1_passed, signal_1_evidence, "
        "signal_2_passed, signal_2_evidence, "
        "signal_3_passed, signal_3_evidence, "
        "signal_4_passed, signal_4_evidence, "
        "signal_5_passed, signal_5_evidence, "
        "signal_6_passed, signal_6_evidence, "
        "signal_7_passed, signal_7_evidence, "
        "signal_8_passed, signal_8_evidence, "
        "expires_at"
    ).in_("company_id", company_ids).order("expires_at", desc=True).execute()

    if not result.data:
        return {}

    # Keep only the most recent row per company_id
    seen = set()
    rows_by_company = {}
    for row in result.data:
        cid = row["company_id"]
        if cid not in seen:
            seen.add(cid)
            rows_by_company[cid] = row

    # Fetch ats_board_url from most recent job check for these companies
    job_resp = db.table("raw_job_events").select(
        "company_id, ats_board_url"
    ).in_("company_id", company_ids).order("expires_at", desc=True).execute()
    job_ats_map: dict[str, str] = {}
    for jrow in (job_resp.data or []):
        cid = jrow["company_id"]
        if cid not in job_ats_map and jrow.get("ats_board_url"):
            job_ats_map[cid] = jrow["ats_board_url"]

    _SIGNAL_DEFS = [
        ("active_hiring",  "active_hiring",  "Actively Hiring (job volume)",    "signal_1"),
        ("hr_ta_role",     "hr_ta_role",     "Hiring HR or TA Role",            "signal_2"),
        ("company_size",   "company_size",   "Company Size 20-200",             "signal_3"),
        ("funded",         "funded",         "Recently Funded",                 "signal_4"),
        ("jobs_open_long", "jobs_open_long", "Jobs Open 30+ Days",              "signal_5"),
        ("hr_tech_buyer",  "hr_tech_buyer",  "Hiring HR Technology Role",       "signal_6"),
        ("icp_fit",        "icp_fit",        "ICP Industry and Profile Fit",    "signal_7"),
        ("ats_confirmed",  "ats_confirmed",  "ATS Platform Confirmed",          "signal_8"),
    ]

    hydrated = {}
    for cid, row in rows_by_company.items():
        # Reconstruct signal_results list from flat DB columns
        signal_results = []
        for signal_id, _, signal_name, col_prefix in _SIGNAL_DEFS:
            passed = row.get(f"{col_prefix}_passed", False)
            evidence = row.get(f"{col_prefix}_evidence", "")
            signal_results.append({
                "signal_id": signal_id,
                "passed": passed,
                "evidence": evidence or ("PASS" if passed else "FAIL"),
                "detail": "",
                "proof_url": "",
            })

        # Re-apply current scoring thresholds to override stale cached verdicts
        # Signal weights: active_hiring up to 3pts (graduated); others binary
        _WEIGHTS = {
            "active_hiring": 3, "hr_ta_role": 3, "company_size": 1,
            "funded": 2, "jobs_open_long": 2, "hr_tech_buyer": 3,
            "icp_fit": 2, "ats_confirmed": 2,
        }
        # Use stored final_score (it was calculated correctly when first evaluated)
        stored_score = row.get("final_score", 0) or 0
        if stored_score >= 10:
            recomputed_verdict = "HOT"
        elif stored_score >= 6:
            recomputed_verdict = "WARM"
        else:
            recomputed_verdict = "COLD"

        hydrated[cid] = {
            "final_score": stored_score,
            "max_score": row.get("max_score", 18),
            "verdict": recomputed_verdict,          # Use threshold-consistent verdict
            "verdict_reason": row.get("verdict_reason", ""),
            "recommended_plan": row.get("recommended_plan", ""),
            "why_they_fit": row.get("why_they_fit", ""),
            "outreach_opener": row.get("outreach_opener", ""),
            "data_confidence": row.get("data_confidence", "LOW"),
            "signal_results": signal_results,
            # Populate proof_urls with ats_board_url from most recent job check
            # company_url and yc_url are filled in by main.py after hydration
            "proof_urls": {
                "ats_board_url": job_ats_map.get(cid, ""),
            },
        }

    return hydrated


def get_pipeline_run_stats(db: Client, run_id: str) -> dict:
    """Returns stats for a specific pipeline run."""
    result = db.table("pipeline_runs").select("*").eq("id", run_id).execute()
    return result.data[0] if result.data else {}
