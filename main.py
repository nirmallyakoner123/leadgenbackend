from config import RunConfig
from scraper import run_all_sources
from filter import deduplicate, apply_basic_filters, apply_job_check_gate, filter_ph_junk
from job_checker import run_job_checks
from enricher import enrich_companies
from ai_brain import run_ai_brain
from output_writer import save_raw_json, save_leads_csv, save_ai_results, print_ai_summary
from database import (
    get_client, start_pipeline_run, finish_pipeline_run,
    log_raw_scrape_bulk, bulk_upsert_companies, bulk_update_enrichment,
    bulk_token_lookup, log_job_check, log_ai_result, normalize_name,
    bulk_fetch_cached_ai_results,
)
import time


def main(config: RunConfig = None):
    """
    Full pipeline with Supabase DB integration and token-aware execution.
    config: RunConfig object. If None, defaults to US-only V2 run.
    """
    if config is None:
        config = RunConfig(geographies=["US"])

    print("=" * 70)
    print("  InterviewScreener -- Lead Generation Engine V3")
    print(f"  Geographies: {', '.join(config.geographies)}")
    print(f"  Sources: YC | Product Hunt | TechCrunch | Google News | LinkedIn | Seek | Reed")
    print(f"  Signals: 8 (incl. ATS Confirmed — G2 replaced)")
    print("=" * 70)

    # ─────────────────────────────────────────
    # STEP 0: Connect to Supabase + Start run
    # ─────────────────────────────────────────
    print("\n[STEP 0] Connecting to Supabase...")
    db = get_client()
    run_id = start_pipeline_run(
        db,
        geographies=config.geographies,
        sources=["YC", "ProductHunt", "TechCrunch", "GoogleNews", "LinkedIn", "Seek", "Reed"],
    )

    run_stats = {
        "companies_scraped": 0,
        "companies_filtered": 0,
        "companies_job_checked": 0,
        "companies_ai_evaluated": 0,
        "hot_count": 0,
        "warm_count": 0,
        "cold_count": 0,
        "total_llm_cost_usd": 0,
        "pipeline_version": config.pipeline_version,
    }

    try:
        # ─────────────────────────────────────────
        # STEP 1: Scrape all sources
        # ─────────────────────────────────────────
        print("\n[STEP 1] Running scrapers...")
        raw_companies = run_all_sources(config)

        if not raw_companies:
            print("No data collected.")
            return  # finally block will call finish_pipeline_run

        run_stats["companies_scraped"] = len(raw_companies)

        # ─────────────────────────────────────────
        # STEP 1.5: Filter PH junk (product names, not real companies)
        # ─────────────────────────────────────────
        raw_companies, ph_junk_dropped = filter_ph_junk(raw_companies)
        if ph_junk_dropped > 0:
            print(f"  [PH FILTER] Dropped {ph_junk_dropped} Product Hunt junk entries (tools, wrappers, extensions)")
            print(f"  [PH FILTER] {len(raw_companies)} entries remaining")

        # Save raw JSON for debugging (legacy)
        save_raw_json(raw_companies)

        # ─────────────────────────────────────────
        # STEP 2: Write ALL raw scrape events to DB (batched — much faster)
        # No filtering here — everything is preserved
        # ─────────────────────────────────────────
        print(f"\n[STEP 2] Writing {len(raw_companies)} raw scrape events to Supabase...")
        try:
            log_raw_scrape_bulk(db, raw_companies, run_id)
            company_id_map = bulk_upsert_companies(db, raw_companies, run_id)
            print(f"  [DB] {len(company_id_map)} unique companies upserted to Supabase")
        except Exception as e:
            print(f"  [DB] Error: {e}")
            raise

        # ─────────────────────────────────────────
        # STEP 3: Dedup + merge in memory
        # ─────────────────────────────────────────
        print("\n[STEP 3] Deduplicating...")
        companies = deduplicate(raw_companies)
        print(f"  Unique companies: {len(companies)}")

        # ─────────────────────────────────────────
        # STEP 4: Token check — what needs work?
        # Single bulk query to Supabase
        # ─────────────────────────────────────────
        print("\n[STEP 4] Checking token status (freshness)...")
        name_list = list(companies.keys())
        token_map = bulk_token_lookup(db, name_list)

        fresh_count = sum(1 for t in token_map.values() if not t["ai_needs_refresh"] and t["exists"])
        stale_count = len(token_map) - fresh_count
        print(f"  Fresh (skip AI Brain): {fresh_count}")
        print(f"  Stale (needs work):    {stale_count}")

        # Attach company_ids to company dicts for downstream use
        for name_norm, data in companies.items():
            token = token_map.get(name_norm, {})
            data["_company_id"] = company_id_map.get(name_norm) or token.get("company_id")
            data["_token"] = token

        # ─────────────────────────────────────────
        # STEP 5: Basic ICP filter (no AI)
        # ─────────────────────────────────────────
        print("\n[STEP 5] Basic ICP filter (size + industry + geography)...")
        pre_qualified = apply_basic_filters(companies)
        run_stats["companies_filtered"] = len(pre_qualified)

        if not pre_qualified:
            print("No companies passed basic filters.")
            return  # finally block will call finish_pipeline_run

        # ─────────────────────────────────────────
        # STEP 6: Job postings check — token-aware
        # Skips companies with fresh job data
        # ─────────────────────────────────────────
        # ─────────────────────────────────────────
        # STEP 6.5: Enrich company context
        # Fetch homepage data so AI has real context
        # ─────────────────────────────────────────
        print(f"\n[STEP 6.5] Enriching company context (homepage fetch)...", flush=True)
        pre_qualified = enrich_companies(pre_qualified, concurrency=config.enricher_concurrency)

        # Write enriched data back to Supabase (URLs, description, team_size, industries)
        print(f"\n  [DB] Enrichment writeback starting for {len(pre_qualified)} companies...", flush=True)
        enriched_count = bulk_update_enrichment(db, pre_qualified)
        print(f"  [DB] Enrichment data written back for {enriched_count} companies", flush=True)

        # ─────────────────────────────────────────
        # STEP 5.5: Smart pre-filter before job check
        # Skip ghost companies, oversized, empty shells
        # ─────────────────────────────────────────
        worth_checking, gate_skipped = apply_job_check_gate(pre_qualified)

        # ─────────────────────────────────────────
        # STEP 6: Job postings check — token-aware
        # ─────────────────────────────────────────
        print(f"\n[STEP 6] Checking active job postings (token-aware)...", flush=True)

        needs_job_check = []
        skip_job_count = 0
        # skip_job_check_if_fresh_days == 0 means force-refresh all job tokens this run
        force_job_refresh = config.skip_job_check_if_fresh_days == 0

        for company in worth_checking:
            token = company.get("_token", {})
            if not force_job_refresh and token.get("exists") and not token.get("job_needs_refresh"):
                skip_job_count += 1
                company["_skip_reason"] = "job_token_fresh"
            else:
                needs_job_check.append(company)

        print(f"  Skipped (fresh token): {skip_job_count}")
        print(f"  Checking job postings: {len(needs_job_check)}")

        job_verified_new = run_job_checks(needs_job_check, min_jobs=1, concurrency=config.job_checker_concurrency)

        # Log job check results to Supabase
        for company in job_verified_new:
            company_id = company.get("_company_id")
            if company_id and company.get("job_check"):
                try:
                    log_job_check(
                        db, company_id, run_id,
                        company["job_check"],
                        country_code=company.get("country_code")
                    )
                except Exception as e:
                    print(f"  [DB] Error logging job check for {company.get('company_name')}: {e}")

        # Combine fresh-token companies + newly checked + gate-skipped (for AI to evaluate)
        skip_verified = [c for c in worth_checking if c.get("_skip_reason") == "job_token_fresh"]
        job_verified = job_verified_new + skip_verified

        run_stats["companies_job_checked"] = len(job_verified)
        save_leads_csv(job_verified)
        print(f"\n  {len(job_verified)} companies ready for AI Brain")

        # ─────────────────────────────────────────
        # STEP 7: AI Brain — token-aware
        # Skips companies with fresh AI verdicts
        # ─────────────────────────────────────────
        print("\n[STEP 7] Running AI Brain (token-aware)...", flush=True)

        needs_ai = []
        skip_ai_count = 0
        # skip_ai_if_fresh_days == 0 means force-refresh all AI tokens this run
        force_ai_refresh = config.skip_ai_if_fresh_days == 0

        for company in job_verified:
            token = company.get("_token", {})
            if not force_ai_refresh and token.get("exists") and not token.get("ai_needs_refresh"):
                skip_ai_count += 1
                company["verdict"] = token["last_verdict"]
                company["final_score"] = token["last_score"]
                company["_used_cache"] = True
            else:
                needs_ai.append(company)

        # Respect per-run AI cap
        if len(needs_ai) > config.max_companies_for_ai:
            print(f"  Capping AI to top {config.max_companies_for_ai} companies by pre-score")
            needs_ai = sorted(needs_ai, key=lambda x: x.get("pre_score", 0), reverse=True)
            needs_ai = needs_ai[:config.max_companies_for_ai]

        print(f"  Skipped (AI token fresh): {skip_ai_count}")
        print(f"  Running AI Brain:         {len(needs_ai)}")

        ai_results_new = run_ai_brain(needs_ai)

        # Log AI results to Supabase
        total_cost = 0
        for result in ai_results_new:
            company_id = result.get("_company_id")
            if company_id:
                try:
                    log_ai_result(db, company_id, run_id, result)
                    total_cost += result.get("llm_cost_usd", 0)
                except Exception as e:
                    print(f"  [DB] Error logging AI result for {result.get('company_name')}: {e}")

        run_stats["total_llm_cost_usd"] = total_cost

        # Combine fresh cached + newly evaluated
        cached_results = [c for c in job_verified if c.get("_used_cache")]

        # ── HYDRATE cached companies with full AI output from DB ──────────
        # Without this step, cached companies show all ✗ signals, empty Plan,
        # missing WHY/OPENER, and have stale verdicts from old scoring thresholds.
        if cached_results:
            cached_ids = [
                c.get("_company_id") for c in cached_results if c.get("_company_id")
            ]
            cached_ai_data = bulk_fetch_cached_ai_results(db, cached_ids)
            if cached_ai_data:
                hydrated_count = 0
                for company in cached_results:
                    cid = company.get("_company_id")
                    if cid and cid in cached_ai_data:
                        ai_cache = cached_ai_data[cid]
                        company["signal_results"]   = ai_cache["signal_results"]
                        company["final_score"]       = ai_cache["final_score"]
                        company["max_score"]         = ai_cache["max_score"]
                        company["verdict"]           = ai_cache["verdict"]   # Threshold-consistent
                        company["verdict_reason"]    = ai_cache["verdict_reason"]
                        company["recommended_plan"]  = ai_cache["recommended_plan"]
                        company["why_they_fit"]      = ai_cache["why_they_fit"]
                        company["outreach_opener"]   = ai_cache["outreach_opener"]
                        company["data_confidence"]   = ai_cache["data_confidence"]
                        # Supplement proof_urls: DB cache first, then fill gaps from company dict
                        proof = dict(ai_cache.get("proof_urls") or {})
                        if not proof.get("company_url"):
                            proof["company_url"] = company.get("company_url") or company.get("website", "")
                        if not proof.get("yc_url"):
                            proof["yc_url"] = company.get("yc_url", "")
                        if not proof.get("ats_board_url") and company.get("job_check"):
                            proof["ats_board_url"] = company["job_check"].get("ats_board_url", "")
                        company["proof_urls"] = proof
                        hydrated_count += 1
                print(f"  [CACHE] Re-hydrated {hydrated_count}/{len(cached_results)} cached companies from DB")
            else:
                print(f"  [CACHE] No DB records found for {len(cached_results)} cached companies — signals will be empty")

        ai_results = ai_results_new + cached_results

        # ─────────────────────────────────────────
        # STEP 8: Save + display results
        # ─────────────────────────────────────────
        # Apply min_score_for_output threshold — only show/write leads above the bar
        output_results = [
            r for r in ai_results
            if r.get("final_score", 0) >= config.min_score_for_output
        ]
        dropped_low = len(ai_results) - len(output_results)
        print(f"\n[STEP 8] Saving results (score >= {config.min_score_for_output})...")
        if dropped_low:
            print(f"  [OUTPUT] {dropped_low} companies below min_score_for_output={config.min_score_for_output} excluded from output")
        save_ai_results(output_results)
        print_ai_summary(output_results)

        # Stats count all evaluated companies, not just the filtered output
        hot = [r for r in ai_results if r.get("verdict") == "HOT"]
        warm = [r for r in ai_results if r.get("verdict") == "WARM"]
        cold = [r for r in ai_results if r.get("verdict") == "COLD"]

        run_stats["companies_ai_evaluated"] = len(ai_results)
        run_stats["hot_count"] = len(hot)
        run_stats["warm_count"] = len(warm)
        run_stats["cold_count"] = len(cold)

    except Exception as e:
        print(f"\n[PIPELINE] Fatal error: {e}")
        raise

    finally:
        # Always finish the run record — even on error
        finish_pipeline_run(db, run_id, run_stats)

    print(f"\nDone.")
    print(f"  Pre-filter CSV   -> output/verified_leads.csv")
    print(f"  AI results CSV   -> output/ai_verified_leads.csv")
    print(f"  AI results JSON  -> output/ai_verified_leads.json")
    print(f"  Supabase DB      -> (see SUPABASE_URL in .env)")
    print(f"  Total AI cost    -> ~${run_stats['total_llm_cost_usd']:.4f}")


if __name__ == "__main__":
    # US run — targets US-based companies via LinkedIn US,
    # Google News US queries, and YC US region.
    config = RunConfig(
        geographies=["US"],          # Options: "US", "GB", "AU", "VN", "IN"
        use_yc=True,
        use_techcrunch=True,
        use_google_news=True,
        use_linkedin=True,
        use_seek=False,              # AU only
        use_reed=False,              # GB only
        use_naukri=False,            # IN only
        max_companies_for_ai=200,
        pipeline_version="v3",
    )
    main(config)
