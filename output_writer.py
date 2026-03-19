import json
import csv
import os
from datetime import datetime
from config import OUTPUT_JSON, OUTPUT_CSV

OUTPUT_AI_CSV  = "output/ai_verified_leads.csv"
OUTPUT_AI_JSON = "output/ai_verified_leads.json"


def save_raw_json(raw_companies: list[dict], path: str = OUTPUT_JSON):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(raw_companies, f, indent=2, ensure_ascii=False)
    print(f"Raw data saved -> {path} ({len(raw_companies)} entries)")


def save_leads_csv(companies: list[dict], path: str = OUTPUT_CSV):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not companies:
        print("No companies to save.")
        return

    fieldnames = [
        "rank", "company_name", "website", "company_url", "pre_score", "team_size",
        "locations", "industries", "yc_batch", "is_actively_hiring",
        "has_funding_signal", "funding_amount", "has_hr_pain_keywords",
        "found_in_sources", "source_count", "description", "yc_url", "article_url",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rank, company in enumerate(companies, start=1):
            signals = company.get("signals", {})
            writer.writerow({
                "rank": rank,
                "company_name": company.get("company_name", ""),
                "website": company.get("website", ""),
                "company_url": company.get("company_url", company.get("website", "")),
                "pre_score": company.get("pre_score", 0),
                "team_size": company.get("team_size", "Unknown"),
                "locations": " | ".join(company.get("locations", [])),
                "industries": " | ".join(company.get("industries", [])),
                "yc_batch": signals.get("yc_batch", ""),
                "is_actively_hiring": "YES" if signals.get("is_actively_hiring") else "NO",
                "has_funding_signal": "YES" if signals.get("has_funding_signal") else "NO",
                "funding_amount": signals.get("funding_amount", ""),
                "has_hr_pain_keywords": "YES" if signals.get("has_hr_pain_keywords") else "NO",
                "found_in_sources": " | ".join(signals.get("found_in_sources", [])),
                "source_count": signals.get("source_count", 1),
                "description": company.get("description", "")[:200],
                "yc_url": company.get("yc_url", ""),
                "article_url": company.get("article_url", ""),
            })

    print(f"Leads saved -> {path} ({len(companies)} companies)")


def _format_job_urls(job_urls: list[dict], max_count: int = 3) -> str:
    """Formats job URLs as 'Title :: URL' pipe-separated string."""
    if not job_urls:
        return ""
    parts = [f"{j.get('title', 'Job')} :: {j.get('url', '')}" for j in job_urls[:max_count]]
    return " | ".join(parts)


def save_ai_results(companies: list[dict]):
    """Saves the full AI Brain output — one CSV and one JSON."""
    os.makedirs("output", exist_ok=True)

    # Save full JSON for debugging
    with open(OUTPUT_AI_JSON, "w", encoding="utf-8") as f:
        json.dump(companies, f, indent=2, ensure_ascii=False, default=str)
    print(f"AI results JSON -> {OUTPUT_AI_JSON}")

    # Save clean CSV for the team
    fieldnames = [
        "rank", "verdict", "data_confidence", "final_score", "max_score",
        "company_name", "website", "company_url", "team_size", "locations",
        "yc_batch", "recommended_plan",
        "s1_active_hiring", "s2_hr_ta_role", "s3_company_size",
        "s4_funded", "s5_jobs_open_long", "s6_hr_tech_buyer", "s7_icp_fit", "s8_ats_confirmed",
        "why_they_fit", "outreach_opener", "verdict_reason",
        "ats_board_url", "top_job_urls",
        "description", "yc_url",
    ]

    with open(OUTPUT_AI_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for rank, company in enumerate(companies, start=1):
            sr = {s["signal_id"]: s for s in company.get("signal_results", [])}

            def sig(sid):
                s = sr.get(sid, {})
                status = "PASS" if s.get("passed") else "FAIL"
                evidence = s.get("evidence", "")
                return f"{status} | {evidence}"

            proof = company.get("proof_urls", {})
            company_url = (
                proof.get("company_url")
                or company.get("company_url")
                or company.get("website", "")
            )
            board_url = (
                proof.get("ats_board_url")
                or company.get("ats_board_url", "")
            )

            writer.writerow({
                "rank": rank,
                "verdict": company.get("verdict", ""),
                "data_confidence": company.get("data_confidence", "LOW"),
                "final_score": company.get("final_score", 0),
                "max_score": company.get("max_score", 18),
                "company_name": company.get("company_name", ""),
                "website": company.get("website", ""),
                "company_url": company_url,
                "team_size": company.get("team_size", ""),
                "locations": " | ".join(company.get("locations", [])),
                "yc_batch": company.get("yc_batch", ""),
                "recommended_plan": company.get("recommended_plan", ""),
                "s1_active_hiring":  sig("active_hiring"),
                "s2_hr_ta_role":     sig("hr_ta_role"),
                "s3_company_size":   sig("company_size"),
                "s4_funded":         sig("funded"),
                "s5_jobs_open_long": sig("jobs_open_long"),
                "s6_hr_tech_buyer":  sig("hr_tech_buyer"),
                "s7_icp_fit":        sig("icp_fit"),
                "s8_ats_confirmed":  sig("ats_confirmed"),
                "why_they_fit":     company.get("why_they_fit", ""),
                "outreach_opener":  company.get("outreach_opener", ""),
                "verdict_reason":   company.get("verdict_reason", ""),
                "ats_board_url":    board_url,
                "top_job_urls":     _format_job_urls(company.get("job_urls", [])),
                "description":      company.get("description", "")[:200],
                "yc_url":           company.get("yc_url", ""),
            })

    print(f"AI leads CSV  -> {OUTPUT_AI_CSV} ({len(companies)} companies)")


def print_ai_summary(companies: list[dict]):
    """Prints the final AI Brain results with PROOF LINKS block per company."""
    hot  = [c for c in companies if c.get("verdict") == "HOT"]
    warm = [c for c in companies if c.get("verdict") == "WARM"]

    print("\n" + "=" * 70)
    print(f"  AI BRAIN RESULTS — InterviewScreener.com")
    print(f"  Run: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  HOT: {len(hot)} | WARM: {len(warm)} | Total: {len(companies)}")
    print("=" * 70)

    for rank, company in enumerate(companies, start=1):
        verdict = company.get("verdict", "COLD")
        score   = company.get("final_score", 0)
        max_s   = company.get("max_score", 18)
        name    = company.get("company_name", "N/A")
        plan    = company.get("recommended_plan", "")
        why     = company.get("why_they_fit", "")
        opener  = company.get("outreach_opener", "")
        conf    = company.get("data_confidence", "LOW")

        verdict_icon = "🔥 HOT" if verdict == "HOT" else ("☀ WARM" if verdict == "WARM" else "❄ COLD")

        print(f"\n  #{rank} [{verdict_icon}]  {name}")
        print(f"       Score: {score}/{max_s}  |  Plan: {plan}  |  Confidence: {conf}")

        sr = {s["signal_id"]: s for s in company.get("signal_results", [])}
        for sig_def in [
            ("active_hiring",  "Hiring 3+ roles "),
            ("hr_ta_role",     "HR/TA role open  "),
            ("company_size",   "Size 20-200      "),
            ("funded",         "Recently funded  "),
            ("jobs_open_long", "Jobs open 30d+   "),
            ("hr_tech_buyer",  "HR tech buyer    "),
            ("icp_fit",        "ICP fit (AI)     "),
            ("ats_confirmed",  "ATS confirmed    "),
        ]:
            sid, label = sig_def
            s = sr.get(sid, {})
            icon = "✓" if s.get("passed") else "✗"
            print(f"       {icon}  {label}  {s.get('evidence', '')[:70]}")

        if why:
            print(f"\n       WHY THEY FIT:")
            print(f"       {why}")
        if opener:
            print(f"\n       OUTREACH OPENER:")
            print(f"       {opener}")

        # ── PROOF LINKS BLOCK ──────────────────────────────────────
        proof = company.get("proof_urls", {})
        company_url = proof.get("company_url") or company.get("company_url") or company.get("website", "")
        board_url   = proof.get("ats_board_url") or company.get("ats_board_url", "")
        yc_url      = proof.get("yc_url") or company.get("yc_url", "")
        job_urls    = company.get("job_urls", [])

        has_any_proof = any([company_url, board_url, yc_url, job_urls])
        if has_any_proof:
            print(f"\n       PROOF LINKS:")
            if company_url:
                print(f"       ├─ Company:   {company_url}")
            if board_url:
                print(f"       ├─ Job Board: {board_url}")
            if yc_url:
                print(f"       ├─ YC:        {yc_url}")
            for idx, j in enumerate(job_urls[:3]):
                prefix = "└─" if idx == len(job_urls[:3]) - 1 and not (idx < 2) else "├─"
                print(f"       {prefix} Job #{idx+1}:   {j.get('url', '')}  [{j.get('title', '')}]")

    print("\n" + "=" * 70)
