import os
import json
import time
import random
from openai import OpenAI
from dotenv import load_dotenv
from job_checker import check_greenhouse, check_lever, check_careers_page, get_slug_candidates

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ─────────────────────────────────────────────────────────────
# ICP CONTEXT — fed into every AI prompt
# ─────────────────────────────────────────────────────────────

ICP_CONTEXT = """
PRODUCT: InterviewScreener.com
WHAT IT DOES: AI-powered candidate screening platform. Recruiters create a job,
AI generates screening questions from the JD, candidates attend async AI interviews,
recruiters get ranked analysis. Reduces screening time by 80%.

PRICING:
- Starter: $49.99/month (~300 candidates)
- Pro: $99.99/month (~1000 candidates) — most popular
- Scale: $299.99/month (~2200 candidates)
- Enterprise: $999.99/month (~5500 candidates)

IDEAL CUSTOMER:
- Company size: 20 to 200 employees
- Has a small HR/TA team (1-3 people) managing multiple open roles
- Hiring 3+ roles simultaneously
- Industries: SaaS, Tech, Staffing, E-commerce, Digital Agencies, Edtech, Healthtech
- Geography: USA, Australia, UK, Vietnam
- Pain: Recruiter spends most of their week on first-round screening calls
- Budget: $50-$300/month is a non-event decision

BUYER PERSONA:
- Primary: HR Manager, Talent Acquisition Manager, Head of People, Recruiter
- Secondary: Founder/COO at companies under 50 employees

TRIGGER EVENTS (signals they need this NOW):
- Hiring 5+ roles simultaneously
- Just raised funding (hiring surge coming)
- Hiring a Recruiter or TA role (current capacity breaking)
- Hiring an HR Technology role (actively buying HR tech)
- Jobs sitting open for 30+ days (screening bottleneck)
"""

# ─────────────────────────────────────────────────────────────
# SIGNAL DEFINITIONS
# G2 removed — was always returning 429 rate-limit errors
# ATS confirmed = replaces G2 as the "HR tech buyer" proof signal
# ─────────────────────────────────────────────────────────────

SIGNALS = [
    {
        "id": "active_hiring",
        "name": "Actively Hiring (job volume)",
        "data_key": "job_count",
        "weight": 3,  # graduated: 1pt=3-4, 2pt=5-7, 3pt=8+
    },
    {
        "id": "hr_ta_role",
        "name": "Hiring HR or TA Role",
        "data_key": "hr_roles_found",
        "weight": 3,
    },
    {
        "id": "company_size",
        "name": "Company Size 20-200",
        "data_key": "team_size",
        "weight": 1,
    },
    {
        "id": "funded",
        "name": "Recently Funded",
        "data_key": "yc_batch",
        "weight": 2,
    },
    {
        "id": "jobs_open_long",
        "name": "Jobs Open 30+ Days",
        "data_key": "job_check",
        "weight": 2,
    },
    {
        "id": "hr_tech_buyer",
        "name": "Hiring HR Technology Role",
        "data_key": "hr_roles_found",
        "weight": 3,
    },
    {
        "id": "icp_fit",
        "name": "ICP Industry and Profile Fit",
        "data_key": "description",
        "weight": 2,
    },
    {
        "id": "ats_confirmed",
        "name": "ATS Platform Confirmed (HR tech investment)",
        "data_key": "job_source",
        "weight": 2,
    },
]

MAX_SCORE = sum(s["weight"] for s in SIGNALS)  # 18


# ─────────────────────────────────────────────────────────────
# SIGNAL EVALUATORS
# ─────────────────────────────────────────────────────────────

def evaluate_active_hiring(company: dict) -> dict:
    """Signal 1: Graduated job count scoring with ATS board URL as proof."""
    job_count = company.get("job_count", 0)

    # If job_count is low, try fetching fresh from Greenhouse/Lever
    if job_count < 3:
        candidates = get_slug_candidates(company.get("company_name", ""), company.get("website", ""))
        for slug in candidates:
            gh = check_greenhouse(slug)
            if gh["found"] and gh["total_jobs"] > job_count:
                job_count = gh["total_jobs"]
                company["job_count"] = job_count
                company["hr_roles_found"] = gh["hr_roles"]
                company["ats_board_url"] = gh.get("ats_board_url", "")
                company["job_urls"] = gh.get("job_urls", [])
                break
            lv = check_lever(slug)
            if lv["found"] and lv["total_jobs"] > job_count:
                job_count = lv["total_jobs"]
                company["job_count"] = job_count
                company["hr_roles_found"] = lv["hr_roles"]
                company["ats_board_url"] = lv.get("ats_board_url", "")
                company["job_urls"] = lv.get("job_urls", [])
                break

    if job_count >= 8:
        points, tier = 3, "HIGH VOLUME"
    elif job_count >= 5:
        points, tier = 2, "MEDIUM VOLUME"
    elif job_count >= 3:
        points, tier = 1, "LOW VOLUME"
    else:
        points, tier = 0, "INSUFFICIENT"

    board_url = company.get("ats_board_url", "")
    proof = f" [proof: {board_url}]" if board_url else ""
    return {
        "signal_id": "active_hiring",
        "passed": points > 0,
        "points": points,
        "max_points": 3,
        "evidence": f"{job_count} open roles — {tier}{proof}",
        "detail": f"Job count: {job_count}, Points: {points}/3",
        "proof_url": board_url,
    }


def search_google_for_hr_roles(company_name: str, website: str) -> list[str]:
    """Fallback: searches Google for HR/TA job postings."""
    import requests
    from bs4 import BeautifulSoup

    hr_keywords = [
        "recruiter", "talent acquisition", "hr manager", "head of people",
        "people operations", "hr director", "recruiting manager",
    ]
    query = f'"{company_name}" recruiter OR "talent acquisition" OR "HR manager" jobs hiring'
    encoded = query.replace(" ", "+").replace('"', "%22")
    url = f"https://www.google.com/search?q={encoded}&num=10"
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            text = soup.get_text().lower()
            return [kw for kw in hr_keywords if kw in text]
    except Exception:
        pass
    return []


def evaluate_hr_ta_role(company: dict) -> dict:
    """Signal 2: Is the company hiring an HR or TA role?"""
    hr_roles = company.get("hr_roles_found", [])
    job_titles = company.get("job_check", {}).get("all_titles", [])

    hr_keywords = [
        "recruiter", "talent acquisition", "talent partner", "hr manager",
        "head of people", "people operations", "people ops", "hr director",
        "hr business partner", "recruiting manager", "vp of people",
    ]

    all_titles = hr_roles + job_titles
    matching = [t for t in all_titles if any(kw in t.lower() for kw in hr_keywords)]

    if not matching:
        name = company.get("company_name", "")
        website = company.get("website", "")
        google_signals = search_google_for_hr_roles(name, website)
        if google_signals:
            matching = [f"[Google signal] {kw}" for kw in google_signals]

    board_url = company.get("ats_board_url", "")
    proof = f" [proof: {board_url}]" if board_url and matching else ""
    return {
        "signal_id": "hr_ta_role",
        "passed": len(matching) > 0,
        "evidence": f"HR/TA roles: {matching[:3]}{proof}" if matching else "No HR/TA roles found",
        "detail": f"Matched {len(matching)} HR/TA titles",
        "proof_url": board_url,
        "matched_roles": matching[:5],
    }


def evaluate_company_size(company: dict) -> dict:
    """Signal 3: Is the company 20-200 employees?"""
    size = company.get("team_size", 0) or 0
    size_source = company.get("team_size_source", "")
    evidence_url = company.get("enrichment_evidence_url", "")

    if size == 0:
        passed = bool(company.get("yc_batch"))
        return {
            "signal_id": "company_size",
            "passed": passed,
            "evidence": "Team size unknown — YC company assumed in range" if passed else "Team size unknown",
            "detail": "Size not available",
            "proof_url": "",
        }

    passed = 20 <= size <= 200
    source_note = f" (from {size_source})" if size_source else ""
    proof = f" [proof: {evidence_url}]" if evidence_url and size_source else ""
    return {
        "signal_id": "company_size",
        "passed": passed,
        "evidence": f"{size} employees{source_note} — {'within' if passed else 'outside'} target range 20-200{proof}",
        "detail": f"Team size: {size}",
        "proof_url": evidence_url,
    }


def evaluate_funded(company: dict) -> dict:
    """Signal 4: Has the company been funded recently?"""
    yc_batch = company.get("yc_batch", "")
    funding_amount = company.get("funding_amount", "")

    recent_batches = ["W24", "S24", "W23", "S23", "W22", "S22"]
    is_recent_yc = yc_batch in recent_batches

    passed = is_recent_yc or bool(funding_amount)
    evidence_parts = []
    if is_recent_yc:
        evidence_parts.append(f"YC {yc_batch} — funded by Y Combinator")
    if funding_amount:
        evidence_parts.append(f"Raised {funding_amount}")

    yc_url = company.get("yc_url", "")
    return {
        "signal_id": "funded",
        "passed": passed,
        "evidence": " | ".join(evidence_parts) if evidence_parts else "No funding signal found",
        "detail": f"YC batch: {yc_batch}, Funding: {funding_amount or 'N/A'}",
        "proof_url": yc_url,
    }


def evaluate_jobs_open_long(company: dict) -> dict:
    """Signal 5: Are jobs sitting open for 30+ days?"""
    job_count = company.get("job_count", 0)
    is_hiring_flag = company.get("is_hiring", False)
    board_url = company.get("ats_board_url", "")

    passed = job_count >= 5 or (job_count >= 3 and is_hiring_flag)
    proof = f" [proof: {board_url}]" if board_url else ""

    if passed:
        evidence = f"{job_count} open roles — high volume suggests roles aging beyond 30 days{proof}"
    else:
        evidence = f"Only {job_count} open roles — insufficient signal for bottleneck"

    return {
        "signal_id": "jobs_open_long",
        "passed": passed,
        "evidence": evidence,
        "detail": f"Job count: {job_count}, isHiring flag: {is_hiring_flag}",
        "proof_url": board_url,
    }


def evaluate_hr_tech_buyer(company: dict) -> dict:
    """Signal 6: Is the company hiring an HR Technology role?"""
    hr_roles = company.get("hr_roles_found", [])
    job_titles = company.get("job_check", {}).get("all_titles", [])

    hr_tech_keywords = [
        "hr technology", "hris", "people technology", "hr systems",
        "talent technology", "hr operations", "hr tech",
        "people systems", "hr tools",
    ]

    all_titles = hr_roles + job_titles
    matching = [t for t in all_titles if any(kw in t.lower() for kw in hr_tech_keywords)]

    board_url = company.get("ats_board_url", "")
    proof = f" [proof: {board_url}]" if board_url and matching else ""
    return {
        "signal_id": "hr_tech_buyer",
        "passed": len(matching) > 0,
        "evidence": f"HR tech buyer roles: {matching}{proof}" if matching else "No HR technology buyer role found",
        "detail": f"Matched {len(matching)} HR tech titles",
        "proof_url": board_url,
    }


def evaluate_ats_confirmed(company: dict) -> dict:
    """
    Signal 8: Does the company use a known ATS platform?
    If we found them on Greenhouse/Lever/Workable etc., they already invest in HR tech.
    This replaces the broken G2 signal.
    """
    job_source = company.get("job_source", "none")
    ats_platforms = ["Greenhouse", "Lever", "Workable", "Ashby", "TeamTailor",
                     "BambooHR", "BreezyHR", "SmartRecruiters",
                     "Jobvite", "Recruitee", "Pinpoint", "JazzHR", "Personio"]
    board_url = company.get("ats_board_url", "")

    passed = job_source in ats_platforms
    if passed:
        evidence = f"Uses {job_source} ATS — confirms existing HR tech investment"
        if board_url:
            evidence += f" [proof: {board_url}]"
    else:
        evidence = f"No known ATS detected (source: {job_source})"

    return {
        "signal_id": "ats_confirmed",
        "passed": passed,
        "evidence": evidence,
        "detail": f"Job source: {job_source}",
        "proof_url": board_url,
    }


# ─────────────────────────────────────────────────────────────
# AI EVALUATION — Signal 7
# ─────────────────────────────────────────────────────────────

def evaluate_icp_fit_with_ai(company: dict, signal_results: list[dict]) -> dict:
    """
    Signal 7: AI research analyst evaluates ICP fit.
    Requires specific, non-generic reasoning. Must name actual roles.
    If data_confidence is LOW, must flag explicitly and not fabricate.
    """
    data_confidence = company.get("data_confidence", "LOW")

    # Build signals summary with proof URLs
    signals_summary = "\n".join([
        f"- {s['signal_id']}: {'PASS' if s['passed'] else 'FAIL'} — {s['evidence']}"
        for s in signal_results
    ])

    # Build job URL proof block
    job_urls = company.get("job_urls", [])
    if job_urls:
        job_url_block = "ACTUAL JOB LISTINGS (verified URLs):\n" + "\n".join([
            f"  - {j['title']}: {j['url']}" for j in job_urls[:5]
        ])
    else:
        job_url_block = "No individual job URLs available (careers page or Google source)."

    hr_roles = company.get("hr_roles_found", [])
    board_url = company.get("ats_board_url", "")
    enrichment_url = company.get("enrichment_evidence_url", "")

    # Build a list of specific roles for the AI to reference
    all_titles = []
    for j in job_urls[:10]:
        all_titles.append(j.get("title", ""))
    for r in hr_roles:
        all_titles.append(r)
    role_list = "\n".join(f"  - {t}" for t in all_titles[:15]) if all_titles else "  No specific role titles available"

    job_count = company.get("job_count", 0)

    prompt = f"""
You are a senior B2B Sales Development Representative analyzing a company for InterviewScreener.com.

{ICP_CONTEXT}

═══════════════════════════════════════════════
COMPANY DATA
═══════════════════════════════════════════════
Name: {company.get('company_name', 'Unknown')}
Website: {company.get('company_url', company.get('website', 'N/A'))}
Description: {company.get('description', 'N/A')}
Long Description: {(company.get('long_description', '') or '')[:500] or 'N/A'}
Team Size: {company.get('team_size', 'Unknown')} {f"(source: {company.get('team_size_source', 'unknown')})" if company.get('team_size') else ''}
Industries: {', '.join(company.get('industries', [])) or 'Unknown'}
Location: {', '.join(company.get('locations', [])) or 'Unknown'}
YC Batch: {company.get('yc_batch', 'N/A')}
ATS Platform: {company.get('job_source', 'Unknown')}
ATS Job Board: {board_url or 'N/A'}
Data Confidence: {data_confidence}
Enriched from: {enrichment_url or 'N/A'}
Total Open Roles: {job_count}

═══════════════════════════════════════════════
ACTUAL JOB TITLES FOUND (use these by name)
═══════════════════════════════════════════════
{role_list}

═══════════════════════════════════════════════
SIGNALS ALREADY VERIFIED (deterministic)
═══════════════════════════════════════════════
{signals_summary}

═══════════════════════════════════════════════
YOUR TASK — Write a SPECIFIC analysis
═══════════════════════════════════════════════

Do NOT write generic text like "they have many open roles indicating high hiring demand."
That is USELESS. Anyone can see the job count.

Instead, you MUST answer these SPECIFIC questions:

1. **SCREENING BOTTLENECK**: Based on the job titles above, how many of these roles require candidate screening calls? For each role that needs screening (e.g., "Senior Software Engineer", "Account Executive"), explain WHY screening is especially time-consuming for that type of role.

2. **HR TEAM CAPACITY**: If they have {job_count} open roles and their team size is {company.get('team_size', 'unknown')}, estimate how many recruiters they likely have (rule of thumb: 1 recruiter per 40-80 employees). Are they likely overwhelmed? Be specific.

3. **INTERVIEWSCREENER FIT**: Explain SPECIFICALLY how InterviewScreener would help THIS company. For example: "With {job_count} technical roles open and likely only 1-2 recruiters, each recruiter is spending ~15 hours/week on first-round screening calls. InterviewScreener would automate this entirely."

4. **OUTREACH OPENER**: Write ONE sentence that would make this company's HR leader stop scrolling. Reference a SPECIFIC role they're hiring for. Bad: "I noticed you're hiring." Good: "I saw {company.get('company_name', 'your team')} is hiring a [specific role] — curious how your team is handling the first-round screens for that pipeline?"

If data_confidence is LOW, be honest: say "we have limited data" and reason conservatively.

═══════════════════════════════════════════════
VERDICT RULES (STRICT — DO NOT OVERRIDE)
═══════════════════════════════════════════════
The verdict is primarily driven by the DETERMINISTIC SCORE, not your opinion.
You should focus on explaining WHY — the score decides the category.

- Your "overall_verdict" will be OVERRIDDEN by the scoring system, so focus on quality of analysis, not the verdict.
- Set icp_fit_passed=true if the company's industry, size, or hiring pattern suggests they could benefit from InterviewScreener.
- Set icp_fit_passed=false ONLY if the company is clearly outside ICP (e.g., a nonprofit, government, or a solo founder with no hiring).

Respond ONLY in this JSON format, no other text:
{{
  "icp_fit_passed": true or false,
  "icp_fit_evidence": "One sentence: what SPECIFIC data point makes this company a fit or not a fit.",
  "why_they_fit": "2-3 sentences answering questions 1-3 above. MUST name specific role titles. MUST estimate recruiter capacity. MUST explain screening bottleneck.",
  "recommended_plan": "Starter" or "Pro" or "Scale" or "Enterprise",
  "outreach_opener": "One personalized sentence answering question 4 above.",
  "overall_verdict": "HOT" or "WARM" or "COLD",
  "verdict_reason": "One sentence referencing the specific trigger event that makes this company worth pursuing or not.",
  "data_confidence": "{data_confidence}"
}}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=700,
        )
        raw = response.choices[0].message.content.strip()

        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        ai_result = json.loads(raw)
        return {
            "signal_id": "icp_fit",
            "passed": ai_result.get("icp_fit_passed", False),
            "evidence": ai_result.get("icp_fit_evidence", ""),
            "ai_output": ai_result,
        }

    except Exception as e:
        print(f"    [AI] Error evaluating {company.get('company_name')}: {e}")
        return {
            "signal_id": "icp_fit",
            "passed": False,
            "evidence": f"AI evaluation failed: {e}",
            "ai_output": {},
        }


# ─────────────────────────────────────────────────────────────
# MAIN AI BRAIN LOOP
# ─────────────────────────────────────────────────────────────

def run_ai_brain(companies: list[dict]) -> list[dict]:
    """
    Main agentic loop.
    Signals 1-6, 8 use deterministic code + targeted scraper calls.
    Signal 7 uses the AI (one LLM call per company).
    Returns scored and annotated lead list.
    """
    results = []
    print(f"\n[AI BRAIN] Evaluating {len(companies)} companies...")
    print(f"[AI BRAIN] Max possible score: {MAX_SCORE} points (Signals 1-8, G2 replaced by ATS-Confirmed)\n")

    for i, company in enumerate(companies):
        name = company.get("company_name", "Unknown")
        data_conf = company.get("data_confidence", "LOW")
        print(f"  [{i+1}/{len(companies)}] Evaluating: {name} [data_confidence={data_conf}]")

        signal_results = []

        s1 = evaluate_active_hiring(company)
        signal_results.append(s1)
        print(f"    Signal 1 (Active Hiring):   {'PASS' if s1['passed'] else 'FAIL'} — {s1['evidence'][:80]}")

        s2 = evaluate_hr_ta_role(company)
        signal_results.append(s2)
        print(f"    Signal 2 (HR/TA Role):       {'PASS' if s2['passed'] else 'FAIL'} — {s2['evidence'][:80]}")

        s3 = evaluate_company_size(company)
        signal_results.append(s3)
        print(f"    Signal 3 (Company Size):     {'PASS' if s3['passed'] else 'FAIL'} — {s3['evidence'][:80]}")

        s4 = evaluate_funded(company)
        signal_results.append(s4)
        print(f"    Signal 4 (Funded):           {'PASS' if s4['passed'] else 'FAIL'} — {s4['evidence'][:80]}")

        s5 = evaluate_jobs_open_long(company)
        signal_results.append(s5)
        print(f"    Signal 5 (Jobs Open Long):   {'PASS' if s5['passed'] else 'FAIL'} — {s5['evidence'][:80]}")

        s6 = evaluate_hr_tech_buyer(company)
        signal_results.append(s6)
        print(f"    Signal 6 (HR Tech Buyer):    {'PASS' if s6['passed'] else 'FAIL'} — {s6['evidence'][:80]}")

        print(f"    Signal 7 (AI ICP Fit):       Calling AI...")
        s7 = evaluate_icp_fit_with_ai(company, signal_results)
        signal_results.append(s7)
        print(f"    Signal 7 (AI ICP Fit):       {'PASS' if s7['passed'] else 'FAIL'} — {s7['evidence'][:80]}")

        s8 = evaluate_ats_confirmed(company)
        signal_results.append(s8)
        print(f"    Signal 8 (ATS Confirmed):    {'PASS' if s8['passed'] else 'FAIL'} — {s8['evidence'][:80]}")

        signal_weight_map = {s["id"]: s["weight"] for s in SIGNALS}
        final_score = 0
        for s in signal_results:
            sid = s["signal_id"]
            if sid == "active_hiring":
                final_score += s.get("points", 0)
            elif s["passed"]:
                final_score += signal_weight_map.get(sid, 0)

        ai_output = s7.get("ai_output", {})
        ai_verdict = ai_output.get("overall_verdict", "COLD")

        # ─────────────────────────────────────────────────────────
        # DETERMINISTIC VERDICT — score overrides AI opinion
        # AI can upgrade (COLD→WARM, WARM→HOT) but NEVER downgrade
        # Score thresholds are the source of truth
        # ─────────────────────────────────────────────────────────
        VERDICT_ORDER = {"COLD": 0, "WARM": 1, "HOT": 2}

        if final_score >= 10:
            score_verdict = "HOT"
        elif final_score >= 6:
            score_verdict = "WARM"
        else:
            score_verdict = "COLD"

        # Take the HIGHER of score-based verdict and AI verdict
        # AI can upgrade a WARM to HOT if it sees strong fit, but can NEVER downgrade
        if VERDICT_ORDER.get(ai_verdict, 0) > VERDICT_ORDER.get(score_verdict, 0):
            verdict = ai_verdict  # AI upgraded
            verdict_note = f" (AI upgraded from {score_verdict})"
        elif VERDICT_ORDER.get(ai_verdict, 0) < VERDICT_ORDER.get(score_verdict, 0):
            verdict = score_verdict  # Score overrode AI downgrade
            verdict_note = f" (score override — AI said {ai_verdict})"
        else:
            verdict = score_verdict
            verdict_note = ""

        opener = ai_output.get("outreach_opener", "")
        company_name = company.get("company_name", "there")
        opener = opener.replace("[Name]", f"[{company_name} team]")
        opener = opener.replace("[name]", f"[{company_name} team]")

        # Build proof URL collection
        proof_urls = {
            "company_url": company.get("company_url") or company.get("website", ""),
            "ats_board_url": company.get("ats_board_url", ""),
            "enrichment_evidence_url": company.get("enrichment_evidence_url", ""),
            "yc_url": company.get("yc_url", ""),
        }

        print(f"    FINAL SCORE: {final_score}/{MAX_SCORE} | VERDICT: {verdict}{verdict_note} | Confidence: {data_conf}\n")

        results.append({
            **company,
            "signal_results": signal_results,
            "final_score": final_score,
            "max_score": MAX_SCORE,
            "verdict": verdict,
            "verdict_reason": ai_output.get("verdict_reason", ""),
            "why_they_fit": ai_output.get("why_they_fit", ""),
            "recommended_plan": ai_output.get("recommended_plan", ""),
            "outreach_opener": opener,
            "data_confidence": data_conf,
            "proof_urls": proof_urls,
        })

        time.sleep(random.uniform(1, 2))

    results.sort(key=lambda x: x["final_score"], reverse=True)

    hot = [r for r in results if r["verdict"] == "HOT"]
    warm = [r for r in results if r["verdict"] == "WARM"]
    cold = [r for r in results if r["verdict"] == "COLD"]

    print(f"[AI BRAIN] Complete.")
    print(f"  HOT leads:  {len(hot)}")
    print(f"  WARM leads: {len(warm)}")
    print(f"  COLD leads: {len(cold)}")

    return results
