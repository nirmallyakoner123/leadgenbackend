from config import MIN_EMPLOYEES, MAX_EMPLOYEES, MIN_OPEN_ROLES

ICP_INDUSTRIES = [
    "b2b", "saas", "software", "technology", "tech",
    "staffing", "recruiting", "recruitment", "hr", "human resources",
    "e-commerce", "ecommerce", "edtech", "education",
    "healthtech", "healthcare", "fintech", "marketing", "sales",
    "engineering", "product", "design", "consumer",
]

# Keywords in description that suggest HR/hiring pain
HR_PAIN_KEYWORDS = [
    "hiring", "recruit", "talent", "hr", "workforce", "staffing",
    "screening", "interview", "candidate", "onboard", "people ops",
    "human resources", "ats", "applicant",
]

# ─────────────────────────────────────────────────────────────
# Product Hunt junk filter — names that are clearly products, not companies
# These are indie tools, wrappers, browser extensions, and APIs
# that will never have an ATS board or hiring needs
# ─────────────────────────────────────────────────────────────
_PH_JUNK_KEYWORDS = [
    " for macos", " for mac", " for ios", " for iphone", " for ipad",
    " for chrome", " for firefox", " for safari", " for android",
    " for notebooklm", " for notion", " for obsidian", " for slack",
    " for figma", " for vscode", " for cursor",
    " mcp ", " mcp", "mcp ", " api", " sdk", " cli",
    " extension", " plugin", " widget", " bot", " wrapper",
    " gpt", "gpt-", "gpt‑", "claude ", " ai lite",
    " mini and ", " nano", " pro and ",
    " double checker", " auto caption",
    "text to speech", "speech to text",
]

_PH_JUNK_EXACT = {
    "openai", "anthropic", "google", "meta", "microsoft",  # giant cos, not leads
}


def filter_ph_junk(companies: list[dict]) -> tuple[list[dict], int]:
    """
    Removes Product Hunt entries that are clearly product names, not companies.
    Returns (kept, dropped_count).
    """
    kept = []
    dropped = 0
    for company in companies:
        source = company.get("source", "")
        # Only filter ProductHunt entries — all other sources are fine
        if source != "ProductHunt":
            kept.append(company)
            continue

        name_lower = company.get("company_name", "").lower().strip()

        # Drop if name matches exact junk set
        if name_lower in _PH_JUNK_EXACT:
            dropped += 1
            continue

        # Drop if name contains junk keywords
        if any(kw in name_lower for kw in _PH_JUNK_KEYWORDS):
            dropped += 1
            continue

        # Drop if name is too long (likely a full product tagline, not a company)
        if len(name_lower) > 40:
            dropped += 1
            continue

        kept.append(company)

    return kept, dropped


def normalize(text: str) -> str:
    return str(text).lower().strip()


def matches_icp_industry(company: dict) -> bool:
    industries = [normalize(i) for i in company.get("industries", [])]
    tags = [normalize(t) for t in company.get("tags", [])]
    description = normalize(company.get("description", "") + " " + company.get("long_description", ""))
    all_text = " ".join(industries + tags) + " " + description
    return any(kw in all_text for kw in ICP_INDUSTRIES)


def is_in_size_range(company: dict) -> bool:
    size = company.get("team_size", 0) or 0
    if size == 0:
        return True  # Unknown size — don't drop, let AI Brain decide later
    return MIN_EMPLOYEES <= size <= MAX_EMPLOYEES


def has_hr_pain_signal(company: dict) -> bool:
    description = normalize(
        company.get("description", "") + " " +
        company.get("long_description", "") + " " +
        company.get("article_title", "")
    )
    return any(kw in description for kw in HR_PAIN_KEYWORDS)


def deduplicate(raw_companies: list[dict]) -> dict[str, dict]:
    """
    Merges companies found across multiple sources into one entry per company.
    Key = normalized company name.
    If same company appears in multiple sources, merges the data and records all sources.
    """
    merged = {}

    for company in raw_companies:
        name = normalize(company.get("company_name", ""))
        if not name or len(name) < 2:
            continue

        if name not in merged:
            merged[name] = {
                "company_name": company.get("company_name", "").strip(),
                "website": company.get("website", ""),
                "description": company.get("description", ""),
                "long_description": company.get("long_description", ""),
                "team_size": company.get("team_size", 0),
                "industries": company.get("industries", []),
                "tags": company.get("tags", []),
                "locations": company.get("locations", []),
                "yc_batch": company.get("yc_batch", ""),
                "is_hiring": company.get("is_hiring", False),
                "yc_url": company.get("yc_url", ""),
                "funding_amount": company.get("funding_amount", ""),
                "article_url": company.get("article_url", ""),
                "hr_roles_found": company.get("hr_roles_found", []),
                "sources": [company.get("source", "")],
                "source_count": 1,
                # Track if this company came from LinkedIn (direct HR hiring signal)
                "from_linkedin": company.get("source") == "LinkedIn",
            }
        else:
            entry = merged[name]
            source = company.get("source", "")
            if source and source not in entry["sources"]:
                entry["sources"].append(source)
                entry["source_count"] += 1

            # Enrich with data from additional sources
            if not entry["website"] and company.get("website"):
                entry["website"] = company["website"]
            if not entry["team_size"] and company.get("team_size"):
                entry["team_size"] = company["team_size"]
            if not entry["funding_amount"] and company.get("funding_amount"):
                entry["funding_amount"] = company["funding_amount"]
            if company.get("is_hiring"):
                entry["is_hiring"] = True
            if company.get("yc_batch") and not entry["yc_batch"]:
                entry["yc_batch"] = company["yc_batch"]
            # Merge HR roles found across sources
            existing_hr = entry.get("hr_roles_found", [])
            new_hr = company.get("hr_roles_found", [])
            for role in new_hr:
                if role not in existing_hr:
                    existing_hr.append(role)
            entry["hr_roles_found"] = existing_hr
            if company.get("source") == "LinkedIn":
                entry["from_linkedin"] = True

    return merged


def apply_basic_filters(companies: dict[str, dict]) -> list[dict]:
    """
    Applies deterministic code-only filters. No AI.
    Drops companies that clearly do not fit the ICP.
    Scores remaining companies and returns sorted list.
    """
    qualified = []
    dropped = 0

    for key, data in companies.items():
        reasons_dropped = []

        # LinkedIn companies already have confirmed HR hiring intent — pass through
        # without industry filter (we don't have industry data for them yet)
        is_linkedin_source = data.get("from_linkedin", False)

        # Filter 1: Must match ICP industry (loosely) — skip for LinkedIn companies
        if not is_linkedin_source and not matches_icp_industry(data):
            reasons_dropped.append("Industry does not match ICP")

        # Filter 2: Size must be in range (skip if unknown)
        if not is_in_size_range(data):
            reasons_dropped.append(
                f"Team size {data['team_size']} outside range {MIN_EMPLOYEES}-{MAX_EMPLOYEES}"
            )

        if reasons_dropped:
            dropped += 1
            continue

        # Pre-score: priority ranking before AI Brain
        score = 0

        # Found in multiple sources = stronger signal
        score += min(data["source_count"] * 2, 4)

        # YC company = funded, vetted, growing
        if data["yc_batch"]:
            score += 2

        # Actively hiring flag from YC
        if data["is_hiring"]:
            score += 2

        # Has funding amount mentioned
        if data["funding_amount"]:
            score += 1

        # Has HR/hiring pain keywords in description
        if has_hr_pain_signal(data):
            score += 2

        # LinkedIn source = direct HR hiring intent (highest signal)
        if data.get("from_linkedin") or data.get("hr_roles_found"):
            score += 3

        # Known team size in sweet spot (50-200)
        size = data.get("team_size", 0) or 0
        if 50 <= size <= 200:
            score += 2
        elif 20 <= size < 50:
            score += 1

        data["pre_score"] = score
        data["signals"] = build_signal_summary(data)
        qualified.append(data)

    qualified.sort(key=lambda x: x["pre_score"], reverse=True)

    print(f"\n[FILTER] {len(companies)} unique companies found")
    print(f"[FILTER] {dropped} dropped by basic filters")
    print(f"[FILTER] {len(qualified)} companies qualify for AI Brain")

    return qualified


def build_signal_summary(data: dict) -> dict:
    return {
        "is_yc_company": bool(data.get("yc_batch")),
        "yc_batch": data.get("yc_batch", ""),
        "is_actively_hiring": data.get("is_hiring", False),
        "has_funding_signal": bool(data.get("funding_amount")),
        "funding_amount": data.get("funding_amount", ""),
        "found_in_sources": data.get("sources", []),
        "source_count": data.get("source_count", 1),
        "team_size": data.get("team_size", 0),
        "has_hr_pain_keywords": has_hr_pain_signal(data),
        "industry_match": matches_icp_industry(data),
        "from_linkedin": data.get("from_linkedin", False),
        "hr_roles_found": data.get("hr_roles_found", []),
    }


# ─────────────────────────────────────────────────────────────
# SMART PRE-FILTER: Skip dead-end companies BEFORE job checker
# ─────────────────────────────────────────────────────────────

# Sources that already carry hiring intent — never skip these
INTENT_SOURCES = {"LinkedIn", "Seek", "Reed"}

# Sources that produce thin data (just a name from a headline)
THIN_SOURCES = {"GoogleNews", "TechCrunch", "ProductHunt"}


def _has_intent_source(company: dict) -> bool:
    """Returns True if company came from a source with confirmed HR hiring intent."""
    sources = set(company.get("sources", []))
    return bool(sources & INTENT_SOURCES)


def _is_yc_company(company: dict) -> bool:
    """YC companies are always high-value — never skip."""
    return bool(company.get("yc_batch"))


def _is_ghost_company(company: dict) -> bool:
    """
    A ghost company has no resolvable website and produced no enrichment data.
    The enricher already tried fetching their homepage and failed.
    If we can't reach them, the job checker won't find their ATS board either.
    """
    # Ghost is a company that has no website and no enrichment success
    enrichment_failed = company.get("enrichment_status") in ("failed", "no_url", "no_valid_page")
    no_description = not company.get("description")
    no_website = not company.get("company_url") and not company.get("website")
    return enrichment_failed and no_description and no_website


def _is_oversized(company: dict) -> bool:
    """Companies with 2000+ employees are enterprise — not ICP for InterviewScreener."""
    size = company.get("team_size", 0) or 0
    return size > 2000


def _is_empty_shell(company: dict) -> bool:
    """
    An empty shell is a company from a thin source (news article) with
    no meaningful data at all — no description, no website, no industries.
    These are just names extracted from headlines with nothing to verify.
    """
    sources = set(company.get("sources", []))
    only_thin_sources = sources and sources.issubset(THIN_SOURCES)

    no_description = not company.get("description")
    no_industries = not company.get("industries")
    no_website = not company.get("company_url") and not company.get("website")

    return only_thin_sources and no_description and no_industries and no_website


def apply_job_check_gate(companies: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Smart pre-filter that runs AFTER enrichment, BEFORE the job checker.
    Skips companies that are guaranteed dead ends to save job checker time.

    Returns:
        (worth_checking, skipped) — two lists
    """
    worth_checking = []
    skipped = []
    skip_reasons = {"ghost": 0, "oversized": 0, "empty_shell": 0}

    for company in companies:
        name = company.get("company_name", "?")

        # ── PROTECTION RULES: Never skip these ──────────────────
        if _has_intent_source(company):
            worth_checking.append(company)
            continue

        if _is_yc_company(company):
            worth_checking.append(company)
            continue

        if company.get("pre_score", 0) >= 5:
            worth_checking.append(company)
            continue

        size = company.get("team_size", 0) or 0
        if MIN_EMPLOYEES <= size <= MAX_EMPLOYEES:
            worth_checking.append(company)
            continue

        # ── SKIP RULES: Applied only to unprotected companies ───
        if _is_oversized(company):
            company["_skip_reason"] = "oversized"
            skipped.append(company)
            skip_reasons["oversized"] += 1
            continue

        if _is_ghost_company(company):
            company["_skip_reason"] = "ghost_company"
            skipped.append(company)
            skip_reasons["ghost"] += 1
            continue

        if _is_empty_shell(company):
            company["_skip_reason"] = "empty_shell"
            skipped.append(company)
            skip_reasons["empty_shell"] += 1
            continue

        # ── DEFAULT: Check it ────────────────────────────────────
        worth_checking.append(company)

    # ── Logging ──────────────────────────────────────────────────
    total = len(companies)
    kept = len(worth_checking)
    dropped = len(skipped)

    print(f"\n[SMART FILTER] Pre-filtering {total} companies before job checker")
    print(f"  Worth checking:  {kept}")
    print(f"  Skipped:         {dropped}")
    if skip_reasons["ghost"]:
        print(f"    Ghost (no website/enrichment):  {skip_reasons['ghost']}")
    if skip_reasons["oversized"]:
        print(f"    Oversized (2000+ employees):    {skip_reasons['oversized']}")
    if skip_reasons["empty_shell"]:
        print(f"    Empty shell (thin source only): {skip_reasons['empty_shell']}")

    return worth_checking, skipped
