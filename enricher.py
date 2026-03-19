"""
enricher.py — Company Context Enrichment

Fetches real company context (description, team size estimate, industry)
from the company's homepage BEFORE the AI Brain runs.

Without this, LinkedIn companies arrive with team_size=0 and empty industries,
making the AI unable to reason about them specifically.
"""

import re
import asyncio
import threading
from http_client import get_session, DEFAULT_TIMEOUT, ENRICH_TIMEOUT  # shared session: retry + pooling
from bs4 import BeautifulSoup


# Patterns to detect employee count in page text
_SIZE_PATTERNS = [
    r"(\d[\d,]+)\+?\s*(?:full[- ]time\s+)?employees",
    r"team\s*of\s*(\d[\d,]+)",
    r"(\d[\d,]+)\s*(?:person|people|member)\s*(?:team|company|organization)",
    r"over\s*(\d[\d,]+)\s*(?:employees|people|staff)",
    r"(\d[\d,]+)\s*staff",
    r"workforce\s*of\s*(\d[\d,]+)",
]

# Industry keywords mapped to clean labels
_INDUSTRY_MAP = {
    "saas": "SaaS",
    "software": "Software",
    "technology": "Technology",
    "fintech": "Fintech",
    "healthtech": "Healthtech",
    "health": "Healthcare",
    "edtech": "Edtech",
    "education": "Education",
    "ecommerce": "E-commerce",
    "e-commerce": "E-commerce",
    "staffing": "Staffing",
    "recruitment": "Recruiting",
    "recruiting": "Recruiting",
    "marketplace": "Marketplace",
    "logistics": "Logistics",
    "insurtech": "Insurtech",
    "proptech": "Proptech",
    "real estate": "Real Estate",
    "legal": "Legal",
    "marketing": "Marketing",
    "analytics": "Analytics",
    "artificial intelligence": "AI/ML",
    "machine learning": "AI/ML",
    "cybersecurity": "Cybersecurity",
    "security": "Cybersecurity",
    "devops": "DevOps",
    "cloud": "Cloud",
    "hr tech": "HR Tech",
    "human resources": "HR Tech",
}


def _clean_url(website: str) -> str:
    """Ensures URL has a scheme."""
    if not website:
        return ""
    w = website.strip().rstrip("/")
    if not w.startswith("http"):
        w = "https://" + w
    return w


def _detect_size_from_text(text: str) -> int:
    """Scans text for employee count patterns. Returns best estimate or 0."""
    text_lower = text.lower().replace(",", "")
    for pattern in _SIZE_PATTERNS:
        m = re.search(pattern, text_lower)
        if m:
            try:
                val = int(m.group(1).replace(",", ""))
                if 5 <= val <= 100_000:   # sanity bounds
                    return val
            except ValueError:
                continue
    return 0


def _detect_industries(text: str) -> list[str]:
    """Extracts industry tags from page text."""
    text_lower = text.lower()
    found = []
    for kw, label in _INDUSTRY_MAP.items():
        if kw in text_lower and label not in found:
            found.append(label)
    return found[:5]


def _extract_meta_description(soup: BeautifulSoup) -> str:
    """Gets the <meta name="description"> content."""
    tag = soup.find("meta", attrs={"name": "description"})
    if tag and tag.get("content"):
        return tag["content"].strip()
    tag = soup.find("meta", attrs={"property": "og:description"})
    if tag and tag.get("content"):
        return tag["content"].strip()
    return ""


def _extract_about_text(soup: BeautifulSoup) -> str:
    """Tries to extract a meaningful company summary from the page body."""
    # Try to find an about/hero section first
    for selector in ["#about", ".about", ".hero", ".mission", ".tagline"]:
        section = soup.select_one(selector)
        if section:
            t = section.get_text(separator=" ", strip=True)
            if len(t) > 60:
                return t[:500]

    # Fallback: concatenate first few non-empty paragraphs
    paragraphs = []
    for p in soup.find_all("p"):
        t = p.get_text(strip=True)
        if len(t) > 60:
            paragraphs.append(t)
        if len(" ".join(paragraphs)) > 400:
            break
    return " ".join(paragraphs)[:500]


# Subdomains that indicate a login/app redirect — not the marketing homepage
_APP_SUBDOMAINS = {"app", "launchpad", "login", "signin", "dashboard",
                   "account", "portal", "console", "platform", "auth"}


def _is_app_redirect(original_url: str, final_url: str) -> bool:
    """Returns True if the redirect landed on an auth/app subdomain."""
    try:
        from urllib.parse import urlparse
        final_host = urlparse(final_url).hostname or ""
        subdomain = final_host.split(".")[0]
        return subdomain in _APP_SUBDOMAINS
    except Exception:
        return False


def enrich_company(company: dict, current_idx: int = 1, total_count: int = 1) -> dict:
    """
    Fetches the company homepage to extract:
    - Confirmed company URL
    - Meta description / about text
    - Estimated team size (if team_size == 0)
    - Industry tags (if industries is empty)

    Returns the company dict with enriched fields added.
    Always safe — failures are caught and the pipeline continues.
    """
    name = company.get("company_name", "")
    website = company.get("website", "")

    # If no website, try constructing one from the company name
    if not website:
        guessed = name.lower().strip()
        guessed = re.sub(r"[^a-z0-9]", "", guessed)
        website = f"https://{guessed}.com"

    url = _clean_url(website)
    print(f"  Enriching [{current_idx}/{total_count}]: {name} | url={url}")
    if not url:
        company["enrichment_status"] = "no_url"
        return company

    # Build a list of URLs to try: original, then www prefix
    from urllib.parse import urlparse
    parsed = urlparse(url)
    host = parsed.hostname or ""
    urls_to_try = [url]
    if not host.startswith("www."):
        urls_to_try.append(f"{parsed.scheme}://www.{host}{parsed.path}")

    r = None
    used_url = url
    for attempt_url in urls_to_try:
        try:
            resp = get_session().get(attempt_url, timeout=ENRICH_TIMEOUT, allow_redirects=True)
            if resp.status_code == 200 and len(resp.text) > 500:
                # If it redirected to a login/app page, try next URL
                if _is_app_redirect(attempt_url, resp.url):
                    continue
                r = resp
                used_url = attempt_url
                break
        except Exception:
            continue

    if r is None or r.status_code != 200 or len(r.text) < 500:
        company["enrichment_status"] = "no_valid_page"
        if "data_confidence" not in company:
            company["data_confidence"] = "LOW"
        return company

    try:

        # Use the final URL after redirects as the confirmed company URL
        confirmed_url = r.url.rstrip("/")
        soup = BeautifulSoup(r.text, "html.parser")

        # Remove noise
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()

        page_text = soup.get_text(separator=" ", strip=True)

        # Extract enrichment data
        meta_desc = _extract_meta_description(soup)
        about_text = _extract_about_text(soup)
        summary = meta_desc or about_text

        size_est = _detect_size_from_text(page_text)
        industries = _detect_industries(page_text)

        # Apply enrichments — only fill gaps, don't overwrite existing data
        company["company_url"] = confirmed_url
        company["enrichment_evidence_url"] = confirmed_url  # URL we fetched
        company["enrichment_status"] = "ok"

        if summary and not company.get("description"):
            company["description"] = summary[:300]
        if summary and not company.get("long_description"):
            company["long_description"] = summary

        if size_est and (not company.get("team_size") or company.get("team_size") == 0):
            company["team_size"] = size_est
            company["team_size_source"] = "homepage_text"

        if industries and not company.get("industries"):
            company["industries"] = industries
            company["industry_source"] = "homepage_text"

        # Data confidence: how much do we actually know now?
        has_description = bool(company.get("description"))
        has_size = bool(company.get("team_size", 0))
        has_industries = bool(company.get("industries"))

        confidence_score = sum([has_description, has_size, has_industries])
        company["data_confidence"] = (
            "HIGH" if confidence_score >= 3
            else "MEDIUM" if confidence_score >= 1
            else "LOW"
        )

        print(f"    [ENRICH] confidence={company['data_confidence']} "
              f"| size={size_est or '?'} | industries={industries[:2]} | url={confirmed_url}")

    except Exception as ssl_err:
        if "SSL" in type(ssl_err).__name__ or "SSL" in str(ssl_err):
            # Try http:// fallback for SSL errors
            try:
                fallback_url = url.replace("https://", "http://")
                r2 = get_session().get(fallback_url, timeout=ENRICH_TIMEOUT, allow_redirects=True)
                if r2.status_code == 200:
                    company["company_url"] = r2.url.rstrip("/")
                    company["enrichment_status"] = "ok_http_fallback"
            except Exception:
                company["enrichment_status"] = "ssl_error"
        else:
            company["enrichment_status"] = f"error: {type(ssl_err).__name__}"

    # Always ensure data_confidence is set
    if "data_confidence" not in company:
        company["data_confidence"] = "LOW"

    return company


# ─────────────────────────────────────────
# CONCURRENT ENRICHMENT (asyncio.to_thread)
# ─────────────────────────────────────────

async def _enrich_one(
    sem: asyncio.Semaphore,
    company: dict,
    idx: int,
    total: int,
    counter: dict,
    lock: threading.Lock,
) -> None:
    """Wraps enrich_company with a semaphore and a hard per-company timeout."""
    async with sem:
        try:
            await asyncio.wait_for(
                asyncio.to_thread(enrich_company, company, idx, total),
                timeout=18.0,  # hard ceiling: abandon any site that stalls > 18s
            )
        except asyncio.TimeoutError:
            company["enrichment_status"] = "timeout"
            company.setdefault("data_confidence", "LOW")
        with lock:
            counter["done"] += 1
            if company.get("enrichment_status") == "ok":
                counter["enriched"] += 1
            done = counter["done"]
        if done % 10 == 0 or done == total:
            print(f"  [ENRICHER PROGRESS] {done}/{total} companies processed")


async def _enrich_all_async(
    companies: list[dict],
    concurrency: int = 10,  # 10 concurrent — reduces OS socket pressure vs 15
) -> list[dict]:
    """Internal async implementation."""
    sem = asyncio.Semaphore(concurrency)
    lock = threading.Lock()
    counter = {"done": 0, "enriched": 0}

    # Pre-filter: skip companies that already have rich data
    tasks = []
    skip_count = 0
    for i, company in enumerate(companies):
        has_data = (
            company.get("team_size", 0) > 0
            and company.get("industries")
            and company.get("description")
        )
        if has_data:
            company["data_confidence"] = "HIGH"
            company["enrichment_status"] = "skipped_already_rich"
            company.setdefault("company_url", company.get("website", ""))
            skip_count += 1
            counter["done"] += 1
            continue
        tasks.append(_enrich_one(sem, company, i + 1, len(companies), counter, lock))

    print(f"  Skipped (already rich data): {skip_count}")
    print(f"  Fetching homepages for: {len(tasks)} companies ({concurrency} concurrent)")

    await asyncio.gather(*tasks)
    return companies, counter["enriched"]


def enrich_companies(companies: list[dict], concurrency: int = 10) -> list[dict]:
    """
    Runs enrichment on all companies CONCURRENTLY.
    Called before the AI Brain so the AI has real data to reason about.
    Uses asyncio.to_thread to run multiple homepage fetches in parallel.
    """
    print(f"\n[ENRICHER] Fetching company context for {len(companies)} companies...")
    print(f"[ENRICHER] Concurrency: {concurrency} simultaneous homepage fetches")

    result, enriched_count = asyncio.run(_enrich_all_async(companies, concurrency))

    print(f"[ENRICHER] Done — {enriched_count}/{len(companies)} companies enriched\n")
    return result
