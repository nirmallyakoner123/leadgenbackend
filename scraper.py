import asyncio
import feedparser
import re
import time
import random
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import httpx
from config import (
    TARGET_INDUSTRIES,
    MIN_EMPLOYEES,
    MAX_EMPLOYEES,
    GEOGRAPHY_CONFIG,
    LINKEDIN_SEARCH_TEMPLATES,
    GOOGLE_NEWS_QUERIES,
    GOOGLE_NEWS_QUERIES_IN,
    RunConfig,
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# Concurrency and retry settings
# Lower concurrency avoids 429/403 from LinkedIn, Seek, YC; still faster than sequential
MAX_CONCURRENT = 5
MAX_CONCURRENT_STRICT = 2   # LinkedIn, Seek — throttle to avoid rate limits
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_BASE = 1.0    # 1s, 2s, 4s for network errors
RETRY_429_BACKOFF = 3.0     # 3s, 6s, 12s for rate limits (429)


async def fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
) -> httpx.Response:
    """
    Fetch URL with retries. Retries on network errors and on 429 (rate limit).
    Respects semaphore for concurrency.
    """
    async with semaphore:
        last_error: Exception | None = None
        for attempt in range(RETRY_ATTEMPTS):
            try:
                resp = await client.get(url, timeout=15.0)
                # Retry on rate limit (429) or temporary block (403) with longer backoff
                if resp.status_code in (403, 429) and attempt < RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(RETRY_429_BACKOFF * (2**attempt))
                    continue
                return resp
            except (httpx.HTTPError, httpx.ConnectError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt < RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(RETRY_BACKOFF_BASE * (2**attempt))
        if last_error is not None:
            raise last_error
        # Should not reach here
        raise RuntimeError("fetch_with_retry exhausted retries")


# ─────────────────────────────────────────
# SOURCE 1: Y Combinator API
# ─────────────────────────────────────────

YC_BATCHES = ["W24", "S24", "W23", "S23", "W22", "S22", "W21", "S21", "W20", "S20", "W19", "S19", "W18", "S18", "W17", "S17", "W16", "S16", "W15", "S15", "W14", "S14", "W13", "S13", "W12", "S12", "W11", "S11", "W10", "S10", "W09", "S09", "W08", "S08", "W07", "S07", "W06", "S06", "W05", "S05", "W04", "S04", "W03", "S03", "W02", "S02", "W01", "S01"]
YC_API_BASE = "https://api.ycombinator.com/v0.1/companies"

YC_TARGET_INDUSTRIES = [
    "B2B", "Engineering, Product and Design", "Human Resources",
    "HR", "Recruiting", "Staffing", "Education", "Healthcare",
    "Fintech", "E-Commerce", "Marketing", "Sales", "Consumer",
]


def _parse_yc_page(data: dict, batch: str, region_keyword: str, country_code: str) -> list[dict]:
    """Parse YC API response into list[dict] (shared structure)."""
    results = []
    companies = data.get("companies", [])
    for c in companies:
        regions = c.get("regions", [])
        if not any(region_keyword in reg for reg in regions):
            continue
        if c.get("status", "") != "Active":
            continue
        team_size = c.get("teamSize", 0) or 0
        locations = c.get("locations", [])
        results.append({
            "company_name": c.get("name", ""),
            "website": c.get("website", ""),
            "description": c.get("oneLiner", ""),
            "long_description": c.get("longDescription", ""),
            "team_size": team_size,
            "industries": c.get("industries", []),
            "tags": c.get("tags", []),
            "locations": locations,
            "yc_batch": batch,
            "is_hiring": "isHiring" in c.get("badges", []),
            "yc_url": c.get("url", ""),
            "source": "YC",
            "country_code": country_code,
        })
    return results


async def scrape_yc(
    country_code: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """
    Pulls companies from the YC public API across recent batches.
    Filters to the requested country. Returns companies actively hiring.
    """
    geo = GEOGRAPHY_CONFIG.get(country_code, GEOGRAPHY_CONFIG["US"])
    region_keyword = geo["yc_region"]
    results: list[dict] = []
    print(f"\n[YC] Scraping Y Combinator companies for {country_code}...")

    async def fetch_page(batch: str, page: int) -> tuple[str, int, dict | None]:
        url = f"{YC_API_BASE}?batch={batch}&page={page}"
        print(f"  [YC] -> Fetching Batch {batch} page {page}...")
        try:
            resp = await fetch_with_retry(client, url, semaphore)
            if resp.status_code != 200:
                print(f"  [YC] ✗ Batch {batch} page {page} FAILED: HTTP {resp.status_code}")
                return batch, page, None
            print(f"  [YC] ✓ Batch {batch} page {page} SUCCESS")
            return batch, page, resp.json()
        except Exception as e:
            print(f"  [YC] ✗ Error on batch {batch} page {page}: {e}")
            return batch, page, None

    # Fetch page 1 of each batch to get totalPages
    first_tasks = [fetch_page(batch, 1) for batch in YC_BATCHES]
    first_pages = await asyncio.gather(*first_tasks)
    extra_tasks: list[asyncio.Task] = []
    for batch, page_num, data in first_pages:
        if data is not None:
            parsed = _parse_yc_page(data, batch, region_keyword, country_code)
            results.extend(parsed)
            print(f"  [YC] Batch {batch} page 1: {len(parsed)} companies")
            total_pages = data.get("totalPages", 1)
            for p in range(2, total_pages + 1):
                extra_tasks.append(asyncio.create_task(fetch_page(batch, p)))

    if extra_tasks:
        more = await asyncio.gather(*extra_tasks)
        for batch, page_num, data in more:
            if data is not None:
                parsed = _parse_yc_page(data, batch, region_keyword, country_code)
                results.extend(parsed)
                if parsed:
                    print(f"  [YC] Batch {batch} page {page_num}: {len(parsed)} companies")

    print(f"  [YC] Total collected ({country_code}): {len(results)} companies")
    return results


# ─────────────────────────────────────────
# SOURCE 1.5: Product Hunt RSS
# ─────────────────────────────────────────

PRODUCT_HUNT_FEED = "https://www.producthunt.com/feed"

def _process_producthunt_entries(feed: object, seen_companies: set) -> list[dict]:
    """Process Product Hunt feed entries."""
    results = []
    for entry in getattr(feed, "entries", []):
        title_full = entry.get("title", "")
        summary = entry.get("summary", "")
        link = entry.get("link", "")
        
        if " - " in title_full:
            company_name = title_full.split(" - ", 1)[0].strip()
        else:
            company_name = title_full.strip()
            
        key = company_name.lower()
        if key in seen_companies or not company_name:
            continue
            
        if len(company_name) > 40 or len(company_name) < 2:
            continue
            
        seen_companies.add(key)
        clean_summary = BeautifulSoup(summary, "html.parser").get_text(separator=' ', strip=True) if summary else title_full

        results.append({
            "company_name": company_name,
            "website": "",
            "description": f"Product Hunt Launch: {clean_summary[:500]}",
            "funding_amount": "",
            "article_title": title_full,
            "article_url": link,
            "days_ago": 0,
            "source": "ProductHunt",
            "is_hiring": False,
            "team_size": 0,
            "industries": [],
            "locations": [],
            "yc_batch": "",
            "country_code": None,
        })
    return results

async def scrape_product_hunt(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Scrapes the Product Hunt home feed for new launches."""
    print("\n[ProductHunt] Scraping recent launches...")
    seen_companies: set[str] = set()
    results: list[dict] = []
    
    print(f"  [PH] -> Fetching feed: {PRODUCT_HUNT_FEED}")
    try:
        resp = await fetch_with_retry(client, PRODUCT_HUNT_FEED, semaphore)
        feed = feedparser.parse(resp.text)
        print(f"  [PH] ✓ Fetched feed: {PRODUCT_HUNT_FEED}")
        
        parsed = _process_producthunt_entries(feed, seen_companies)
        results.extend(parsed)
        print(f"  [PH] Feed: {PRODUCT_HUNT_FEED} — {len(getattr(feed, 'entries', []))} entries")
    except Exception as e:
        print(f"  [PH] ✗ Error fetching Product Hunt: {e}")
        
    print(f"  [ProductHunt] Total companies: {len(results)}")
    return results

# ─────────────────────────────────────────
# SOURCE 2: TechCrunch RSS — Funding News
# ─────────────────────────────────────────

TECHCRUNCH_FEEDS = [
    "https://techcrunch.com/feed/",
    "https://techcrunch.com/category/startups/feed/",
    "https://techcrunch.com/category/venture/feed/",
]

FUNDING_KEYWORDS = [
    "raises", "funding", "series a", "series b", "seed round",
    "million", "investment", "backed", "venture", "capital",
]


def extract_company_from_title(title: str) -> str:
    """Extracts company name from funding/hiring news article titles."""
    title = re.sub(
        r"^(Funding:|Exclusive:|Breaking:|Report:|Scoop:)\s*",
        "", title.strip(), flags=re.IGNORECASE
    )

    action_verbs = (
        r"raises|secures|lands|closes|gets|nabs|bags|announces|valued|"
        r"launches|acquires|partners|expands|appoints|hires|names|"
        r"completes|receives|wins|scores|snags|pulls in|picks up|"
        r"increases|doubles|triples|grows|adds|joins|unveils|debuts|"
        r"introduces|releases|ships|builds|creates|develops"
    )

    word_pat = r"[A-Z][A-Za-z0-9\.\&\-]{0,29}"
    name_pat = rf"({word_pat}(?:\s+{word_pat}){{0,3}})"

    action_verb_set = {
        "raises", "secures", "lands", "closes", "gets", "nabs", "bags",
        "announces", "valued", "launches", "acquires", "partners", "expands",
        "appoints", "hires", "names", "completes", "receives", "wins", "scores",
        "snags", "increases", "doubles", "triples", "grows", "adds", "joins",
        "unveils", "debuts", "introduces", "releases", "ships", "builds",
        "creates", "develops",
    }

    start_patterns = [
        rf"^{name_pat}\s+(?:{action_verbs})",
        rf"^{name_pat},\s+[a-z].*?,\s+(?:{action_verbs})",
        rf"^{name_pat}\s*[:\-–]\s*(?:{action_verbs})",
    ]

    for pattern in start_patterns:
        match = re.match(pattern, title.strip())
        if match:
            name = match.group(1).strip().rstrip(",.;:")
            last_word = name.split()[-1].lower() if name else ""
            if last_word in action_verb_set:
                continue
            if _is_valid_company_name(name):
                return name

    title_case_verbs = (
        r"Raises|Secures|Lands|Closes|Gets|Nabs|Bags|Announces|Valued|"
        r"Launches|Acquires|Partners|Expands|Completes|Receives|Wins|"
        r"Increases|Doubles|Triples|Grows|Adds|Unveils|Debuts|Introduces"
    )
    tc_match = re.match(rf"^{name_pat}\s+(?:{title_case_verbs})\b", title.strip())
    if tc_match:
        name = tc_match.group(1).strip().rstrip(",.;:")
        last_word = name.split()[-1].lower() if name else ""
        if last_word not in action_verb_set and _is_valid_company_name(name):
            return name

    mid_patterns = [
        rf"\bas\s+([A-Z][A-Za-z0-9\.\&\-]{{1,30}}(?:\s+[A-Z][A-Za-z0-9\.\&\-]{{1,20}}){{0,3}})\s+(?:{action_verbs})",
        rf"\bfor\s+([A-Z][A-Za-z0-9\.\&\-]{{1,30}}(?:\s+[A-Z][A-Za-z0-9\.\&\-]{{1,20}}){{0,3}})\s+(?:{action_verbs})",
    ]
    for pattern in mid_patterns:
        match = re.search(pattern, title)
        if match:
            name = match.group(1).strip().rstrip(",.;:")
            if _is_valid_company_name(name):
                return name

    funding_context = re.search(
        r"(?:startup|platform|company|firm|app)\s+([A-Z][A-Za-z0-9\.\&]{2,25})\s+(?:in\s+funding|raises|secures|valued)",
        title, re.IGNORECASE
    )
    if funding_context:
        name = funding_context.group(1).strip()
        if _is_valid_company_name(name):
            return name

    return ""


def _is_valid_company_name(name: str) -> bool:
    if len(name) < 2 or len(name) > 60:
        return False
    generic = {
        "the", "a", "an", "this", "new", "top", "best", "how", "why",
        "what", "when", "where", "who", "startup", "company", "firm",
        "platform", "software", "app", "funding", "series", "round",
        "million", "billion", "vc", "tech", "saas", "report",
        "exclusive", "breaking", "google", "apple", "microsoft", "amazon",
        "meta", "openai", "anthropic",
        "almost", "nearly", "over", "under", "about", "just", "only",
    }
    if name.lower() == "ai":
        return False
    if name.lower() in generic:
        return False
    if not name[0].isupper():
        return False
    return True


def parse_funding_amount(text: str) -> str:
    match = re.search(r"\$[\d\.]+\s*[MB]illion|\$[\d\.]+[MB]", text, re.IGNORECASE)
    return match.group(0) if match else ""


def _process_techcrunch_entries(feed: object, seen_titles: set, cutoff_date: datetime) -> list[dict]:
    """Process TechCrunch feed entries into results."""
    results = []
    for entry in getattr(feed, "entries", []):
        title = entry.get("title", "")
        summary = entry.get("summary", "")
        link = entry.get("link", "")

        if title in seen_titles:
            continue
        seen_titles.add(title)

        combined = title.lower() + " " + summary.lower()
        if not any(kw in combined for kw in FUNDING_KEYWORDS):
            continue

        published = entry.get("published_parsed")
        if published:
            pub_date = datetime(*published[:6])
            if pub_date < cutoff_date:
                continue
            days_ago = (datetime.now() - pub_date).days
        else:
            days_ago = 999

        company_name = extract_company_from_title(title)
        if not company_name:
            continue

        results.append({
            "company_name": company_name,
            "website": "",
            "description": summary[:300],
            "funding_amount": parse_funding_amount(title + " " + summary),
            "article_title": title,
            "article_url": link,
            "days_ago": days_ago,
            "source": "TechCrunch",
            "is_hiring": False,
            "team_size": 0,
            "industries": [],
            "locations": [],
            "yc_batch": "",
            "country_code": None,
        })
    return results


async def scrape_techcrunch(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """
    TechCrunch is a global feed — no geo filter applied.
    Country code is left as None (enriched later from company location).
    """
    results: list[dict] = []
    seen_titles: set[str] = set()
    cutoff_date = datetime.now() - timedelta(days=180)
    print("\n[TechCrunch] Scraping funding news...")

    async def fetch_feed(url: str) -> tuple[str, object | None]:
        print(f"  [TC] -> Fetching feed: {url}")
        try:
            resp = await fetch_with_retry(client, url, semaphore)
            feed = feedparser.parse(resp.text)
            print(f"  [TC] ✓ Fetched feed: {url}")
            return url, feed
        except Exception as e:
            print(f"  [TC] ✗ Error fetching {url}: {e}")
            return url, None

    feeds = await asyncio.gather(*[fetch_feed(url) for url in TECHCRUNCH_FEEDS])
    for feed_url, feed in feeds:
        if feed is not None:
            parsed = _process_techcrunch_entries(feed, seen_titles, cutoff_date)
            results.extend(parsed)
            print(f"  [TC] Feed: {feed_url} — {len(getattr(feed, 'entries', []))} entries")

    print(f"  [TechCrunch] Total funding articles: {len(results)}")
    return results


# ─────────────────────────────────────────
# SOURCE 3: Google News RSS — per geography
# ─────────────────────────────────────────

def _process_google_news_entries(
    feed: object, query: str, seen: set[str], cutoff_date: datetime, country_code: str
) -> list[dict]:
    """Process Google News feed entries."""
    results = []
    for entry in getattr(feed, "entries", []):
        title = entry.get("title", "")
        link = entry.get("link", "")

        if title in seen:
            continue
        seen.add(title)

        if not any(kw in title.lower() for kw in FUNDING_KEYWORDS):
            continue

        published = entry.get("published_parsed")
        if published:
            pub_date = datetime(*published[:6])
            if pub_date < cutoff_date:
                continue
            days_ago = (datetime.now() - pub_date).days
        else:
            days_ago = 999

        company_name = extract_company_from_title(title)
        if not company_name:
            continue

        results.append({
            "company_name": company_name,
            "website": "",
            "description": title,
            "funding_amount": parse_funding_amount(title),
            "article_title": title,
            "article_url": link,
            "days_ago": days_ago,
            "source": "GoogleNews",
            "is_hiring": "hiring" in title.lower(),
            "team_size": 0,
            "industries": [],
            "locations": [],
            "yc_batch": "",
            "country_code": country_code,
        })
    return results


async def scrape_google_news(
    country_code: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """
    Uses Google News RSS to find recently funded companies.
    Uses per-geography gl= and ceid= parameters.
    """
    geo = GEOGRAPHY_CONFIG.get(country_code, GEOGRAPHY_CONFIG["US"])
    gl = geo["google_news_gl"]
    hl = geo["google_news_hl"]
    ceid = geo["google_news_ceid"]
    # Use India-specific queries when running for IN; fall back to global pool
    query_pool = GOOGLE_NEWS_QUERIES_IN if country_code == "IN" else GOOGLE_NEWS_QUERIES
    results: list[dict] = []
    seen: set[str] = set()
    cutoff_date = datetime.now() - timedelta(days=90)
    print(f"\n[Google News] Scraping funding + hiring signals for {country_code}...")

    urls = [
        f"https://news.google.com/rss/search?q={q.replace(' ', '+')}&hl={hl}&gl={gl}&ceid={ceid}"
        for q in query_pool
    ]

    async def fetch_feed(url: str, q: str) -> tuple[str, object | None]:
        print(f"  [GNews] -> Fetching query: '{q}'...")
        try:
            resp = await fetch_with_retry(client, url, semaphore)
            print(f"  [GNews] ✓ Fetched query: '{q}'")
            return q, feedparser.parse(resp.text)
        except Exception as e:
            print(f"  [GNews] ✗ Error on query '{q}': {e}")
            return q, None

    tasks = [fetch_feed(url, q) for url, q in zip(urls, query_pool)]
    feeds = await asyncio.gather(*tasks)
    for query, feed in feeds:
        if feed is not None:
            parsed = _process_google_news_entries(feed, query, seen, cutoff_date, country_code)
            results.extend(parsed)
            print(f"  [GNews] Query '{query}': {len(getattr(feed, 'entries', []))} entries")

    print(f"  [Google News] Total articles ({country_code}): {len(results)}")
    return results


# ─────────────────────────────────────────
# SOURCE 4: LinkedIn Public Jobs — per geography
# ─────────────────────────────────────────

LINKEDIN_EXCLUDE_COMPANIES = {
    "meta", "google", "amazon", "microsoft", "apple", "netflix", "salesforce",
    "oracle", "ibm", "intel", "cisco", "adobe", "uber", "airbnb", "lyft",
    "twitter", "linkedin", "facebook", "walmart", "target", "costco",
    "chipotle", "mcdonald", "starbucks", "jpmorgan", "goldman", "deloitte",
    "accenture", "mckinsey", "pwc", "kpmg", "ey", "bain", "bcg",
    "mrbeast", "zara", "pandora", "carnival", "j.crew", "spacex",
    "duolingo", "asana", "ripple", "opentable", "seatgeek", "moloco",
    "pigment", "orchestra", "flex", "huntress", "appian", "wpp",
    "emarketer", "gusto", "glossgenius", "dorsia", "videamp",
    "rivian", "tesla", "ford", "gm", "boeing", "lockheed",
    "bank of america", "wells fargo", "citi", "vanguard", "fidelity",
    "blackrock", "four seasons", "hilton", "marriott", "hyatt",
    "sony", "disney", "warner", "universal", "equifax", "experian",
    "transunion", "ross stores", "bath", "bj's", "gnc", "chanel",
}


def _parse_linkedin_page(html: str, search_label: str) -> list[tuple[str, str, str]]:
    """Parse LinkedIn job page HTML. Returns list of (company_name, job_title, location)."""
    soup = BeautifulSoup(html, "html.parser")
    company_els = soup.select(".base-search-card__subtitle")
    title_els = soup.select("h3.base-search-card__title")
    location_els = soup.select(".job-search-card__location")
    out: list[tuple[str, str, str]] = []
    for company_el, title_el, loc_el in zip(company_els, title_els, location_els):
        company_name = company_el.get_text(strip=True)
        job_title = title_el.get_text(strip=True)
        location = loc_el.get_text(strip=True) if loc_el else ""
        out.append((company_name, job_title, location))
    return out


async def scrape_linkedin_hr_jobs(
    country_code: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """
    Scrapes LinkedIn public job listing pages for companies hiring HR/TA roles.
    Uses per-geography LinkedIn location parameter.
    """
    geo = GEOGRAPHY_CONFIG.get(country_code, GEOGRAPHY_CONFIG["US"])
    linkedin_location = geo["linkedin_location"]
    searches = [
        {"label": label, "url": url_template.replace("{linkedin_location}", linkedin_location.replace(" ", "+"))}
        for label, url_template in LINKEDIN_SEARCH_TEMPLATES.items()
    ]
    LINKEDIN_PAGES_PER_SEARCH = 4

    urls: list[tuple[str, str, int]] = []
    for search in searches:
        for page_num in range(LINKEDIN_PAGES_PER_SEARCH):
            start = page_num * 25
            url = search["url"] + ("&" if "?" in search["url"] else "?") + f"start={start}"
            urls.append((search["label"], url, page_num + 1))

    print(f"\n[LinkedIn] Scraping HR/TA job listings for {country_code} ({linkedin_location})...")

    async def fetch_page(label: str, url: str, p: int) -> tuple[str, int, str | None]:
        print(f"  [LI] -> Fetching '{label}' page {p}...")
        try:
            resp = await fetch_with_retry(client, url, semaphore)
            if resp.status_code != 200:
                print(f"  [LI] ✗ '{label}' p{p} FAILED: HTTP {resp.status_code}")
                return label, p, None
            print(f"  [LI] ✓ '{label}' p{p} SUCCESS")
            return label, p, resp.text
        except Exception as e:
            print(f"  [LI] ✗ Error on '{label} p{p}': {e}")
            return label, p, None

    tasks = [fetch_page(label, url, p) for label, url, p in urls]
    pages = await asyncio.gather(*tasks)

    results: list[dict] = []
    seen_companies: set[str] = set()
    search_counts: dict[str, int] = {s["label"]: 0 for s in searches}

    for (label, p, html) in pages:
        if html is None:
            continue
        tuples = _parse_linkedin_page(html, label)
        count = 0
        for company_name, job_title, location in tuples:
            if company_name.lower().split()[0] in LINKEDIN_EXCLUDE_COMPANIES:
                continue
            if any(excl in company_name.lower() for excl in LINKEDIN_EXCLUDE_COMPANIES):
                continue
            agency_keywords = ["staffing", "recruiting", "talent agency", "search firm", "headhunter"]
            if any(kw in company_name.lower() for kw in agency_keywords):
                continue
            key = company_name.lower().strip()
            if key in seen_companies:
                for r_item in results:
                    if r_item["company_name"].lower() == key and job_title not in r_item.get("hr_roles_found", []):
                        r_item["hr_roles_found"].append(job_title)
                continue
            seen_companies.add(key)
            count += 1
            results.append({
                "company_name": company_name,
                "website": "",
                "description": f"Hiring: {job_title}",
                "team_size": 0,
                "industries": [],
                "locations": [location] if location else [],
                "yc_batch": "",
                "is_hiring": True,
                "hr_roles_found": [job_title],
                "source": "LinkedIn",
                "funding_amount": "",
                "article_title": "",
                "article_url": "",
                "days_ago": 0,
                "country_code": country_code,
            })
        search_counts[label] = search_counts.get(label, 0) + count

    for label in search_counts:
        print(f"  [LI] {label}: {search_counts[label]} new companies")
    print(f"  [LinkedIn] Total unique companies ({country_code}): {len(results)}")
    return results


# ─────────────────────────────────────────
# SOURCE 5: Seek.com.au — Australia only
# ─────────────────────────────────────────

SEEK_SEARCHES = [
    {"label": "Head of People",        "path": "/head-of-people-jobs"},
    {"label": "Talent Acquisition",    "path": "/talent-acquisition-manager-jobs"},
    {"label": "HR Manager",            "path": "/hr-manager-jobs"},
    {"label": "Recruiter",             "path": "/recruiter-jobs"},
    {"label": "HR Technology",         "path": "/hr-technology-manager-jobs"},
]


def _parse_seek_page(html: str) -> list[tuple[str, str, str]]:
    """Parse Seek job page. Returns list of (company_name, job_title, location)."""
    soup = BeautifulSoup(html, "html.parser")
    job_cards = soup.select("[data-automation='normalJob']")
    out: list[tuple[str, str, str]] = []
    for card in job_cards:
        company_el = card.select_one("[data-automation='jobCompany']")
        title_el = card.select_one("[data-automation='jobTitle']")
        loc_el = card.select_one("[data-automation='jobLocation']")
        if not company_el or not title_el:
            continue
        company_name = company_el.get_text(strip=True)
        job_title = title_el.get_text(strip=True)
        location = loc_el.get_text(strip=True) if loc_el else "Australia"
        out.append((company_name, job_title, location))
    return out


async def scrape_seek(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Scrapes Seek.com.au for AU companies hiring HR/TA roles."""
    base_url = "https://www.seek.com.au"
    SEEK_PAGES_PER_SEARCH = 3
    urls: list[tuple[str, str, int]] = []
    for search in SEEK_SEARCHES:
        for page in range(1, SEEK_PAGES_PER_SEARCH + 1):
            url = f"{base_url}{search['path']}?where=All+Australia&daterange=30&page={page}"
            urls.append((search["label"], url, page))

    print("\n[Seek] Scraping HR/TA job listings for AU...")

    async def fetch_page(label: str, url: str, p: int) -> tuple[str, int, str | None]:
        print(f"  [Seek] -> Fetching '{label}' page {p}...")
        try:
            resp = await fetch_with_retry(client, url, semaphore)
            if resp.status_code != 200:
                print(f"  [Seek] ✗ '{label}' p{p} FAILED: HTTP {resp.status_code}")
                return label, p, None
            print(f"  [Seek] ✓ '{label}' p{p} SUCCESS")
            return label, p, resp.text
        except Exception as e:
            print(f"  [Seek] ✗ Error on '{label} p{p}': {e}")
            return label, p, None

    pages = await asyncio.gather(*[fetch_page(l, u, p) for l, u, p in urls])
    results: list[dict] = []
    seen_companies: set[str] = set()
    search_counts: dict[str, int] = {s["label"]: 0 for s in SEEK_SEARCHES}

    for (label, p, html) in pages:
        if html is None:
            continue
        tuples = _parse_seek_page(html)
        count = 0
        for company_name, job_title, location in tuples:
            key = company_name.lower().strip()
            if key in seen_companies:
                continue
            seen_companies.add(key)
            count += 1
            results.append({
                "company_name": company_name,
                "website": "",
                "description": f"Hiring: {job_title}",
                "team_size": 0,
                "industries": [],
                "locations": [location],
                "yc_batch": "",
                "is_hiring": True,
                "hr_roles_found": [job_title],
                "source": "Seek",
                "funding_amount": "",
                "article_title": "",
                "article_url": "",
                "days_ago": 0,
                "country_code": "AU",
            })
        search_counts[label] = search_counts.get(label, 0) + count

    for label in SEEK_SEARCHES:
        print(f"  [Seek] {label['label']}: {search_counts.get(label['label'], 0)} new companies")
    print(f"  [Seek] Total unique companies: {len(results)}")
    return results


# ─────────────────────────────────────────
# SOURCE 6: Reed.co.uk — UK only
# ─────────────────────────────────────────

REED_SEARCHES = [
    {"label": "Head of People",     "keyword": "Head+of+People"},
    {"label": "Talent Acquisition", "keyword": "Talent+Acquisition+Manager"},
    {"label": "HR Manager",         "keyword": "HR+Manager"},
    {"label": "Recruiter",          "keyword": "Recruiter"},
    {"label": "HR Technology",      "keyword": "HR+Technology+Manager"},
]


def _parse_reed_page(html: str) -> list[tuple[str, str, str]]:
    """Parse Reed job page. Returns list of (company_name, job_title, location)."""
    soup = BeautifulSoup(html, "html.parser")
    job_cards = soup.select("article.job-result")
    out: list[tuple[str, str, str]] = []
    for card in job_cards:
        company_el = card.select_one(".recruiter")
        title_el = card.select_one("h3.title")
        loc_el = card.select_one(".job-metadata__item--location")
        if not company_el or not title_el:
            continue
        company_name = company_el.get_text(strip=True)
        job_title = title_el.get_text(strip=True)
        location = loc_el.get_text(strip=True) if loc_el else "United Kingdom"
        out.append((company_name, job_title, location))
    return out


async def scrape_reed(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Scrapes Reed.co.uk for UK companies hiring HR/TA roles."""
    base_url = "https://www.reed.co.uk"
    REED_PAGES_PER_SEARCH = 3
    urls: list[tuple[str, str, int]] = []
    for search in REED_SEARCHES:
        path = f"/jobs/{search['keyword'].lower().replace('+', '-')}-jobs"
        for page in range(1, REED_PAGES_PER_SEARCH + 1):
            url = f"{base_url}{path}?page={page}" if page > 1 else f"{base_url}{path}"
            urls.append((search["label"], url, page))

    print("\n[Reed] Scraping HR/TA job listings for GB...")

    async def fetch_page(label: str, url: str, p: int) -> tuple[str, int, str | None]:
        print(f"  [Reed] -> Fetching '{label}' page {p}...")
        try:
            resp = await fetch_with_retry(client, url, semaphore)
            if resp.status_code != 200:
                print(f"  [Reed] ✗ '{label}' p{p} FAILED: HTTP {resp.status_code}")
                return label, p, None
            print(f"  [Reed] ✓ '{label}' p{p} SUCCESS")
            return label, p, resp.text
        except Exception as e:
            print(f"  [Reed] ✗ Error on '{label} p{p}': {e}")
            return label, p, None

    pages = await asyncio.gather(*[fetch_page(l, u, p) for l, u, p in urls])
    results: list[dict] = []
    seen_companies: set[str] = set()
    search_counts: dict[str, int] = {s["label"]: 0 for s in REED_SEARCHES}

    for (label, p, html) in pages:
        if html is None:
            continue
        tuples = _parse_reed_page(html)
        count = 0
        for company_name, job_title, location in tuples:
            key = company_name.lower().strip()
            if key in seen_companies:
                continue
            seen_companies.add(key)
            count += 1
            results.append({
                "company_name": company_name,
                "website": "",
                "description": f"Hiring: {job_title}",
                "team_size": 0,
                "industries": [],
                "locations": [location],
                "yc_batch": "",
                "is_hiring": True,
                "hr_roles_found": [job_title],
                "source": "Reed",
                "funding_amount": "",
                "article_title": "",
                "article_url": "",
                "days_ago": 0,
                "country_code": "GB",
            })
        search_counts[label] = search_counts.get(label, 0) + count

    for search in REED_SEARCHES:
        print(f"  [Reed] {search['label']}: {search_counts.get(search['label'], 0)} new companies")
    print(f"  [Reed] Total unique companies: {len(results)}")
    return results


# ─────────────────────────────────────────
# SOURCE 7: Naukri.com — India only
# Uses Naukri's internal JSON search API (jobapi/v3/search)
# HTML scraping does not work — Naukri renders job cards via React (JS).
# ─────────────────────────────────────────

NAUKRI_SEARCHES = [
    {"label": "Head of People",     "keyword": "head of people",          "slug": "head-of-people"},
    {"label": "Talent Acquisition", "keyword": "talent acquisition manager", "slug": "talent-acquisition-manager"},
    {"label": "HR Manager",         "keyword": "hr manager",              "slug": "hr-manager"},
    {"label": "Recruiter",          "keyword": "recruiter",               "slug": "recruiter"},
    {"label": "HR Technology",      "keyword": "hr technology manager",   "slug": "hr-technology-manager"},
]

# Naukri's internal API — same endpoint their React frontend hits
NAUKRI_API_BASE = "https://www.naukri.com/jobapi/v3/search"

# Headers Naukri's own web app sends with every search request
# NOTE: Content-Type must NOT be set on GET requests — causes HTTP 406 rejection
NAUKRI_API_HEADERS = {
    "Appid":           "109",
    "Systemid":        "109",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,en-IN;q=0.8",
    "Referer":         "https://www.naukri.com/",
    "Origin":          "https://www.naukri.com",
    "User-Agent":      (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
}


def _parse_naukri_api_response(data: dict) -> list[tuple[str, str, str]]:
    """
    Parse a single Naukri JSON API response page.
    Returns list of (company_name, job_title, location).
    """
    out: list[tuple[str, str, str]] = []
    jobs = data.get("jobDetails", [])
    for job in jobs:
        company_name = (
            job.get("companyName", "")
            or job.get("company", {}).get("label", "")
        ).strip()
        job_title = (
            job.get("title", "")
            or job.get("jobTitle", "")
        ).strip()
        # locations is a list of dicts with .label, or a plain string
        locations_raw = job.get("placeholders", [])
        location = "India"
        for ph in locations_raw:
            if ph.get("type") == "location":
                location = ph.get("label", "India")
                break
        if company_name and len(company_name) > 1:
            out.append((company_name, job_title, location))
    return out


async def scrape_naukri(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """
    Fetches Naukri's internal JSON search API for IN companies hiring HR/TA roles.
    Naukri is India's dominant job board — 70%+ of Indian job listings.
    The HTML page is React-rendered and returns no job cards to a plain GET.
    The JSON API returns structured company+title+location data directly.
    """
    NAUKRI_PAGES_PER_SEARCH = 3
    RESULTS_PER_PAGE = 20

    # Build list of (label, api_url, page_number)
    api_calls: list[tuple[str, str, int]] = []
    for search in NAUKRI_SEARCHES:
        for page in range(1, NAUKRI_PAGES_PER_SEARCH + 1):
            params = (
                f"noOfResults={RESULTS_PER_PAGE}"
                f"&urlType=search_by_key_loc"
                f"&searchType=adv"
                f"&keyword={search['keyword'].replace(' ', '%20')}"
                f"&pageNo={page}"
                f"&seoKey={search['slug']}-jobs"
                f"&src=jobsearchDesk"
            )
            url = f"{NAUKRI_API_BASE}?{params}"
            api_calls.append((search["label"], url, page))

    print("\n[Naukri] Fetching HR/TA job listings via JSON API for IN (India)...")

    async def fetch_api(label: str, url: str, p: int) -> tuple[str, int, dict | None]:
        print(f"  [Naukri] -> Fetching '{label}' page {p}...")
        try:
            resp = await client.get(url, headers=NAUKRI_API_HEADERS, timeout=15.0)
            if resp.status_code != 200:
                print(f"  [Naukri] ✗ '{label}' p{p} FAILED: HTTP {resp.status_code}")
                return label, p, None
            data = resp.json()
            print(f"  [Naukri] ✓ '{label}' p{p} SUCCESS ({len(data.get('jobDetails', []))} jobs)")
            return label, p, data
        except Exception as e:
            print(f"  [Naukri] ✗ Error on '{label}' p{p}: {e}")
            return label, p, None

    # Run under the strict semaphore to be polite with Naukri's servers
    async def fetch_with_sem(label: str, url: str, p: int) -> tuple[str, int, dict | None]:
        async with semaphore:
            return await fetch_api(label, url, p)

    responses = await asyncio.gather(*[fetch_with_sem(l, u, p) for l, u, p in api_calls])

    results: list[dict] = []
    seen_companies: set[str] = set()
    search_counts: dict[str, int] = {s["label"]: 0 for s in NAUKRI_SEARCHES}

    for (label, p, data) in responses:
        if data is None:
            continue
        tuples = _parse_naukri_api_response(data)
        count = 0
        for company_name, job_title, location in tuples:
            key = company_name.lower().strip()
            if key in seen_companies:
                for r_item in results:
                    if r_item["company_name"].lower() == key and job_title not in r_item.get("hr_roles_found", []):
                        r_item["hr_roles_found"].append(job_title)
                continue
            seen_companies.add(key)
            count += 1
            results.append({
                "company_name": company_name,
                "website":      "",
                "description":  f"Hiring: {job_title}",
                "team_size":    0,
                "industries":   [],
                "locations":    [location],
                "yc_batch":     "",
                "is_hiring":    True,
                "hr_roles_found": [job_title],
                "source":       "Naukri",
                "funding_amount": "",
                "article_title": "",
                "article_url":  "",
                "days_ago":     0,
                "country_code": "IN",
            })
        search_counts[label] = search_counts.get(label, 0) + count

    for search in NAUKRI_SEARCHES:
        print(f"  [Naukri] {search['label']}: {search_counts.get(search['label'], 0)} new companies")
    print(f"  [Naukri] Total unique companies: {len(results)}")
    return results


# ─────────────────────────────────────────
# MAIN: Run all sources with geography awareness
# ─────────────────────────────────────────


async def _run_all_sources_async(config: RunConfig) -> list[dict]:
    """Async implementation: runs all scrapers with shared httpx client and semaphore."""
    yc_total = ph_total = tc_total = gn_total = li_total = seek_total = reed_total = naukri_total = 0

    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        semaphore_strict = asyncio.Semaphore(MAX_CONCURRENT_STRICT)  # YC, LinkedIn, Seek, Reed
        all_results = []

        # Product Hunt is global — run once
        if getattr(config, "use_product_hunt", False):
            print("\n[SCRAPER] Starting ProductHunt...", flush=True)
            ph_results = await scrape_product_hunt(client, semaphore)
            all_results.extend(ph_results)
            ph_total = len(ph_results)

        # TechCrunch is global — run once
        if config.use_techcrunch:
            print("\n[SCRAPER] Starting TechCrunch...", flush=True)
            tc_results = await scrape_techcrunch(client, semaphore)
            all_results.extend(tc_results)
            tc_total = len(tc_results)

        # Per-geography sources — run geo-scrapers in parallel where possible
        # Brief pause between geographies lets rate limits (YC, LinkedIn) cool down
        for i, geo in enumerate(config.geographies):
            if i > 0:
                await asyncio.sleep(5)
            print(f"\n{'='*50}", flush=True)
            print(f"  GEOGRAPHY: {geo}", flush=True)
            print(f"{'='*50}", flush=True)

            SOURCE_TIMEOUT = 90  # hard kill per source — prevents TCP hangs blocking forever

            async def run_with_timeout(coro, name: str) -> list[dict]:
                try:
                    return await asyncio.wait_for(coro, timeout=SOURCE_TIMEOUT)
                except asyncio.TimeoutError:
                    print(f"  [SCRAPER] ⚠ {name} timed out after {SOURCE_TIMEOUT}s — skipping", flush=True)
                    return []
                except Exception as e:
                    print(f"  [SCRAPER] ✗ {name} error: {e}", flush=True)
                    return []

            tasks: list[asyncio.Task] = []
            task_names: list[str] = []
            if config.use_yc:
                tasks.append(asyncio.create_task(run_with_timeout(scrape_yc(geo, client, semaphore_strict), f"YC/{geo}")))
                task_names.append(f"YC/{geo}")
            if config.use_google_news:
                tasks.append(asyncio.create_task(run_with_timeout(scrape_google_news(geo, client, semaphore), f"GoogleNews/{geo}")))
                task_names.append(f"GoogleNews/{geo}")
            if config.use_linkedin:
                tasks.append(asyncio.create_task(run_with_timeout(scrape_linkedin_hr_jobs(geo, client, semaphore_strict), f"LinkedIn/{geo}")))
                task_names.append(f"LinkedIn/{geo}")
            if config.use_seek and geo == "AU":
                tasks.append(asyncio.create_task(run_with_timeout(scrape_seek(client, semaphore_strict), "Seek/AU")))
                task_names.append("Seek/AU")
            if config.use_reed and geo == "GB":
                tasks.append(asyncio.create_task(run_with_timeout(scrape_reed(client, semaphore_strict), "Reed/GB")))
                task_names.append("Reed/GB")
            if getattr(config, "use_naukri", True) and geo == "IN":
                tasks.append(asyncio.create_task(run_with_timeout(scrape_naukri(client, semaphore_strict), "Naukri/IN")))
                task_names.append("Naukri/IN")

            if tasks:
                print(f"  [SCRAPER] Starting {len(tasks)} sources for {geo}: {', '.join(task_names)}", flush=True)
                geo_results = await asyncio.gather(*tasks)
                for res in geo_results:
                    all_results.extend(res)
                    if any(r.get("source") == "YC" for r in res):
                        yc_total += len([r for r in res if r.get("source") == "YC"])
                    if any(r.get("source") == "GoogleNews" for r in res):
                        gn_total += len([r for r in res if r.get("source") == "GoogleNews"])
                    if any(r.get("source") == "LinkedIn" for r in res):
                        li_total += len([r for r in res if r.get("source") == "LinkedIn"])
                    if any(r.get("source") == "Seek" for r in res):
                        seek_total += len(res)
                    if any(r.get("source") == "Reed" for r in res):
                        reed_total += len(res)

    # Recompute totals from all_results for accuracy
    yc_total     = len([r for r in all_results if r.get("source") == "YC"])
    ph_total     = len([r for r in all_results if r.get("source") == "ProductHunt"])
    gn_total     = len([r for r in all_results if r.get("source") == "GoogleNews"])
    li_total     = len([r for r in all_results if r.get("source") == "LinkedIn"])
    seek_total   = len([r for r in all_results if r.get("source") == "Seek"])
    reed_total   = len([r for r in all_results if r.get("source") == "Reed"])
    naukri_total = len([r for r in all_results if r.get("source") == "Naukri"])

    print(f"\n[SCRAPER] Total raw entries across all sources: {len(all_results)}")
    print(f"  YC:           {yc_total}")
    print(f"  ProductHunt:  {ph_total}")
    print(f"  TechCrunch:   {tc_total}")
    print(f"  Google News:  {gn_total}")
    print(f"  LinkedIn:     {li_total}")
    print(f"  Seek (AU):    {seek_total}")
    print(f"  Reed (GB):    {reed_total}")
    print(f"  Naukri (IN):  {naukri_total}")

    return all_results


def run_all_sources(config: RunConfig = None) -> list[dict]:
    """
    Runs all configured sources for all configured geographies.
    Returns combined raw list. Each item has country_code tagged.
    config: RunConfig object (defaults to US-only if not provided)
    """
    if config is None:
        config = RunConfig()
    return asyncio.run(_run_all_sources_async(config))
