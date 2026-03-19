import re
import time
import random
import asyncio
import threading
from http_client import get_session, DEFAULT_TIMEOUT, FAST_TIMEOUT  # shared session: retry + pooling
from bs4 import BeautifulSoup

HR_TITLE_KEYWORDS = [
    "recruiter", "talent acquisition", "talent partner",
    "hr manager", "hr director", "head of people", "head of hr",
    "people operations", "people ops", "hr business partner",
    "hr technology", "hris manager", "people technology",
    "hr systems", "talent operations", "recruiting manager",
    "vp of people", "chief people", "hr generalist",
]

JOB_TITLE_SIGNALS = [
    "engineer", "manager", "designer", "analyst", "lead",
    "director", "recruiter", "sales", "marketing", "product",
    "operations", "finance", "legal", "support", "data",
]

CAREER_PATHS = [
    "/careers", "/jobs", "/about/careers", "/company/careers",
    "/careers/", "/join", "/join-us", "/work-with-us", "/open-roles",
    "/opportunities", "/vacancies", "/career", "/hiring",
    "/about/jobs", "/en/careers", "/positions",
]

# Generic slugs that could match any company's ATS board
_GENERIC_SLUGS = {
    "app", "web", "go", "get", "my", "the", "hi", "hey", "io",
    "api", "dev", "run", "try", "use", "one", "all", "top",
    "lab", "hub", "now", "pro", "ai", "hr", "us", "uk",
}

# Common suffixes to strip when building short slug variants
_STRIP_SUFFIXES = [
    " group", " ltd", " inc", " llc", " limited", " corp", " co",
    " technologies", " technology", " solutions", " services",
    " software", " digital", " global", " international",
]


# ─────────────────────────────────────────────────────────────
#  Slug candidate generator
#  Returns multiple slug variations to maximise ATS hit rate
# ─────────────────────────────────────────────────────────────

def get_slug_candidates(company_name: str, website: str, company_url: str = "") -> list[str]:
    """
    Returns an ordered list of slug candidates to try against ATS APIs.
    Tries domain-derived slug first (most reliable), then name variants.
    company_url is the enricher-confirmed URL (may differ from original website).
    """
    candidates = []

    def _domain_slug(url: str):
        if not url:
            return ""
        domain = (url
                  .replace("https://", "")
                  .replace("http://", "")
                  .replace("www.", ""))
        return domain.split(".")[0].split("/")[0].lower().strip()

    # 1. Domain-derived slug from enricher URL (most trustworthy)
    enricher_slug = _domain_slug(company_url)
    if enricher_slug and enricher_slug not in candidates:
        candidates.append(enricher_slug)

    # 2. Domain-derived slug from original website
    website_slug = _domain_slug(website)
    if website_slug and website_slug not in candidates:
        candidates.append(website_slug)

    # 3. Full name → kebab-case (e.g. "Tenth Revolution Group" → "tenth-revolution-group")
    base = company_name.lower().strip()
    base = re.sub(r"[^a-z0-9\s-]", "", base)
    full_slug = re.sub(r"\s+", "-", base).strip("-")
    if full_slug and full_slug not in candidates:
        candidates.append(full_slug)

    # 4. Name without common suffixes (e.g. "Tenth Revolution Group" → "tenth-revolution")
    stripped = base
    for suffix in _STRIP_SUFFIXES:
        if stripped.endswith(suffix):
            stripped = stripped[: -len(suffix)].strip()
            break
    short_slug = re.sub(r"\s+", "-", stripped).strip("-")
    if short_slug and short_slug != full_slug and short_slug not in candidates:
        candidates.append(short_slug)

    # 5. No-hyphen compact slug (e.g. "10threvolution")
    compact = re.sub(r"-", "", full_slug)
    if compact and compact != full_slug and compact not in candidates:
        candidates.append(compact)

    # 6. First word only (only if >5 chars to avoid generic matches)
    first_word = full_slug.split("-")[0]
    if first_word and len(first_word) > 5 and first_word not in candidates:
        candidates.append(first_word)

    return candidates


# ─────────────────────────────────────────────────────────────
#  FALSE POSITIVE VALIDATOR
#  Prevents generic slugs from matching the wrong company
# ─────────────────────────────────────────────────────────────

def _validate_ats_match(company_name: str, slug: str, result: dict) -> bool:
    """
    Validates that an ATS result actually belongs to this company.
    Short/generic slugs can match any company's board — we need to confirm
    by checking that the job titles or board name relate to the company.

    Returns True if the match is trustworthy, False if likely a false positive.
    """
    # Domain-derived slugs > 5 chars are almost always correct
    if len(slug) > 5 and slug not in _GENERIC_SLUGS:
        return True

    # If the slug IS generic or very short, validate by checking job titles
    name_lower = company_name.lower().strip()
    name_words = set(re.sub(r"[^a-z0-9\s]", "", name_lower).split())
    # Remove common filler words
    filler = {"the", "and", "of", "in", "for", "a", "an", "pty", "ltd", "inc", "llc", "plc", "group", "co"}
    name_words -= filler

    if not name_words:
        return True  # Can't validate, let it pass

    # Check if any significant company name word appears in the job titles
    titles = result.get("all_titles", [])
    titles_text = " ".join(t.lower() for t in titles)

    # Also check the board URL for the company name
    board_url = result.get("ats_board_url", "").lower()

    # If the slug exactly matches the full company name kebab-case, it's fine
    name_slug = re.sub(r"\s+", "-", re.sub(r"[^a-z0-9\s]", "", name_lower)).strip("-")
    if slug == name_slug:
        return True

    # For short slugs: require at least one significant name word in titles or board
    for word in name_words:
        if len(word) >= 3 and (word in titles_text or word in board_url):
            return True

    return False


# ─────────────────────────────────────────────────────────────
#  ATS API checkers — each now returns ats_board_url + job_urls
# ─────────────────────────────────────────────────────────────

def check_greenhouse(slug: str) -> dict:
    """Checks Greenhouse public job board API. Returns job URLs as proof."""
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    try:
        r = get_session().get(url, timeout=FAST_TIMEOUT)
        if r.status_code == 200:
            jobs = r.json().get("jobs", [])
            if not jobs:
                return {"found": False}
            titles = [j.get("title", "") for j in jobs]
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            # Capture individual job URLs as proof
            job_urls = [
                {"title": j.get("title", ""), "url": j.get("absolute_url", "")}
                for j in jobs
                if j.get("absolute_url")
            ]
            # Human-facing board URL
            board_url = f"https://boards.greenhouse.io/{slug}"
            return {
                "found": True,
                "source": "Greenhouse",
                "ats_board_url": board_url,
                "total_jobs": len(jobs),
                "hr_roles": hr_roles,
                "job_urls": job_urls[:10],
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


def check_lever(slug: str) -> dict:
    """Checks Lever public job board API. Returns job URLs as proof."""
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    try:
        r = get_session().get(url, timeout=FAST_TIMEOUT)
        if r.status_code == 200:
            jobs = r.json()
            if not isinstance(jobs, list) or not jobs:
                return {"found": False}
            titles = [j.get("text", "") for j in jobs]
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            job_urls = [
                {"title": j.get("text", ""), "url": j.get("hostedUrl", "")}
                for j in jobs
                if j.get("hostedUrl")
            ]
            board_url = f"https://jobs.lever.co/{slug}"
            return {
                "found": True,
                "source": "Lever",
                "ats_board_url": board_url,
                "total_jobs": len(jobs),
                "hr_roles": hr_roles,
                "job_urls": job_urls[:10],
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


def check_workable(slug: str) -> dict:
    """Checks Workable public job widget API."""
    api_url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}/jobs"
    try:
        r = get_session().get(api_url, timeout=FAST_TIMEOUT)
        if r.status_code == 200:
            jobs = r.json().get("jobs", [])
            if not jobs:
                return {"found": False}
            titles = [j.get("title", "") for j in jobs]
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            # Workable job URLs: https://apply.workable.com/{slug}/j/{shortcode}
            job_urls = [
                {
                    "title": j.get("title", ""),
                    "url": f"https://apply.workable.com/{slug}/j/{j.get('shortcode', '')}",
                }
                for j in jobs
                if j.get("shortcode")
            ]
            board_url = f"https://apply.workable.com/{slug}"
            return {
                "found": True,
                "source": "Workable",
                "ats_board_url": board_url,
                "total_jobs": len(jobs),
                "hr_roles": hr_roles,
                "job_urls": job_urls[:10],
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


def check_ashby(slug: str) -> dict:
    """Checks Ashby public job board API."""
    api_url = f"https://jobs.ashbyhq.com/api/non-mercer-jobs/{slug}"
    try:
        r = get_session().get(api_url, timeout=FAST_TIMEOUT)
        if r.status_code == 200:
            jobs = r.json().get("jobs", [])
            if not jobs:
                return {"found": False}
            titles = [j.get("title", "") for j in jobs]
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            job_urls = [
                {
                    "title": j.get("title", ""),
                    "url": f"https://jobs.ashbyhq.com/{slug}/{j.get('id', '')}",
                }
                for j in jobs
                if j.get("id")
            ]
            board_url = f"https://jobs.ashbyhq.com/{slug}"
            return {
                "found": True,
                "source": "Ashby",
                "ats_board_url": board_url,
                "total_jobs": len(jobs),
                "hr_roles": hr_roles,
                "job_urls": job_urls[:10],
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


def check_teamtailor(slug: str) -> dict:
    """Checks TeamTailor job board (popular in Europe & AU)."""
    api_url = f"https://{slug}.teamtailor.com/jobs.json"
    try:
        r = get_session().get(api_url, timeout=FAST_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            raw_jobs = data if isinstance(data, list) else data.get("data", [])
            titles = []
            job_urls = []
            for j in raw_jobs:
                attrs = j.get("attributes", {}) if isinstance(j, dict) else {}
                t = attrs.get("title", "") or j.get("title", "")
                link = attrs.get("apply-url", "") or j.get("links", {}).get("careersite-job-url", "")
                if t:
                    titles.append(t)
                    if link:
                        job_urls.append({"title": t, "url": link})
            if not titles:
                return {"found": False}
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            board_url = f"https://{slug}.teamtailor.com/jobs"
            return {
                "found": True,
                "source": "TeamTailor",
                "ats_board_url": board_url,
                "total_jobs": len(titles),
                "hr_roles": hr_roles,
                "job_urls": job_urls[:10],
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


def check_bamboohr(slug: str) -> dict:
    """Checks BambooHR public job board."""
    url = f"https://{slug}.bamboohr.com/jobs/embed2.php"
    try:
        r = get_session().get(url, timeout=FAST_TIMEOUT)
        if r.status_code == 200 and len(r.text) > 500:
            soup = BeautifulSoup(r.text, "html.parser")
            titles = [el.get_text(strip=True) for el in soup.find_all(class_="jss-name")]
            if not titles:
                titles = [
                    li.get_text(strip=True) for li in soup.find_all("li")
                    if 5 < len(li.get_text(strip=True)) < 80
                ]
            if not titles:
                return {"found": False}
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            board_url = f"https://{slug}.bamboohr.com/jobs/"
            return {
                "found": True,
                "source": "BambooHR",
                "ats_board_url": board_url,
                "total_jobs": len(titles),
                "hr_roles": hr_roles,
                "job_urls": [],  # BambooHR embed doesn't expose direct job links easily
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


def check_breezy(slug: str) -> dict:
    """Checks Breezy HR public job board."""
    url = f"https://{slug}.breezy.hr/json"
    try:
        r = get_session().get(url, timeout=FAST_TIMEOUT)
        if r.status_code == 200:
            jobs = r.json() if isinstance(r.json(), list) else []
            if not jobs:
                return {"found": False}
            titles = [j.get("name", "") for j in jobs]
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            job_urls = [
                {"title": j.get("name", ""), "url": f"https://{slug}.breezy.hr/p/{j.get('friendly_id', '')}"}
                for j in jobs
                if j.get("friendly_id")
            ]
            board_url = f"https://{slug}.breezy.hr"
            return {
                "found": True,
                "source": "BreezyHR",
                "ats_board_url": board_url,
                "total_jobs": len(titles),
                "hr_roles": hr_roles,
                "job_urls": job_urls[:10],
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


def check_smartrecruiters(slug: str) -> dict:
    """Checks SmartRecruiters public job postings."""
    api_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    try:
        r = get_session().get(api_url, timeout=FAST_TIMEOUT)
        if r.status_code == 200:
            jobs = r.json().get("content", [])
            if not jobs:
                return {"found": False}
            titles = [j.get("name", "") for j in jobs]
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            job_urls = [
                {"title": j.get("name", ""), "url": f"https://jobs.smartrecruiters.com/{slug}/{j.get('id', '')}"}
                for j in jobs
                if j.get("id")
            ]
            board_url = f"https://jobs.smartrecruiters.com/{slug}"
            return {
                "found": True,
                "source": "SmartRecruiters",
                "ats_board_url": board_url,
                "total_jobs": len(titles),
                "hr_roles": hr_roles,
                "job_urls": job_urls[:10],
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


# ─────────────────────────────────────────────────────────────
#  NEW ATS PLATFORMS: Jobvite, Recruitee, Pinpoint, JazzHR, Personio
# ─────────────────────────────────────────────────────────────

def check_jobvite(slug: str) -> dict:
    """Checks Jobvite public job board."""
    url = f"https://jobs.jobvite.com/{slug}/jobs"
    try:
        r = get_session().get(url, timeout=FAST_TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and len(r.text) > 1000:
            soup = BeautifulSoup(r.text, "html.parser")
            # Jobvite lists jobs in <a> tags with class containing 'jv-job-list'
            job_links = soup.find_all("a", href=re.compile(r"/job/"))
            if not job_links:
                # Fallback: look for any job-like elements
                job_links = soup.find_all("a", class_=re.compile(r"job", re.I))
            titles = [a.get_text(strip=True) for a in job_links if 5 < len(a.get_text(strip=True)) < 100]
            titles = list(dict.fromkeys(titles))
            if not titles:
                return {"found": False}
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            board_url = f"https://jobs.jobvite.com/{slug}/jobs"
            return {
                "found": True,
                "source": "Jobvite",
                "ats_board_url": board_url,
                "total_jobs": len(titles),
                "hr_roles": hr_roles,
                "job_urls": [],
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


def check_recruitee(slug: str) -> dict:
    """Checks Recruitee public job board API."""
    api_url = f"https://{slug}.recruitee.com/api/offers"
    try:
        r = get_session().get(api_url, timeout=FAST_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            jobs = data.get("offers", []) if isinstance(data, dict) else []
            if not jobs:
                return {"found": False}
            titles = [j.get("title", "") for j in jobs]
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            job_urls = [
                {"title": j.get("title", ""), "url": j.get("careers_url", "")}
                for j in jobs
                if j.get("careers_url")
            ]
            board_url = f"https://{slug}.recruitee.com/"
            return {
                "found": True,
                "source": "Recruitee",
                "ats_board_url": board_url,
                "total_jobs": len(titles),
                "hr_roles": hr_roles,
                "job_urls": job_urls[:10],
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


def check_pinpoint(slug: str) -> dict:
    """Checks Pinpoint ATS public job board."""
    url = f"https://{slug}.pinpointhq.com/postings.json"
    try:
        r = get_session().get(url, timeout=FAST_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            jobs = data.get("data", []) if isinstance(data, dict) else data if isinstance(data, list) else []
            if not jobs:
                return {"found": False}
            titles = [j.get("title", "") or j.get("attributes", {}).get("title", "") for j in jobs]
            titles = [t for t in titles if t]
            if not titles:
                return {"found": False}
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            board_url = f"https://{slug}.pinpointhq.com/"
            return {
                "found": True,
                "source": "Pinpoint",
                "ats_board_url": board_url,
                "total_jobs": len(titles),
                "hr_roles": hr_roles,
                "job_urls": [],
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


def check_jazzhr(slug: str) -> dict:
    """Checks JazzHR public job board."""
    url = f"https://{slug}.applytojob.com/apply"
    try:
        r = get_session().get(url, timeout=FAST_TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and len(r.text) > 500:
            soup = BeautifulSoup(r.text, "html.parser")
            # JazzHR lists jobs in links to /apply/{jobId}
            job_links = soup.find_all("a", href=re.compile(r"/apply/"))
            titles = [a.get_text(strip=True) for a in job_links if 5 < len(a.get_text(strip=True)) < 100]
            titles = list(dict.fromkeys(titles))
            if not titles:
                return {"found": False}
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            board_url = f"https://{slug}.applytojob.com/apply"
            return {
                "found": True,
                "source": "JazzHR",
                "ats_board_url": board_url,
                "total_jobs": len(titles),
                "hr_roles": hr_roles,
                "job_urls": [],
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


def check_personio(slug: str) -> dict:
    """Checks Personio public job board."""
    api_url = f"https://{slug}.jobs.personio.de/search.json"
    try:
        r = get_session().get(api_url, timeout=FAST_TIMEOUT)
        if r.status_code == 200:
            jobs = r.json()
            if not isinstance(jobs, list) or not jobs:
                return {"found": False}
            titles = [j.get("name", "") for j in jobs]
            titles = [t for t in titles if t]
            if not titles:
                return {"found": False}
            hr_roles = [t for t in titles if any(kw in t.lower() for kw in HR_TITLE_KEYWORDS)]
            board_url = f"https://{slug}.jobs.personio.de/"
            return {
                "found": True,
                "source": "Personio",
                "ats_board_url": board_url,
                "total_jobs": len(titles),
                "hr_roles": hr_roles,
                "job_urls": [],
                "all_titles": titles[:10],
            }
    except Exception:
        pass
    return {"found": False}


# ─────────────────────────────────────────────────────────────
#  Fallback: aggressive careers page scanner
#  Replaces useless Google search with deep careers page check
# ─────────────────────────────────────────────────────────────

# Known ATS iframe/script patterns — detect embedded ATS on careers page
_ATS_EMBED_PATTERNS = [
    ("Greenhouse", re.compile(r"boards\.greenhouse\.io", re.I)),
    ("Lever", re.compile(r"jobs\.lever\.co", re.I)),
    ("Workable", re.compile(r"apply\.workable\.com", re.I)),
    ("Ashby", re.compile(r"jobs\.ashbyhq\.com", re.I)),
    ("BambooHR", re.compile(r"bamboohr\.com", re.I)),
    ("SmartRecruiters", re.compile(r"smartrecruiters\.com", re.I)),
    ("Jobvite", re.compile(r"jobs\.jobvite\.com", re.I)),
    ("iCIMS", re.compile(r"icims\.com", re.I)),
    ("Recruitee", re.compile(r"recruitee\.com", re.I)),
    ("Personio", re.compile(r"personio\.de", re.I)),
]


def _scrape_jobs_from_page(r, url: str) -> dict:
    """
    Shared helper: given a successful HTTP response, parse job listings
    and embedded ATS signals. Returns a result dict or {} if nothing found.
    """
    soup = BeautifulSoup(r.text, "html.parser")

    # Check for embedded ATS iframes/scripts/links (high confidence)
    page_html = r.text.lower()
    embed_url = ""
    for ats_name, pattern in _ATS_EMBED_PATTERNS:
        if pattern.search(page_html):
            match = pattern.search(r.text)
            if match:
                for iframe in soup.find_all("iframe"):
                    src = iframe.get("src", "")
                    if pattern.search(src):
                        embed_url = src
                        break
                if not embed_url:
                    for a in soup.find_all("a", href=True):
                        if pattern.search(a["href"]):
                            embed_url = a["href"]
                            break

    for tag in soup(["script", "style", "nav", "footer"]):
        tag.decompose()
    text = soup.get_text(separator=" ").lower()
    hr_mentions = sum(1 for kw in HR_TITLE_KEYWORDS if kw in text)

    job_elements = []
    for tag in ["h2", "h3", "h4", "li", "a", "span", "div"]:
        for el in soup.find_all(tag):
            txt = el.get_text(strip=True)
            if 8 < len(txt) < 80:
                if any(kw in txt.lower() for kw in JOB_TITLE_SIGNALS):
                    job_elements.append(txt)

    unique_jobs = list(dict.fromkeys(job_elements))[:20]
    hr_roles_found = [j for j in unique_jobs if any(kw in j.lower() for kw in HR_TITLE_KEYWORDS)]

    if unique_jobs or hr_mentions >= 3 or embed_url:
        board_url = embed_url if embed_url else url
        return {
            "found": True,
            "source": "CareersPage",
            "ats_board_url": board_url,
            "careers_page_url": url,
            "total_jobs": len(unique_jobs),
            "hr_roles": hr_roles_found,
            "hr_keyword_mentions": hr_mentions,
            "job_urls": [],
            "all_titles": unique_jobs[:10],
        }
    return {}


def check_careers_page(website: str) -> dict:
    """
    Checks the company's own careers page.
    Phase A: career subdomains  (careers.domain.com, jobs.domain.com, …)
    Phase B: path-based URLs    (domain.com/careers, domain.com/jobs, …)
    Returns the first URL that yields real job signal.
    """
    if not website:
        return {"found": False}

    raw = website.rstrip("/")
    if not raw.startswith("http"):
        raw = "https://" + raw

    # ── Derive the bare hostname (strip scheme + www) ──────────────────────
    bare = (raw
            .replace("https://", "")
            .replace("http://", "")
            .replace("www.", ""))
    # Keep only the registered domain (e.g. "tyro.com" from "tyro.com/about")
    bare_domain = bare.split("/")[0]

    # ── Phase A: career subdomains ─────────────────────────────────────────
    # Many companies (Tyro, Canva, Shopify, …) host their jobs board at a
    # dedicated subdomain instead of a path on the main site.
    CAREER_SUBDOMAINS = ["careers", "jobs", "work", "join"]
    for sub in CAREER_SUBDOMAINS:
        subdomain_url = f"https://{sub}.{bare_domain}"
        try:
            r = get_session().get(
                subdomain_url, timeout=FAST_TIMEOUT, allow_redirects=True
            )
            if r.status_code == 200 and len(r.text) > 1000:
                result = _scrape_jobs_from_page(r, subdomain_url)
                if result:
                    return result
        except Exception:
            continue

    # ── Phase B: path-based URLs on the main domain ────────────────────────
    base = raw  # e.g. https://tyro.com
    for path in CAREER_PATHS:
        try:
            url = base + path
            r = get_session().get(url, timeout=FAST_TIMEOUT, allow_redirects=True)
            if r.status_code == 200 and len(r.text) > 1000:
                result = _scrape_jobs_from_page(r, url)
                if result:
                    return result
        except Exception:
            continue

    return {"found": False}


def check_google_jobs(company_name: str, website: str) -> dict:
    """
    Last resort: Google search signal.
    Kept as final fallback but with lower weight.
    """
    query = f'"{company_name}" recruiter OR "talent acquisition" OR "HR manager" jobs'
    encoded = query.replace(" ", "+").replace('"', "%22")
    search_url = f"https://www.google.com/search?q={encoded}&num=10"
    try:
        r = get_session().get(search_url, timeout=DEFAULT_TIMEOUT)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            text = soup.get_text().lower()
            hr_signals = sum(1 for kw in HR_TITLE_KEYWORDS if kw in text)
            hiring_mentioned = "hiring" in text or "open position" in text or "job opening" in text

            count_match = re.search(r"(\d+)\s+(open\s+)?(job|position|role|vacanc)", text)
            total_jobs_est = int(count_match.group(1)) if count_match else 0

            return {
                "found": True,
                "source": "GoogleSearch",
                "ats_board_url": "",
                "google_search_url": search_url,
                "hr_signals_in_results": hr_signals,
                "hiring_mentioned": hiring_mentioned,
                "total_jobs": total_jobs_est,
                "hr_roles": [],
                "job_urls": [],
            }
    except Exception:
        pass
    return {"found": False}


# ─────────────────────────────────────────────────────────────
#  Ordered waterfall: ATS APIs → Careers Page → Google
#  Now 13 ATS platforms (was 8)
# ─────────────────────────────────────────────────────────────

ATS_CHECKERS = [
    ("Greenhouse",      check_greenhouse),
    ("Lever",           check_lever),
    ("Workable",        check_workable),
    ("Ashby",           check_ashby),
    ("TeamTailor",      check_teamtailor),
    ("BambooHR",        check_bamboohr),
    ("BreezyHR",        check_breezy),
    ("SmartRecruiters", check_smartrecruiters),
    ("Jobvite",         check_jobvite),
    ("Recruitee",       check_recruitee),
    ("Pinpoint",        check_pinpoint),
    ("JazzHR",          check_jazzhr),
    ("Personio",        check_personio),
]


def _log_source_result(ats_name: str, result: dict) -> str:
    if result.get("found") and result.get("total_jobs", 0) > 0:
        return f"{ats_name}: {result['total_jobs']} jobs"
    if result.get("found"):
        return f"{ats_name}: found but 0 jobs"
    return f"{ats_name}: —"


def check_jobs_for_company(company: dict, current_idx: int = 1, total_count: int = 1) -> dict:
    """
    Main function. Tries all slug candidates × all ATS APIs, then careers page, then Google.
    Returns enriched company dict with job data + URL proof attached.
    Now validates results to prevent false positive slug matches.
    """
    name = company.get("company_name", "")
    website = company.get("website", "")
    company_url = company.get("company_url", "")  # From enricher

    slug_candidates = get_slug_candidates(name, website, company_url)
    print(f"  Checking jobs [{current_idx}/{total_count}]: {name}")
    print(f"    Slug candidates: {slug_candidates}")

    job_result = {"total_jobs": 0, "hr_roles": [], "source": "none", "found": False, "job_urls": [], "ats_board_url": ""}
    matched_slug = None

    # Phase 1: Try each ATS × each slug candidate
    # Stop at first VALIDATED (ATS, slug) pair that returns real jobs
    ats_tried = set()
    for ats_name, checker_fn in ATS_CHECKERS:
        for slug in slug_candidates:
            result = checker_fn(slug)
            if result["found"] and result.get("total_jobs", 0) > 0:
                # VALIDATION: Ensure this ATS board actually belongs to this company
                if _validate_ats_match(name, slug, result):
                    job_result = result
                    matched_slug = slug
                    print(f"    → {ats_name} [{slug}]: ✓ {result['total_jobs']} jobs | Board: {result.get('ats_board_url', '')}")
                    break
                else:
                    print(f"    → {ats_name} [{slug}]: ✗ REJECTED (false positive — jobs don't match '{name}')")
            ats_tried.add(f"{ats_name}:{slug}")
        if job_result["found"]:
            break

    if not job_result["found"]:
        print(f"    → All ATS APIs: no hit across {len(slug_candidates)} slug variants")

    # Phase 2: Fallback to careers page scraping (only if we have a website)
    if not job_result["found"] and website:
        cp = check_careers_page(website)
        if cp["found"] and cp.get("total_jobs", 0) > 0:
            job_result = cp
            print(f"    → CareersPage: ✓ {cp['total_jobs']} items | URL: {cp.get('careers_page_url', '')}")
        else:
            print(f"    → CareersPage: —")

    # Phase 3: Last resort — Google search signal
    if not job_result["found"]:
        gs = check_google_jobs(name, website)
        if gs["found"]:
            job_result = gs
            status = "✓ signal"
            if gs.get("total_jobs", 0) > 0:
                status = f"✓ ~{gs['total_jobs']} jobs estimated"
            print(f"    → Google: {status} | Search: {gs.get('google_search_url', '')}")
        else:
            print(f"    → Google: —")

    # Attach all job data + proof URLs to company
    company["job_check"] = job_result
    company["job_count"] = job_result.get("total_jobs", 0)
    company["hr_roles_found"] = job_result.get("hr_roles", [])
    company["has_hr_role"] = len(job_result.get("hr_roles", [])) > 0
    company["job_source"] = job_result.get("source", "none")
    company["ats_board_url"] = job_result.get("ats_board_url", "")
    company["job_urls"] = job_result.get("job_urls", [])        # list of {title, url} dicts
    company["matched_ats_slug"] = matched_slug or ""

    # Ensure website is captured
    if not company.get("website") and website:
        company["website"] = website

    time.sleep(random.uniform(0.2, 0.5))
    return company


# ─────────────────────────────────────────────────────────────
# CONCURRENT JOB CHECKS (asyncio.to_thread)
# ─────────────────────────────────────────────────────────────

async def _check_one(
    sem: asyncio.Semaphore,
    company: dict,
    idx: int,
    total: int,
    counter: dict,
    lock: threading.Lock,
    min_jobs: int,
) -> tuple[dict | None, bool]:
    """Wraps check_jobs_for_company with a semaphore for concurrency control."""
    async with sem:
        result = await asyncio.to_thread(
            check_jobs_for_company, company, idx, total
        )

        job_count = result.get("job_count", 0)
        hr_signal = result.get("has_hr_role", False)
        google_signal = result.get("job_check", {}).get("hiring_mentioned", False)
        hr_keyword_mentions = result.get("job_check", {}).get("hr_keyword_mentions", 0)
        google_jobs_est = result.get("job_check", {}).get("total_jobs", 0) if result.get("job_source") == "GoogleSearch" else 0

        keep = (
            job_count >= min_jobs
            or hr_signal
            or (google_signal and google_jobs_est >= 1)
            or hr_keyword_mentions >= 2
        )

        with lock:
            counter["done"] += 1
            if keep:
                counter["kept"] += 1
            else:
                counter["dropped"] += 1
            done = counter["done"]

        if keep:
            status = f"{job_count} jobs"
            if hr_signal:
                status += " | HR role ✓"
            board = result.get("ats_board_url", "")
            board_str = f" | {board}" if board else ""
            print(f"    KEEP: {result['company_name']} — {status} [{result.get('job_source', '')}]{board_str}")
        else:
            print(f"    DROP: {result['company_name']} — no verifiable job signal")

        if done % 10 == 0 or done == total:
            print(f"  [JOB CHECK PROGRESS] {done}/{total} companies checked")

        return result if keep else None


async def _run_job_checks_async(
    companies: list[dict],
    min_jobs: int,
    concurrency: int,
) -> list[dict]:
    """Internal async implementation."""
    sem = asyncio.Semaphore(concurrency)
    lock = threading.Lock()
    counter = {"done": 0, "kept": 0, "dropped": 0}

    tasks = [
        _check_one(sem, company, i + 1, len(companies), counter, lock, min_jobs)
        for i, company in enumerate(companies)
    ]

    results = await asyncio.gather(*tasks)
    enriched = [r for r in results if r is not None]
    return enriched, counter["dropped"]


def run_job_checks(companies: list[dict], min_jobs: int = 1, concurrency: int = 10) -> list[dict]:
    """
    Runs job checks on all companies CONCURRENTLY.
    Drops companies with zero jobs AND no hiring signal.
    Returns enriched and filtered list.
    Uses asyncio.to_thread to run multiple ATS waterfalls in parallel.
    """
    print(f"\n[JOB CHECK] Checking {len(companies)} companies across 13 ATS APIs + CareersPage + Google")
    print(f"[JOB CHECK] Using multi-slug matching for each company")
    print(f"[JOB CHECK] Concurrency: {concurrency} simultaneous company checks\n")

    enriched, dropped = asyncio.run(
        _run_job_checks_async(companies, min_jobs, concurrency)
    )

    print(f"\n[JOB CHECK] {len(enriched)} kept | {dropped} dropped")
    return enriched
