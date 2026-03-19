# config.py — All ICP settings + Geography configs + RunConfig
# Change nothing else to retarget. All decisions live here.

from dataclasses import dataclass, field
from typing import Optional

PRODUCT = "InterviewScreener.com"

# ─────────────────────────────────────────
# ICP — Buyer & Targeting
# ─────────────────────────────────────────

TARGET_JOB_TITLES = [
    "Talent Acquisition Manager",
    "Talent Acquisition",
    "HR Manager",
    "Recruiter",
    "Head of Talent",
    "Head of People",
    "People Operations",
    "HR Director",
    "Recruiting Manager",
    "HR Business Partner",
    "Hiring Manager",
]

HR_TECH_BUYER_TITLES = [
    "HR Technology",
    "HR Systems",
    "HRIS",
    "People Technology",
    "HR Operations Manager",
    "Talent Technology",
]

TARGET_INDUSTRIES = [
    "software", "saas", "technology", "tech",
    "staffing", "recruitment", "recruiting",
    "e-commerce", "ecommerce", "digital agency",
    "edtech", "healthtech", "fintech",
]

MIN_EMPLOYEES = 20
MAX_EMPLOYEES = 200
MAX_JOB_AGE_DAYS = 30
MIN_OPEN_ROLES = 2

# ─────────────────────────────────────────
# GEOGRAPHY CONFIGURATION
# Drives which scrapers and job boards run per country
# In Phase 3 this will come from frontend input
# ─────────────────────────────────────────

GEOGRAPHY_CONFIG = {
    "US": {
        "country_code": "US",
        "country_name": "United States",
        "linkedin_location": "United States",
        "google_news_gl": "US",
        "google_news_hl": "en-US",
        "google_news_ceid": "US:en",
        "job_boards": ["greenhouse", "lever", "careers_page", "google_search"],
        "yc_region": "United States",
        "seek_enabled": False,
        "reed_enabled": False,
    },
    "GB": {
        "country_code": "GB",
        "country_name": "United Kingdom",
        "linkedin_location": "United Kingdom",
        "google_news_gl": "GB",
        "google_news_hl": "en-GB",
        "google_news_ceid": "GB:en",
        "job_boards": ["greenhouse", "lever", "careers_page", "reed", "google_search"],
        "yc_region": "United Kingdom",
        "seek_enabled": False,
        "reed_enabled": True,
        "reed_base_url": "https://www.reed.co.uk",
    },
    "AU": {
        "country_code": "AU",
        "country_name": "Australia",
        "linkedin_location": "Australia",
        "google_news_gl": "AU",
        "google_news_hl": "en-AU",
        "google_news_ceid": "AU:en",
        "job_boards": ["greenhouse", "lever", "careers_page", "seek", "google_search"],
        "yc_region": "Australia",
        "seek_enabled": True,
        "reed_enabled": False,
        "seek_base_url": "https://www.seek.com.au",
    },
    "VN": {
        "country_code": "VN",
        "country_name": "Vietnam",
        "linkedin_location": "Vietnam",
        "google_news_gl": "VN",
        "google_news_hl": "en",
        "google_news_ceid": "VN:en",
        "job_boards": ["greenhouse", "lever", "careers_page", "google_search"],
        "yc_region": "Vietnam",
        "seek_enabled": False,
        "reed_enabled": False,
        "naukri_enabled": False,
    },
    "IN": {
        "country_code": "IN",
        "country_name": "India",
        "linkedin_location": "India",
        "google_news_gl": "IN",
        "google_news_hl": "en-IN",
        "google_news_ceid": "IN:en",
        "job_boards": ["greenhouse", "lever", "careers_page", "naukri", "google_search"],
        "yc_region": "India",
        "seek_enabled": False,
        "reed_enabled": False,
        "naukri_enabled": True,
    },
}

# LinkedIn job search URL templates per geography
LINKEDIN_SEARCH_TEMPLATES = {
    "Head of People / People Ops": "https://www.linkedin.com/jobs/search/?keywords=Head+of+People+OR+%22People+Operations%22&location={linkedin_location}&f_CS=B%2CC&f_TPR=r2592000",
    "Talent Acquisition Manager":  "https://www.linkedin.com/jobs/search/?keywords=%22Talent+Acquisition+Manager%22+OR+%22TA+Manager%22&location={linkedin_location}&f_CS=B%2CC&f_TPR=r2592000",
    "HR Manager":                  "https://www.linkedin.com/jobs/search/?keywords=%22HR+Manager%22+OR+%22HR+Director%22&location={linkedin_location}&f_CS=B%2CC&f_TPR=r2592000",
    "Recruiter":                   "https://www.linkedin.com/jobs/search/?keywords=Recruiter&location={linkedin_location}&f_CS=B%2CC&f_TPR=r604800",
    "HR Technology / HRIS":        "https://www.linkedin.com/jobs/search/?keywords=%22HR+Technology%22+OR+%22HRIS+Manager%22+OR+%22People+Technology%22&location={linkedin_location}&f_CS=B%2CC&f_TPR=r2592000",
}

# Google News query pool — combined with per-geo gl= parameter
GOOGLE_NEWS_QUERIES = [
    "startup raises Series A hiring 2024",
    "startup raises Series A hiring 2025",
    "SaaS company raises funding hiring",
    "HR tech startup funding 2024",
    "HR tech startup funding 2025",
    "recruiting software startup funding",
    "talent acquisition software raises",
    "staffing software startup raises funding",
    "HR platform raises Series A",
    "people operations software funding",
    "workforce management startup raises",
    "applicant tracking system startup funding",
    "candidate screening software raises",
    "hiring automation startup funding",
]

# India-specific Google News queries — supplements the global pool when geo=IN
GOOGLE_NEWS_QUERIES_IN = [
    "Indian startup raises Series A hiring 2025",
    "Bengaluru startup funding hiring 2025",
    "Mumbai startup raises funding hiring",
    "Delhi NCR SaaS startup funding 2025",
    "India SaaS company raises Series A",
    "Indian HR tech startup funding",
    "Nasscom startup raises funding hiring",
    "Indian startup Series B hiring HR",
    "YC India startup raises funding",
    "SoftBank India startup funding hiring",
    "Sequoia India startup raises funding",
    "Indian fintech startup hiring HR manager",
    "Indian edtech startup raises funding hiring",
    "India healthtech startup funding 2025",
]

# ─────────────────────────────────────────
# RUN CONFIG
# Controls what the pipeline does in a single run
# In Phase 3 this object will be built from a frontend POST request
# ─────────────────────────────────────────

@dataclass
class RunConfig:
    # Which geographies to scrape (must be keys in GEOGRAPHY_CONFIG)
    geographies: list[str] = field(default_factory=lambda: ["US"])

    # Which sources to include
    use_yc: bool = True
    use_product_hunt: bool = True
    use_techcrunch: bool = True
    use_google_news: bool = True
    use_linkedin: bool = True
    use_seek: bool = True       # Only runs for AU
    use_reed: bool = True       # Only runs for GB
    use_naukri: bool = True     # Only runs for IN

    # Token control — skip work if data is fresher than these thresholds
    skip_job_check_if_fresh_days: int = 7
    skip_ai_if_fresh_days: int = 7

    # Cost + throughput control
    max_companies_for_ai: int = 200    # Hard cap on AI Brain evaluations per run
    min_score_for_output: int = 4      # Only output leads scoring above this

    # Concurrency control — how many companies to process simultaneously
    enricher_concurrency: int = 5      # Homepage fetches — low to avoid thread accumulation hang
    job_checker_concurrency: int = 20  # ATS API waterfalls (heavier)

    # Pipeline version tag (stored in pipeline_runs)
    pipeline_version: str = "v3"


# ─────────────────────────────────────────
# OUTPUT PATHS
# ─────────────────────────────────────────

OUTPUT_JSON = "output/raw_companies.json"
OUTPUT_CSV = "output/verified_leads.csv"

# ─────────────────────────────────────────
# INDEED CONFIG (reserved for Phase 2)
# ─────────────────────────────────────────

INDEED_SEARCH_QUERIES = [
    "Talent Acquisition Manager",
    "HR Manager hiring",
    "Recruiter",
    "Head of People",
    "People Operations Manager",
    "HR Technology Manager",
]

INDEED_LOCATION = "United States"
INDEED_BASE_URL = "https://www.indeed.com"
