# InterviewScreener — Lead Generation Engine V3

AI-powered pipeline that finds and verifies B2B leads for InterviewScreener.com.
Scrapes 6 sources across 4 geographies, verifies active hiring via 13 ATS platforms,
checks 8 buying signals per company (including ATS-confirmed HR tech investment),
persists everything to Supabase, and outputs a ranked HOT / WARM / COLD lead list
with outreach copy — ready to send.

---

## What It Does

```
YC API + TechCrunch RSS + Google News RSS + LinkedIn + Seek (AU) + Reed (GB)
                              │
                              ▼ ~400–600 raw entries
              Write ALL events to Supabase (append-only)
              Upsert company master records
                              │
                              ▼
              Deduplicate + merge in memory
                              │
                              ▼
              Bulk token check — what's fresh vs. stale?
                              │
                              ▼
              Basic ICP filter (size + industry)
                              │
                              ▼ ~260 companies
    Job check — TOKEN-AWARE (8 ATS APIs → careers page → Google)
    Fresh companies skip this step entirely
                              │
                              ▼ ~40–60 companies
    AI Brain — TOKEN-AWARE (8 signals, 1 LLM call per company)
    Fresh companies use cached verdict — no LLM call
                              │
                              ▼
    output/ai_verified_leads.csv   ← your lead list
    Supabase active_leads view     ← queryable by country
```

**Output per lead:** score out of 18, HOT/WARM/COLD verdict, why they fit,
recommended pricing plan, and a personalized outreach opener.

**Token caching:** Companies evaluated in the last 7 days are served from
Supabase cache. Repeat runs are fast and cheap — only new or stale companies
go through the full pipeline.

---

## Requirements

- Python 3.10 or higher
- An OpenAI API key (GPT-4o-mini — costs ~$0.50–$2 per full run)
- A Supabase project (free tier is sufficient)

---

## Setup

### 1. Unzip or clone the project

```
leadgen/
├── main.py           ← entry point
├── scraper.py        ← 6 data sources
├── filter.py         ← dedup + ICP filter + pre-scoring
├── job_checker.py    ← 13 ATS platforms + careers page + Google
├── ai_brain.py       ← 8-signal evaluation + GPT-4o-mini
├── output_writer.py  ← CSV / JSON output + terminal summary
├── database.py       ← Supabase client + all DB operations
├── config.py         ← all ICP settings + geography config
├── requirements.txt
├── .env              ← you create this (see step 3)
└── output/           ← results appear here after each run
```

### 2. Create a virtual environment and install dependencies

```bash
# Windows
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Mac / Linux
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Set up your environment variables

Create a file called `.env` in the `leadgen/` folder:

```
OPENAI_API_KEY=your_openai_api_key_here
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_SERVICE_KEY=your_supabase_service_role_key_here
```

- **OpenAI key:** https://platform.openai.com/api-keys
- **Supabase URL + key:** https://supabase.com → your project → Settings → API

> Use the **service role** key (not the anon key) — the pipeline writes to the database.

### 4. Set up the Supabase database

Run the following SQL in your Supabase SQL editor to create the required tables and views:

```sql
-- Companies master table
CREATE TABLE companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name_normalized TEXT UNIQUE NOT NULL,
  name_display TEXT,
  website TEXT,
  yc_batch TEXT,
  yc_url TEXT,
  sources TEXT[],
  source_count INT DEFAULT 1,
  description TEXT,
  long_description TEXT,
  team_size INT,
  industries TEXT[],
  tags TEXT[],
  is_hiring BOOLEAN DEFAULT FALSE,
  funding_amount TEXT,
  country_code TEXT,
  city TEXT,
  state_region TEXT,
  raw_location TEXT,
  first_seen_run UUID,
  profile_fetched_at TIMESTAMPTZ,
  profile_expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Pipeline runs audit table
CREATE TABLE pipeline_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  geographies_run TEXT[],
  sources_run TEXT[],
  companies_scraped INT,
  companies_filtered INT,
  companies_job_checked INT,
  companies_ai_evaluated INT,
  hot_count INT,
  warm_count INT,
  cold_count INT,
  total_llm_cost_usd NUMERIC(10,6),
  pipeline_version TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Raw scrape events (append-only)
CREATE TABLE raw_scrape_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id UUID REFERENCES pipeline_runs(id),
  source TEXT,
  country_code TEXT,
  company_name_raw TEXT,
  website_raw TEXT,
  description_raw TEXT,
  long_description_raw TEXT,
  team_size_raw INT,
  industries_raw TEXT[],
  tags_raw TEXT[],
  locations_raw TEXT[],
  yc_batch_raw TEXT,
  yc_url_raw TEXT,
  is_hiring_raw BOOLEAN,
  funding_amount_raw TEXT,
  article_title_raw TEXT,
  article_url_raw TEXT,
  hr_roles_found_raw TEXT[],
  days_ago_raw INT,
  scraped_at TIMESTAMPTZ DEFAULT NOW()
);

-- Job check results
CREATE TABLE raw_job_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id UUID REFERENCES pipeline_runs(id),
  company_id UUID REFERENCES companies(id),
  check_method TEXT,
  country_code TEXT,
  job_count INT,
  job_titles_raw TEXT[],
  hr_role_found BOOLEAN,
  hr_tech_role_found BOOLEAN,
  hr_role_evidence TEXT,
  raw_response_url TEXT,
  http_status INT,
  expires_at TIMESTAMPTZ,
  checked_at TIMESTAMPTZ DEFAULT NOW()
);

-- Signal check results
CREATE TABLE raw_signal_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id UUID REFERENCES pipeline_runs(id),
  company_id UUID REFERENCES companies(id),
  signal_number INT,
  signal_name TEXT,
  passed BOOLEAN,
  score_awarded INT,
  max_score INT,
  evidence TEXT,
  raw_llm_response JSONB,
  expires_at TIMESTAMPTZ,
  checked_at TIMESTAMPTZ DEFAULT NOW()
);

-- AI Brain results
CREATE TABLE raw_ai_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id UUID REFERENCES pipeline_runs(id),
  company_id UUID REFERENCES companies(id),
  final_score INT,
  max_score INT,
  verdict TEXT,
  verdict_reason TEXT,
  recommended_plan TEXT,
  why_they_fit TEXT,
  outreach_opener TEXT,
  signal_1_passed BOOLEAN, signal_1_evidence TEXT,
  signal_2_passed BOOLEAN, signal_2_evidence TEXT,
  signal_3_passed BOOLEAN, signal_3_evidence TEXT,
  signal_4_passed BOOLEAN, signal_4_evidence TEXT,
  signal_5_passed BOOLEAN, signal_5_evidence TEXT,
  signal_6_passed BOOLEAN, signal_6_evidence TEXT,
  signal_7_passed BOOLEAN, signal_7_evidence TEXT,
  signal_8_passed BOOLEAN, signal_8_evidence TEXT,
  llm_model_used TEXT,
  llm_tokens_used INT,
  llm_cost_usd NUMERIC(10,6),
  expires_at TIMESTAMPTZ,
  evaluated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Token status view (used by pipeline to check freshness)
CREATE VIEW token_status AS
SELECT
  c.id AS company_id,
  c.name_display,
  c.country_code,
  c.profile_expires_at < NOW() AS profile_needs_refresh,
  COALESCE(
    (SELECT expires_at < NOW() FROM raw_job_events
     WHERE company_id = c.id ORDER BY checked_at DESC LIMIT 1),
    TRUE
  ) AS job_needs_refresh,
  COALESCE(
    (SELECT expires_at < NOW() FROM raw_ai_results
     WHERE company_id = c.id ORDER BY evaluated_at DESC LIMIT 1),
    TRUE
  ) AS ai_needs_refresh,
  (SELECT verdict FROM raw_ai_results
   WHERE company_id = c.id ORDER BY evaluated_at DESC LIMIT 1) AS last_verdict,
  (SELECT final_score FROM raw_ai_results
   WHERE company_id = c.id ORDER BY evaluated_at DESC LIMIT 1) AS last_score
FROM companies c;

-- Active leads view
CREATE VIEW active_leads AS
SELECT
  c.name_display AS company_name,
  c.website,
  c.country_code,
  c.team_size,
  c.yc_batch,
  r.verdict,
  r.final_score,
  r.max_score,
  r.recommended_plan,
  r.why_they_fit,
  r.outreach_opener,
  r.evaluated_at
FROM raw_ai_results r
JOIN companies c ON c.id = r.company_id
WHERE r.verdict IN ('HOT', 'WARM')
  AND r.expires_at > NOW()
ORDER BY r.final_score DESC;
```

### 5. Create the output folder

```bash
mkdir output
```

---

## Run

```bash
# Make sure your virtual environment is active
python main.py
```

**To change which geographies to run**, edit the bottom of `main.py`:

```python
config = RunConfig(
    geographies=["US"],           # Options: "US", "GB", "AU", "VN"
    use_yc=True,
    use_techcrunch=True,
    use_google_news=True,
    use_linkedin=True,
    use_seek=True,                # Only runs for AU
    use_reed=True,                # Only runs for GB
    max_companies_for_ai=60,      # Hard cap on AI evaluations per run
    pipeline_version="v3",
)
```

**Expected runtime:** 45–90 minutes for a full US run. Repeat runs are faster
because companies with fresh tokens skip the job check and AI Brain entirely.

---

## Output Files

| File | What It Contains |
|---|---|
| `output/ai_verified_leads.csv` | Final ranked lead list — open this first |
| `output/ai_verified_leads.json` | Same data with full signal details |
| `output/verified_leads.csv` | Companies that passed job check (pre-AI) |
| `output/raw_companies.json` | Everything collected before any filtering |

### Key columns in `ai_verified_leads.csv`

| Column | Meaning |
|---|---|
| `verdict` | HOT / WARM / COLD |
| `final_score` | Points out of 18 |
| `why_they_fit` | 2–3 sentence fit explanation for the sales team |
| `outreach_opener` | Ready-to-use first line for cold email |
| `recommended_plan` | Starter / Pro / Scale / Enterprise |
| `signal_1_passed` through `signal_8_passed` | PASS / FAIL per signal |

---

## The 8 Signals

| # | Signal | Weight | Source |
|---|---|---|---|
| 1 | Active hiring — job volume | 3 pts (graduated) | 13 ATS platforms / Careers page |
| 2 | Hiring HR or TA role | 3 pts | ATS APIs / Google search |
| 3 | Company size 20–200 | 1 pt | YC API / LinkedIn |
| 4 | Funded in last 18 months | 2 pts | YC batch / TechCrunch / Google News |
| 5 | Jobs open 30+ days | 2 pts | Job count proxy |
| 6 | Hiring HR Technology role | 3 pts | ATS APIs / Careers page |
| 7 | ICP fit (AI) | 2 pts | GPT-4o-mini |
| 8 | ATS confirmed — existing HR tech investment | 2 pts | ATS slug match (Greenhouse, Lever, etc.) |

**Signal 1 graduated:** 3–4 jobs = 1pt, 5–7 = 2pt, 8+ = 3pt. Max score: 18.

---

## Job Checker — ATS Waterfall

For each company, the job checker tries these in order and stops at the first hit:

1. **Greenhouse** — `boards.greenhouse.io/{slug}/jobs`
2. **Lever** — `jobs.lever.co/{slug}`
3. **Workable** — `apply.workable.com/{slug}/jobs`
4. **Ashby** — `jobs.ashbyhq.com/{slug}`
5. **TeamTailor** — `{slug}.teamtailor.com/jobs`
6. **BambooHR** — `{slug}.bamboohr.com/jobs`
7. **BreezyHR** — `{slug}.breezy.hr/p`
8. **SmartRecruiters** — `careers.smartrecruiters.com/{slug}`
9. **Careers page** — tries 9 common URL paths on the company website
10. **Google search** — fallback signal search

---

## Token Caching (How Repeat Runs Stay Cheap)

Every job check and AI Brain result is stored in Supabase with an expiry timestamp:

| Data | TTL |
|---|---|
| Job check result | 7 days |
| AI verdict | 7 days |
| LinkedIn signal | 14 days |
| Company profile | 180 days |
| Funding data | Permanent |

On each run, a single bulk query to the `token_status` view tells the pipeline
exactly which companies need re-evaluation. Companies with fresh tokens are
passed through without any API calls.

---

## Adjusting the ICP

All targeting settings are in `config.py`:

| Setting | What It Controls |
|---|---|
| `MIN_EMPLOYEES` / `MAX_EMPLOYEES` | Company size range (default: 20–200) |
| `TARGET_INDUSTRIES` | Industries to include |
| `TARGET_JOB_TITLES` | HR/TA titles to look for |
| `HR_TECH_BUYER_TITLES` | Strongest intent signal titles |
| `GEOGRAPHY_CONFIG` | Per-country LinkedIn location, Google News locale, job boards |
| `LINKEDIN_SEARCH_TEMPLATES` | LinkedIn search URLs (5 HR role searches) |
| `GOOGLE_NEWS_QUERIES` | 14 funding/hiring news queries |

---

## Project Files

| File | Purpose |
|---|---|
| `main.py` | Entry point — 8-step pipeline orchestrator |
| `scraper.py` | 6 sources: YC, TechCrunch, Google News, LinkedIn, Seek, Reed |
| `filter.py` | Deduplication, ICP filter, pre-scoring |
| `job_checker.py` | 13 ATS platforms + careers page + Google fallback |
| `ai_brain.py` | 8-signal evaluation loop + GPT-4o-mini |
| `output_writer.py` | CSV / JSON output + terminal summary |
| `database.py` | Supabase client, upsert, token check, all DB writes |
| `config.py` | ICP settings, geography config, RunConfig dataclass |

---

## Cost

| Item | Cost |
|---|---|
| All data sources (YC, TechCrunch, Google News, LinkedIn, Seek, Reed) | Free |
| All ATS platforms (Greenhouse, Lever, Workable, and 10 more) | Free |
| Supabase (free tier: 500MB storage, 2GB bandwidth) | Free |
| OpenAI GPT-4o-mini | ~$0.50–$2 per full run |
| **Total per run** | **~$0.50–$2** |

---

## Notes

- The pipeline is hardcoded for **InterviewScreener.com** in V1–V3
- LinkedIn, Seek, and Reed are scraped from public pages — no accounts required
- ATS confirmation (Signal 8) uses public job board APIs — no accounts required
- The only paid service is OpenAI
- Supabase free tier is sufficient for hundreds of runs
- To run multiple geographies, add them to the `geographies` list at the bottom of `main.py`
