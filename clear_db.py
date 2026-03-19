"""
clear_db.py — Wipes all pipeline DATA tables so the next run starts from scratch.

Safe to run repeatedly. Only data rows are deleted; the schema (tables, views,
functions) is left untouched. This script will NOT try to delete from views
(token_status, active_leads — those reflect data, not store it).

Env vars required (same as main.py):
  SUPABASE_URL          — your project URL
  SUPABASE_SERVICE_KEY  — service role key (not the anon key)
"""

import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# Only real data tables — NOT views like token_status or active_leads.
# Order matters: delete child tables before parent tables to avoid FK violations.
DATA_TABLES = [
    "raw_ai_results",       # AI Brain verdicts
    "raw_signal_events",    # Individual signal check rows
    "raw_job_events",       # Job check results
    "raw_scrape_events",    # Raw scrape log (was called raw_companies in old schema)
    "pipeline_runs",        # Run audit log
    "companies",            # Master company records (cleared last)
]


def clear_database():
    url: str = os.getenv("SUPABASE_URL", "")
    key: str = os.getenv("SUPABASE_SERVICE_KEY", "")   # ← correct env var name

    if not url or not key:
        print("Error: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
        return

    print(f"Connecting to Supabase...")
    db: Client = create_client(url, key)

    print("Clearing pipeline data tables (schema preserved)...\n")
    total_ok = 0
    for table in DATA_TABLES:
        try:
            # Supabase requires a filter for DELETE — use a wide-matching filter.
            # `neq("id", "00000000-0000-0000-0000-000000000000")` matches every
            # real UUID row without touching schema objects.
            db.table(table).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            print(f"  ✓  {table}")
            total_ok += 1
        except Exception as e:
            print(f"  ✗  {table} — {e}")

    print(f"\n{total_ok}/{len(DATA_TABLES)} tables cleared. Ready for a fresh run.")


if __name__ == "__main__":
    clear_database()
