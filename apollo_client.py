"""
apollo_client.py — Apollo.io API wrapper for LeadGen
Finds decision-maker contacts (HR/TA roles) for target companies.

API flow:
  1. POST /v1/mixed_people/api_search — search by domain + job titles (no credits)
  2. POST /v1/people/bulk_match — enrich partial profiles to get verified emails (credits charged)

Docs: https://docs.apollo.io/reference/people-api-search
Rate limit: 100 req / 5 min
"""

import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")
APOLLO_BASE_URL = "https://api.apollo.io"

# Decision-maker titles to search for (reuses ICP from config)
SEARCH_TITLES = [
    "HR Manager",
    "Head of People",
    "Head of Talent",
    "Talent Acquisition Manager",
    "People Operations Manager",
    "Recruiter",
    "HR Director",
    "Recruiting Manager",
    "VP of People",
    "Chief People Officer",
    "HR Business Partner",
    "Founder",
    "CEO",
    "COO",
    "CTO",
]

# Seniority levels we care about (decision makers)
TARGET_SENIORITIES = ["manager", "director", "vp", "c_suite", "owner", "founder"]

MAX_CONTACTS_PER_COMPANY = 3


def _headers():
    """Apollo API auth headers."""
    return {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": APOLLO_API_KEY,
    }


def _extract_domain(website: str) -> str:
    """Extract clean domain from a URL."""
    if not website:
        return ""
    domain = website.lower().strip()
    for prefix in ["https://", "http://", "www."]:
        domain = domain.replace(prefix, "")
    domain = domain.rstrip("/").split("/")[0]
    return domain


def search_people(company_name: str, domain: str, titles: list[str] = None) -> list[dict]:
    """
    Search Apollo for people at a company matching decision-maker titles.
    Uses /v1/mixed_people/search — returns partial profiles (no credits charged).
    
    Returns list of partial person dicts with apollo IDs.
    """
    if not APOLLO_API_KEY:
        print("  [Apollo] No API key configured — skipping")
        return []

    search_titles = titles or SEARCH_TITLES
    clean_domain = _extract_domain(domain)

    if not clean_domain and not company_name:
        return []

    payload = {
        "per_page": 10,
        "person_titles": search_titles,
        "person_seniorities": TARGET_SENIORITIES,
    }

    # Use domain if available, otherwise company name
    if clean_domain:
        payload["organization_domains"] = [clean_domain]
    else:
        payload["q_organization_name"] = company_name

    try:
        resp = requests.post(
            f"{APOLLO_BASE_URL}/v1/mixed_people/search",
            headers=_headers(),
            json=payload,
            timeout=15,
        )

        if resp.status_code == 429:
            print("  [Apollo] Rate limited — waiting 60s")
            time.sleep(60)
            resp = requests.post(
                f"{APOLLO_BASE_URL}/v1/mixed_people/search",
                headers=_headers(),
                json=payload,
                timeout=15,
            )

        if resp.status_code != 200:
            print(f"  [Apollo] Search failed: HTTP {resp.status_code} — {resp.text[:200]}")
            return []

        data = resp.json()
        people = data.get("people", [])
        print(f"  [Apollo] Found {len(people)} contacts at {company_name or clean_domain}")
        return people

    except Exception as e:
        print(f"  [Apollo] Search error for {company_name}: {e}")
        return []


def enrich_person(person_id: str = None, email: str = None, 
                  first_name: str = None, last_name: str = None,
                  organization_name: str = None, domain: str = None) -> dict:
    """
    Enrich a single person via /v1/people/match.
    Credits are charged for successful matches.
    
    Can match by:
    - person_id (Apollo ID from search)
    - email
    - first_name + last_name + organization_name/domain
    """
    if not APOLLO_API_KEY:
        return {}

    payload = {}
    if person_id:
        payload["id"] = person_id
    if email:
        payload["email"] = email
    if first_name:
        payload["first_name"] = first_name
    if last_name:
        payload["last_name"] = last_name
    if organization_name:
        payload["organization_name"] = organization_name
    if domain:
        payload["domain"] = _extract_domain(domain)

    try:
        resp = requests.post(
            f"{APOLLO_BASE_URL}/v1/people/match",
            headers=_headers(),
            json=payload,
            timeout=15,
        )

        if resp.status_code == 429:
            print("  [Apollo] Rate limited — waiting 60s")
            time.sleep(60)
            resp = requests.post(
                f"{APOLLO_BASE_URL}/v1/people/match",
                headers=_headers(),
                json=payload,
                timeout=15,
            )

        if resp.status_code != 200:
            print(f"  [Apollo] Enrich failed: HTTP {resp.status_code}")
            return {}

        data = resp.json()
        return data.get("person", {})

    except Exception as e:
        print(f"  [Apollo] Enrich error: {e}")
        return {}


def find_contacts_for_lead(company_name: str, website: str) -> list[dict]:
    """
    High-level function: find decision-maker contacts for one company.
    
    1. Search Apollo for people matching target titles
    2. Filter for verified emails only
    3. Return top N contacts with clean structure
    """
    domain = _extract_domain(website)
    
    # Step 1: Search
    raw_people = search_people(company_name, domain)
    
    if not raw_people:
        # Fallback: try with just company name
        if domain:
            raw_people = search_people(company_name, "")
    
    if not raw_people:
        return []

    # Step 2: Parse and filter
    contacts = []
    for person in raw_people:
        if not person:
            continue
            
        email = person.get("email", "")
        email_status = person.get("email_status", "")
        
        # Prefer verified emails, but keep guessed as backup
        if not email or email_status == "unavailable":
            continue

        contact = {
            "apollo_person_id": person.get("id", ""),
            "full_name": f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
            "first_name": person.get("first_name", ""),
            "last_name": person.get("last_name", ""),
            "title": person.get("title", ""),
            "email": email,
            "email_status": email_status,  # "verified", "guessed", etc.
            "linkedin_url": person.get("linkedin_url", ""),
            "seniority": person.get("seniority", ""),
            "departments": person.get("departments", []),
            "phone": (person.get("phone_numbers") or [{}])[0].get("sanitized_number", "") if person.get("phone_numbers") else "",
            "organization_name": person.get("organization", {}).get("name", "") if person.get("organization") else "",
            "headline": person.get("headline", ""),
        }
        contacts.append(contact)

    # Step 3: Sort — verified first, then by seniority relevance
    seniority_rank = {"c_suite": 0, "vp": 1, "director": 2, "founder": 3, "owner": 4, "manager": 5}
    
    def sort_key(c):
        email_priority = 0 if c["email_status"] == "verified" else 1
        seniority_priority = seniority_rank.get(c["seniority"], 6)
        return (email_priority, seniority_priority)
    
    contacts.sort(key=sort_key)

    # Return top N
    result = contacts[:MAX_CONTACTS_PER_COMPANY]
    verified = sum(1 for c in result if c["email_status"] == "verified")
    print(f"  [Apollo] Returning {len(result)} contacts ({verified} verified) for {company_name}")
    
    return result


def batch_find_contacts(companies: list[dict], delay: float = 1.5) -> dict:
    """
    Find contacts for a batch of companies.
    
    Args:
        companies: list of dicts with 'id', 'name_display', 'website'
        delay: seconds between API calls (rate limit protection)
    
    Returns: dict mapping company_id -> list of contact dicts
    """
    results = {}
    total = len(companies)
    
    print(f"\n[Apollo] Starting contact search for {total} companies...\n")
    
    for i, company in enumerate(companies):
        company_id = company.get("id", "")
        name = company.get("name_display", "")
        website = company.get("website", "")
        
        print(f"  [{i+1}/{total}] Searching: {name} ({website})")
        
        contacts = find_contacts_for_lead(name, website)
        if contacts:
            results[company_id] = contacts
        
        # Rate limit protection
        if i < total - 1:
            time.sleep(delay)
    
    total_contacts = sum(len(v) for v in results.values())
    print(f"\n[Apollo] Complete. Found {total_contacts} contacts across {len(results)} companies.\n")
    
    return results
