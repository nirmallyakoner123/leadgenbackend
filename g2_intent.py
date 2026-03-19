"""
G2 Intent Signal Checker
------------------------
Checks whether a company is actively evaluating competitor tools on G2.

Strategy:
  G2 blocks direct page scraping (403), but their product RSS feeds are open.
  We use two approaches:

  1. G2 RSS reviews — fetch recent reviews of InterviewScreener's direct
     competitors. Reviews mention reviewer's industry and company size.
     We use this to understand which company SIZE BANDS are actively buying,
     and flag our leads that match those bands as "G2 intent match."

  2. Google search — search for "[company name] site:g2.com" to check if
     the company itself has a G2 profile or is mentioned in G2 reviews.
     If they appear on G2 as a reviewer or have a profile, that is strong
     intent signal (they are actively evaluating HR tech tools).
"""

import re
import time
import random
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# Direct competitors of InterviewScreener on G2
COMPETITOR_SLUGS = [
    "hirevue",
    "spark-hire",
    "vidcruiter",
    "talview",
    "interviewer-ai",
    "hirequest",
    "screenloop",
    "myinterview",
]

# G2 company size bands that match our ICP (20-200 employees)
ICP_SIZE_BANDS = {"11-50 employees", "51-200 employees", "51-1,000 employees"}


def fetch_g2_competitor_reviews() -> dict:
    """
    Fetches recent G2 reviews for all competitor products.
    Returns a dict with aggregate stats about who is buying:
    {
        "total_reviews": int,
        "icp_size_reviews": int,    # reviews from companies in our size range
        "icp_size_pct": float,      # % of reviews from ICP-sized companies
        "size_distribution": dict,  # breakdown by company size
        "roles_distribution": dict, # breakdown by reviewer role
    }
    """
    all_sizes = {}
    all_roles = {}
    total = 0
    icp_count = 0

    for slug in COMPETITOR_SLUGS:
        url = f"https://www.g2.com/products/{slug}/reviews.rss"
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, features="xml")
            items = soup.find_all("item")

            for item in items:
                desc = item.find("description")
                if not desc:
                    continue
                text = desc.get_text()

                role_m = re.search(r"role:\s*(.+?)[\n<\]]", text)
                size_m = re.search(r"size:\s*(.+?)[\n<\]]", text)

                role = role_m.group(1).strip() if role_m else "Unknown"
                size = size_m.group(1).strip() if size_m else "Unknown"

                all_sizes[size] = all_sizes.get(size, 0) + 1
                all_roles[role] = all_roles.get(role, 0) + 1
                total += 1

                if size in ICP_SIZE_BANDS:
                    icp_count += 1

            time.sleep(random.uniform(0.5, 1.0))

        except Exception:
            continue

    icp_pct = round((icp_count / total * 100), 1) if total > 0 else 0
    return {
        "total_reviews": total,
        "icp_size_reviews": icp_count,
        "icp_size_pct": icp_pct,
        "size_distribution": dict(sorted(all_sizes.items(), key=lambda x: -x[1])),
        "roles_distribution": dict(sorted(all_roles.items(), key=lambda x: -x[1])),
    }


def check_company_on_g2(company_name: str, website: str = "", delay: float = 2.0) -> dict:
    """
    Checks if a specific company appears on G2 — either as a product reviewer
    or as having their own G2 profile (meaning they use HR tech tools).

    Uses Google search since G2 pages themselves block direct access.

    Returns:
    {
        "found": bool,
        "signal_type": "reviewer" | "has_profile" | "mentioned" | "none",
        "evidence": str,
        "g2_url": str,
    }
    """
    query = f'site:g2.com "{company_name}"'
    encoded = query.replace(" ", "+").replace('"', "%22")
    url = f"https://www.google.com/search?q={encoded}&num=5"

    try:
        time.sleep(delay)  # Respect Google rate limits
        r = requests.get(url, headers=HEADERS, timeout=8)
        if r.status_code == 429:
            # Rate limited — back off and retry once
            time.sleep(10)
            r = requests.get(url, headers=HEADERS, timeout=8)
        if r.status_code != 200:
            return {"found": False, "signal_type": "none", "evidence": f"Google search returned {r.status_code}", "g2_url": ""}

        soup = BeautifulSoup(r.text, "html.parser")
        results = soup.find_all("h3")
        snippets = soup.find_all("div", class_=re.compile(r"BNeawe|VwiC3b|s3v9rd"))

        result_texts = [r.get_text(strip=True).lower() for r in results]
        snippet_texts = [s.get_text(strip=True).lower() for s in snippets]
        all_text = " ".join(result_texts + snippet_texts)

        company_lower = company_name.lower()

        # Check for G2 profile (they have their own product listed)
        if f"g2.com/products/{company_lower.replace(' ', '-')}" in all_text or \
           f"{company_lower} reviews" in all_text and "g2" in all_text:
            return {
                "found": True,
                "signal_type": "has_profile",
                "evidence": f"{company_name} has a G2 product profile — they are in the HR tech ecosystem",
                "g2_url": f"https://www.g2.com/products/{company_lower.replace(' ', '-')}/reviews",
            }

        # Check if they appear as a reviewer
        if company_lower in all_text and ("review" in all_text or "g2.com" in all_text):
            return {
                "found": True,
                "signal_type": "reviewer",
                "evidence": f"{company_name} appears in G2 reviews — actively evaluating HR tech tools",
                "g2_url": "",
            }

        # Mentioned on G2 in any context
        if company_lower in all_text:
            return {
                "found": True,
                "signal_type": "mentioned",
                "evidence": f"{company_name} mentioned on G2",
                "g2_url": "",
            }

    except Exception as e:
        return {"found": False, "signal_type": "none", "evidence": f"Error: {e}", "g2_url": ""}

    return {"found": False, "signal_type": "none", "evidence": "Not found on G2", "g2_url": ""}


def get_g2_market_context() -> str:
    """
    Returns a human-readable summary of who is buying competitor tools on G2.
    Used to enrich the AI Brain's context.
    """
    stats = fetch_g2_competitor_reviews()
    if stats["total_reviews"] == 0:
        return "G2 market data unavailable."

    top_sizes = list(stats["size_distribution"].items())[:3]
    top_roles = list(stats["roles_distribution"].items())[:3]

    size_str = ", ".join([f"{s} ({c} reviews)" for s, c in top_sizes])
    role_str = ", ".join([f"{r} ({c} reviews)" for r, c in top_roles])

    return (
        f"G2 competitor analysis ({stats['total_reviews']} reviews across {len(COMPETITOR_SLUGS)} competitors): "
        f"Top buyer sizes: {size_str}. "
        f"Top reviewer roles: {role_str}. "
        f"{stats['icp_size_pct']}% of buyers are in the 11-200 employee range (our ICP)."
    )
