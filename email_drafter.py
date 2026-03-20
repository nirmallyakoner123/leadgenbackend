"""
email_drafter.py — AI-powered email drafting for LeadGen outreach
Uses GPT-4o-mini to write personalized cold emails based on lead intelligence.

Each email is crafted using:
  - Company data (description, team size, industry, funding)
  - AI Brain signals (8 scored signals + why_they_fit + outreach_opener)
  - Contact data (name, title, seniority)
  - InterviewScreener.com product context
"""

import os
import json
import time
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


PRODUCT_CONTEXT = """
PRODUCT: InterviewScreener.com
WHAT: AI-assisted candidate screening tool. Recruiters add a job, the system generates relevant questions, and candidates interview with an AI agent using audio. The hiring team gets structured summaries.

KEY VALUE PROPS:
1. Streamlines candidate screening with AI-guided audio interviews.
2. Candidates interview on their own schedule (async).
3. Hiring teams get a consistent evaluation baseline.
"""


def draft_email(contact: dict, company_data: dict, ai_data: dict) -> dict:
    """
    Generate a personalized cold email for one contact.
    
    Args:
        contact: dict with full_name, first_name, title, email, seniority
        company_data: dict with name_display, website, description, team_size, industries, etc.
        ai_data: dict with why_they_fit, outreach_opener, signal_results, recommended_plan, etc.
    
    Returns:
        dict with subject, body
    """
    company_name = company_data.get("name_display", "your company")
    contact_name = contact.get("first_name")
    if not contact_name and contact.get("full_name"):
        contact_name = contact.get("full_name").split()[0]
    if not contact_name:
        contact_name = "there"
    
    contact_title = contact.get("title", "")
    
    # Extract only the SINGLE BEST positive signal evidence to keep context extremely sharp
    passed_signals = [s for s in ai_data.get("signal_results", []) if s.get("passed")]
    if passed_signals:
        best_signal = passed_signals[0]
        signal_text = f"Strong indicator: {best_signal.get('evidence', '')[:150]}"
    else:
        signal_text = "No specific positive signals found. Rely on general company context."
    
    
    open_roles_count = ai_data.get("open_roles_count", 0)
    open_roles_titles = ai_data.get("open_roles_titles", [])
    open_roles_text = f"{open_roles_count} roles (e.g. {', '.join(open_roles_titles[:3])})" if open_roles_count else ""
    
    # Only include hiring observation if we have actual open roles data
    hiring_observation_rule = (
        "- Start with a natural observation about their hiring volume."
        if open_roles_count
        else "- Start with a brief, relevant observation about their company or industry. Do NOT assume they are actively hiring."
    )

    prompt = f"""
You are a highly adaptable B2B cold email copywriter.

{PRODUCT_CONTEXT}

RECIPIENT & COMPANY
Name: {contact_name}
Title: {contact_title}
Company: {company_name}
Team Size: {company_data.get('team_size', 'Unknown')}
Open Roles: {open_roles_text or 'Unknown'}
Funding: {company_data.get('funding_amount', 'Unknown')}

RELEVANT SIGNALS
{signal_text}

TASK
Write a natural, credible cold email to {contact_name}.

Rules:
- Subject line: max 7 words, specific, non-catchy/non-salesy
- Body length: 50 to 100 words (extremely concise)
- Use 1 strong personalization point, or 2 if they naturally flow together (make it sound human, not AI-researched).
{hiring_observation_rule}
- Softly highlight the usual friction in candidate screening (do not exaggerate).
- Introduce InterviewScreener.com naturally (simply include the URL "InterviewScreener.com" in the text).
- Mention 1-2 practical benefits (e.g., async interviews, structured tech summaries) without absolute claims.
- 1 CTA only: Ask a low-friction question to gauge interest (e.g., "Open to exploring how?", "Worth a quick chat?", or "Does this align with your current priorities?"). Do NOT always ask for a 15-minute demo.
- Tone: peer-to-peer, credible, casual. No hyperbole.
- No filler ("Hope you are well").
- Greeting must use first name.
- Sign off as Nirmallya.
- Output valid JSON only with keys: subject, body.

Return exactly:
{{
  "subject": "...",
  "body": "Hi {contact_name},\\n\\nYour email body here.\\n\\nBest,\\nNirmallya"
}}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
            max_tokens=500,
            response_format={"type": "json_object"}
        )
        raw = response.choices[0].message.content.strip()

        result = json.loads(raw)
        subject_text = result.get("subject", "").strip()
        body_text = result.get("body", "").strip()

        # Output Validation
        word_count = len(body_text.split())
        if word_count < 20 or word_count > 150:
            raise ValueError(f"Body length ({word_count} words) out of bounds.")
        if len(subject_text.split()) > 10:
            raise ValueError("Subject line too long.")
        if "InterviewScreener.com" not in body_text:
            raise ValueError("Missing product URL.")
        if "Nirmallya" not in body_text:
            raise ValueError("Missing sign-off.")
        if contact_name != "there" and contact_name.lower() not in body_text.lower():
            raise ValueError("Missing contact name in body.")

        return {
            "subject": subject_text or f"Quick note for {contact_name}",
            "body": body_text,
        }

    except Exception as e:
        print(f"  [Email Drafter] Validation/generation failed for {contact.get('full_name', '?')}: {e}")
        # Top-tier Fallback template (no longer sounds like a standard SaaS template)
        return {
            "subject": f"Question about {company_name}'s screening process",
            "body": (
                f"Hi {contact_name},\n\n"
                f"I noticed {company_name} is growing, and I wanted to see how your team is handling candidate screening right now.\n\n"
                f"Instead of scheduling endless intro calls, hiring teams use InterviewScreener.com to let candidates complete AI-guided audio interviews. You get a consistent evaluation baseline on your own schedule before committing to live interviews.\n\n"
                f"Would it be helpful to see how other teams are doing this?\n\n"
                f"Best,\nNirmallya"
            ),
        }


def draft_batch(contacts_with_data: list[dict]) -> list[dict]:
    """
    Draft emails for a list of contacts with their company + AI data.
    
    Args:
        contacts_with_data: list of dicts, each containing:
            - contact: contact dict (full_name, title, email, etc.)
            - company: company data dict
            - ai_data: AI brain results dict
            - contact_id: UUID from outreach_contacts table
            - company_id: UUID from companies table
    
    Returns:
        list of draft dicts with: contact_id, company_id, subject, body
    """
    drafts = []
    total = len(contacts_with_data)
    
    print(f"\n[Email Drafter] Drafting {total} personalized emails...\n")
    
    for i, item in enumerate(contacts_with_data):
        contact = item.get("contact", {})
        company = item.get("company", {})
        ai_data = item.get("ai_data", {})
        
        name = contact.get("full_name", "Unknown")
        company_name = company.get("name_display", "Unknown")
        
        print(f"  [{i+1}/{total}] Drafting email for {name} at {company_name}...")
        
        result = draft_email(contact, company, ai_data)
        
        drafts.append({
            "contact_id": item.get("contact_id", ""),
            "company_id": item.get("company_id", ""),
            "subject": result["subject"],
            "body": result["body"],
        })
        
        # Small delay between AI calls
        if i < total - 1:
            time.sleep(0.5)
    
    print(f"\n[Email Drafter] Complete. Drafted {len(drafts)} emails.\n")
    return drafts
