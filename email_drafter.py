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
WHAT: AI-powered candidate screening platform. Recruiters create a job, AI generates
screening questions from the JD, candidates attend async AI video interviews,
recruiters get ranked analysis with AI-scored summaries. Reduces screening time by 80%.

PRICING:
- Starter: $49.99/month (~300 candidates) — perfect for small teams
- Pro: $99.99/month (~1000 candidates) — most popular
- Scale: $299.99/month (~2200 candidates)
- Enterprise: $999.99/month (~5500 candidates)

KEY VALUE PROPS:
1. Eliminates first-round phone screens entirely
2. Candidates complete screening interviews on their own time (async)
3. AI ranks and scores every candidate automatically
4. Structured evaluation — consistent, bias-free screening
5. Integrates with any ATS via webhooks
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
    contact_name = contact.get("first_name") or contact.get("full_name", "").split()[0] if contact.get("full_name") else "there"
    contact_title = contact.get("title", "")
    
    # Build signal summary
    signals = ai_data.get("signal_results", [])
    signal_text = "\n".join([
        f"  - {s.get('signal_id', '')}: {'✅ PASS' if s.get('passed') else '❌ FAIL'} — {s.get('evidence', '')[:100]}"
        for s in signals
    ]) if signals else "No signal data available"
    
    why_fit = ai_data.get("why_they_fit", "")
    opener = ai_data.get("outreach_opener", "")
    plan = ai_data.get("recommended_plan", "Pro")
    score = ai_data.get("final_score", 0)
    
    prompt = f"""
You are a world-class B2B cold email copywriter for InterviewScreener.com.

{PRODUCT_CONTEXT}

═══════════════════════════════════════════════
RECIPIENT
═══════════════════════════════════════════════
Name: {contact.get('full_name', 'there')}
Title: {contact_title}
Company: {company_name}
Seniority: {contact.get('seniority', 'unknown')}

═══════════════════════════════════════════════
COMPANY INTELLIGENCE (from our pipeline)
═══════════════════════════════════════════════
Website: {company_data.get('website', 'N/A')}
Description: {company_data.get('description', 'N/A')[:300]}
Team Size: {company_data.get('team_size', 'Unknown')} employees
Industries: {', '.join(company_data.get('industries', [])) or 'Unknown'}
Country: {company_data.get('country_code', 'Unknown')}
Lead Score: {score}/18

═══════════════════════════════════════════════
SIGNALS (why they're a fit)
═══════════════════════════════════════════════
{signal_text}

AI Analysis: {why_fit[:300] if why_fit else 'N/A'}
Suggested Opener: {opener[:200] if opener else 'N/A'}
Recommended Plan: {plan}

═══════════════════════════════════════════════
YOUR TASK
═══════════════════════════════════════════════

Write a cold email that will make {contact_name} want to reply. Follow these rules:

SUBJECT LINE:
- Max 8 words
- Must reference something SPECIFIC about their company (a role they're hiring, their team size, their industry)
- NO generic subjects like "Quick question" or "Thought of you"
- Good examples: "Screening {company_name}'s 12 open roles faster" or "{contact_name}, your TA team's new superpower"

BODY:
- Max 5 sentences total (SHORT)
- Sentence 1: Observation — cite a SPECIFIC fact (e.g., "I noticed {company_name} has 8 open engineering roles on Greenhouse")
- Sentence 2: Pain point — connect it to a real pain (e.g., "With a team of ~60, that's likely 1-2 recruiters handling 20+ screening calls per week")
- Sentence 3: Solution — explain what InterviewScreener does in ONE line
- Sentence 4: Social proof or specific benefit — mention the {plan} plan or a concrete time saving
- Sentence 5: CTA — Low-commitment ask (e.g., "Would a 15-minute demo make sense?")

TONE:
- Conversational, like a peer-to-peer recommendation
- NOT salesy, NOT corporate, NOT pushy
- Reference their actual title ({contact_title}) naturally if appropriate
- If they're a Founder/CEO, adjust tone to be more strategic/ROI-focused
- If they're HR/TA, focus on daily pain relief

IMPORTANT:
- Do NOT use "Hope this finds you well" or any filler
- Do NOT use exclamation marks
- Do NOT use words like "revolutionary" or "game-changing"
- DO use their first name ({contact_name}) in the greeting
- DO sign off as "Nirmallya" (the sender's name)

Respond ONLY in this JSON format:
{{
  "subject": "Your subject line here",
  "body": "Hi {contact_name},\\n\\nYour email body here.\\n\\nBest,\\nNirmallya\\nInterviewScreener.com"
}}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=500,
        )
        raw = response.choices[0].message.content.strip()

        # Clean up markdown code block if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)
        return {
            "subject": result.get("subject", f"Quick note for {contact_name}"),
            "body": result.get("body", ""),
        }

    except Exception as e:
        print(f"  [Email Drafter] Error drafting for {contact.get('full_name', '?')}: {e}")
        # Fallback template
        return {
            "subject": f"Screening {company_name}'s open roles faster",
            "body": (
                f"Hi {contact_name},\n\n"
                f"I came across {company_name} and noticed you're actively hiring. "
                f"With {company_data.get('team_size', 'your')} employees "
                f"and multiple open roles, your team is probably spending significant time on first-round screens.\n\n"
                f"InterviewScreener.com automates that entirely — candidates complete AI-powered screening interviews "
                f"on their own time, and you get ranked results with AI analysis.\n\n"
                f"Would a quick 15-minute demo make sense this week?\n\n"
                f"Best,\nNirmallya\nInterviewScreener.com"
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
