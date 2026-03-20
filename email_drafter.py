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


# Structured for cold email: model should pick ONE pain + ONE benefit + one clear CTA.
# If you export text from the product PDF/deck, paste the best lines into PRODUCT_CONTEXT_SOURCE.md
# and mirror them here (keep this block the single source the prompt reads).
PRODUCT_CONTEXT = """
=== INTERVIEWSCREENER.COM — PRODUCT CONTEXT (official deck; do not invent features not listed here) ===

BRAND & URL
- Name: InterviewScreener.com
- URL (always mention as plain text in email body): InterviewScreener.com
- Headline (marketing): the heavy lift of the hiring process automated — you still make the final hire.

CORE POSITIONING — FULL PIPELINE (do NOT shrink the story to "phone screens" or "first round only")
- One product spans: role definition → AI-generated interview questions → share to portals/social → resume upload → AI filter/rank → one-click interview link → candidate joins → AI-led interview → AI score/rank → full candidate profile → your team makes the final hire.

THE FULL PIPELINE (9 steps — never list all in one email; at most hint at 2–3 adjacent stages OR say "end-to-end pipeline" once)
1. Describe the role
2. AI creates interview questions
3. Share on portals & social media
4. Upload resumes → AI filters & ranks
5. Share interview link in one click
6. Candidate joins → AI interviews
7. AI scores & ranks
8. AI builds full candidate profile
9. You make the final hire

COMMERCIAL (optional one short clause in email — exact figures only)
- Pay as you go: $0.005 per resume, $0.15 per minute.
- Choose your AI model.
- No contracts; start free.

WHO USES IT
- TA, recruiters, HR/people ops, hiring managers with many reqs, high resume volume, or tools fragmented across hiring stages.

PROBLEMS TO MENTION (pick ONE — prefer pipeline-wide, not only calendar/phone)
- Resume volume and manual triage before qualified people surface.
- Posting the same role everywhere without a single flow through interviews and scoring.
- Gap between applicants and structured interviews + comparable scores/profiles.
- Hard to compare candidates fairly end-to-end.
- Many open roles → operational load scales faster than headcount.

OUTCOMES (pick ONE)
- One flow from role to ranked, profiled candidates; you decide who to hire.
- Less manual work per applicant across resume handling, interviews, and scoring.
- Structured AI interviews plus scores and profiles without rebuilding the stack in five tools.

DIFFERENTIATORS (at most one per email)
- Full hiring pipeline vs a point fix sold only as "replace phone screens".
- Model choice + usage pricing + no contract.

FRAMING RULES (critical)
- DO: hiring pipeline automation, resume-to-profile, AI interviews as part of the flow, final hire stays human.
- DO NOT: frame the product as ONLY first-round screens, ONLY phone-call reduction, or ONLY scheduling — too narrow vs the deck.
- OK to mention AI audio interviews as one strong piece — always in context of the broader pipeline.

WHAT THIS IS NOT
- Does not remove your final hiring decision (deck: you make the final hire).
- No legal/compliance/bias/guarantee claims.

VOICE & FORBIDDEN WORDS
- Tone: sharp operator / founder, not sales hype.
- Forbidden: game-changer, revolutionary, cutting-edge, 10x, guaranteed, "runs hiring with zero humans".

OPTIONAL ONE-LINERS (paraphrase; max one)
- "From job description to ranked profiles — your call on the final hire."
- "Resumes filtered, candidates interviewed by AI, scored and profiled — usage-based ($0.005/resume, $0.15/min), no contract."
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
You are an expert B2B cold email copywriter who specializes in getting replies from busy hiring leaders.
Your emails sound like they were written by a sharp founder, not a sales rep.

{PRODUCT_CONTEXT}

RECIPIENT & COMPANY
Name: {contact_name}
Title: {contact_title}
Company: {company_name}
Team Size: {company_data.get('team_size', 'Unknown')}
Open Roles: {open_roles_text or 'Unknown'}
Funding: {company_data.get('funding_amount', 'Unknown')}
Company Description: {company_data.get('description', '')[:200]}

RELEVANT SIGNALS
{signal_text}

WHY THEY FIT
{ai_data.get('why_they_fit', '')}

OUTREACH OPENER SUGGESTION
{ai_data.get('outreach_opener', '')}

TASK
Write a cold email to {contact_name} that feels personal, credible, and earns a reply.

STRICT RULES:
1. Subject line: 4-7 words. Specific, curiosity-driven, not salesy. Avoid "Quick question", "Following up", or generic phrases. Make it feel like it was written just for them.
2. Body length: 60 to 90 words ONLY. Every word must earn its place.
3. Opening line: Use the outreach_opener or a specific observation about their company/hiring situation. It must feel like you actually looked them up — NOT like a template. Do NOT start with "I noticed you're hiring" or "With X open roles". Be more specific and human.
4. Pain point: Name ONE concrete friction aligned with the FULL PIPELINE story — e.g. resume triage at scale, fragmented tools across hiring stages, hard to compare candidates end-to-end, bottleneck between applicants and structured interviews/scores, ops load with many reqs. Do NOT default to "phone screens" or "first-round calls" as the only pain unless the opener already implies that specific bottleneck.
5. Product mention: Introduce InterviewScreener.com in ONE natural sentence that reflects the PIPELINE (role → questions → distribution/resume handling → AI interview → scoring/profiles → they still make the final hire). You may briefly include AI interviews as part of that flow — not the sole value prop. Include the URL as plain text. Optional: one exact pricing clause ($0.005/resume, $0.15/min) OR "no contract / start free" — never invent other numbers.
6. Social proof (optional): One subtle line max — no fake stats, no "game changer".
7. CTA: End with ONE low-friction yes/no question. Prefer pipeline language over "phone screen" only, e.g. "Worth a quick look at the full flow?", "Open to seeing how the pipeline works?", "Does consolidating resume → interview → scores sound useful?". Vary the CTA.
8. Tone: Peer-to-peer. Confident but not pushy. Casual but professional. No hyperbole, no filler ("Hope you're well", "I wanted to reach out").
9. Greeting: Use first name only.
10. Sign off: "Best,\\nNirmallya"
11. Output valid JSON only.

WHAT MAKES THIS EMAIL FAIL (avoid at all costs):
- Positioning InterviewScreener as ONLY replacing phone screens or ONLY first-round scheduling (too narrow vs product)
- Generic opener that could apply to any company
- Feature dumping instead of one sharp pain
- Weak, vague CTA
- SaaS-template voice; buzzwords (including "game changer")
- Exaggerating beyond the PRODUCT_CONTEXT (e.g. zero human involvement in hiring)

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
        if word_count < 40 or word_count > 130:
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
        # Fallback template — specific enough to not feel generic
        roles_hint = f"with {open_roles_count} roles open" if open_roles_count else "while scaling the team"
        return {
            "subject": f"Hiring pipeline at {company_name}",
            "body": (
                f"Hi {contact_name},\n\n"
                f"Teams {roles_hint} often get crushed by the whole funnel — resume triage, getting people into structured interviews, "
                f"and ending up with comparable scores and profiles before anyone makes a final call.\n\n"
                f"InterviewScreener.com runs that end-to-end flow (role → questions → resumes filtered/ranked → AI interviews → scoring and full candidate profiles). "
                f"You still make the final hire. Usage-based ($0.005/resume, $0.15/min), no contract.\n\n"
                f"Open to a quick look at how the pipeline fits your team?\n\n"
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
