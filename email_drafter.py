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
=== INTERVIEWSCREENER.COM — PRODUCT CONTEXT FOR OUTREACH (do not invent features not listed here) ===

BRAND & URL
- Name: InterviewScreener.com
- URL (always mention as plain text in email body): InterviewScreener.com
- Category: AI-assisted, async candidate screening (audio-first interviews + structured output for hiring teams)

ONE-LINE POSITIONING
- Let candidates complete a structured, role-relevant screening conversation by voice, on their own time; hiring teams get consistent summaries before live interviews.

WHO USES IT
- Primary: Talent acquisition, recruiters, HR / people ops running high-volume or multi-role hiring.
- Secondary: Hiring managers who want a fair first pass without burning calendar on every applicant.
- Candidates: Self-serve async interview (no live scheduling for the screening step).

HOW IT WORKS (4 steps — use at most ONE short phrase from this in an email; never list all four)
1. Hiring team adds / defines the job in the product.
2. System generates relevant, job-specific interview questions (not a one-size-fits-all form).
3. Candidate completes an AI-guided audio interview asynchronously.
4. Team receives structured summaries to compare candidates and decide who moves forward.

PROBLEMS IT ADDRESSES (pick ONE pain that best matches the lead — do not stack)
- Early phone screens and intro calls consume huge calendar time across many applicants.
- Hard to keep first-round evaluation consistent across interviewers and time zones.
- Scheduling back-and-forth and no-shows slow the funnel.
- Notes and impressions vary by person; hard to compare candidates fairly on the same bar.
- Teams with many open roles struggle to give every applicant a timely first touch.

OUTCOMES & BENEFITS (pick ONE; no fabricated metrics)
- Frees live interview time for candidates who already cleared a structured screen.
- Same question set and format for every candidate = easier comparison.
- Candidates progress without waiting for a recruiter slot.
- Hiring team reviews written/audio-derived summaries instead of relying on memory.
- Scales better when applicant volume or role count goes up.

DIFFERENTIATORS (mention at most one in a single sentence)
- Audio / voice screening (not only text forms or chatbots).
- Questions tied to the specific job, not generic screening templates.
- Async by design — built for volume and scheduling friction, not replacing final human interviews.
- Output is structured for hiring decisions (summaries), not just a recording dump.

WHAT THIS IS NOT (avoid implying these in cold email)
- Not a replacement for full final-round or culture-fit interviews with the team.
- Not a promise to "automate hiring end-to-end" or remove human judgment.
- Do not claim legal compliance, bias elimination, or guaranteed hire quality.

VOICE & CLAIMS FOR EMAILS
- Tone: practical, peer-level, recruiting ops — not hype, not "AI revolution" language.
- Forbidden words/phrases in the email: revolutionary, game-changer, cutting-edge, guaranteed, 10x, replace your recruiters.
- Do not cite percentages, customer counts, or ROI unless provided in lead data — never invent.

OPTIONAL PHRASES (paraphrase; do not use more than one)
- "Structured audio screen before you block calendar."
- "Same bar for every candidate on the first pass."
- "They interview when it works for them; you get summaries when it works for you."
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
4. Pain point: Name one concrete friction that hiring teams at this scale actually feel (e.g., "phone screens eating up your week", "inconsistent feedback across interviewers", "candidates ghosting after scheduling"). Pick the most relevant one — do not list multiple.
5. Product mention: Introduce InterviewScreener.com in ONE natural sentence. Include the URL as plain text. Do NOT use marketing language like "cutting-edge" or "revolutionary".
6. Social proof (optional but preferred): Add a brief implied proof if it fits naturally (e.g., "teams hiring at this pace often...", "a few recruiting leads we work with..."). Keep it subtle — no fake stats.
7. CTA: End with ONE specific, low-friction yes/no question that invites a real answer. Examples: "Is async screening something your team has explored?", "Would it be worth a 10-minute look?", "Open to seeing how it works?". Vary the CTA — do NOT always use "Does this align with your current priorities?".
8. Tone: Peer-to-peer. Confident but not pushy. Casual but professional. No hyperbole, no filler phrases ("Hope you're well", "I wanted to reach out").
9. Greeting: Use first name only.
10. Sign off: "Best,\\nNirmallya"
11. Output valid JSON only.

WHAT MAKES THIS EMAIL FAIL (avoid at all costs):
- Generic opener that could apply to any company
- Listing features instead of naming a pain
- Weak, vague CTA that's easy to ignore
- Sounding like a SaaS sales template
- Exaggerating claims or using buzzwords

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
            "subject": f"Candidate screening at {company_name}",
            "body": (
                f"Hi {contact_name},\n\n"
                f"Recruiting teams {roles_hint} often tell us that phone screens alone eat up more time than the actual hiring decision. "
                f"We built InterviewScreener.com so candidates complete an AI-guided audio interview async — "
                f"your team gets structured summaries and a consistent baseline before committing to live rounds.\n\n"
                f"Is async screening something {company_name} has looked into?\n\n"
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
