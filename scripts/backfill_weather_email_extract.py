"""
Backfill bronze.weather_email_extract from Gmail history.

Why: the new WeatherIntelligenceAgent stopped writing to this table on
Feb 13, 2026 when the pipeline was refactored. The synthesizer's
week-over-week comparison and special-reports section need historical
extracts to work, so we replay every World Weather email from a chosen
start date through today.

What it does:
  - Connects to Gmail via the same OAuth token the daily agent uses
  - For each (sender, day) window, queries `after:Y/M/D before:Y/M/D`
    so the per-query 50 result cap is never hit
  - Runs each email through the existing classifier + text extractor
    (regex-based — no LLM cost)
  - Upserts into bronze.weather_email_extract via the agent's
    _save_extract_to_bronze method (ON CONFLICT email_id = idempotent)

No emails are sent. No briefs are synthesized. This is purely the
historical extract replay.

Usage:
    python scripts/backfill_weather_email_extract.py --start 2026-02-13
    python scripts/backfill_weather_email_extract.py --start 2026-02-13 --end 2026-04-30
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
AGENT_DIR = PROJECT_ROOT / "rlc_scheduler" / "agents"
sys.path.insert(0, str(AGENT_DIR))

from weather_intelligence_agent import WeatherIntelligenceAgent  # noqa: E402


def daterange(start: date, end_exclusive: date):
    """Yield each date from start up to (not including) end."""
    cur = start
    while cur < end_exclusive:
        yield cur
        cur += timedelta(days=1)


def fetch_emails_for_window(agent: WeatherIntelligenceAgent,
                            sender: str, day: date) -> list:
    """Query Gmail for one sender on one day. Returns email_data dicts."""
    after_str = day.strftime("%Y/%m/%d")
    before_str = (day + timedelta(days=1)).strftime("%Y/%m/%d")
    query = f"from:{sender} after:{after_str} before:{before_str}"

    try:
        results = agent.gmail_service.users().messages().list(
            userId='me', q=query, maxResults=50,
        ).execute()
    except Exception as e:
        print(f"      Gmail list failed for {sender} {day}: {e}", flush=True)
        return []

    messages = results.get('messages', [])
    emails = []
    for msg in messages:
        # Don't skip via processed_ids — backfill should replay everything;
        # bronze ON CONFLICT handles idempotency.
        ed = agent._get_email_details(msg['id'])
        if ed:
            emails.append(ed)
    return emails


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', type=str, default='2026-02-13',
                    help='First date (inclusive) YYYY-MM-DD')
    ap.add_argument('--end', type=str, default=None,
                    help='Last date (exclusive) YYYY-MM-DD; default = today')
    args = ap.parse_args()

    start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    end_date = (datetime.strptime(args.end, '%Y-%m-%d').date()
                if args.end else date.today() + timedelta(days=1))

    days = (end_date - start_date).days
    print(f"Backfilling weather extracts {start_date} to {end_date} "
          f"({days} days)")

    agent = WeatherIntelligenceAgent()
    if not agent.connect_gmail():
        sys.exit("Gmail auth failed — check token at "
                 "C:/Users/torem/RLC Dropbox/RLC Team Folder/RLC Documents/"
                 "LLM Model and Documents/Projects/Desktop Assistant/token_work.json")

    senders = agent.config["meteorologist_senders"]
    print(f"Senders: {senders}")
    print()

    t_total = time.time()
    n_fetched = n_persisted = n_failed = 0

    for d in daterange(start_date, end_date):
        for sender in senders:
            emails = fetch_emails_for_window(agent, sender, d)
            if not emails:
                continue
            print(f"  {d} {sender[:30]:30s} fetched={len(emails)}", flush=True)
            n_fetched += len(emails)
            for email in emails:
                try:
                    classification = agent.classifier.classify(
                        subject=email['subject'],
                        body_preview=email['body'][:500] if email['body'] else "",
                        attachments=[a['filename'] for a in email.get('attachments', [])],
                    )
                    extracted = agent.text_extractor.extract(
                        email_id=email['id'],
                        subject=email['subject'],
                        body=email['body'],
                        sender=email['from'],
                        received_at=email['received_at'],
                        email_type=classification.email_type,
                        classification=classification.to_dict(),
                    )
                    saved = agent._save_extract_to_bronze(extracted)
                    if saved:
                        n_persisted += 1
                    else:
                        n_failed += 1
                except Exception as e:
                    n_failed += 1
                    print(f"      extraction failed for {email['id']}: {e}",
                          flush=True)

        # Progress every ~7 days
        if (d - start_date).days % 7 == 0:
            elapsed = time.time() - t_total
            print(f"  ... through {d}: fetched={n_fetched} "
                  f"persisted={n_persisted} failed={n_failed} "
                  f"({elapsed:.0f}s elapsed)", flush=True)

    elapsed = time.time() - t_total
    print()
    print(f"DONE in {elapsed/60:.1f} min")
    print(f"  fetched:   {n_fetched}")
    print(f"  persisted: {n_persisted}  (ON CONFLICT updates count here)")
    print(f"  failed:    {n_failed}")


if __name__ == "__main__":
    main()
