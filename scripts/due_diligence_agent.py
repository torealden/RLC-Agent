"""
FIC Layer 4 — Due-Diligence Agent.

Generates a structured due-diligence report for one facility by:
  1. Pulling everything we know about it (profile + edges + permits +
     sentiment + recent news).
  2. If the operator is publicly traded, pulling their last N SEC filings
     (8-K + 10-K + 10-Q) extracted JSONs from
     domain_knowledge/company_reports/{TICKER}/.
  3. Asking Claude to synthesize a structured report covering exec
     summary, facility profile, operator overview, market position,
     material events, risk factors, and a recommendation block.
  4. Persisting both markdown rendering + structured JSON to
     silver.due_diligence_report.

Cloud (Claude) rather than local LLM because the output is client-facing
(banker due diligence) — per `reference_local_vs_cloud_llm.md`.

Usage:
    # Print to stdout
    python scripts/due_diligence_agent.py --facility-id ia.agp_eagle_grove

    # Save to silver.due_diligence_report
    python scripts/due_diligence_agent.py --facility-id ia.agp_eagle_grove --save

    # Force regenerate even if a recent report exists
    python scripts/due_diligence_agent.py --facility-id ia.agp_eagle_grove --save --force

    # Pretty-print the JSON only
    python scripts/due_diligence_agent.py --facility-id ia.agp_eagle_grove --json-only
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import anthropic

from src.services.database.db_config import get_connection

REPORTS_DIR = ROOT / "domain_knowledge" / "company_reports"

# ---------------------------------------------------------------------------
# Operator -> ticker map (extend over time)
# ---------------------------------------------------------------------------

OPERATOR_TICKER_MAP = [
    ("archer-daniels-midland", "ADM"),
    ("archer daniels", "ADM"),
    ("archer", "ADM"),
    ("adm", "ADM"),
    ("bunge", "BG"),
    ("tyson", "TSN"),
    ("hormel", "HRL"),
    ("jbs", "JBS"),
    ("green plains", "GPRE"),
    ("ingredion", "INGR"),
    ("valero", "VLO"),
    ("chevron renewable", "CVX"),
    ("chevron", "CVX"),
    ("darling", "DAR"),
    ("smithfield", "SFD"),
    ("perdue", None),    # private
    ("cargill", None),   # private
    ("agp", None),       # private (cooperative)
    ("ag processing", None),
    ("poet", None),      # private
    ("chs", "CHSCP"),
    ("calmaine", "CALM"),
    ("cal-maine", "CALM"),
    ("pilgrim", "PPC"),
    ("post holdings", "POST"),
    ("conagra", "CAG"),
    ("ricebran", "RIBT"),
    ("corteva", "CTVA"),
    ("nutrien", "NTR"),
    ("mosaic", "MOS"),
    ("cf industries", "CF"),
    ("intrepid", "IPI"),
    ("fmc", "FMC"),
]


def operator_to_ticker(operator: str | None) -> str | None:
    if not operator:
        return None
    op = operator.lower().strip()
    for needle, ticker in OPERATOR_TICKER_MAP:
        if needle in op:
            return ticker
    return None


# ---------------------------------------------------------------------------
# Data gathering
# ---------------------------------------------------------------------------

def fetch_facility(cur, facility_id: str) -> dict | None:
    cur.execute(
        """
        SELECT facility_id, name, industry_code, operator, parent_company,
               city, county, state, country, lat, lon, status,
               data_source, sources, notes,
               verified_at, verified_by, verification_method
        FROM reference.facility_master
        WHERE facility_id = %s
        UNION ALL
        SELECT facility_id, name, 'oilseed_crush' AS industry_code,
               operator, parent_company, city, county, state, country, lat, lon,
               status, data_source, sources, notes,
               verified_at, verified_by, verification_method
        FROM reference.oilseed_crush_facilities
        WHERE facility_id = %s AND is_canonical = TRUE
        LIMIT 1
        """,
        (facility_id, facility_id),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def fetch_edges(cur, facility_id: str) -> dict:
    cur.execute(
        """
        SELECT edge_type, target_facility_id, weight, notes
        FROM reference.facility_edge_weights
        WHERE source_facility_id = %s AND is_active = TRUE
        ORDER BY edge_type, weight DESC
        """,
        (facility_id,),
    )
    out = [dict(r) for r in cur.fetchall()]
    cur.execute(
        """
        SELECT edge_type, source_facility_id, weight, notes
        FROM reference.facility_edge_weights
        WHERE target_facility_id = %s AND is_active = TRUE
        ORDER BY edge_type, weight DESC
        """,
        (facility_id,),
    )
    inc = [dict(r) for r in cur.fetchall()]
    return {"outgoing": out, "incoming": inc}


def fetch_permits(cur, fac: dict) -> list[dict]:
    cur.execute(
        """
        SELECT facility_name, operator, city, permit_number, permit_type,
               expiration_date, n_units, units, facility_totals
        FROM silver.facility_air_permit_capacity
        WHERE state = %s
        """,
        (fac.get("state") or "",),
    )
    out = []
    op = (fac.get("operator") or "").lower()
    nm = (fac.get("name") or "").lower()
    cty = (fac.get("city") or "").lower()
    for r in cur.fetchall():
        d = dict(r)
        f_name = (d.get("facility_name") or "").lower()
        f_op = (d.get("operator") or "").lower()
        f_cty = (d.get("city") or "").lower()
        if cty and f_cty and cty != f_cty:
            continue
        if (op and (op in f_name or op in f_op or f_op in op)) or \
           (nm and (nm in f_name or f_name in nm)):
            out.append(d)
    return out


def fetch_sentiment(cur, facility_id: str, days: int = 90) -> dict:
    # `days` is an int we control; safe to format into the SQL.
    cur.execute(
        f"""
        SELECT as_of_date, topic_sentiments, oil_share, news_count
        FROM gold.facility_sentiment_daily
        WHERE facility_id = %s
          AND as_of_date > CURRENT_DATE - INTERVAL '{int(days)} days'
        ORDER BY as_of_date
        """,
        (facility_id,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    if not rows:
        return {"days_observed": 0, "summary": None}
    # Cheap summary
    news_total = sum((r.get("news_count") or 0) for r in rows)
    return {
        "days_observed": len(rows),
        "first_date": str(rows[0]["as_of_date"]),
        "last_date": str(rows[-1]["as_of_date"]),
        "news_mentions_total": news_total,
        "recent_topics": rows[-1].get("topic_sentiments"),
    }


def fetch_recent_news(cur, facility_id: str, limit: int = 10) -> list[dict]:
    try:
        cur.execute(
            """
            SELECT a.title, a.published_at, a.source_name, a.article_url,
                   c.topic_scores, c.confidence_score
            FROM bronze.news_article a
            JOIN silver.news_classified c ON c.news_article_id = a.id
            WHERE c.facility_relevance_keys::text LIKE %s
               OR c.locality::text LIKE %s
            ORDER BY a.published_at DESC NULLS LAST
            LIMIT %s
            """,
            (f"%{facility_id}%", f"%{facility_id}%", limit),
        )
        return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []


def fetch_sec_extractions(ticker: str, max_filings: int = 10) -> list[dict]:
    """Read the most recent extraction.json files for this ticker."""
    if not ticker:
        return []
    tdir = REPORTS_DIR / ticker
    if not tdir.exists():
        return []
    out = []
    for sub in sorted(tdir.iterdir(), reverse=True):
        if not sub.is_dir():
            continue
        ext = sub / "extraction.json"
        if not ext.exists():
            continue
        try:
            data = json.loads(ext.read_text(encoding="utf-8"))
            # Trim to essentials so we don't blast the prompt
            slim = {
                "filing_dir": sub.name,
                "form": data.get("filing_metadata", {}).get("filing_type")
                        or data.get("_provenance", {}).get("filing_meta", {}).get("form"),
                "date": data.get("filing_metadata", {}).get("filing_date")
                        or data.get("_provenance", {}).get("filing_meta", {}).get("filing_date"),
                "summary": data.get("summary") or data.get("business_overview"),
                "earnings_release": data.get("earnings_release"),
                "segments": data.get("segments") or data.get("segment_commentary"),
                "facilities_named": data.get("facilities_named")
                                     or data.get("facilities_mentioned"),
                "risk_factors_top": data.get("risk_factors_top"),
                "forward_guidance": data.get("forward_guidance"),
                "polarity_overall": data.get("polarity_overall"),
            }
            # Drop empty values
            slim = {k: v for k, v in slim.items() if v not in (None, [], {})}
            out.append(slim)
        except Exception:
            continue
        if len(out) >= max_filings:
            break
    return out


def fetch_kg_context(cur, industry_code: str) -> list[dict]:
    """Pull KG contexts relevant to this facility's industry. Best-effort."""
    industry_to_node_keys = {
        "oilseed_crush": ["oilseed_crushing_plant_model", "soybean_oil",
                          "soybean_meal", "crusher_feasibility_model",
                          "us_soybean_crush"],
        "ethanol": ["ethanol", "rfs2", "rin_oversupply_model"],
        "biodiesel": ["bbd_balance_sheet_model", "bbd_margin_model",
                      "feedstock_supply_chain_model"],
        "renewable_diesel": ["renewable_diesel", "diamond_green_diesel",
                              "rd_price_stack"],
        "pork_packing": ["us_pork_complex"],
        "egg_layers": ["us_egg_complex"],
    }
    keys = industry_to_node_keys.get(industry_code, [])
    if not keys:
        return []
    out = []
    try:
        cur.execute(
            """
            SELECT n.node_key, n.label, c.context_type, c.body
            FROM core.kg_node n
            JOIN core.kg_context c ON c.node_id = n.id
            WHERE n.node_key = ANY(%s)
            LIMIT 12
            """,
            (keys,),
        )
        for r in cur.fetchall():
            d = dict(r)
            body = d.get("body")
            if isinstance(body, dict):
                ctx_text = body.get("context") or body.get("description") \
                           or body.get("summary") or ""
            else:
                ctx_text = str(body)[:500]
            out.append({
                "node_key": d["node_key"],
                "label": d["label"],
                "type": d["context_type"],
                "context": (ctx_text or "")[:500],
            })
    except Exception:
        pass
    return out


# ---------------------------------------------------------------------------
# Prompt + Claude call
# ---------------------------------------------------------------------------

PROMPT_VERSION = "dd-v1-2026-05-09"
DEFAULT_MODEL = "claude-sonnet-4-6"

REPORT_SCHEMA = {
    "exec_summary": "3-5 sentences. Plain English. What this facility is, "
                    "key strengths, key risks, overall takeaway.",
    "facility_profile": {
        "operator": "string",
        "location": "string",
        "industry": "string",
        "estimated_capacity": "with unit, e.g. '40 mil bu/yr soy crush'; null if unknown",
        "status": "active | idle | closed | announced | under_construction | unknown",
        "permit_summary": "1-2 sentences on what permits / units we know about",
        "geographic_position": "1 sentence — proximity to feedstock, transport, peers"
    },
    "operator_overview": {
        "type": "public | private | subsidiary | cooperative",
        "ticker": "if public; else null",
        "recent_financial_highlights": "if public, 2-3 sentences citing actual numbers from the SEC filings provided; else null",
        "segment_exposure": "what business lines / commodities matter",
        "recent_strategic_actions": "M&A, divestitures, expansions, capex from filings; else null"
    },
    "market_position": {
        "regional_peers": "list of nearby same-industry facilities from the edges data",
        "competitive_assessment": "1-2 sentences",
        "draw_area_implications": "1 sentence — what's the catchment area context"
    },
    "material_events_recent": [
        {"date": "ISO date", "event": "1-line", "source": "string", "polarity": "positive | negative | neutral"}
    ],
    "risk_factors": {
        "commodity_exposure": "string",
        "regulatory": "string",
        "climate_weather": "string",
        "operational": "string",
        "financial": "string"
    },
    "recommendation": {
        "greenlight_signals": ["list of strings"],
        "yellow_flags": ["list of strings"],
        "dealbreakers": ["empty list if none, else strings"],
        "overall_assessment": "1-2 sentence summary recommendation"
    }
}

SYSTEM_PROMPT = """You are a senior commodity-markets analyst writing a
due-diligence brief for a banker considering financing a facility.

You will be given a structured data packet about one facility — its
profile, geographic relationships to peers, any extracted air-permit
data, sentiment timeseries, recent news, and (for publicly-traded
operators) extracted SEC filings.

Your output MUST be a single JSON object matching the provided schema
exactly. No prose outside the JSON. No markdown fences.

Rules:
1. Use null (or empty array for list fields) when the data packet doesn't
   support a value. Never invent a number, capacity, ticker, or fact.
2. When citing financials or events, reference the actual source provided
   (e.g. "ADM Q1 2026 8-K", "permit IA-XXXX-EU03").
3. Be specific. Vague phrases like "various risks" are useless. Name them.
4. The recommendation block is the most important — be honest. If the
   data packet is too thin to assess, say so explicitly in
   overall_assessment.
5. Keep total prose under ~600 words across all fields. Brevity > volume.
"""


def build_prompt(facility: dict, edges: dict, permits: list, sentiment: dict,
                 news: list, sec_filings: list, kg_context: list) -> str:
    payload = {
        "facility_profile": facility,
        "geographic_relationships": edges,
        "air_permits_extracted": permits if permits else "(none extracted yet for this facility)",
        "sentiment_timeseries_summary": sentiment,
        "recent_news": news if news else "(no news tagged to this facility)",
        "operator_sec_filings_extracted": sec_filings if sec_filings
                                          else "(operator not public, or no filings extracted)",
        "kg_analytical_context": kg_context if kg_context
                                 else "(no industry-level context available)",
    }
    schema_str = json.dumps(REPORT_SCHEMA, indent=2)
    return (
        "Produce a due-diligence report on this facility. "
        "Output a single JSON object matching this schema exactly:\n\n"
        f"{schema_str}\n\n"
        "Here is the data packet:\n\n"
        f"{json.dumps(payload, indent=2, default=str)}\n\n"
        "Emit only the JSON."
    )


def call_claude(prompt: str, model: str = DEFAULT_MODEL,
                max_tokens: int = 4096) -> tuple[dict, dict]:
    """Returns (parsed_json, usage_dict)."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise SystemExit("FATAL: ANTHROPIC_API_KEY not set in .env")
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    # Extract the text response
    text = "".join(blk.text for blk in msg.content if hasattr(blk, "text"))
    # Strip any accidental markdown fences (defensive)
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```", 2)[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.rsplit("```", 1)[0].strip()
    parsed = json.loads(text)
    usage = {
        "input_tokens": msg.usage.input_tokens,
        "output_tokens": msg.usage.output_tokens,
        "model": msg.model,
    }
    return parsed, usage


# Sonnet 4.6 pricing as of 2026-05 (per million tokens, rough)
COST_PER_M_INPUT_USD = 3.00
COST_PER_M_OUTPUT_USD = 15.00


def estimate_cost(input_tokens: int, output_tokens: int) -> float:
    return (input_tokens * COST_PER_M_INPUT_USD
            + output_tokens * COST_PER_M_OUTPUT_USD) / 1_000_000


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------

def render_markdown(report: dict, fac: dict) -> str:
    g = lambda d, k, default="—": (d.get(k) if isinstance(d, dict) else None) or default
    nm = fac.get("name") or fac.get("operator") or fac["facility_id"]
    md = [f"# Due-Diligence Report — {nm}",
          f"**Facility ID:** `{fac['facility_id']}`  "
          f"·  **Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"]

    md.append("## 1. Executive Summary\n")
    md.append(report.get("exec_summary") or "_No summary generated._")
    md.append("")

    md.append("## 2. Facility Profile")
    fp = report.get("facility_profile") or {}
    for k, label in [("operator", "Operator"), ("location", "Location"),
                     ("industry", "Industry"),
                     ("estimated_capacity", "Estimated capacity"),
                     ("status", "Status"),
                     ("permit_summary", "Permit summary"),
                     ("geographic_position", "Geographic position")]:
        md.append(f"- **{label}:** {g(fp, k)}")
    md.append("")

    md.append("## 3. Operator Overview")
    oo = report.get("operator_overview") or {}
    md.append(f"- **Type:** {g(oo, 'type')}")
    if oo.get("ticker"):
        md.append(f"- **Ticker:** `{oo['ticker']}`")
    if oo.get("recent_financial_highlights"):
        md.append(f"- **Recent financials:** {oo['recent_financial_highlights']}")
    if oo.get("segment_exposure"):
        md.append(f"- **Segments / commodities:** {oo['segment_exposure']}")
    if oo.get("recent_strategic_actions"):
        md.append(f"- **Recent strategic actions:** {oo['recent_strategic_actions']}")
    md.append("")

    md.append("## 4. Market Position")
    mp = report.get("market_position") or {}
    if mp.get("regional_peers"):
        md.append("- **Regional peers:**")
        for p in mp["regional_peers"]:
            md.append(f"  - {p}")
    md.append(f"- **Competitive assessment:** {g(mp, 'competitive_assessment')}")
    md.append(f"- **Draw-area implications:** {g(mp, 'draw_area_implications')}")
    md.append("")

    md.append("## 5. Material Events")
    me = report.get("material_events_recent") or []
    if not me:
        md.append("_None reported._")
    else:
        for e in me:
            md.append(f"- **{e.get('date', '?')}** — {e.get('event', '')}  "
                      f"_({e.get('source', '?')} · {e.get('polarity', '?')})_")
    md.append("")

    md.append("## 6. Risk Factors")
    rf = report.get("risk_factors") or {}
    for k, label in [("commodity_exposure", "Commodity exposure"),
                     ("regulatory", "Regulatory"),
                     ("climate_weather", "Climate / weather"),
                     ("operational", "Operational"),
                     ("financial", "Financial")]:
        md.append(f"- **{label}:** {g(rf, k)}")
    md.append("")

    md.append("## 7. Recommendation")
    rc = report.get("recommendation") or {}
    if rc.get("greenlight_signals"):
        md.append("**Greenlight signals:**")
        for s in rc["greenlight_signals"]:
            md.append(f"- 🟢 {s}")
    if rc.get("yellow_flags"):
        md.append("\n**Yellow flags:**")
        for s in rc["yellow_flags"]:
            md.append(f"- 🟡 {s}")
    if rc.get("dealbreakers"):
        md.append("\n**Dealbreakers:**")
        for s in rc["dealbreakers"]:
            md.append(f"- 🔴 {s}")
    if rc.get("overall_assessment"):
        md.append(f"\n**Overall assessment:** {rc['overall_assessment']}")

    return "\n".join(md)


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------

def generate_report(facility_id: str, model: str = DEFAULT_MODEL) -> dict:
    """Full pipeline: gather, prompt, call, render. Returns a dict with
    everything needed to persist or display."""
    started = time.time()

    with get_connection() as conn:
        cur = conn.cursor()
        fac = fetch_facility(cur, facility_id)
        if not fac:
            raise SystemExit(f"Facility {facility_id} not found.")
        edges = fetch_edges(cur, facility_id)
        permits = fetch_permits(cur, fac)
        sentiment = fetch_sentiment(cur, facility_id)
        news = fetch_recent_news(cur, facility_id)
        kg = fetch_kg_context(cur, fac.get("industry_code") or "")

    ticker = operator_to_ticker(fac.get("operator"))
    sec_filings = fetch_sec_extractions(ticker) if ticker else []

    input_summary = {
        "ticker_inferred": ticker,
        "edges_outgoing_count": len(edges["outgoing"]),
        "edges_incoming_count": len(edges["incoming"]),
        "permits_matched": len(permits),
        "sentiment_days": sentiment.get("days_observed", 0),
        "news_count": len(news),
        "sec_filings_used": len(sec_filings),
        "kg_context_count": len(kg),
    }

    prompt = build_prompt(fac, edges, permits, sentiment, news, sec_filings, kg)
    parsed, usage = call_claude(prompt, model=model)
    md = render_markdown(parsed, fac)
    cost = estimate_cost(usage["input_tokens"], usage["output_tokens"])
    elapsed = time.time() - started

    return {
        "facility_id": facility_id,
        "facility": fac,
        "report_json": parsed,
        "report_markdown": md,
        "input_summary": input_summary,
        "model": usage["model"],
        "prompt_version": PROMPT_VERSION,
        "input_tokens": usage["input_tokens"],
        "output_tokens": usage["output_tokens"],
        "cost_usd": cost,
        "elapsed_sec": elapsed,
    }


def save_report(result: dict, generated_by: str = "fic_user") -> int:
    """Persist to silver.due_diligence_report; return the new id."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO silver.due_diligence_report (
                facility_id, generated_by, model, prompt_version,
                report_json, report_markdown, input_summary,
                input_tokens, output_tokens, cost_usd, elapsed_sec
            )
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                result["facility_id"], generated_by, result["model"],
                result["prompt_version"],
                json.dumps(result["report_json"]),
                result["report_markdown"],
                json.dumps(result["input_summary"]),
                result["input_tokens"], result["output_tokens"],
                round(result["cost_usd"], 4),
                round(result["elapsed_sec"], 2),
            ),
        )
        row = cur.fetchone()
        return row["id"] if isinstance(row, dict) else row[0]


def fetch_latest_report(facility_id: str) -> dict | None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, generated_at, generated_by, model, prompt_version,
                   report_json, report_markdown, input_summary,
                   input_tokens, output_tokens, cost_usd, elapsed_sec
            FROM silver.due_diligence_report
            WHERE facility_id = %s
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (facility_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    # Windows cmd uses cp1252 by default; emojis in the markdown break print().
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--facility-id", required=True, help="e.g. ia.agp_eagle_grove")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--save", action="store_true",
                        help="Persist to silver.due_diligence_report")
    parser.add_argument("--force", action="store_true",
                        help="Generate even if a recent report exists")
    parser.add_argument("--json-only", action="store_true",
                        help="Print only the structured JSON")
    args = parser.parse_args()

    if not args.force:
        existing = fetch_latest_report(args.facility_id)
        if existing:
            age = datetime.now(existing["generated_at"].tzinfo) - existing["generated_at"]
            if age < timedelta(days=1):
                print(f"Existing report from {existing['generated_at']} "
                      f"({age.total_seconds()/3600:.1f}h old). "
                      f"Use --force to regenerate.")
                if args.json_only:
                    print(json.dumps(existing["report_json"], indent=2))
                else:
                    print(existing["report_markdown"])
                return

    result = generate_report(args.facility_id, model=args.model)

    if args.json_only:
        print(json.dumps(result["report_json"], indent=2))
    else:
        print(result["report_markdown"])
        print(f"\n---\n_Tokens: {result['input_tokens']}↓ / "
              f"{result['output_tokens']}↑   "
              f"Cost: ${result['cost_usd']:.4f}   "
              f"Elapsed: {result['elapsed_sec']:.1f}s   "
              f"Model: {result['model']}_")

    if args.save:
        rid = save_report(result)
        print(f"\nSaved as silver.due_diligence_report id={rid}", file=sys.stderr)


if __name__ == "__main__":
    main()
