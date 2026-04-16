"""Seed the weather_adjusted_yield callable into core.kg_callable."""

import json
import psycopg2
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / '.env')


def connect():
    return psycopg2.connect(
        host=os.environ['RLC_PG_HOST'],
        port=os.environ.get('RLC_PG_PORT', 5432),
        database=os.environ.get('RLC_PG_DATABASE', 'rlc_commodities'),
        user=os.environ['RLC_PG_USER'],
        password=os.environ['RLC_PG_PASSWORD'],
        sslmode='require',
    )


SIGNATURE = {
    "inputs": {
        "commodity":              {"type": "enum", "values": ["corn", "soybeans", "wheat"], "required": True},
        "region":                  {"type": "str",  "required": True,
                                    "source": "region_node_key",
                                    "examples": ["us.corn_belt", "us.soy_belt", "br.mato_grosso"]},
        "growth_stage":            {"type": "enum", "values": ["vegetative", "pollination", "flowering",
                                                                 "grain_fill", "pod_fill", "jointing",
                                                                 "heading", "mature"],
                                    "required": True, "source": "usda.crop_progress.development"},
        "current_yield_bpa":       {"type": "float", "units": "bushels_per_acre", "required": True,
                                    "source": "gold.fas_us_corn_balance_sheet"},
        "forecast_rain_in_30d":    {"type": "float", "units": "inches", "range": [0, 25], "required": True,
                                    "source": "silver.weather_forecast_daily"},
        "forecast_temp_f_avg_30d": {"type": "float", "units": "fahrenheit", "range": [-20, 120], "required": True,
                                    "source": "silver.weather_forecast_daily"},
        "forecast_month":          {"type": "int", "range": [1, 12], "required": True},
        "soil_moisture_pct":       {"type": "float", "units": "percent", "range": [0, 100], "required": False,
                                    "source": "silver.weather_observation.soil_moisture_0_7cm"},
    },
    "output": {
        "type": "dict",
        "fields": {
            "predicted_yield_bpa": {"type": "float", "units": "bushels_per_acre"},
            "delta_bpa":           {"type": "float", "units": "bushels_per_acre"},
            "confidence":          {"type": "float", "range": [0, 1]},
            "reasoning":           {"type": "str"},
            "analog_years":        {"type": "list[int]"},
            "warnings":            {"type": "list[str]"},
        },
    },
}

DEFAULTS = {
    "soil_moisture_pct": None,
}

TEST_CASES = [
    {
        "inputs": {
            "commodity": "corn", "region": "us.corn_belt", "growth_stage": "grain_fill",
            "current_yield_bpa": 183.0, "forecast_rain_in_30d": 1.5,
            "forecast_temp_f_avg_30d": 84.0, "forecast_month": 9, "soil_moisture_pct": 38,
        },
        "expected": {"delta_bpa": -2.7, "predicted_yield_bpa": 180.3},
        "tolerance": 0.1,
    },
    {
        "inputs": {
            "commodity": "corn", "region": "us.corn_belt", "growth_stage": "pollination",
            "current_yield_bpa": 183.0, "forecast_rain_in_30d": 4.0,
            "forecast_temp_f_avg_30d": 82.0, "forecast_month": 7,
        },
        "expected": {"delta_bpa": 0.0},
        "tolerance": 0.1,
    },
]

SELF_EXPLORATION = {
    "sweep_params": ["forecast_rain_in_30d", "forecast_temp_f_avg_30d"],
    "ranges": {
        "forecast_rain_in_30d": {"min": 0.0, "max": 6.0, "step": 0.5, "units": "inches"},
        "forecast_temp_f_avg_30d": {"min": 75, "max": 98, "step": 2, "units": "fahrenheit"},
    },
    "baseline_hints": {
        "baseline_rain_in_30d": 3.0,
        "baseline_temp_f": 82.0,
    },
    "downstream_nodes": [
        "corn",                          # yield changes re-score cross-market links
        "fas_us_corn_balance_sheet",     # propagate to ending stocks
        "cftc.cot",                      # extreme yield deltas may trigger positioning rules
    ],
    "threshold_rules": [
        {"when": "delta_bpa < -2.0", "surface": "Material downside to USDA yield; check stocks-to-use cascade"},
        {"when": "delta_bpa > 2.0",  "surface": "Upside surprise; check whether analyst consensus is below USDA"},
    ],
    "report_format": "sensitivity_table",
    "function": "self_explore",  # alternate entry point
}


def main():
    conn = connect()
    cur = conn.cursor()

    # Find the corn_yield_model or crop_condition_yield_model node
    cur.execute("""
        SELECT id FROM core.kg_node WHERE node_key = 'crop_condition_yield_model' LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        raise SystemExit("Node 'crop_condition_yield_model' not found — cannot attach callable.")
    node_id = row[0]

    # Find the source context (expert_rule / yield_model_parameters)
    cur.execute("""
        SELECT id FROM core.kg_context
        WHERE node_id = %s AND context_key = 'yield_model_parameters' LIMIT 1
    """, (node_id,))
    ctx = cur.fetchone()
    source_context_id = ctx[0] if ctx else None

    cur.execute("""
        INSERT INTO core.kg_callable (
            callable_key, node_id, label, description,
            callable_type, signature, implementation,
            defaults, units, test_cases, self_exploration,
            source_context_id, source_note, confidence, status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (callable_key) DO UPDATE SET
            signature = EXCLUDED.signature,
            implementation = EXCLUDED.implementation,
            defaults = EXCLUDED.defaults,
            test_cases = EXCLUDED.test_cases,
            self_exploration = EXCLUDED.self_exploration,
            source_context_id = EXCLUDED.source_context_id,
            confidence = EXCLUDED.confidence,
            updated_at = NOW()
        RETURNING id
    """, (
        'weather_adjusted_yield',
        node_id,
        'Weather-Adjusted Yield Estimate',
        'Given commodity, region, growth stage, current USDA yield, forecast rain + temp, '
        'returns an adjusted yield with confidence, reasoning, and analog years. Implements '
        'kg_context yield_model_parameters with region-specific rain/temp sensitivities.',
        'python',
        json.dumps(SIGNATURE),
        'src.kg.callables.weather_yield.run',
        json.dumps(DEFAULTS),
        json.dumps({"output_main": "bushels_per_acre"}),
        json.dumps(TEST_CASES),
        json.dumps(SELF_EXPLORATION),
        source_context_id,
        'kg_context #yield_model_parameters (HB signature methodology) + placeholder region climatology',
        0.70,
        'draft',
    ))
    new_id = cur.fetchone()[0]
    conn.commit()
    print(f"Seeded kg_callable id={new_id}, callable_key=weather_adjusted_yield")
    print(f"  Attached to node_id={node_id} (crop_condition_yield_model)")
    print(f"  Source context id={source_context_id}")

    # Verify via detail view
    cur.execute("""
        SELECT callable_key, node_key, callable_type, has_self_exploration, has_tests, confidence
        FROM core.kg_callable_detail WHERE callable_key = 'weather_adjusted_yield'
    """)
    r = cur.fetchone()
    print(f"  Verified via view: {r}")

    conn.close()


if __name__ == '__main__':
    main()
