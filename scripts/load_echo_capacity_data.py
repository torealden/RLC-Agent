#!/usr/bin/env python3
"""
Load EPA ECHO facility data and state capacity data into the database.

Reads existing Excel (ECHO facilities) and JSON (state capacity) outputs,
then populates:
  - bronze.epa_echo_facility  (~200 US facilities)
  - bronze.permit_capacity    (state capacity summaries: IA, NE, ...)
  - bronze.permit_emission_unit (equipment detail)

Also attempts to match permit records to ECHO facilities by
(state, city, facility_name) to populate the frs_registry_id link.

Usage:
    python scripts/load_echo_capacity_data.py
    python scripts/load_echo_capacity_data.py --echo-only
    python scripts/load_echo_capacity_data.py --iowa-only
    python scripts/load_echo_capacity_data.py --nebraska-only
    python scripts/load_echo_capacity_data.py --state NE --state-json path/to/ne.json
"""

import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / '.env')

import psycopg2
import psycopg2.extras


# =============================================================================
# IOWA SOY PERMITS — mirror of iowa_capacity_collector.py IOWA_SOY_PERMITS
# Used to fill city, facility_number fields in permit_capacity
# =============================================================================

IOWA_SOY_PERMITS = {
    'agp_algona': {
        'name': 'Ag Processing Inc. - Algona',
        'facility_number': '55-01-032',
        'city': 'Algona',
        'permit_number': '15-TV-008R2',
        'media_id': 2707,
    },
    'agp_eagle_grove': {
        'name': 'Ag Processing, Inc. - Eagle Grove',
        'facility_number': '99-01-001',
        'city': 'Eagle Grove',
        'permit_number': '05-TV-005R3',
        'media_id': 2826,
    },
    'agp_emmetsburg': {
        'name': 'Ag Processing, Inc. - Emmetsburg',
        'facility_number': '74-01-012',
        'city': 'Emmetsburg',
        'permit_number': '04-TV-013R2',
        'media_id': 2761,
    },
    'agp_manning': {
        'name': 'Ag Processing, Inc. - Manning',
        'facility_number': '14-02-003',
        'city': 'Manning',
        'permit_number': '11-TV-004R2',
        'media_id': 2808,
    },
    'agp_mason_city': {
        'name': 'Ag Processing, Inc. - Mason City',
        'facility_number': '17-01-027',
        'city': 'Mason City',
        'permit_number': '12-TV-003R2',
        'media_id': 2776,
    },
    'agp_sergeant_bluff': {
        'name': 'Ag Processing, Inc. - Sergeant Bluff',
        'facility_number': '97-04-005',
        'city': 'Sergeant Bluff',
        'permit_number': '99-TV-004R3',
        'media_id': 2909,
    },
    'agp_sheldon': {
        'name': 'Ag Processing, Inc. - Sheldon',
        'facility_number': '71-01-001',
        'city': 'Sheldon',
        'permit_number': '12-TV-001R2',
        'media_id': 2818,
    },
    'adm_des_moines': {
        'name': 'Archer Daniels Midland - Des Moines',
        'facility_number': '77-01-045',
        'city': 'Des Moines',
        'permit_number': '04-TV-020R1',
        'media_id': 2879,
    },
    'bunge_council_bluffs': {
        'name': 'Bunge North America, Inc. - Council Bluffs',
        'facility_number': '78-01-085',
        'city': 'Council Bluffs',
        'permit_number': '02-TV-017R3-M002',
        'media_id': 2736,
    },
    'cargill_cedar_rapids_57004': {
        'name': 'Cargill, Inc. - Cedar Rapids (57-01-004)',
        'facility_number': '57-01-004',
        'city': 'Cedar Rapids',
        'permit_number': '07-TV-006R3',
        'media_id': 2925,
    },
    'cargill_cedar_rapids_east': {
        'name': 'Cargill, Inc. - Soybean East Plant',
        'facility_number': '57-01-003',
        'city': 'Cedar Rapids',
        'permit_number': '99-TV-044R4',
        'media_id': 2777,
    },
    'cargill_cedar_rapids_west': {
        'name': 'Cargill Cedar Rapids West',
        'facility_number': '57-01-002',
        'city': 'Cedar Rapids',
        'permit_number': '07-TV-010R3',
        'media_id': 7165,
    },
    'cargill_eddyville': {
        'name': 'Cargill, Inc. - Eddyville',
        'facility_number': '68-09-001',
        'city': 'Eddyville',
        'permit_number': '06-TV-006R1',
        'media_id': 2795,
    },
    'cargill_fort_dodge': {
        'name': 'Cargill, Inc. - Fort Dodge',
        'facility_number': '94-01-080',
        'city': 'Fort Dodge',
        'permit_number': '17-TV-003R1',
        'media_id': 2881,
    },
    'cargill_iowa_falls': {
        'name': 'Cargill, Inc. - Iowa Falls',
        'facility_number': '42-01-003',
        'city': 'Iowa Falls',
        'permit_number': '99-TV-050R5',
        'media_id': 2708,
    },
    'cargill_sioux_city': {
        'name': 'Cargill, Inc. - Sioux City',
        'facility_number': '97-01-001',
        'city': 'Sioux City',
        'permit_number': '99-TV-013R5',
        'media_id': 2694,
    },
    'cargill_vitamin_e': {
        'name': 'Cargill - Vitamin E - Eddyville',
        'facility_number': '68-09-005',
        'city': 'Eddyville',
        'permit_number': '04-TV-004R3',
        'media_id': 2723,
    },
}


# =============================================================================
# DATABASE HELPERS
# =============================================================================

def get_db_connection():
    """Get psycopg2 connection using .env credentials."""
    password = (os.environ.get('RLC_PG_PASSWORD') or
                os.environ.get('DATABASE_PASSWORD') or
                os.environ.get('DB_PASSWORD'))

    return psycopg2.connect(
        host=os.environ.get('DATABASE_HOST', 'localhost'),
        port=os.environ.get('DATABASE_PORT', '5432'),
        database=os.environ.get('DATABASE_NAME', 'rlc_commodities'),
        user=os.environ.get('DATABASE_USER', 'postgres'),
        password=password,
    )


def create_ingest_run(cursor, data_source_code, job_name, params=None):
    """Create an audit.ingest_run record and return the run UUID."""
    run_id = str(uuid.uuid4())

    cursor.execute("""
        INSERT INTO audit.ingest_run (
            id, data_source_id, job_type, job_name,
            agent_id, agent_version, status, request_params
        ) VALUES (
            %s,
            (SELECT id FROM public.data_source WHERE code = %s),
            'FULL', %s,
            'load_echo_capacity_data.py', '1.0.0',
            'RUNNING', %s
        )
    """, (run_id, data_source_code, job_name, json.dumps(params or {})))

    return run_id


def complete_ingest_run(cursor, run_id, records_inserted, records_updated,
                        status='SUCCESS'):
    """Update an ingest_run record as complete."""
    cursor.execute("""
        UPDATE audit.ingest_run
        SET completed_at = NOW(),
            status = %s,
            records_inserted = %s,
            records_updated = %s
        WHERE id = %s
    """, (status, records_inserted, records_updated, run_id))


# =============================================================================
# 1. LOAD ECHO FACILITIES
# =============================================================================

def load_echo_facilities(excel_path, cursor, run_id):
    """
    Load EPA ECHO facility data from the collector's Excel output.

    Reads the 'Facilities' sheet and upserts into bronze.epa_echo_facility.
    Returns (inserted_count, updated_count).
    """
    print('Loading ECHO facilities from: {}'.format(excel_path))

    df = pd.read_excel(str(excel_path), sheet_name='Facilities')
    print('  Read {} facilities from Excel'.format(len(df)))

    # Replace NaN with None for proper NULL handling
    df = df.where(pd.notnull(df), None)

    upsert_sql = """
        INSERT INTO bronze.epa_echo_facility (
            frs_registry_id, facility_name, street_address, city, state,
            zip_code, county_name, county_fips, epa_region,
            latitude, longitude,
            sic_codes, naics_codes, dfr_naics, dfr_sic,
            operating_status, air_programs, air_classification, air_universe,
            source_id, caa_permit_ids, npdes_permit_ids, rcra_handler_ids,
            tri_facility_id, ghg_reporter_id,
            compliance_status, enforcement_actions,
            search_profile, source_endpoint, collector_version,
            ingest_run_id, collected_at
        ) VALUES (
            %(frs_registry_id)s, %(facility_name)s, %(street_address)s,
            %(city)s, %(state)s, %(zip_code)s, %(county_name)s, %(county_fips)s,
            %(epa_region)s, %(latitude)s, %(longitude)s,
            %(sic_codes)s, %(naics_codes)s, %(dfr_naics)s, %(dfr_sic)s,
            %(operating_status)s, %(air_programs)s, %(air_classification)s,
            %(air_universe)s, %(source_id)s,
            %(caa_permit_ids)s, %(npdes_permit_ids)s, %(rcra_handler_ids)s,
            %(tri_facility_id)s, %(ghg_reporter_id)s,
            %(compliance_status)s, %(enforcement_actions)s,
            %(search_profile)s, %(source_endpoint)s, %(collector_version)s,
            %(ingest_run_id)s, %(collected_at)s
        )
        ON CONFLICT (frs_registry_id) DO UPDATE SET
            facility_name = EXCLUDED.facility_name,
            street_address = EXCLUDED.street_address,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            zip_code = EXCLUDED.zip_code,
            county_name = EXCLUDED.county_name,
            county_fips = EXCLUDED.county_fips,
            epa_region = EXCLUDED.epa_region,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            sic_codes = EXCLUDED.sic_codes,
            naics_codes = EXCLUDED.naics_codes,
            dfr_naics = EXCLUDED.dfr_naics,
            dfr_sic = EXCLUDED.dfr_sic,
            operating_status = EXCLUDED.operating_status,
            air_programs = EXCLUDED.air_programs,
            air_classification = EXCLUDED.air_classification,
            air_universe = EXCLUDED.air_universe,
            source_id = EXCLUDED.source_id,
            caa_permit_ids = EXCLUDED.caa_permit_ids,
            npdes_permit_ids = EXCLUDED.npdes_permit_ids,
            rcra_handler_ids = EXCLUDED.rcra_handler_ids,
            tri_facility_id = EXCLUDED.tri_facility_id,
            ghg_reporter_id = EXCLUDED.ghg_reporter_id,
            compliance_status = EXCLUDED.compliance_status,
            enforcement_actions = EXCLUDED.enforcement_actions,
            search_profile = EXCLUDED.search_profile,
            source_endpoint = EXCLUDED.source_endpoint,
            collector_version = EXCLUDED.collector_version,
            ingest_run_id = EXCLUDED.ingest_run_id,
            collected_at = EXCLUDED.collected_at,
            updated_at = NOW()
    """

    inserted = 0
    updated = 0

    for _, row in df.iterrows():
        frs_id = str(row.get('frs_registry_id', '') or '')
        if not frs_id:
            continue

        params = {
            'frs_registry_id': frs_id,
            'facility_name': str(row.get('facility_name', '') or ''),
            'street_address': str(row.get('street_address', '') or '') or None,
            'city': str(row.get('city', '') or '') or None,
            'state': str(row.get('state', '') or '') or None,
            'zip_code': str(row.get('zip_code', '') or '') or None,
            'county_name': str(row.get('county_name', '') or '') or None,
            'county_fips': str(row.get('county_fips', '') or '') or None,
            'epa_region': str(row.get('epa_region', '') or '') or None,
            'latitude': _to_float(row.get('latitude')),
            'longitude': _to_float(row.get('longitude')),
            'sic_codes': str(row.get('sic_codes', '') or '') or None,
            'naics_codes': str(row.get('naics_codes', '') or '') or None,
            'dfr_naics': str(row.get('dfr_naics', '') or '') or None,
            'dfr_sic': str(row.get('dfr_sic', '') or '') or None,
            'operating_status': str(row.get('operating_status', '') or '') or None,
            'air_programs': str(row.get('air_programs', '') or '') or None,
            'air_classification': str(row.get('air_classification', '') or '') or None,
            'air_universe': str(row.get('air_universe', '') or '') or None,
            'source_id': str(row.get('source_id', '') or '') or None,
            'caa_permit_ids': str(row.get('caa_permit_ids', '') or '') or None,
            'npdes_permit_ids': str(row.get('npdes_permit_ids', '') or '') or None,
            'rcra_handler_ids': str(row.get('rcra_handler_ids', '') or '') or None,
            'tri_facility_id': str(row.get('tri_facility_id', '') or '') or None,
            'ghg_reporter_id': str(row.get('ghg_reporter_id', '') or '') or None,
            'compliance_status': str(row.get('compliance_status', '') or '') or None,
            'enforcement_actions': str(row.get('enforcement_actions', '') or '') or None,
            'search_profile': 'soybean_oilseed',
            'source_endpoint': str(row.get('source_endpoint', '') or '') or None,
            'collector_version': str(row.get('collector_version', '') or '') or None,
            'ingest_run_id': run_id,
            'collected_at': str(row.get('collected_at', '') or '') or None,
        }

        cursor.execute(upsert_sql, params)

        # Check if it was insert or update via xmax
        # (xmax = 0 means fresh insert, xmax > 0 means updated existing row)
        inserted += 1

    print('  Loaded {} ECHO facilities'.format(inserted))
    return inserted, updated


def _to_float(val):
    """Convert value to float, or None."""
    if val is None:
        return None
    try:
        result = float(val)
        if pd.isna(result):
            return None
        return result
    except (ValueError, TypeError):
        return None


# =============================================================================
# 2. LOAD IOWA CAPACITY
# =============================================================================

def load_iowa_capacity(json_path, cursor, run_id):
    """
    Load Iowa capacity summary data from the collector's JSON output.

    Reads the JSON and upserts into bronze.permit_capacity.
    Returns (inserted_count, updated_count).
    """
    print('Loading Iowa capacity from: {}'.format(json_path))

    with open(str(json_path), 'r', encoding='utf-8') as f:
        data = json.load(f)

    print('  Read {} facilities from JSON'.format(len(data)))

    upsert_sql = """
        INSERT INTO bronze.permit_capacity (
            state, permit_number,
            facility_name, facility_number, city, facility_description,
            crush_capacity_tons_per_hour, crush_capacity_bushels_per_day,
            crush_description,
            has_refinery, refinery_capacity_tons_per_hour,
            refinery_capacity_tons_per_year, refinery_description, refining_type,
            has_biodiesel, biodiesel_capacity_mgy,
            permit_source_agency, permit_type, permit_pdf_url,
            total_emission_units_found, crush_units_count,
            refinery_units_count, biodiesel_units_count,
            ingest_run_id, collected_at
        ) VALUES (
            %(state)s, %(permit_number)s,
            %(facility_name)s, %(facility_number)s, %(city)s,
            %(facility_description)s,
            %(crush_capacity_tons_per_hour)s, %(crush_capacity_bushels_per_day)s,
            %(crush_description)s,
            %(has_refinery)s, %(refinery_capacity_tons_per_hour)s,
            %(refinery_capacity_tons_per_year)s, %(refinery_description)s,
            %(refining_type)s,
            %(has_biodiesel)s, %(biodiesel_capacity_mgy)s,
            %(permit_source_agency)s, %(permit_type)s, %(permit_pdf_url)s,
            %(total_emission_units_found)s, %(crush_units_count)s,
            %(refinery_units_count)s, %(biodiesel_units_count)s,
            %(ingest_run_id)s, %(collected_at)s
        )
        ON CONFLICT (state, permit_number) DO UPDATE SET
            facility_name = EXCLUDED.facility_name,
            facility_number = EXCLUDED.facility_number,
            city = EXCLUDED.city,
            facility_description = EXCLUDED.facility_description,
            crush_capacity_tons_per_hour = EXCLUDED.crush_capacity_tons_per_hour,
            crush_capacity_bushels_per_day = EXCLUDED.crush_capacity_bushels_per_day,
            crush_description = EXCLUDED.crush_description,
            has_refinery = EXCLUDED.has_refinery,
            refinery_capacity_tons_per_hour = EXCLUDED.refinery_capacity_tons_per_hour,
            refinery_capacity_tons_per_year = EXCLUDED.refinery_capacity_tons_per_year,
            refinery_description = EXCLUDED.refinery_description,
            refining_type = EXCLUDED.refining_type,
            has_biodiesel = EXCLUDED.has_biodiesel,
            biodiesel_capacity_mgy = EXCLUDED.biodiesel_capacity_mgy,
            permit_source_agency = EXCLUDED.permit_source_agency,
            permit_type = EXCLUDED.permit_type,
            permit_pdf_url = EXCLUDED.permit_pdf_url,
            total_emission_units_found = EXCLUDED.total_emission_units_found,
            crush_units_count = EXCLUDED.crush_units_count,
            refinery_units_count = EXCLUDED.refinery_units_count,
            biodiesel_units_count = EXCLUDED.biodiesel_units_count,
            ingest_run_id = EXCLUDED.ingest_run_id,
            collected_at = EXCLUDED.collected_at,
            updated_at = NOW()
    """

    count = 0
    for key, summary in data.items():
        permit_info = IOWA_SOY_PERMITS.get(key, {})
        permit_number = (summary.get('permit_number') or
                         permit_info.get('permit_number', ''))
        if not permit_number:
            print('  WARNING: No permit number for {}, skipping'.format(key))
            continue

        media_id = permit_info.get('media_id', '')
        pdf_url = ('https://www.iowadnr.gov/media/{}/download'.format(media_id)
                   if media_id else None)

        params = {
            'state': 'IA',
            'permit_number': permit_number,
            'facility_name': (summary.get('facility_name') or
                              permit_info.get('name', '')),
            'facility_number': permit_info.get('facility_number'),
            'city': permit_info.get('city'),
            'facility_description': summary.get('facility_description'),
            'crush_capacity_tons_per_hour': _to_float(
                summary.get('crush_capacity_tons_per_hour')),
            'crush_capacity_bushels_per_day': _to_float(
                summary.get('crush_capacity_bushels_per_day')),
            'crush_description': summary.get('crush_description') or None,
            'has_refinery': summary.get('has_refinery', False),
            'refinery_capacity_tons_per_hour': _to_float(
                summary.get('refinery_capacity_tons_per_hour')),
            'refinery_capacity_tons_per_year': _to_float(
                summary.get('refinery_capacity_tons_per_year')),
            'refinery_description': summary.get('refinery_description') or None,
            'refining_type': summary.get('refining_type') or None,
            'has_biodiesel': summary.get('has_biodiesel', False),
            'biodiesel_capacity_mgy': _to_float(
                summary.get('biodiesel_capacity_mgy')),
            'permit_source_agency': 'Iowa DNR',
            'permit_type': 'Title V',
            'permit_pdf_url': pdf_url,
            'total_emission_units_found': summary.get(
                'total_emission_units_found'),
            'crush_units_count': summary.get('crush_units'),
            'refinery_units_count': summary.get('refinery_units'),
            'biodiesel_units_count': summary.get('biodiesel_units'),
            'ingest_run_id': run_id,
            'collected_at': datetime.now().isoformat(),
        }

        cursor.execute(upsert_sql, params)
        count += 1

    print('  Loaded {} Iowa capacity records'.format(count))
    return count, 0


# =============================================================================
# 3. LOAD IOWA EMISSION UNITS
# =============================================================================

def load_iowa_emission_units(excel_path, cursor, run_id):
    """
    Load equipment-level detail from the Iowa capacity Excel output.

    Reads the 'Equipment Detail' sheet and inserts into
    bronze.permit_emission_unit with ON CONFLICT DO NOTHING.
    Returns count of rows inserted.
    """
    print('Loading Iowa emission units from: {}'.format(excel_path))

    try:
        df = pd.read_excel(str(excel_path), sheet_name='Equipment Detail')
    except Exception as e:
        print('  WARNING: Could not read Equipment Detail sheet: {}'.format(e))
        return 0

    print('  Read {} emission units from Excel'.format(len(df)))
    df = df.where(pd.notnull(df), None)

    # Build a mapping from facility_name -> permit_number
    name_to_permit = {}
    for key, info in IOWA_SOY_PERMITS.items():
        name_to_permit[info['name']] = info['permit_number']

    insert_sql = """
        INSERT INTO bronze.permit_emission_unit (
            state, permit_number, page, eu_id, description,
            capacity_value, capacity_unit, category,
            raw_line, ingest_run_id
        ) VALUES (
            %(state)s, %(permit_number)s, %(page)s, %(eu_id)s,
            %(description)s, %(capacity_value)s, %(capacity_unit)s,
            %(category)s, %(raw_line)s, %(ingest_run_id)s
        )
        ON CONFLICT DO NOTHING
    """

    count = 0
    skipped = 0

    for _, row in df.iterrows():
        facility_name = str(row.get('Facility', '') or '')

        # Look up permit number from facility name
        permit_number = None
        for name, pn in name_to_permit.items():
            if name in facility_name or facility_name in name:
                permit_number = pn
                break

        # Fallback: try matching against JSON summary names
        if not permit_number:
            for key, info in IOWA_SOY_PERMITS.items():
                if info['city'] in facility_name:
                    permit_number = info['permit_number']
                    break

        if not permit_number:
            skipped += 1
            continue

        # Map category names to lowercase DB values
        raw_category = str(row.get('Category', '') or '')
        category_map = {
            'Crush/Processing': 'crush',
            'Refinery': 'refinery',
            'Biodiesel': 'biodiesel',
            'Boiler': 'boiler',
            'Other': 'other',
        }
        category = category_map.get(raw_category, 'other')

        params = {
            'state': 'IA',
            'permit_number': permit_number,
            'page': int(row['Page']) if row.get('Page') is not None else None,
            'eu_id': str(row.get('EU ID', '') or '') or None,
            'description': str(row.get('Description', '') or '') or None,
            'capacity_value': _to_float(row.get('Capacity Value')),
            'capacity_unit': str(row.get('Capacity Unit', '') or '') or None,
            'category': category,
            'raw_line': str(row.get('Raw Line', '') or '') or None,
            'ingest_run_id': run_id,
        }

        try:
            cursor.execute(insert_sql, params)
            count += 1
        except Exception as e:
            # Skip duplicates silently
            pass

    if skipped:
        print('  Skipped {} units (could not match facility)'.format(skipped))
    print('  Loaded {} emission unit records'.format(count))
    return count


# =============================================================================
# 3b. LOAD STATE CAPACITY (Generic — supports NE and future states)
# =============================================================================

# Nebraska facility permit mapping (mirrors nebraska_capacity_collector.py)
NE_SOY_PERMITS = {
    'adm_lincoln_extraction': {
        'name': 'ADM Soybean Oil Extraction Plant',
        'city': 'Lincoln',
        'frs_registry_id': '110002382680',
    },
    'adm_milling_lincoln': {
        'name': 'ADM Milling',
        'city': 'Lincoln',
        'frs_registry_id': '110015682260',
    },
    'adm_fremont': {
        'name': 'Archer Daniels Midland Co',
        'city': 'Fremont',
        'frs_registry_id': '110000447437',
    },
    'bruning_grain': {
        'name': 'Bruning Grain & Feed Co',
        'city': 'Bruning',
        'frs_registry_id': '110000724306',
    },
    'eco_energy_beatrice': {
        'name': 'Eco-Energy Distrb-Beatrice LLC',
        'city': 'Beatrice',
        'frs_registry_id': '110045416974',
    },
    'frontier_cooperative': {
        'name': 'Frontier Cooperative',
        'city': 'Columbus',
        'frs_registry_id': '110041346843',
    },
    'ingredion_ssc': {
        'name': 'Ingredion Incorporated',
        'city': 'South Sioux City',
        'frs_registry_id': '110040498235',
    },
    'kansas_organic': {
        'name': 'Kansas Organic Producers Assn',
        'city': 'Du Bois',
        'frs_registry_id': '110007131004',
    },
    'ne_soybean_processing': {
        'name': 'Nebraska Soybean Processing',
        'city': 'Scribner',
        'frs_registry_id': '110002441947',
    },
    'richardson_milling': {
        'name': 'Richardson Milling Inc',
        'city': 'South Sioux City',
        'frs_registry_id': '110000497686',
    },
    'agp_david_city': {
        'name': 'AG Processing Inc',
        'city': 'David City',
        'frs_registry_id': '110071344603',
    },
    'norfolk_crush': {
        'name': 'Norfolk Crush LLC',
        'city': 'Norfolk',
        'frs_registry_id': '110071274379',
    },
}

# Registry of state permit mappings for generic loader
STATE_PERMIT_REGISTRY = {
    'IA': {
        'permits': IOWA_SOY_PERMITS,
        'agency': 'Iowa DNR',
        'pdf_url_template': 'https://www.iowadnr.gov/media/{media_id}/download',
    },
    'NE': {
        'permits': NE_SOY_PERMITS,
        'agency': 'Nebraska NDEE',
        'pdf_url_template': None,  # NDEE does not serve PDFs online
    },
}


def load_state_capacity(json_path, state_code, cursor, run_id):
    """
    Generic state capacity loader. Reads a collector JSON output and upserts
    into bronze.permit_capacity.

    Works for any state whose JSON follows the standard format:
    {
      "facility_key": {
        "facility_name": "...",
        "permit_number": "...",
        "crush_capacity_tons_per_hour": 250.0,
        ...
      }
    }

    Returns (inserted_count, updated_count).
    """
    print('Loading {} capacity from: {}'.format(state_code, json_path))

    with open(str(json_path), 'r', encoding='utf-8') as f:
        data = json.load(f)

    print('  Read {} facilities from JSON'.format(len(data)))

    state_info = STATE_PERMIT_REGISTRY.get(state_code, {})
    state_permits = state_info.get('permits', {})
    agency = state_info.get('agency', 'State Air Agency')
    pdf_template = state_info.get('pdf_url_template')

    upsert_sql = """
        INSERT INTO bronze.permit_capacity (
            state, permit_number,
            facility_name, facility_number, city, facility_description,
            crush_capacity_tons_per_hour, crush_capacity_bushels_per_day,
            crush_description,
            has_refinery, refinery_capacity_tons_per_hour,
            refinery_capacity_tons_per_year, refinery_description, refining_type,
            has_biodiesel, biodiesel_capacity_mgy,
            permit_source_agency, permit_type, permit_pdf_url,
            total_emission_units_found, crush_units_count,
            refinery_units_count, biodiesel_units_count,
            frs_registry_id,
            ingest_run_id, collected_at
        ) VALUES (
            %(state)s, %(permit_number)s,
            %(facility_name)s, %(facility_number)s, %(city)s,
            %(facility_description)s,
            %(crush_capacity_tons_per_hour)s, %(crush_capacity_bushels_per_day)s,
            %(crush_description)s,
            %(has_refinery)s, %(refinery_capacity_tons_per_hour)s,
            %(refinery_capacity_tons_per_year)s, %(refinery_description)s,
            %(refining_type)s,
            %(has_biodiesel)s, %(biodiesel_capacity_mgy)s,
            %(permit_source_agency)s, %(permit_type)s, %(permit_pdf_url)s,
            %(total_emission_units_found)s, %(crush_units_count)s,
            %(refinery_units_count)s, %(biodiesel_units_count)s,
            %(frs_registry_id)s,
            %(ingest_run_id)s, %(collected_at)s
        )
        ON CONFLICT (state, permit_number) DO UPDATE SET
            facility_name = EXCLUDED.facility_name,
            facility_number = EXCLUDED.facility_number,
            city = EXCLUDED.city,
            facility_description = EXCLUDED.facility_description,
            crush_capacity_tons_per_hour = EXCLUDED.crush_capacity_tons_per_hour,
            crush_capacity_bushels_per_day = EXCLUDED.crush_capacity_bushels_per_day,
            crush_description = EXCLUDED.crush_description,
            has_refinery = EXCLUDED.has_refinery,
            refinery_capacity_tons_per_hour = EXCLUDED.refinery_capacity_tons_per_hour,
            refinery_capacity_tons_per_year = EXCLUDED.refinery_capacity_tons_per_year,
            refinery_description = EXCLUDED.refinery_description,
            refining_type = EXCLUDED.refining_type,
            has_biodiesel = EXCLUDED.has_biodiesel,
            biodiesel_capacity_mgy = EXCLUDED.biodiesel_capacity_mgy,
            permit_source_agency = EXCLUDED.permit_source_agency,
            permit_type = EXCLUDED.permit_type,
            permit_pdf_url = EXCLUDED.permit_pdf_url,
            total_emission_units_found = EXCLUDED.total_emission_units_found,
            crush_units_count = EXCLUDED.crush_units_count,
            refinery_units_count = EXCLUDED.refinery_units_count,
            biodiesel_units_count = EXCLUDED.biodiesel_units_count,
            frs_registry_id = EXCLUDED.frs_registry_id,
            ingest_run_id = EXCLUDED.ingest_run_id,
            collected_at = EXCLUDED.collected_at,
            updated_at = NOW()
    """

    count = 0
    for key, summary in data.items():
        permit_info = state_permits.get(key, {})

        # Get permit number from multiple sources
        permit_number = (summary.get('permit_number')
                         or summary.get('ndee_appno')
                         or permit_info.get('permit_number', ''))
        if not permit_number:
            # Use facility key as fallback permit identifier
            permit_number = '{}-{}'.format(state_code, key)

        # Build PDF URL if template available
        pdf_url = None
        if pdf_template and permit_info.get('media_id'):
            pdf_url = pdf_template.format(media_id=permit_info['media_id'])

        # Try to get FRS registry ID from multiple sources
        frs_id = (summary.get('frs_registry_id')
                  or permit_info.get('frs_registry_id'))

        params = {
            'state': state_code,
            'permit_number': permit_number,
            'facility_name': (summary.get('facility_name')
                              or permit_info.get('name', '')),
            'facility_number': permit_info.get('facility_number'),
            'city': permit_info.get('city')
                    or summary.get('city'),
            'facility_description': summary.get('facility_description'),
            'crush_capacity_tons_per_hour': _to_float(
                summary.get('crush_capacity_tons_per_hour')),
            'crush_capacity_bushels_per_day': _to_float(
                summary.get('crush_capacity_bushels_per_day')),
            'crush_description': summary.get('crush_description') or None,
            'has_refinery': summary.get('has_refinery', False),
            'refinery_capacity_tons_per_hour': _to_float(
                summary.get('refinery_capacity_tons_per_hour')),
            'refinery_capacity_tons_per_year': _to_float(
                summary.get('refinery_capacity_tons_per_year')),
            'refinery_description': summary.get('refinery_description') or None,
            'refining_type': summary.get('refining_type') or None,
            'has_biodiesel': summary.get('has_biodiesel', False),
            'biodiesel_capacity_mgy': _to_float(
                summary.get('biodiesel_capacity_mgy')),
            'permit_source_agency': agency,
            'permit_type': summary.get('ndee_permit_type') or 'Title V',
            'permit_pdf_url': pdf_url,
            'total_emission_units_found': summary.get(
                'total_emission_units_found'),
            'crush_units_count': summary.get('crush_units'),
            'refinery_units_count': summary.get('refinery_units'),
            'biodiesel_units_count': summary.get('biodiesel_units'),
            'frs_registry_id': frs_id,
            'ingest_run_id': run_id,
            'collected_at': datetime.now().isoformat(),
        }

        cursor.execute(upsert_sql, params)
        count += 1

    print('  Loaded {} {} capacity records'.format(count, state_code))
    return count, 0


# =============================================================================
# 4. MATCH FRS IDs
# =============================================================================

def match_frs_ids(cursor):
    """
    Attempt to link permit_capacity records to ECHO facilities.

    Matches on (state, city) plus facility_name substring similarity.
    Updates permit_capacity.frs_registry_id where a confident match is found.
    Returns count of matched records.
    """
    print('Matching permit capacity records to ECHO facilities...')

    # Get all unmatched permit records
    cursor.execute("""
        SELECT id, state, city, facility_name
        FROM bronze.permit_capacity
        WHERE frs_registry_id IS NULL
    """)
    unmatched = cursor.fetchall()

    if not unmatched:
        print('  All permit records already have FRS IDs')
        return 0

    matched = 0
    unmatched_names = []

    for record in unmatched:
        pc_id = record['id']
        pc_state = record['state']
        pc_city = record['city']
        pc_name = record['facility_name']

        if not pc_city:
            unmatched_names.append(pc_name)
            continue

        # Search ECHO facilities in same state and city
        cursor.execute("""
            SELECT frs_registry_id, facility_name, city
            FROM bronze.epa_echo_facility
            WHERE state = %s AND LOWER(city) = LOWER(%s)
        """, (pc_state, pc_city))

        candidates = cursor.fetchall()

        if not candidates:
            unmatched_names.append('{} ({}, {})'.format(
                pc_name, pc_city, pc_state))
            continue

        # Find best match by name similarity
        best_match = None
        pc_name_lower = pc_name.lower()

        for cand in candidates:
            cand_name_lower = cand['facility_name'].lower()

            # Check if either name contains the other, or share key words
            if (cand_name_lower in pc_name_lower or
                    pc_name_lower in cand_name_lower):
                best_match = cand['frs_registry_id']
                break

            # Check for company name overlap (first word match)
            pc_words = set(pc_name_lower.replace(',', '').replace('.', '').split())
            cand_words = set(cand_name_lower.replace(',', '').replace('.', '').split())
            # Ignore common filler words
            filler = {'inc', 'inc.', 'llc', 'co', 'co.', 'the', '-', 'north',
                       'america'}
            pc_words -= filler
            cand_words -= filler

            overlap = pc_words & cand_words
            if len(overlap) >= 2:
                best_match = cand['frs_registry_id']
                break

        if best_match:
            cursor.execute("""
                UPDATE bronze.permit_capacity
                SET frs_registry_id = %s, updated_at = NOW()
                WHERE id = %s
            """, (best_match, pc_id))
            matched += 1
        else:
            unmatched_names.append('{} ({}, {})'.format(
                pc_name, pc_city, pc_state))

    print('  Matched {} of {} permit records'.format(
        matched, len(unmatched)))

    if unmatched_names:
        print('  Unmatched facilities (may need manual linking):')
        for name in unmatched_names:
            print('    - {}'.format(name))

    return matched


# =============================================================================
# MAIN
# =============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Load EPA ECHO and state capacity data into the database')
    parser.add_argument('--echo-only', action='store_true',
                        help='Only load ECHO facility data')
    parser.add_argument('--iowa-only', action='store_true',
                        help='Only load Iowa capacity data')
    parser.add_argument('--nebraska-only', action='store_true',
                        help='Only load Nebraska capacity data')
    parser.add_argument('--state', default=None,
                        help='Load a specific state (e.g., NE, IA)')
    parser.add_argument('--state-json', default=None,
                        help='Path to state capacity JSON (used with --state)')
    parser.add_argument('--skip-match', action='store_true',
                        help='Skip FRS ID matching step')
    parser.add_argument('--echo-excel', default=None,
                        help='Path to ECHO Excel file')
    parser.add_argument('--iowa-json', default=None,
                        help='Path to Iowa capacity JSON file')
    parser.add_argument('--iowa-excel', default=None,
                        help='Path to Iowa capacity Excel file (for emission units)')
    parser.add_argument('--nebraska-json', default=None,
                        help='Path to Nebraska capacity JSON file')
    args = parser.parse_args()

    # Default file paths
    echo_dir = PROJECT_ROOT / 'collectors' / 'epa_echo' / 'output'

    echo_excel = args.echo_excel or str(
        echo_dir / 'epa_echo_soybean_oilseed_facilities_2026-02-11.xlsx')
    iowa_json = args.iowa_json or str(
        echo_dir / 'iowa_soy_capacity_2026-02-11.json')
    iowa_excel = args.iowa_excel or str(
        echo_dir / 'iowa_soy_capacity_2026-02-11.xlsx')

    # Find latest Nebraska JSON if not specified
    ne_json = args.nebraska_json
    if not ne_json:
        ne_jsons = sorted(echo_dir.glob('nebraska_soy_capacity_*.json'))
        if ne_jsons:
            ne_json = str(ne_jsons[-1])

    # Determine what to load
    load_echo = not (args.iowa_only or args.nebraska_only
                     or (args.state and args.state != 'ECHO'))
    load_iowa = not (args.echo_only or args.nebraska_only
                     or (args.state and args.state != 'IA'))
    load_nebraska = (args.nebraska_only
                     or (args.state and args.state.upper() == 'NE')
                     or (not args.echo_only and not args.iowa_only
                         and not args.state))

    print('=' * 70)
    print('EPA ECHO + State Capacity Database Loader')
    print('=' * 70)
    print()

    conn = get_db_connection()
    conn.autocommit = False
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        total_inserted = 0
        total_updated = 0

        # --- ECHO Facilities ---
        if load_echo:
            echo_run_id = create_ingest_run(
                cursor, 'EPA_ECHO', 'load_echo_facilities',
                {'source_file': echo_excel})

            ins, upd = load_echo_facilities(echo_excel, cursor, echo_run_id)
            total_inserted += ins
            total_updated += upd

            complete_ingest_run(cursor, echo_run_id, ins, upd)
            print()

        # --- Iowa Capacity ---
        if load_iowa:
            permit_run_id = create_ingest_run(
                cursor, 'STATE_PERMIT', 'load_iowa_capacity',
                {'source_file': iowa_json, 'state': 'IA'})

            ins, _ = load_iowa_capacity(iowa_json, cursor, permit_run_id)
            total_inserted += ins

            eu_count = load_iowa_emission_units(
                iowa_excel, cursor, permit_run_id)
            total_inserted += eu_count

            complete_ingest_run(cursor, permit_run_id, ins + eu_count, 0)
            print()

        # --- Nebraska Capacity ---
        if load_nebraska and ne_json and Path(ne_json).exists():
            ne_run_id = create_ingest_run(
                cursor, 'STATE_PERMIT', 'load_nebraska_capacity',
                {'source_file': ne_json, 'state': 'NE'})

            ins, _ = load_state_capacity(ne_json, 'NE', cursor, ne_run_id)
            total_inserted += ins

            complete_ingest_run(cursor, ne_run_id, ins, 0)
            print()
        elif load_nebraska and not ne_json:
            print('Nebraska JSON not found in {}. Skipping NE.'.format(
                echo_dir))
            print('  Run nebraska_capacity_collector.py first.')
            print()

        # --- Generic --state loading ---
        if args.state and args.state.upper() not in ('IA', 'NE', 'ECHO'):
            state_code = args.state.upper()
            state_json = args.state_json
            if state_json and Path(state_json).exists():
                st_run_id = create_ingest_run(
                    cursor, 'STATE_PERMIT',
                    'load_{}_capacity'.format(state_code.lower()),
                    {'source_file': state_json, 'state': state_code})

                ins, _ = load_state_capacity(
                    state_json, state_code, cursor, st_run_id)
                total_inserted += ins

                complete_ingest_run(cursor, st_run_id, ins, 0)
                print()
            else:
                print('No JSON file for state {}. Use --state-json.'.format(
                    state_code))

        # --- FRS ID Matching ---
        if not args.skip_match and not args.echo_only:
            match_count = match_frs_ids(cursor)
            total_updated += match_count
            print()

        # Commit all changes
        conn.commit()

        # --- Verification ---
        print('=' * 70)
        print('VERIFICATION')
        print('=' * 70)

        cursor.execute(
            "SELECT COUNT(*) as cnt FROM bronze.epa_echo_facility")
        echo_count = cursor.fetchone()['cnt']

        cursor.execute("""
            SELECT state, COUNT(*) as cnt
            FROM bronze.permit_capacity
            GROUP BY state ORDER BY state
        """)
        state_counts = cursor.fetchall()

        cursor.execute("""
            SELECT state, COUNT(*) as cnt
            FROM bronze.permit_emission_unit
            GROUP BY state ORDER BY state
        """)
        eu_counts = cursor.fetchall()

        cursor.execute("""
            SELECT COUNT(*) as cnt FROM bronze.permit_capacity
            WHERE frs_registry_id IS NOT NULL
        """)
        matched_count = cursor.fetchone()['cnt']

        total_permits = sum(s['cnt'] for s in state_counts)

        print('  bronze.epa_echo_facility:    {} records'.format(echo_count))
        for s in state_counts:
            print('  bronze.permit_capacity ({}): {} records'.format(
                s['state'], s['cnt']))
        for s in eu_counts:
            print('  bronze.permit_emission_unit ({}): {} records'.format(
                s['state'], s['cnt']))
        print('  FRS ID matched:              {} of {} permits'.format(
            matched_count, total_permits))

        # Test gold views
        print()
        print('Testing gold views...')

        cursor.execute("""
            SELECT facility_name, city, state, crush_capacity_tons_per_hour,
                   refining_type, has_echo_data, has_permit_capacity
            FROM gold.facility_capacity
            WHERE has_permit_capacity = TRUE
            ORDER BY crush_capacity_tons_per_hour DESC NULLS LAST
            LIMIT 5
        """)
        rows = cursor.fetchall()
        if rows:
            print('  gold.facility_capacity (top 5 with capacity):')
            for r in rows:
                print('    {:<35s} {:>8s} {:>4s} {:>10s} {}'.format(
                    r['facility_name'][:35],
                    r['city'] or '',
                    r['state'] or '',
                    '{:.1f}'.format(r['crush_capacity_tons_per_hour'])
                    if r['crush_capacity_tons_per_hour'] else '-',
                    r['refining_type'] or '',
                ))

        cursor.execute("SELECT * FROM gold.state_crush_capacity")
        states = cursor.fetchall()
        if states:
            print()
            print('  gold.state_crush_capacity:')
            for s in states:
                print('    {} - {} facilities, {:.0f} tph total crush, {} refining'.format(
                    s['state'],
                    s['facility_count'],
                    float(s['total_crush_tons_per_hour'] or 0),
                    s['refining_types_present'] or 'none',
                ))

        print()
        print('Done! Total: {} inserted, {} updated'.format(
            total_inserted, total_updated))

    except Exception as e:
        conn.rollback()
        print('ERROR: {}'.format(e))
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
