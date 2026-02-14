"""
Direct PRGID1 search on NDEE website.
Try multiple approaches to find facilities by their PRGID1.
"""

import requests
import re
import time
import json

JSON_PATH = r"C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\collectors\epa_echo\raw\ndee_all_permits.json"

with open(JSON_PATH, "r", encoding="utf-8") as f:
    permits = json.load(f)

# Target facilities
targets = [
    {"name": "ADM Milling", "prgid1": "109 00003", "county": "Lancaster", "city": "Lincoln"},
    {"name": "Bruning Grain & Feed Co", "prgid1": "169 00005", "county": "Thayer", "city": "Bruning"},
    {"name": "Frontier Cooperative", "prgid1": "141 00103", "county": "Platte", "city": "Columbus"},
    {"name": "Kansas Organic Producers Assn", "prgid1": "133 00018", "county": "Pawnee", "city": "Du Bois"},
    {"name": "Nebraska Soybean Processing", "prgid1": "053 00070", "county": "Dodge", "city": "Scribner"},
]

# ==============================================================================
# Approach 1: Search NDEE website directly for each PRGID1
# ==============================================================================
print("=" * 100)
print("APPROACH 1: Direct NDEE search by PRGID1")
print("=" * 100)

# Try the NDEE search endpoint with prgid parameter
for target in targets:
    prgid = target["prgid1"]
    print(f"\nSearching NDEE for PRGID1 = '{prgid}' ({target['name']})...")

    # Try the main search page with prgid parameter
    search_url = "https://deq-iis.ne.gov/zs/permit/result.php"
    params = {
        "prgid": prgid,
    }
    try:
        resp = requests.get(search_url, params=params, timeout=30)
        if resp.status_code == 200:
            # Look for MODSQ links in results
            modsqs = re.findall(r'modsq=(\d+)', resp.text)
            names = re.findall(r'<td[^>]*>([^<]+)</td>', resp.text)
            if modsqs:
                print(f"  Found MODSQ values: {modsqs[:10]}")
                # Show any name/address context
                for m in modsqs[:5]:
                    idx = resp.text.find(f'modsq={m}')
                    if idx > 0:
                        context = resp.text[max(0,idx-200):idx+200]
                        context = re.sub(r'<[^>]+>', ' ', context)
                        context = re.sub(r'\s+', ' ', context).strip()
                        print(f"    MODSQ={m}: {context[:200]}")
            else:
                # Check if page has any useful content
                text = re.sub(r'<[^>]+>', ' ', resp.text)
                text = re.sub(r'\s+', ' ', text).strip()
                if len(text) > 100:
                    print(f"  No MODSQ found. Page text (first 500 chars): {text[:500]}")
                else:
                    print(f"  Empty or no results")
        else:
            print(f"  HTTP {resp.status_code}")
    except Exception as e:
        print(f"  Error: {e}")
    time.sleep(0.5)

# ==============================================================================
# Approach 2: Try POST-based search
# ==============================================================================
print("\n" + "=" * 100)
print("APPROACH 2: POST-based NDEE search")
print("=" * 100)

for target in targets:
    prgid = target["prgid1"]
    print(f"\nPOST search for PRGID1 = '{prgid}' ({target['name']})...")

    search_url = "https://deq-iis.ne.gov/zs/permit/result.php"
    data = {
        "prgid": prgid,
        "county": "",
        "name": "",
        "city": "",
    }
    try:
        resp = requests.post(search_url, data=data, timeout=30)
        if resp.status_code == 200:
            modsqs = re.findall(r'modsq=(\d+)', resp.text)
            if modsqs:
                print(f"  Found MODSQ values: {modsqs[:10]}")
                for m in modsqs[:5]:
                    idx = resp.text.find(f'modsq={m}')
                    if idx > 0:
                        context = resp.text[max(0,idx-200):idx+200]
                        context = re.sub(r'<[^>]+>', ' ', context)
                        context = re.sub(r'\s+', ' ', context).strip()
                        print(f"    MODSQ={m}: {context[:200]}")
            else:
                text = re.sub(r'<[^>]+>', ' ', resp.text)
                text = re.sub(r'\s+', ' ', text).strip()
                print(f"  No MODSQ found. Text length: {len(text)}")
                if "prgid" in text.lower() or "program" in text.lower():
                    # Find the relevant section
                    for kw in ["prgid", "program", "search"]:
                        idx = text.lower().find(kw)
                        if idx >= 0:
                            print(f"    Near '{kw}': ...{text[max(0,idx-50):idx+100]}...")
        else:
            print(f"  HTTP {resp.status_code}")
    except Exception as e:
        print(f"  Error: {e}")
    time.sleep(0.5)

# ==============================================================================
# Approach 3: Check the NDEE search form to understand parameters
# ==============================================================================
print("\n" + "=" * 100)
print("APPROACH 3: Examining NDEE search form")
print("=" * 100)

try:
    resp = requests.get("https://deq-iis.ne.gov/zs/permit/", timeout=30)
    if resp.status_code == 200:
        # Extract form fields
        forms = re.findall(r'<form[^>]*>(.*?)</form>', resp.text, re.DOTALL)
        for i, form in enumerate(forms):
            inputs = re.findall(r'<input[^>]*name=["\']([^"\']+)["\'][^>]*>', form)
            selects = re.findall(r'<select[^>]*name=["\']([^"\']+)["\'][^>]*>', form)
            print(f"  Form {i+1} fields: inputs={inputs}, selects={selects}")

        # Also look for any links or endpoints
        links = re.findall(r'href=["\']([^"\']*permit[^"\']*)["\']', resp.text, re.I)
        print(f"  Permit-related links: {links[:10]}")

        # Show form action
        actions = re.findall(r'<form[^>]*action=["\']([^"\']+)["\'][^>]*>', resp.text)
        print(f"  Form actions: {actions}")
except Exception as e:
    print(f"  Error: {e}")

# ==============================================================================
# Approach 4: Search by facility name on NDEE
# ==============================================================================
print("\n" + "=" * 100)
print("APPROACH 4: Name-based NDEE web search")
print("=" * 100)

name_searches = [
    ("ADM Milling", "ADM Milling"),
    ("ADM Milling", "ADM"),
    ("Bruning Grain", "Bruning Grain"),
    ("Frontier Cooperative Columbus", "Frontier Cooperative"),
    ("Kansas Organic", "Kansas Organic"),
    ("Nebraska Soybean", "Nebraska Soybean"),
    ("Nebraska Soybean", "Soybean Processing"),
    ("Nebraska Soybean", "AGP"),
]

for label, name_query in name_searches:
    print(f"\nSearching NDEE for name = '{name_query}'...")
    search_url = "https://deq-iis.ne.gov/zs/permit/result.php"
    params = {"name": name_query}
    try:
        resp = requests.get(search_url, params=params, timeout=30)
        if resp.status_code == 200:
            modsqs = re.findall(r'modsq=(\d+)', resp.text)
            if modsqs:
                print(f"  Found {len(modsqs)} MODSQ values: {modsqs[:20]}")
                # Try to extract table rows with facility info
                rows = re.findall(r'<tr[^>]*>(.*?)</tr>', resp.text, re.DOTALL)
                for row in rows:
                    if 'modsq=' in row:
                        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                        cells_clean = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
                        modsq_in_row = re.findall(r'modsq=(\d+)', row)
                        if modsq_in_row:
                            print(f"    MODSQ={modsq_in_row[0]}: {' | '.join(cells_clean[:6])}")
            else:
                text = re.sub(r'<[^>]+>', ' ', resp.text)
                text = re.sub(r'\s+', ' ', text).strip()
                print(f"  No results. Page length: {len(text)} chars")
        else:
            print(f"  HTTP {resp.status_code}")
    except Exception as e:
        print(f"  Error: {e}")
    time.sleep(0.5)

# ==============================================================================
# Approach 5: For facilities with known county FIPS prefix, scan detail pages
# of Dodge County records to find 053 00070
# ==============================================================================
print("\n" + "=" * 100)
print("APPROACH 5: Gap-filling - looking for missing PRGID numbers in county records")
print("=" * 100)

# From the Dodge County scan, we found these PRGID1s:
# 053 00001, 006, 007, 008, 009, 018, 019, 022, 025, 030, 050, 057, 063, 064, 065, 070?, 074, 082, 085, 095, 097, 098, 101, 130, 136, 142, 143, 144, 146, 150, 151
# Missing 053 00070 - it's between 065 and 074
# We need to find the MODSQ that has PRGID1 = 053 00070

# Let's search Dodge County records that are in the gap area
dodge_recs = [p for p in permits if p.get("COUNTY", "").strip().upper() == "DODGE"]
dodge_unique = {}
for r in dodge_recs:
    if r["MODSQ"] not in dodge_unique:
        dodge_unique[r["MODSQ"]] = r

# We already checked many - let's find ones we haven't checked
already_checked = {
    "86815", "9169", "9540", "72698", "113269", "48849", "9563", "95516",
    "63478", "48442", "58504", "10105", "65547", "65546", "48534", "9319",
    "76680", "48518", "9176", "58328", "9147", "103073", "9174", "100568",
    "9608", "75990", "9629", "9491", "9662", "9155", "102500", "111337",
    "62569", "9149", "9954", "118806", "120672", "9402", "9304", "74021",
    "117034", "9520", "117324", "10076", "121686", "118380", "117618",
    "117422", "9510", "48609", "117423", "87248", "117424", "74161",
    "117341", "117516",
}

unchecked_dodge = {k: v for k, v in dodge_unique.items() if k not in already_checked}
print(f"\nDodge County: {len(dodge_unique)} unique MODSQ, {len(unchecked_dodge)} not yet checked")
print(f"Checking remaining Dodge County records for PRGID1 = '053 00070'...")

found_target = False
for i, (modsq, rec) in enumerate(unchecked_dodge.items()):
    if found_target:
        break
    html = requests.get(f"https://deq-iis.ne.gov/zs/permit/result_detail.php?modsq={modsq}", timeout=30).text
    if "053 00070" in html:
        print(f"\n  *** FOUND PRGID1 '053 00070' in MODSQ={modsq} ***")
        print(f"  Record: NAME={rec['NAME'].strip()}, CITY={rec.get('CITY','').strip()}")
        context_idx = html.find("053 00070")
        context = html[max(0,context_idx-300):context_idx+300]
        context = re.sub(r'<[^>]+>', ' ', context)
        context = re.sub(r'\s+', ' ', context).strip()
        print(f"  Context: {context[:500]}")
        found_target = True
    else:
        if (i+1) % 10 == 0:
            print(f"  Checked {i+1}/{len(unchecked_dodge)} Dodge records...")
    time.sleep(0.3)

if not found_target:
    print(f"  PRGID1 '053 00070' not found in any Dodge County record")

# Do the same for Lancaster County (ADM Milling, target 109 00003)
print(f"\n--- Lancaster County: searching for PRGID1 = '109 00003' ---")
lancaster_recs = [p for p in permits if p.get("COUNTY", "").strip().upper() == "LANCASTER"]
lancaster_unique = {}
for r in lancaster_recs:
    if r["MODSQ"] not in lancaster_unique:
        lancaster_unique[r["MODSQ"]] = r

# Already checked: 59643, 28863
already_checked_lanc = {"59643", "28863"}
unchecked_lanc = {k: v for k, v in lancaster_unique.items() if k not in already_checked_lanc}

# For Lancaster, we know ADM Milling is MODSQ 59643 but it didn't have the PRGID1.
# The PRGID 109 00003 would be a CAA source at that facility.
# Let's check if MODSQ 59643 detail page has any program IDs at all
print(f"Lancaster County: {len(lancaster_unique)} unique MODSQ, {len(unchecked_lanc)} unchecked")
print("This is too many to scan all. Let's focus on specific candidates.")

# For the ADM Milling, let's look at the detail page more carefully
print("\nRe-checking MODSQ=59643 (ADM Milling) detail page...")
html = requests.get("https://deq-iis.ne.gov/zs/permit/result_detail.php?modsq=59643", timeout=30).text
# Look for ANY ID patterns
all_ids = re.findall(r'\b(\d{3}\s+\d{5})\b', html)
print(f"  All NNN NNNNN patterns: {all_ids}")
# Also look for the facility PRGID in different format
alt_patterns = re.findall(r'10900003|109-00003|109\.00003', html)
print(f"  Alternative PRGID formats: {alt_patterns}")
# Extract full text for inspection
text = re.sub(r'<[^>]+>', '\n', html)
text = re.sub(r'\n+', '\n', text)
lines = [l.strip() for l in text.split('\n') if l.strip()]
print(f"  Detail page has {len(lines)} text lines. First 50:")
for line in lines[:50]:
    print(f"    {line}")

# Check MODSQ 28863 (Archer Daniels Midland) - it had 109 00011
print("\nRe-checking MODSQ=28863 (Archer Daniels Midland Co) detail page...")
html = requests.get("https://deq-iis.ne.gov/zs/permit/result_detail.php?modsq=28863", timeout=30).text
all_ids = re.findall(r'\b(\d{3}\s+\d{5})\b', html)
print(f"  All NNN NNNNN patterns: {all_ids}")
text = re.sub(r'<[^>]+>', '\n', html)
text = re.sub(r'\n+', '\n', text)
lines = [l.strip() for l in text.split('\n') if l.strip()]
print(f"  Detail page has {len(lines)} text lines. First 50:")
for line in lines[:50]:
    print(f"    {line}")

print("\n\nDone.")
