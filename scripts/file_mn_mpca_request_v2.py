"""
Retry the MN MPCA filing after MNIT rejected v1 (likely unicode in subject
or attachment scan).

Changes from v1:
  - ASCII-only subject (no em-dash, no special chars).
  - Full request text inline in body (no PDF attachment).
  - Single recipient (Sean Bryant only) to look less like bulk mail.
  - Plain text only (no HTML alternative).
"""
import base64
import os
import sys
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
from dotenv import load_dotenv
load_dotenv()

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

GMAIL_TOKEN = Path(
    r"C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC Documents"
    r"\LLM Model and Documents\Projects\Desktop Assistant\token_work.json"
)
SEAN_EMAIL = "sean.bryant@state.mn.us"
TORE_EMAIL = "tore.alden@roundlakescommodities.com"

SUBJECT = "Bundled Title V air permit request - 41 MN facilities"

BODY = """Hello Sean,

This is a consolidated Information Request covering 41 Minnesota facilities for which Round Lake Companies would like Title V air-permit documents. Round Lake Companies is an agricultural-commodity analytics firm; we use facility-level operating-permit data to build models of US production capacity and emissions in the biomass-based diesel and feedstock supply chain.

We have an earlier individual request already in your queue:
  - Subject: Information Request - Title V air permit, Ag Processing Inc. - Dawson (AQ07300002)
  - Originally submitted 2026-05-13, acknowledged by MPCA on 2026-05-19.

Please consolidate that original request into this bundle and disregard it as a standalone.

The facilities are prioritized below in tiers so MPCA can batch-respond. If it is easier to process priority-by-priority, that works on our end. The request is large enough to be worth your team's bundling rather than 41 separate filings.

DOCUMENTS REQUESTED PER FACILITY:
  1. Current active Title V (Major Air Operating Permit) document, including all attachments and appendices.
  2. Permit modifications, amendments, and administrative revisions from the past 5 years.
  3. Facility-wide emission unit inventory (Form GI-05A and/or GI-05B equivalents).
  4. CAM plans, Title V semi-annual deviation reports, and CD/CA compliance certifications from the past 3 years.
  5. Construction permit documents for any new emission unit installed in the past 5 years.

FACILITY LIST (41, prioritized):

Tier 1 - BBD feedstock direct (15)
  1.  Ag Processing Inc. (AGP) - Dawson  [original AQ07300002]
  2.  Archer Daniels Midland - Red Wing
  3.  CHS Fairmont
  4.  CHS Mankato
  5.  Minnesota Soybean Processors - Brewster
  6.  Archer Daniels Midland - Mankato (fats/oils refining)
  7.  ADM - Marshall (wet corn milling, ethanol + DCO)
  8.  REG Albert Lea LLC (oleochemical / biodiesel)
  9.  Minnesota Clean Fuels Inc. (chemical mfg, biofuel) - Dundas
  10. Cargill Inc. - Blooming Prairie (oleochemical)
  11. Flint Hills Resources Pine Bend Refinery - Rosemount
  12. Marathon Saint Paul Park Refinery
  13. Western Refining Terminals - Cottage Grove
  14. NUOL Green Chemistry - Little Falls
  15. Primary Products Ingredients Americas LLC - Duluth

Tier 2 - Ethanol corridor (7)
  16. Bushmills Ethanol - Atwater
  17. Chippewa Valley Ethanol Company - Benson
  18. DENCO II LLC - Morris
  19. Greenfield Global Winnebago LLC
  20. Green Plains Fairmont LLC
  21. Heartland Corn Products - Winthrop
  22. POET Biorefining - Preston

Tier 3 - Ethanol-adjacent / DCO food mfg (3)
  23. POET Biorefining - Bingham Lake
  24. POET Biorefining - Lake Crystal LLC
  25. Valero Renewable Fuels Co. - Welcome Plant

Tier 4 - Additional oleochemical (4)
  26. Agri-Energy LLC - Luverne
  27. Al-Corn Clean Fuel LLC - Claremont
  28. Granite Falls Energy LLC
  29. Heron Lake Bioenergy LLC

Tier 5 - Flour milling (9, lower urgency)
  30. ADM Milling - Atkinson Flour Mill - Minneapolis
  31. ADM Milling - Nokomis Flour Mill - Minneapolis
  32. Ardent Mills Flour Mill - Hastings
  33. Bay State Milling Co - Winona
  34. Cargill Inc. - Mankato (flour)
  35. Conagra Flour Milling - Wabasha
  36. General Mills Inc. - Fridley
  37. General Mills Operations - Purity - Minneapolis
  38. J. Rettenmaier USA LP - Cambridge

Tier 6 - Dairy / lipid byproduct (1)
  39. Land O' Lakes Inc. - Melrose

Tier 7 - Other lubricants and chemical (2, low urgency)
  40. Conklin Company Inc. - Shakopee
  41. Loadmaster Lubricants LLC - Hugo

If any of the facility names above do not match how MPCA indexes them (DBA changes, FRS registry IDs, etc.), please use your best match against MPCA records and flag anything ambiguous in your response. Happy to clarify by phone if it helps.

Thank you for the bundling - I know this is more efficient on your end than 41 separate filings.

Tore Alden
Round Lake Companies
203-554-5028
tore.alden@roundlakescommodities.com

(Note: an earlier version of this request was sent on 2026-05-21 with a PDF attachment and was bounced by MNIT mail filtering with no specific reason. This re-send replaces the request inline so the content is visible to your mail gateway.)
"""

# Send
creds = Credentials.from_authorized_user_file(str(GMAIL_TOKEN))
if creds.expired and creds.refresh_token:
    creds.refresh(Request())
    GMAIL_TOKEN.write_text(creds.to_json())
svc = build('gmail', 'v1', credentials=creds)

msg = MIMEText(BODY, 'plain', 'us-ascii')
msg['Subject'] = SUBJECT
msg['To'] = SEAN_EMAIL
msg['Cc'] = TORE_EMAIL
msg['From'] = TORE_EMAIL

raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
result = svc.users().messages().send(userId='me', body={'raw': raw}).execute()
print(f"Sent v2:")
print(f"  Subject:    {SUBJECT}")
print(f"  To:         {SEAN_EMAIL}")
print(f"  Cc:         {TORE_EMAIL}")
print(f"  Message-Id: {result.get('id')}")
print(f"  Body length: {len(BODY)} chars (inline, no attachment)")
