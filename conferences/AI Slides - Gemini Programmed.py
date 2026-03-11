import base64
import requests
import os

def generate_mermaid_png(mermaid_code, output_filename):
    """
    Generates a PNG image from Mermaid code using the mermaid.ink API.
    """
    print(f"Processing {output_filename}...")
    
    # Encode the mermaid code to base64 to pass it in the URL
    graphbytes = mermaid_code.encode("utf8")
    base64_bytes = base64.b64encode(graphbytes)
    base64_string = base64_bytes.decode("ascii")
    
    # Construct the API URL
    url = "https://mermaid.ink/img/" + base64_string
    
    try:
        # Make the request to the rendering service
        response = requests.get(url, stream=True)
        
        # Check if the request was successful
        if response.status_code == 200:
            with open(output_filename, 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            print(f"Successfully created: {output_filename}")
        else:
            print(f"Failed to render {output_filename}. HTTP Status code: {response.status_code}")
            # Sometimes mermaid.ink returns text explaining syntax errors
            print("Error details:", response.text)
            
    except requests.exceptions.RequestException as e:
        print(f"An error occurred connecting to the rendering service: {e}")

# ==========================================
# Definition 1: System Flowchart
# ==========================================
system_flowchart_code = """
flowchart LR
  subgraph Inputs
    A[USDA WASDE releases]
    B[USDA FAS PSD API]
    C[Census Intl Trade API]
    D[NOAA NCEI CDO API]
    E[EIA API v2]
    F[EPA RFS/RIN public data]
    G[Internal docs + notes\n(OIC, HOBO, Motiva)\n+ BioTrack AI rail module]
  end

  subgraph Medallion
    BR[Bronze\nimmutable snapshots + metadata]
    SI[Silver\nvalidated canonical objects]
    GO[Gold\nscenarios, briefs, dashboards]
  end

  subgraph Agents
    C1[Collector agents\nsnapshot + tag]
    P1[Parser/Normalizer agents\nschema mapping]
    V1[Validator/Reconciler agents\nunits, ranges, diffs, staleness]
    A1[Analyst agents\nscenarios from Silver]
    W1[Writer agents\ncitation-first outputs]
  end

  LOG[Audit log\nrun_id, inputs, versions, approvals]
  HR[Human review gate\nhigh-stakes threshold]

  A-->C1
  B-->C1
  C-->C1
  D-->C1
  E-->C1
  F-->C1
  G-->C1

  C1-->BR
  BR-->P1
  P1-->V1
  V1-->SI
  SI-->A1
  A1-->GO
  GO-->W1
  W1-->HR
  HR-->GO

  C1-->LOG
  P1-->LOG
  V1-->LOG
  A1-->LOG
  W1-->LOG
"""

# ==========================================
# Definition 2: WASDE-day Timeline (Gantt)
# ==========================================
# Note: I cleaned up a few extra spaces in the provided text 
# to ensure the Gantt engine parses it correctly.
wasde_gantt_code = """
gantt
  title WASDE-day pipeline (release to cited brief)
  dateFormat  HH:mm
  axisFormat  %H:%M

  section Release & ingestion
  WASDE released (typ. 12:00 ET) :milestone, m1, 12:00, 0m
  Snapshot to Bronze + metadata  :12:00, 1m

  section Parsing & validation
  Extract target tables (oilseeds/grains) :12:01, 2m
  Normalize keys/units                    :12:03, 1m
  Compute m/m deltas + outlier flags      :12:04, 2m
  Validation gates + conflict checks      :12:06, 1m

  section Brief production
   Draft implications (from validated objs) :12:07, 1m
   Publish cited one-pager + run manifest :12:08, 1m

  section Review & distribution
   Human review (if required)             :12:09, 5m
   Distribute to desk/dashboard           :12:14, 1m
"""

if __name__ == "__main__":
    print("Starting image generation...")
    generate_mermaid_png(system_flowchart_code, "system_flowchart.png")
    print("-" * 20)
    generate_mermaid_png(wasde_gantt_code, "wasde_timeline.png")
    print("Finished.")