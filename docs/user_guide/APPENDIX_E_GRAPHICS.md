# Appendix E: Graphic Specifications

[← Back to Table of Contents](00_COVER_AND_TOC.md)

---

This appendix provides specifications for graphics referenced throughout the user guide. Each specification can be used to generate the graphic programmatically.

---

## Architecture Diagram

**Referenced in:** [Part 1: Getting Started](01_GETTING_STARTED.md#12-system-architecture)

**Description:** System architecture showing data flow from sources through collectors to the medallion layers to outputs.

**Specifications:**

```yaml
graphic_id: architecture-diagram
type: flowchart
direction: top-to-bottom
dimensions:
  width: 900
  height: 700

elements:
  - id: sources
    type: box
    label: "DATA SOURCES"
    sublabel: "USDA | Census | CFTC | EIA | CME | CONAB | Weather"
    position: top
    width: 800
    style:
      fill: "#E3F2FD"
      border: "#1976D2"

  - id: collectors
    type: box
    label: "COLLECTORS"
    sublabel: "Python agents with rate limiting, retry logic, logging"
    position: below:sources
    width: 800
    style:
      fill: "#FFF3E0"
      border: "#F57C00"

  - id: bronze
    type: box
    label: "BRONZE LAYER"
    sublabel: "Raw data, exactly as received"
    details:
      - "wasde_cell"
      - "census_trade_raw"
      - "cftc_raw"
    position: below:collectors
    width: 800
    style:
      fill: "#CD7F32"  # Bronze color
      border: "#8B4513"
      text: white

  - id: silver
    type: box
    label: "SILVER LAYER"
    sublabel: "Standardized: (series_id, time, value)"
    details:
      - "observation"
      - "trade_flow"
      - "price"
    position: below:bronze
    width: 800
    style:
      fill: "#C0C0C0"  # Silver color
      border: "#808080"

  - id: gold
    type: box
    label: "GOLD LAYER"
    sublabel: "Analysis-ready views"
    details:
      - "us_corn_balance_sheet"
      - "wasde_changes"
      - "trade_summary"
    position: below:silver
    width: 800
    style:
      fill: "#FFD700"  # Gold color
      border: "#DAA520"

  - id: outputs
    type: box_group
    items:
      - label: "Power BI"
        icon: chart
      - label: "Reports"
        icon: document
      - label: "LLM Analysis"
        icon: brain
    position: below:gold
    arrangement: horizontal
    style:
      fill: "#E8F5E9"
      border: "#4CAF50"

arrows:
  - from: sources
    to: collectors
    style: solid
  - from: collectors
    to: bronze
    style: solid
  - from: bronze
    to: silver
    style: solid
  - from: silver
    to: gold
    style: solid
  - from: gold
    to: outputs
    style: dashed_fan  # One source to multiple targets

fonts:
  title: "Arial Bold, 14pt"
  label: "Arial Bold, 12pt"
  sublabel: "Arial, 10pt"
  details: "Consolas, 9pt"
```

**Python Generation Script Outline:**

```python
"""
Generate architecture diagram using matplotlib or diagrams library.

Libraries to use:
- matplotlib + patches for custom boxes
- OR: diagrams library (pip install diagrams)
- OR: graphviz (pip install graphviz)

Output: PNG at 300 DPI, 900x700 pixels
"""

# Using matplotlib approach:
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

def create_architecture_diagram():
    fig, ax = plt.subplots(figsize=(12, 9))

    # Define layer positions (y-coordinates, top to bottom)
    layers = {
        'sources': 0.85,
        'collectors': 0.70,
        'bronze': 0.55,
        'silver': 0.40,
        'gold': 0.25,
        'outputs': 0.08
    }

    # Draw boxes for each layer
    # ... (implementation details)

    plt.savefig('docs/user_guide/images/architecture_diagram.png', dpi=300)
```

---

## Medallion Flow Diagram

**Referenced in:** [Part 2: Understanding the Data](02_UNDERSTANDING_DATA.md#22-the-medallion-architecture)

**Description:** Horizontal flow showing Bronze → Silver → Gold transformation with example data at each stage.

**Specifications:**

```yaml
graphic_id: medallion-flow
type: horizontal-flow
dimensions:
  width: 1000
  height: 400

elements:
  - id: bronze_box
    type: panel
    position: left
    width: 280
    header:
      text: "BRONZE"
      background: "#CD7F32"
    content:
      type: code_block
      text: |
        {
          "table_id": "04",
          "row_id": "production",
          "value_text": "14,900"
        }
    footer:
      text: "Raw JSON as received"
      style: italic

  - id: silver_box
    type: panel
    position: center
    width: 280
    header:
      text: "SILVER"
      background: "#C0C0C0"
    content:
      type: table
      headers: ["series_id", "time", "value"]
      rows:
        - ["4521", "2025-01", "14900"]
    footer:
      text: "Standardized numeric"
      style: italic

  - id: gold_box
    type: panel
    position: right
    width: 280
    header:
      text: "GOLD"
      background: "#FFD700"
    content:
      type: table
      headers: ["MY", "Production", "S/U"]
      rows:
        - ["24/25", "14,900M", "10.2%"]
    footer:
      text: "Business-ready"
      style: italic

arrows:
  - from: bronze_box
    to: silver_box
    label: "Transform"
    style: thick
  - from: silver_box
    to: gold_box
    label: "Aggregate"
    style: thick
```

---

## Data Sources World Map

**Referenced in:** [Part 2: Understanding the Data](02_UNDERSTANDING_DATA.md#21-data-sources-overview)

**Description:** World map showing geographic distribution of data sources with markers.

**Specifications:**

```yaml
graphic_id: data-sources-map
type: geographic-map
projection: robinson
dimensions:
  width: 1000
  height: 600

regions:
  - id: usa
    highlight: true
    color: "#1976D2"
    markers:
      - location: [38.9, -77.0]  # Washington DC
        label: "USDA, Census, CFTC"
        icon: star
      - location: [29.7, -95.4]  # Houston
        label: "EIA"
        icon: dot
      - location: [41.9, -87.6]  # Chicago
        label: "CME"
        icon: dot

  - id: brazil
    highlight: true
    color: "#4CAF50"
    markers:
      - location: [-15.8, -47.9]  # Brasilia
        label: "CONAB"
        icon: star
      - location: [-15.6, -56.1]  # Cuiaba
        label: "IMEA"
        icon: dot

  - id: argentina
    highlight: true
    color: "#81C784"
    markers:
      - location: [-34.6, -58.4]  # Buenos Aires
        label: "MagyP"
        icon: dot

  - id: canada
    highlight: true
    color: "#64B5F6"
    markers:
      - location: [45.4, -75.7]  # Ottawa
        label: "StatCan, CGC"
        icon: dot

  - id: malaysia
    highlight: true
    color: "#FFB74D"
    markers:
      - location: [3.1, 101.7]  # KL
        label: "MPOB"
        icon: dot

legend:
  position: bottom-left
  items:
    - color: "#1976D2"
      label: "US Government"
    - color: "#4CAF50"
      label: "South America"
    - color: "#64B5F6"
      label: "Canada"
    - color: "#FFB74D"
      label: "Asia-Pacific"
```

**Python Library:** `geopandas` + `matplotlib` or `folium` for interactive

---

## Dashboard Screenshot (Annotated)

**Referenced in:** [Part 3: Daily Operations](03_DAILY_OPERATIONS.md#dashboard-components)

**Description:** Screenshot of Operations Dashboard with numbered annotations pointing to key components.

**Specifications:**

```yaml
graphic_id: dashboard-annotated
type: annotated-screenshot
source: operations-dashboard
dimensions:
  width: 1200
  height: 800

annotations:
  - id: 1
    target: top-left
    label: "Health Score"
    description: "Overall system status (0-100)"
    style:
      color: red
      shape: circle

  - id: 2
    target: top-center
    label: "Summary Metrics"
    description: "OK, Overdue, Failed counts"
    style:
      color: red
      shape: circle

  - id: 3
    target: upper-middle
    label: "Data Freshness Table"
    description: "Status of each data source"
    style:
      color: red
      shape: rectangle

  - id: 4
    target: middle
    label: "Active Alerts"
    description: "Unacknowledged warnings"
    style:
      color: red
      shape: rectangle

  - id: 5
    target: lower-middle
    label: "Collection Trends"
    description: "30-day success/failure chart"
    style:
      color: red
      shape: rectangle

  - id: 6
    target: sidebar
    label: "Dispatcher Status"
    description: "Running/Stopped indicator"
    style:
      color: red
      shape: circle

callout_style:
  line: dashed
  color: "#FF5722"
  background: white
  font: "Arial, 10pt"
```

**Generation approach:**
1. Take actual screenshot of running dashboard
2. Use Python PIL/Pillow to add annotations
3. Or use Figma/design tool manually

---

## Dashboard Layout Template

**Referenced in:** [Part 4: Working with Power BI](04_POWER_BI.md#recommended-dashboard-structure)

**Description:** Wireframe showing recommended Power BI dashboard layout.

**Specifications:**

```yaml
graphic_id: dashboard-layout
type: wireframe
dimensions:
  width: 1000
  height: 700
background: "#F5F5F5"

grid:
  columns: 12
  rows: 8
  gutter: 10

elements:
  - id: header
    type: bar
    grid_position: [1, 1, 12, 1]
    content:
      - type: dropdown
        label: "Commodity"
      - type: dropdown
        label: "Marketing Year"
      - type: dropdown
        label: "Date Range"
      - type: button
        label: "Refresh"
    style:
      background: "#E0E0E0"

  - id: kpi_cards
    type: card_row
    grid_position: [1, 2, 6, 2]
    cards:
      - title: "Stocks-to-Use"
        value: "10.2%"
        subtitle: "vs 12.5% LY"
      - title: "Ending Stocks"
        value: "1.55B"
        subtitle: "-50M MoM"
      - title: "Export Pace"
        value: "78%"
        subtitle: "vs 81% LY"

  - id: price_chart
    type: line_chart
    grid_position: [7, 2, 12, 3]
    title: "Price ($/bu)"
    placeholder: "[Line chart area]"

  - id: balance_table
    type: table
    grid_position: [1, 4, 12, 5]
    title: "Balance Sheet"
    placeholder: "[Matrix with MY columns]"

  - id: export_progress
    type: progress_bar
    grid_position: [1, 6, 5, 7]
    title: "Export Pace"
    placeholder: "[Gauge or progress bar]"

  - id: positioning
    type: bar_chart
    grid_position: [6, 6, 12, 7]
    title: "CFTC Positioning"
    placeholder: "[Bar chart]"

  - id: footer
    type: text
    grid_position: [1, 8, 12, 8]
    content: "Last refreshed: {timestamp} | Data through: {date}"
    style:
      font_size: 9
      color: "#757575"
```

---

## Power BI Get Data Dialog

**Referenced in:** [Part 4: Working with Power BI](04_POWER_BI.md#creating-a-connection)

**Type:** Screenshot with highlight

**Instructions:**
1. Open Power BI Desktop
2. Click Get Data > More
3. Search "PostgreSQL"
4. Take screenshot
5. Add red box around PostgreSQL option

---

## Power BI Navigator Dialog

**Referenced in:** [Part 4: Working with Power BI](04_POWER_BI.md#step-5-select-tables)

**Type:** Screenshot with highlight

**Instructions:**
1. Connect to database
2. Expand gold schema in Navigator
3. Check several tables
4. Take screenshot
5. Add annotation arrows pointing to gold schema and checkboxes

---

## Generation Script Template

Save as `scripts/generate_user_guide_graphics.py`:

```python
#!/usr/bin/env python
"""
Generate graphics for the RLC User Guide.

This script creates all diagrams referenced in the user guide.
Output directory: docs/user_guide/images/

Requirements:
    pip install matplotlib pillow graphviz geopandas

Usage:
    python scripts/generate_user_guide_graphics.py
    python scripts/generate_user_guide_graphics.py --graphic architecture
"""

import argparse
import os
from pathlib import Path

# Ensure output directory exists
OUTPUT_DIR = Path("docs/user_guide/images")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def generate_architecture_diagram():
    """Generate the system architecture diagram."""
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches

    fig, ax = plt.subplots(figsize=(12, 9))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 9)
    ax.axis('off')

    # Color scheme
    colors = {
        'sources': '#E3F2FD',
        'collectors': '#FFF3E0',
        'bronze': '#CD7F32',
        'silver': '#C0C0C0',
        'gold': '#FFD700',
        'outputs': '#E8F5E9'
    }

    # Draw layers (bottom to top for z-ordering)
    layers = [
        (1, 0.5, 10, 1.2, 'outputs', 'OUTPUTS\nPower BI | Reports | LLM'),
        (1, 2.0, 10, 1.2, 'gold', 'GOLD LAYER\nus_corn_balance_sheet, wasde_changes'),
        (1, 3.5, 10, 1.2, 'silver', 'SILVER LAYER\nobservation, trade_flow, price'),
        (1, 5.0, 10, 1.2, 'bronze', 'BRONZE LAYER\nwasde_cell, census_trade_raw, cftc_raw'),
        (1, 6.5, 10, 1.0, 'collectors', 'COLLECTORS\nPython agents with retry, rate limiting'),
        (1, 7.8, 10, 1.0, 'sources', 'DATA SOURCES\nUSDA | Census | CFTC | EIA | CME | CONAB'),
    ]

    for x, y, w, h, layer, text in layers:
        rect = patches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.05",
            facecolor=colors[layer],
            edgecolor='#333333',
            linewidth=2
        )
        ax.add_patch(rect)
        ax.text(x + w/2, y + h/2, text,
                ha='center', va='center',
                fontsize=10, fontweight='bold')

    # Draw arrows
    arrow_props = dict(arrowstyle='->', color='#333333', lw=2)
    for y_start, y_end in [(7.8, 7.5), (6.5, 6.2), (5.0, 4.7), (3.5, 3.2), (2.0, 1.7)]:
        ax.annotate('', xy=(6, y_end), xytext=(6, y_start),
                    arrowprops=arrow_props)

    plt.title('RLC Commodities Platform Architecture', fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'architecture_diagram.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("Generated: architecture_diagram.png")


def generate_medallion_flow():
    """Generate the medallion layer flow diagram."""
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 5)
    ax.axis('off')

    # Three boxes: Bronze, Silver, Gold
    boxes = [
        (0.5, 0.5, 4, 4, '#CD7F32', 'BRONZE\n\n{"table_id": "04",\n "row_id": "production",\n "value_text": "14,900"}'),
        (5.0, 0.5, 4, 4, '#C0C0C0', 'SILVER\n\nseries_id: 4521\ntime: 2025-01\nvalue: 14900'),
        (9.5, 0.5, 4, 4, '#FFD700', 'GOLD\n\nMY: 2024/25\nProduction: 14,900M\nS/U: 10.2%'),
    ]

    for x, y, w, h, color, text in boxes:
        rect = patches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.1",
            facecolor=color,
            edgecolor='#333333',
            linewidth=2
        )
        ax.add_patch(rect)
        ax.text(x + w/2, y + h/2, text,
                ha='center', va='center',
                fontsize=9, family='monospace')

    # Arrows
    ax.annotate('', xy=(5.0, 2.5), xytext=(4.5, 2.5),
                arrowprops=dict(arrowstyle='->', lw=2))
    ax.text(4.75, 3.2, 'Transform', ha='center', fontsize=8)

    ax.annotate('', xy=(9.5, 2.5), xytext=(9.0, 2.5),
                arrowprops=dict(arrowstyle='->', lw=2))
    ax.text(9.25, 3.2, 'Aggregate', ha='center', fontsize=8)

    plt.title('Medallion Architecture Data Flow', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'medallion_flow.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("Generated: medallion_flow.png")


def main():
    parser = argparse.ArgumentParser(description='Generate user guide graphics')
    parser.add_argument('--graphic', help='Generate specific graphic only')
    args = parser.parse_args()

    graphics = {
        'architecture': generate_architecture_diagram,
        'medallion': generate_medallion_flow,
    }

    if args.graphic:
        if args.graphic in graphics:
            graphics[args.graphic]()
        else:
            print(f"Unknown graphic: {args.graphic}")
            print(f"Available: {', '.join(graphics.keys())}")
    else:
        for name, func in graphics.items():
            try:
                func()
            except Exception as e:
                print(f"Error generating {name}: {e}")


if __name__ == "__main__":
    main()
```

---

## Summary of Graphics Needed

| Graphic ID | Type | Complexity | Generation Method |
|------------|------|------------|-------------------|
| architecture-diagram | Flowchart | Medium | Python matplotlib |
| medallion-flow | Horizontal flow | Low | Python matplotlib |
| data-sources-map | Geographic | High | geopandas + matplotlib |
| dashboard-annotated | Annotated screenshot | Low | Screenshot + PIL |
| dashboard-layout | Wireframe | Medium | Figma or matplotlib |
| powerbi-getdata | Screenshot | Low | Manual screenshot |
| powerbi-navigator | Screenshot | Low | Manual screenshot |

---

[← Back to Table of Contents](00_COVER_AND_TOC.md)
