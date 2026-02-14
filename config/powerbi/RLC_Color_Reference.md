# RLC Color Reference

These colors can't be embedded in the Power BI theme but should be used manually when building visuals.

## Brand Colors

| Name | Hex | Use |
|------|-----|-----|
| Primary Teal | `#1B4D4D` | Headers, titles, primary accent |
| Secondary Teal | `#168980` | Data points, chart lines |
| Accent Gold | `#C4A35A` | Secondary accent, wheat |
| Background Cream | `#F5F1EA` | Optional warm background |
| Text Dark | `#1A1A1A` | Primary text |
| Text Muted | `#5F6B6D` | Labels, secondary text |

## Commodity Colors

| Commodity | Hex | Preview |
|-----------|-----|---------|
| Soybeans | `#168980` | Teal |
| Corn | `#F4D25A` | Yellow |
| Wheat | `#C4A35A` | Gold |
| Soybean Meal | `#6B5B95` | Purple |
| Soybean Oil | `#4682B4` | Steel Blue |
| Cotton | `#F5F5DC` | Beige |
| Rice | `#E8E4C9` | Cream |

## Country Colors

| Country | Hex | Notes |
|---------|-----|-------|
| US | `#3C3B6E` | Navy (flag) |
| Brazil | `#009C3B` | Green (flag) |
| Argentina | `#74ACDF` | Light Blue (flag) |
| China | `#DE2910` | Red (flag) |
| EU | `#003399` | Blue (flag) |
| Russia | `#0039A6` | Blue |
| Ukraine | `#FFD500` | Yellow (flag) |
| Australia | `#00008B` | Dark Blue |
| Canada | `#FF0000` | Red (flag) |

## Sentiment / Variance Colors

| Sentiment | Hex | Use |
|-----------|-----|-----|
| Bullish Strong | `#1B5E20` | Dark Green |
| Bullish | `#2E7D32` | Green |
| Bullish Mild | `#66BB6A` | Light Green |
| Neutral | `#FFC107` | Gold/Amber |
| Bearish Mild | `#EF5350` | Light Red |
| Bearish | `#C62828` | Red |
| Bearish Strong | `#B71C1C` | Dark Red |

## Supply Tightness Scale

| Level | Hex | S/U Range |
|-------|-----|-----------|
| Comfortable | `#2E7D32` | > 18% |
| Adequate | `#66BB6A` | 12-18% |
| Balanced | `#FFC107` | 10-12% |
| Tight | `#FF9800` | 8-10% |
| Critical | `#C62828` | < 8% |

## Usage in Power BI

When setting colors manually:
1. Select the visual element
2. Format pane > Data colors (or specific property)
3. Click the color picker
4. Select "More colors"
5. Enter the hex code from this reference

For conditional formatting:
1. Format pane > Conditional formatting
2. Use "Rules" with these hex values
3. Or use DAX measures that return these colors
