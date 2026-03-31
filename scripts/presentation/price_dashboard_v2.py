"""Weekly Feedstock Price Dashboard — v2 with proper layout."""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

BG = '#0f1318'
PANEL = '#181d24'
GOLD = '#C8963E'
WHITE = '#FFFFFF'
LIGHT = '#D4CFC5'
GRAY = '#6B6560'
GREEN = '#548235'
RED = '#c00000'
BLUE = '#2e75b6'

def cc(s):
    try:
        v = float(s.replace('+','').replace('$','').replace('C','').replace('N/A','0').replace(chr(8212),'0').replace(chr(8364),'').replace(chr(162),''))
        if v > 0: return GREEN
        if v < 0: return RED
    except: pass
    return GRAY

fig = plt.figure(figsize=(11, 17))
fig.patch.set_facecolor(BG)

# Title
fig.text(0.04, 0.99, 'WEEKLY FEEDSTOCK PRICE DASHBOARD', fontsize=18,
         fontweight='bold', color=WHITE, va='top')
fig.text(0.04, 0.979, 'Week Ending March 28, 2026', fontsize=9, color=GRAY, va='top')
fig.text(0.96, 0.99, 'Round Lakes Companies', fontsize=9, fontweight='bold',
         color=GOLD, va='top', ha='right')

# Gold accent line (thin)
fig.add_axes([0.03, 0.974, 0.94, 0.001]).set_facecolor(GOLD)
fig.axes[-1].axis('off')

# ── KEY BENCHMARKS ─────────────────────────────────────────────────
ax0 = fig.add_axes([0.03, 0.945, 0.94, 0.025])
ax0.set_facecolor(PANEL)
ax0.axis('off')

for i, (nm, pr, ch) in enumerate([
    ('SBO (ZL)', '64.48c/lb', '+1.23'),
    ('ULSD', '$2.41/gal', '-0.08'),
    ('Brent', '$99.94/bbl', '-11.20'),
    ('D4 RIN', '$1.52', '+0.04'),
    ('D6 RIN', '$0.78', '-0.02'),
    ('LCFS', '$52/MT', '-3.00'),
    ('45Z Credit', '$0.50/gal', 'flat'),
]):
    x = 0.01 + i * 0.143
    ax0.text(x, 0.82, nm, fontsize=7, color=GRAY, fontweight='bold',
             transform=ax0.transAxes, va='top')
    ax0.text(x, 0.25, pr, fontsize=10, color=WHITE, fontweight='bold',
             transform=ax0.transAxes, va='center')
    ax0.text(x + 0.085, 0.25, ch, fontsize=7, color=cc(ch),
             transform=ax0.transAxes, va='center')

# ── FEEDSTOCK PRICES (left) ────────────────────────────────────────
ax1 = fig.add_axes([0.03, 0.59, 0.45, 0.34])
ax1.set_facecolor(PANEL)
ax1.axis('off')

ax1.text(0.03, 0.98, 'FEEDSTOCK PRICES', fontsize=12, fontweight='bold',
         color=GOLD, transform=ax1.transAxes, va='top')
ax1.text(0.97, 0.98, 'All prices in cents/lb unless noted', fontsize=7, color=GOLD,
         transform=ax1.transAxes, va='top', ha='right')

for lbl, xp in [('Commodity', 0.03), ('Price', 0.54), ('WoW', 0.69),
                 ('MoM', 0.83), ('YoY', 0.97)]:
    ax1.text(xp, 0.935, lbl, fontsize=7, color=GRAY, fontweight='bold',
             transform=ax1.transAxes, ha='left' if lbl == 'Commodity' else 'right')

y = 0.895
for nm, pr, wow, mom, yoy, hdr in [
    ('VEGETABLE OILS', '', '', '', '', True),
    ('  Soybean Oil (CBOT)', '64.48', '+1.23', '+3.40', '+8.72', False),
    ('  Canola Oil', '67.20', '+0.85', '+2.10', '+5.30', False),
    ('  Palm Oil (CIF NOLA)', '52.10', '-0.40', '+1.80', '-3.20', False),
    ('  Sunflower Oil', '58.30', '+2.10', '+4.50', '+12.40', False),
    ('  Corn Oil (DCO)', '48.50', '+0.30', '+1.20', '+6.80', False),
    ('', '', '', '', '', False),
    ('ANIMAL FATS & GREASES', '', '', '', '', True),
    ('  Inedible Tallow', '44.25', '+1.80', '+3.50', '+9.20', False),
    ('  Edible Tallow', '48.50', '+1.50', '+2.80', '+7.10', False),
    ('  Yellow Grease', '38.75', '+0.90', '+2.20', '+5.40', False),
    ('  Choice White Grease', '41.20', '+1.10', '+2.60', '+6.30', False),
    ('  Poultry Fat', '36.50', '+0.60', '+1.50', '+4.20', False),
    ('  Lard', '43.80', '+0.70', '+1.90', '+3.80', False),
    ('  UCO', '35.00', '+2.50', '+5.80', '+14.50', False),
]:
    if not nm:
        y -= 0.012
        continue
    if hdr:
        ax1.text(0.03, y, nm, fontsize=8, color=BLUE, fontweight='bold',
                 transform=ax1.transAxes)
    else:
        ax1.text(0.03, y, nm, fontsize=7.5, color=LIGHT, transform=ax1.transAxes)
        ax1.text(0.54, y, pr, fontsize=7.5, color=WHITE, fontweight='bold',
                 transform=ax1.transAxes, ha='right')
        for val, xp in [(wow, 0.69), (mom, 0.83), (yoy, 0.97)]:
            ax1.text(xp, y, val, fontsize=7, color=cc(val),
                     transform=ax1.transAxes, ha='right')
    y -= 0.043

# ── REGIONAL SPOTLIGHT (right) ─────────────────────────────────────
ax2 = fig.add_axes([0.52, 0.59, 0.45, 0.34])
ax2.set_facecolor(PANEL)
ax2.axis('off')

ax2.text(0.03, 0.98, 'REGIONAL FEEDSTOCK SPOTLIGHT', fontsize=12,
         fontweight='bold', color=GOLD, transform=ax2.transAxes, va='top')
ax2.text(0.97, 0.98, 'All prices in cents/lb unless noted', fontsize=7,
         color=GOLD, transform=ax2.transAxes, va='top', ha='right')

y = 0.92
for region, prices in [
    ('GULF COAST  (TX, LA)', [
        ('Inedible Tallow (Houston)', '43.80', '+1.90'),
        ('UCO Imports (NOLA)', '34.50', '+2.80'),
        ('Palm Oil (CIF NOLA)', '52.10', '-0.40'),
        ('ULSD Gulf Coast', '$2.38/g', '-0.06'),
    ]),
    ('MIDWEST  (IA, IL, IN)', [
        ('SBO (Decatur)', '64.20', '+1.15'),
        ('DCO (Central IL)', '48.50', '+0.30'),
        ('Tallow (Chicago)', '44.50', '+1.70'),
        ('Ethanol Rack (IA)', '$1.72/g', '+0.04'),
    ]),
    ('WEST COAST  (CA, WA, OR)', [
        ('UCO (LA Basin)', '36.50', '+3.10'),
        ('Tallow (Portland)', '45.00', '+2.00'),
        ('SBO (PNW)', '65.80', '+1.40'),
        ('LCFS Credit Value', '$52/MT', '-3.00'),
    ]),
    ('NORTHERN PLAINS  (ND, MN)', [
        ('Canola Seed (Velva)', '$12.40/bu', '+0.25'),
        ('Canola Oil (MN)', '67.00', '+0.80'),
        ('Canola Meal', '$285/ST', '+5.00'),
        ('Camelina*', 'N/A', '--'),
    ]),
    ('SOUTHEAST  (AL, MS, GA)', [
        ('Poultry Fat (Atlanta)', '36.80', '+0.55'),
        ('Inedible Tallow', '44.00', '+1.60'),
        ('Yellow Grease', '38.50', '+0.85'),
        ('UCO (Southeast)', '34.80', '+2.40'),
    ]),
]:
    ax2.text(0.03, y, region, fontsize=8, color=BLUE, fontweight='bold',
             transform=ax2.transAxes)
    y -= 0.033
    for nm, pr, ch in prices:
        ax2.text(0.06, y, nm, fontsize=7, color=LIGHT, transform=ax2.transAxes)
        ax2.text(0.73, y, pr, fontsize=7, color=WHITE, fontweight='bold',
                 transform=ax2.transAxes, ha='right')
        ax2.text(0.86, y, ch, fontsize=7, color=cc(ch),
                 transform=ax2.transAxes, ha='right')
        y -= 0.028
    y -= 0.013

ax2.text(0.03, 0.02, '* Camelina pricing source in development',
         fontsize=6, color=GRAY, style='italic', transform=ax2.transAxes)

# ── CRUSH MARGINS (bottom left) ───────────────────────────────────
ax3 = fig.add_axes([0.03, 0.27, 0.30, 0.30])
ax3.set_facecolor(PANEL)
ax3.axis('off')

ax3.text(0.05, 0.96, 'CRUSH MARGINS', fontsize=10, fontweight='bold',
         color=GOLD, transform=ax3.transAxes, va='top')

y = 0.87
for nm, mg, ch in [
    ('Soybean Board Crush', '$2.45/bu', '+0.18'),
    ('Canola (Canada)', 'C$48/MT', '-2.50'),
    ('Palm Oil (Malaysia)', '$85/MT', '+5.20'),
    ('Sunflower (EU)', 'E42/MT', '+3.10'),
    ('Cottonseed (US)', '$15/ST', '+2.00'),
]:
    ax3.text(0.05, y, nm, fontsize=8, color=LIGHT, transform=ax3.transAxes)
    ax3.text(0.95, y, mg, fontsize=8, color=WHITE, fontweight='bold',
             transform=ax3.transAxes, ha='right')
    ax3.text(0.95, y - 0.055, ch + ' WoW', fontsize=7, color=cc(ch),
             transform=ax3.transAxes, ha='right')
    y -= 0.145

# ── BIOFUEL MARGINS (bottom center) ──────────────────────────────
ax4 = fig.add_axes([0.35, 0.27, 0.30, 0.30])
ax4.set_facecolor(PANEL)
ax4.axis('off')

ax4.text(0.05, 0.96, 'BIOFUEL MARGINS', fontsize=10, fontweight='bold',
         color=GOLD, transform=ax4.transAxes, va='top')

y = 0.87
for nm, mg, ch in [
    ('RD from SBO (IL)', '$0.42/gal', '-0.08'),
    ('RD from Tallow (TX)', '$0.85/gal', '+0.05'),
    ('RD from UCO (CA+LCFS)', '$1.12/gal', '-0.15'),
    ('Biodiesel from SBO', '$0.28/gal', '-0.12'),
    ('Ethanol (IA plant)', '$0.35/gal', '+0.02'),
]:
    ax4.text(0.05, y, nm, fontsize=8, color=LIGHT, transform=ax4.transAxes)
    ax4.text(0.95, y, mg, fontsize=8, color=WHITE, fontweight='bold',
             transform=ax4.transAxes, ha='right')
    ax4.text(0.95, y - 0.055, ch + ' WoW', fontsize=7, color=cc(ch),
             transform=ax4.transAxes, ha='right')
    y -= 0.145

# ── KEY SPREADS (bottom right) ───────────────────────────────────
ax5 = fig.add_axes([0.67, 0.27, 0.30, 0.30])
ax5.set_facecolor(PANEL)
ax5.axis('off')

ax5.text(0.05, 0.96, 'KEY SPREADS', fontsize=10, fontweight='bold',
         color=GOLD, transform=ax5.transAxes, va='top')

y = 0.87
for nm, sp, ch in [
    ('SBO - Palm Oil', '12.38c', '+1.63'),
    ('SBO - Tallow', '20.23c', '-0.57'),
    ('Tallow - UCO', '9.25c', '-0.70'),
    ('RD - ULSD Premium', '+15c/gal', '-3.00'),
    ('D4 - D6 RIN Spread', '74c', '+6.00'),
    ('SBO/SBM Ratio', '2.12x', '+0.04'),
]:
    ax5.text(0.05, y, nm, fontsize=8, color=LIGHT, transform=ax5.transAxes)
    ax5.text(0.95, y, sp, fontsize=8, color=WHITE, fontweight='bold',
             transform=ax5.transAxes, ha='right')
    ax5.text(0.95, y - 0.05, ch + ' WoW', fontsize=7, color=cc(ch),
             transform=ax5.transAxes, ha='right')
    y -= 0.125

# ── IRAN BANNER ───────────────────────────────────────────────────
ax6 = fig.add_axes([0.03, 0.09, 0.94, 0.12])
ax6.set_facecolor('#1a1210')
ax6.axis('off')

ax6.text(0.02, 0.88, 'IRAN CRISIS MARKET IMPACT', fontsize=10,
         fontweight='bold', color=RED, transform=ax6.transAxes, va='top')
ax6.text(0.02, 0.60,
    'Strait of Hormuz disruption ongoing  |  Brent -11% WoW to $99.94 after $112 high  |  '
    'Urea +50% since Feb 28  |  20-30% of global fertilizer transits Hormuz',
    fontsize=7.5, color=LIGHT, transform=ax6.transAxes, va='top')
ax6.text(0.02, 0.20,
    'Implication: Rising corn input costs > potential acreage shift to soybeans > '
    'bullish SBO long-term  |  Energy spike supports RIN values but compresses RD margins vs ULSD',
    fontsize=7.5, color=GOLD, style='italic', transform=ax6.transAxes, va='top')

# Footer
fig.text(0.04, 0.06, 'Sources: CME, ICE, USDA AMS, EIA, EPA, CARB/LCFS',
         fontsize=6, color=GRAY)
fig.text(0.96, 0.06, 'Round Lakes Companies  |  roundlakescommodities.com',
         fontsize=6, color=GRAY, ha='right')
fig.text(0.96, 0.045, 'Prices are indicative and for informational purposes only.',
         fontsize=5, color=GRAY, ha='right', style='italic')

plt.savefig('data/generated_graphics/charts/weekly_price_dashboard_v2.png',
            dpi=150, facecolor=fig.get_facecolor())
plt.close()
print('Saved weekly_price_dashboard_v2.png')
