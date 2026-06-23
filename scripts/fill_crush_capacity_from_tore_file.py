"""
Fill missing oilseed-crush capacity in reference.oilseed_crush_facilities from Tore's
curated "North American Oilseed Crushing Capacity.xlsx" (US Soy Crush sheet).

Strong-not-canon source (Tore, 2026-06-23). Fills nameplate_mmbu_yr ONLY where currently
NULL (never overwrites our own numbers), matched by company-token + city + state, and
appends provenance to notes. Idempotent; --apply to write (dry by default).
"""
import openpyxl, re, sys, argparse
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

XLSX = r"C:/Users/torem/OneDrive/Desktop/Models/Oilseeds/North American Oilseed Crushing Capacity.xlsx"
PROV = "[capacity from NA Oilseed Crushing Capacity file (Tore, Dec2025), strong-not-canon]"

def norm(s):
    s=(s or "").lower()
    s=re.sub(r"\b(inc|llc|corp|corporation|company|co|lp|ltd|the|ag|processing|industries)\b"," ",s)
    return re.sub(r"[^a-z0-9]+"," ",s).strip()
def f2(x):
    try: return float(x)
    except: return None

def load_tore():
    wb=openpyxl.load_workbook(XLSX, read_only=True, data_only=True); ws=wb["US Soy Crush"]
    rows=list(ws.iter_rows(values_only=True)); hdr=[str(h).strip() if h else "" for h in rows[0]]
    ci={h:i for i,h in enumerate(hdr)}; out=[]
    for r in rows[1:]:
        comp=r[ci["Company"]]; cap=f2(r[ci.get("Yearly Capacity (bushels)")])
        if comp and cap: out.append({"company":comp,"city":r[ci["City"]],"state":r[ci["State"]],"cap_bu":cap})
    wb.close(); return out

def match(o, tore):
    oc=norm(o['city']); ost=(o['state'] or "").upper(); on=norm(o['name'])+" "+norm(o.get('operator'))
    for t in tore:
        if (t['state'] or "").upper()!=ost or norm(t['city'])!=oc: continue
        tc=norm(t['company'])
        if tc and (tc in on or any(tok in on for tok in tc.split() if len(tok)>3)): return t
    return None

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--apply",action="store_true"); args=ap.parse_args()
    tore=load_tore()
    with get_connection() as c:
        cur=c.cursor()
        cur.execute("SELECT name, operator, city, state, nameplate_mmbu_yr, nameplate_tpd, notes FROM reference.oilseed_crush_facilities")
        ours=[dict(r) if isinstance(r,dict) else dict(zip(['name','operator','city','state','nameplate_mmbu_yr','nameplate_tpd','notes'],r)) for r in cur.fetchall()]
        fills=[]
        for o in ours:
            if o['nameplate_mmbu_yr'] or o['nameplate_tpd']: continue
            m=match(o,tore)
            if m: fills.append((o, round(m['cap_bu']/1e6, 2)))
        print(f"gaps fillable: {len(fills)}")
        for o,mmbu in fills: print(f"   {o['name'][:34]:34s} {o['city']},{o['state']} <- {mmbu} mmbu/yr")
        if args.apply:
            for o,mmbu in fills:
                note=((o['notes'] or "")+" "+PROV).strip()
                cur.execute("""UPDATE reference.oilseed_crush_facilities
                               SET nameplate_mmbu_yr=%s, notes=%s
                               WHERE name=%s AND nameplate_mmbu_yr IS NULL AND nameplate_tpd IS NULL""",
                            (mmbu, note, o['name']))
            c.commit()
            print(f"APPLIED: filled {len(fills)} facilities (NULL-only, provenance noted)")

if __name__=="__main__": main()
