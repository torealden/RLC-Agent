"""Two-part defensive fix across all updater .bas modules:

1. Add `Attribute VB_Name = "{module}"` as line 1 where missing — so
   re-imports replace the existing module cleanly instead of auto-renaming
   to Module1, Module2, etc.

2. Rewrite each `Application.OnKey "^x", "ProcName"` to use the qualified
   `"ModuleName.ProcName"` form so the binding resolves correctly even
   when two modules expose procedures with the same name (e.g.,
   UpdateFeedstockData lives in three modules currently).

Mirrors the convention already used in ExportSalesUpdaterSQL.bas.
"""

import re
from pathlib import Path

TOOLS_DIR = Path('src/tools')

def patch_file(path: Path) -> dict:
    text = path.read_text(encoding='utf-8')
    module_name = path.stem
    changes = {'attr_added': False, 'onkey_qualified': 0}

    # 1. Add Attribute VB_Name if missing
    if not text.startswith('Attribute VB_Name'):
        text = f'Attribute VB_Name = "{module_name}"\n' + text
        changes['attr_added'] = True

    # 2. Qualify Application.OnKey bindings — only the 2-arg form (binding,
    # not unbinding). Pattern matches:
    #   Application.OnKey "^x", "ProcName"
    #   Application.OnKey "^+x", "ProcName"
    # And replaces with:
    #   Application.OnKey "^x", "ModuleName.ProcName"
    # but ONLY if the second string doesn't already contain a dot.

    def replace_onkey(m):
        key = m.group(1)
        proc = m.group(2)
        suffix = m.group(3) or ''
        if '.' in proc:
            return m.group(0)  # already qualified
        changes['onkey_qualified'] += 1
        return f'Application.OnKey "{key}", "{module_name}.{proc}"{suffix}'

    pattern = re.compile(
        r'Application\.OnKey\s+"([^"]+)"\s*,\s*"([^"]+)"(\s*\'[^\n]*)?'
    )
    text = pattern.sub(replace_onkey, text)

    path.write_text(text, encoding='utf-8')
    return changes


results = []
for f in sorted(TOOLS_DIR.glob('*.bas')):
    r = patch_file(f)
    results.append((f.name, r))

print(f"{'file':<40s}  attr  onkey")
for name, r in results:
    attr = 'ADDED' if r['attr_added'] else '-'
    cnt = r['onkey_qualified']
    if r['attr_added'] or cnt > 0:
        print(f"  {name:<38s}  {attr:<6s} {cnt}")
    else:
        print(f"  {name:<38s}  (no change)")
