"""Step 6 -- workbook inventory, content hashes, external links and embedded VBA.

This is the step that answers Q2. `us_soybean_complex_bal_sheets.xlsm` carries an external
link to `../../Biofuels/eia_data.xlsm`, and eia_data is fed by
`bronze.historical_feedstock_allocation`, which disagrees with `gold.bbd_feedstock_raked` by
+647 mil lb on CY2024 SBO. That chain is live in the production balance sheet, and it is
machine-visible -- which is the whole argument for building this.

Everything here reads the xlsx zip directly rather than through openpyxl. Sheet names and
external-link targets live in three small XML parts; openpyxl would parse the whole workbook
to get them, and one of these files holds 698k formula cells. openpyxl is reserved for step 7,
where the formulas are actually the point.

VBA note, measured 2026-07-21: the .bas files in git are NOT the modules that run. 7 of the 8
modules present in both places had drifted, `TradeUpdaterSQL` exists as six distinct forks
under one name across eight workbooks, and `WASDECompUpdater` -- embedded in the live corn,
wheat and soybean balance sheets -- is not in git at all. So the embedded module is the node
that matters and the git .bas file is a separate node joined to it by DEPLOYED_AS.
"""

from __future__ import annotations

import hashlib
import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]

WB_EXT = {".xlsx", ".xlsm", ".xlsb", ".xlam"}
MACRO_EXT = {".xlsm", ".xlsb", ".xlam"}

# A workbook extension with something appended -- `us_used_cooking_oil_balance.xlsx.bak_20260526_141050`.
# These are byte-identical xlsx zips with a renamed extension, so a suffix test misses them
# entirely: the first scan saw 627 workbooks and silently skipped 25 of these under models/
# alone. They are exactly the files the cleanup pass is looking for, so they must be in the
# inventory -- flagged as backups, not omitted.
SUFFIXED_WB = re.compile(r"\.(xlsx|xlsm|xlsb|xlam)\.", re.I)


def _is_workbook(p: Path) -> bool:
    return p.suffix.lower() in WB_EXT or bool(SUFFIXED_WB.search(p.name))


def _effective_ext(p: Path) -> str:
    if p.suffix.lower() in WB_EXT:
        return p.suffix.lower()
    m = SUFFIXED_WB.search(p.name)
    return f".{m.group(1).lower()}" if m else ""


SKIP_PARTS = {".git", "__pycache__", "node_modules", ".venv", "venv", ".pytest_cache"}

NS_MAIN = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
NS_R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
NS_PKG = "{http://schemas.openxmlformats.org/package/2006/relationships}"

ARCHIVE_PAT = re.compile(r"(?:^|[/\\])archive(?:[/\\]|$)|_old\b|\bold\b|conflicted copy", re.I)
BACKUP_PAT = re.compile(
    r"backup[_.]?\d{6,}|[_.]backup\b|\.bak[_.]|\bcopy \(\d\)|\(\d\)\.xls", re.I
)

PROC_RE = re.compile(
    r"^\s*(?:(Public|Private|Friend)\s+)?(?:Static\s+)?(Sub|Function)\s+([A-Za-z_]\w*)",
    re.M | re.I,
)


def _lifecycle(rel_path: str) -> str:
    """Heuristic only. R4: for a workbook, "nothing links to this" is nearly no evidence at
    all -- a human opens it by double-clicking. So this proposes; sys.declaration disposes."""
    if BACKUP_PAT.search(rel_path):
        return "backup"
    if ARCHIVE_PAT.search(rel_path):
        return "archive"
    return "unknown"


def _sha256(path: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    n = 0
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
            n += len(chunk)
    return h.hexdigest(), n


def _read_zip_parts(path: Path):
    """-> (sheet_names, ordered_external_targets, error). External targets are returned in
    workbook.xml order, so index i+1 is the `[i+1]` prefix Excel uses in formulas."""
    sheets: list[str] = []
    ext_targets: list[str] = []
    try:
        with zipfile.ZipFile(path) as z:
            names = set(z.namelist())

            if "xl/workbook.xml" in names:
                root = ET.fromstring(z.read("xl/workbook.xml"))
                for s in root.iter(f"{NS_MAIN}sheet"):
                    sheets.append(s.get("name", ""))
                ext_rids = [
                    e.get(f"{NS_R}id")
                    for e in root.iter(f"{NS_MAIN}externalReference")
                ]
            else:
                ext_rids = []

            rid_to_part = {}
            if "xl/_rels/workbook.xml.rels" in names:
                rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
                for r in rels.iter(f"{NS_PKG}Relationship"):
                    rid_to_part[r.get("Id")] = r.get("Target", "")

            for rid in ext_rids:
                part = rid_to_part.get(rid, "")
                part = part.lstrip("/").replace("../", "")
                rels_part = f"xl/externalLinks/_rels/{Path(part).name}.rels"
                target = ""
                if rels_part in names:
                    er = ET.fromstring(z.read(rels_part))
                    for r in er.iter(f"{NS_PKG}Relationship"):
                        if "externalLinkPath" in (r.get("Type") or "") or r.get("TargetMode") == "External":
                            target = r.get("Target", "")
                            break
                ext_targets.append(target)
    except Exception as exc:  # noqa: BLE001
        return sheets, ext_targets, f"{type(exc).__name__}: {exc}"
    return sheets, ext_targets, None


def _normalise_target(raw: str, from_path: Path) -> tuple[str, bool]:
    """Resolve an external-link target to a repo-relative path when we can.
    -> (key_or_raw, resolved)."""
    if not raw:
        return "", False
    t = raw.replace("\\", "/")
    if t.startswith("file:///"):
        t = t[len("file:///"):]
    elif t.startswith("file://"):
        t = t[len("file://"):]
    t = t.replace("%20", " ")
    cand = Path(t)
    if not cand.is_absolute():
        cand = (from_path.parent / t)
    try:
        resolved = cand.resolve()
        rel = resolved.relative_to(ROOT)
        return f"wb:{rel.as_posix()}", resolved.exists()
    except (ValueError, OSError):
        return f"wb:EXTERNAL/{t}", False


def extract(conn, store, live_relations: set[str]) -> dict:
    stats = {
        "workbooks": 0, "workbooks_hashed": 0, "zip_errors": 0,
        "worksheets": 0, "external_links": 0, "external_links_unresolved": 0,
        "vba_workbooks": 0, "vba_modules": 0, "vba_procedures": 0,
        "vba_relation_edges": 0, "deployed_as_identical": 0,
        "deployed_as_drifted": 0, "deployed_as_orphan_git": 0, "embedded_not_in_git": 0,
    }

    workbooks = []
    for p in ROOT.rglob("*"):
        if p.name.startswith("~$") or not p.is_file():
            continue
        if SKIP_PARTS & set(p.parts) or not _is_workbook(p):
            continue
        workbooks.append(p)

    cur = conn.cursor()
    ext_index: dict[str, list[str]] = {}   # wb node key -> ordered external targets (for step 7)

    for path in sorted(workbooks):
        rel = path.relative_to(ROOT).as_posix()
        key = f"wb:{rel}"
        stats["workbooks"] += 1

        digest, size = _sha256(path)
        stats["workbooks_hashed"] += 1

        sheets, ext_targets, err = _read_zip_parts(path)
        if err:
            stats["zip_errors"] += 1

        store.add_node(
            "workbook", key, label=path.name,
            properties={
                "path": rel, "ext": _effective_ext(path), "size_bytes": size,
                "renamed_ext": path.suffix.lower() if path.suffix.lower() not in WB_EXT else None,
                "sha256": digest, "sheet_count": len(sheets),
                "external_link_count": len(ext_targets),
                "macro_enabled": _effective_ext(path) in MACRO_EXT,
                "zip_error": err,
            },
            lifecycle=_lifecycle(rel),
            extraction_method="xlsx_extlink" if ext_targets else "regex",
            confidence=1.00,
        )

        cur.execute(
            """
            INSERT INTO sys.workbook_hash (workbook_path, content_sha256, size_bytes, last_scanned, scan_error)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (workbook_path) DO UPDATE SET
                content_sha256 = EXCLUDED.content_sha256,
                size_bytes     = EXCLUDED.size_bytes,
                last_scanned   = EXCLUDED.last_scanned,
                scan_error     = EXCLUDED.scan_error
            """,
            (rel, digest, size, store.scan_id, err),
        )

        for name in sheets:
            ws_key = f"{key}#{name}"
            store.add_node("worksheet", ws_key, label=name,
                           properties={"workbook": rel, "sheet": name},
                           lifecycle=_lifecycle(rel),
                           extraction_method="xlsx_extlink", confidence=1.00)
            # Containment is always stored container -> member (see trace.CONTAINMENT).
            store.add_edge(key, "DEFINES", ws_key,
                           extraction_method="xlsx_extlink", confidence=1.00)
            stats["worksheets"] += 1

        resolved_targets = []
        for i, raw in enumerate(ext_targets, start=1):
            tkey, exists = _normalise_target(raw, path)
            resolved_targets.append(tkey)
            if not tkey:
                continue
            stats["external_links"] += 1
            if not exists:
                stats["external_links_unresolved"] += 1
                store.add_node("workbook", tkey, label=Path(tkey).name,
                               properties={"phantom": True, "raw_target": raw},
                               extraction_method="xlsx_extlink", confidence=0.40,
                               resolution_status="unresolved")
            # Data flows FROM the linked workbook INTO the one holding the link.
            store.add_edge(
                tkey, "LINKS_TO", key,
                properties={"link_index": i},
                evidence={"raw_target": raw},
                extraction_method="xlsx_extlink", confidence=1.00,
                resolution_status="resolved" if exists else "unresolved",
            )
        ext_index[key] = resolved_targets

    conn.commit()
    stats.update(_vba(store, workbooks, live_relations))
    stats["external_link_index"] = ext_index
    return stats


# ---------------------------------------------------------------------------
# VBA
# ---------------------------------------------------------------------------

def _procs(code: str) -> list[tuple[str, str]]:
    """-> [(proc_name, body)] split on Sub/Function headers."""
    marks = [(m.start(), m.group(3)) for m in PROC_RE.finditer(code)]
    out = []
    for i, (pos, name) in enumerate(marks):
        end = marks[i + 1][0] if i + 1 < len(marks) else len(code)
        out.append((name, code[pos:end]))
    return out


def _norm(code: str) -> str:
    lines = [ln.rstrip() for ln in code.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    return "\n".join(ln for ln in lines if ln and not ln.startswith("Attribute "))


def _vba(store, workbooks: list[Path], live_relations: set[str]) -> dict:
    from oletools.olevba import VBA_Parser

    from src.sysgraph.coderefs import refs_in_text

    stats = {
        "vba_workbooks": 0, "vba_modules": 0, "vba_procedures": 0, "vba_relation_edges": 0,
        "deployed_as_identical": 0, "deployed_as_drifted": 0,
        "deployed_as_orphan_git": 0, "embedded_not_in_git": 0,
    }

    git_bas: dict[str, str] = {}
    for p in (ROOT / "src" / "tools").glob("*.bas"):
        git_bas[p.stem] = _norm(p.read_text(encoding="utf-8", errors="replace"))
    matched_git: set[str] = set()

    for path in workbooks:
        if _effective_ext(path) not in MACRO_EXT:
            continue
        wb_key = f"wb:{path.relative_to(ROOT).as_posix()}"
        try:
            vp = VBA_Parser(str(path))
            has = vp.detect_vba_macros()
        except Exception:  # noqa: BLE001
            continue
        if not has:
            vp.close()
            continue
        stats["vba_workbooks"] += 1

        for _f, _s, raw_name, code in vp.extract_macros():
            if not code or not code.strip():
                continue
            mod_name = re.sub(r"\.(bas|cls|frm)$", "", raw_name, flags=re.I)
            mod_kind = raw_name.rsplit(".", 1)[-1].lower() if "." in raw_name else "bas"
            mod_key = f"{wb_key}#{mod_name}"
            body = _norm(code)

            store.add_node(
                "vba_module", mod_key, label=mod_name,
                properties={"workbook": path.relative_to(ROOT).as_posix(),
                            "module": mod_name, "kind": mod_kind,
                            "lines": len(body.splitlines())},
                lifecycle=_lifecycle(path.relative_to(ROOT).as_posix()),
                extraction_method="vba_parse", confidence=0.90,
            )
            store.add_edge(wb_key, "DEFINES", mod_key,
                           extraction_method="vba_parse", confidence=0.90)
            stats["vba_modules"] += 1

            # DEPLOYED_AS: git source -> the module that actually runs.
            git_src = git_bas.get(mod_name)
            if git_src is not None:
                matched_git.add(mod_name)
                identical = git_src == body
                stats["deployed_as_identical" if identical else "deployed_as_drifted"] += 1
                store.add_edge(
                    f"repo:src/tools/{mod_name}.bas", "DEPLOYED_AS", mod_key,
                    properties={"identical": identical,
                                "git_lines": len(git_src.splitlines()),
                                "workbook_lines": len(body.splitlines())},
                    extraction_method="vba_parse",
                    confidence=1.00 if identical else 0.70,
                )
            else:
                stats["embedded_not_in_git"] += 1

            # Relation refs, attributed to the procedure that contains them.
            proc_list = _procs(code)
            for pname, pbody in proc_list:
                proc_key = f"{mod_key}.{pname}"
                store.add_node("vba_procedure", proc_key, label=f"{mod_name}.{pname}",
                               properties={"workbook": path.relative_to(ROOT).as_posix(),
                                           "module": mod_name, "procedure": pname},
                               lifecycle=_lifecycle(path.relative_to(ROOT).as_posix()),
                               extraction_method="vba_parse", confidence=0.90)
                store.add_edge(mod_key, "DEFINES", proc_key,
                               extraction_method="vba_parse", confidence=0.90)
                stats["vba_procedures"] += 1

                for rel, intent, snip in refs_in_text(pbody):
                    resolved = rel in live_relations
                    if not resolved and not store.has_node(rel):
                        store.add_node("db_relation", rel, label=rel,
                                       properties={"phantom": True},
                                       extraction_method="vba_parse", confidence=0.40,
                                       resolution_status="unresolved")
                    if intent == "write":
                        store.add_edge(proc_key, "WRITES", rel,
                                       properties={"intent": intent},
                                       evidence={"snippet": snip[:180]},
                                       extraction_method="vba_parse", confidence=0.90,
                                       resolution_status="resolved" if resolved else "unresolved")
                    else:
                        store.add_edge(rel, "READS", proc_key,
                                       properties={"intent": intent},
                                       evidence={"snippet": snip[:180]},
                                       extraction_method="vba_parse",
                                       confidence=0.90 if intent == "read" else 0.40,
                                       resolution_status="resolved" if resolved else "unresolved")
                    stats["vba_relation_edges"] += 1
        vp.close()

    stats["deployed_as_orphan_git"] = len(set(git_bas) - matched_git)
    stats["orphan_git_modules"] = sorted(set(git_bas) - matched_git)
    return stats
