"""
Document registry — the shared spine for non-API / file sources.

Bespoke acquirers call register() for every fetched file; the parse/QC/load
spine calls list_pending() -> parse -> mark(). Provenance + dedup + parse-queue
in one place (bronze.source_documents, migration 141).

  register(...)      -> (doc_id, is_new); idempotent on (source, source_key, sha256)
  list_pending(...)  -> rows needing extraction
  mark(...)          -> update parse lifecycle
"""

import hashlib
import sys
from pathlib import Path
from typing import Optional, List, Dict

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection


def sha256_of(path: Path) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def _page_count(path: Path) -> Optional[int]:
    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            return len(pdf.pages)
    except Exception:
        return None


def register(source: str, doc_type: str, source_key: Optional[str] = None,
             url: Optional[str] = None, local_path: Optional[str] = None,
             title: Optional[str] = None, published_date=None, vintage=None,
             conn=None) -> tuple:
    """Register a fetched document. Idempotent on (source, source_key, sha256).
    Returns (doc_id, is_new). A new sha256 for an existing key = a revised doc."""
    sha = pages = None
    if local_path:
        p = Path(local_path)
        sha = sha256_of(p)
        if str(p).lower().endswith(".pdf"):
            pages = _page_count(p)
    close = conn is None
    if conn is None:
        conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bronze.source_documents
                (source, source_key, doc_type, title, url, local_path, sha256,
                 page_count, published_date, vintage)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (source, COALESCE(source_key,''), COALESCE(sha256,''))
            DO UPDATE SET fetched_at = NOW(), url = COALESCE(EXCLUDED.url, bronze.source_documents.url)
            RETURNING id, (xmax = 0) AS is_new
        """, (source, source_key, doc_type, title, url, local_path, sha,
              pages, published_date, vintage))
        row = cur.fetchone()
        if close:
            conn.commit()
        return (row['id'], row['is_new']) if isinstance(row, dict) else (row[0], row[1])
    finally:
        if close:
            conn.close()


def list_pending(doc_type: Optional[str] = None, source: Optional[str] = None,
                 limit: Optional[int] = None, conn=None) -> List[Dict]:
    close = conn is None
    if conn is None:
        conn = get_connection()
    try:
        cur = conn.cursor()
        q = "SELECT * FROM bronze.source_documents WHERE parse_status='pending'"
        params = []
        if doc_type:
            q += " AND doc_type=%s"; params.append(doc_type)
        if source:
            q += " AND source=%s"; params.append(source)
        q += " ORDER BY fetched_at"
        if limit:
            q += " LIMIT %s"; params.append(limit)
        cur.execute(q, params)
        return [dict(r) for r in cur.fetchall()]
    finally:
        if close:
            conn.close()


def mark(doc_id: int, status: str, method: Optional[str] = None,
         model: Optional[str] = None, confidence: Optional[str] = None,
         output_ref: Optional[str] = None, error: Optional[str] = None,
         conn=None) -> None:
    close = conn is None
    if conn is None:
        conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE bronze.source_documents SET
                parse_status=%s, parse_method=COALESCE(%s,parse_method),
                parse_model=COALESCE(%s,parse_model), parse_confidence=COALESCE(%s,parse_confidence),
                output_ref=COALESCE(%s,output_ref), last_error=%s,
                attempts = attempts + CASE WHEN %s IN ('parsed','failed') THEN 1 ELSE 0 END,
                parsed_at = CASE WHEN %s='parsed' THEN NOW() ELSE parsed_at END
            WHERE id=%s
        """, (status, method, model, confidence, output_ref, error, status, status, doc_id))
        if close:
            conn.commit()
    finally:
        if close:
            conn.close()
