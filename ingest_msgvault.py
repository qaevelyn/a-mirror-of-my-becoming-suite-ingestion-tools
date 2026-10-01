#!/usr/bin/env python3
"""
ingest_msgvault.py — A Mirror of My Becoming, Suite: Ingestion Tools
Reads msgvault.db (SQLite mail archive), feeds the ingest pipeline into a
Chroma vector store. Idempotent: the msgvault embed_gen watermark is the
checkpoint — NULL means needs embedding; stamped after successful upsert.

Author: Evelyn Caro (architecture, direction) — AGPL-3.0
Normalization pattern per RFC 5322; credit: Manjo tempmail PR #247.
Schema receipts: ~/.msgvault/msgvault.db recon 2026-09-30.

Usage:
  python3 ingest_msgvault.py --db ~/.msgvault/msgvault.db \
      --store ~/Mirror-Food/vector-store-quarantine-2026-09-27 \
      --collection mirror_food_emails --dry-run
"""

import argparse
import os
import hashlib
import html
import json
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone

# ----------------------------------------------------------------NORMALIZE--
_MSGID_RE = re.compile(r"^\s*<?(.+?)>?\s*$")

def normalize_message_id(raw):
    """RFC 5322 msg-id canonical form: strip one <> pair + whitespace.
    Reject NULL/empty/>255b/control chars. Case preserved (IDs are case-sensitive)."""
    if raw is None:
        return None
    s = raw.strip()
    if not s or len(s.encode("utf-8")) > 255:
        return None
    if any(ord(c) <= 0x1F or ord(c) == 0x7F for c in s):
        return None
    m = _MSGID_RE.match(s)
    canon = (m.group(1) if m else s).strip()
    return canon or None

def iso_to_z(sqlite_dt):
    """msgvault 'YYYY-MM-DD HH:MM:SS+00:00' -> ISO8601 Z (Store-1 canonical form)."""
    if not sqlite_dt:
        return None
    try:
        return datetime.fromisoformat(sqlite_dt).astimezone(timezone.utc)\
            .strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None

def strip_html(h):
    if not h:
        return None
    text = re.sub(r"<[^>]+>", " ", h)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip() or None

def stable_id(canonical_msgid):
    """Deterministic Chroma doc id — same message, same id, idempotent upserts."""
    return hashlib.sha256(("msgid:" + canonical_msgid).encode("utf-8")).hexdigest()[:32]

# ------------------------------------------------------------------EXTRACT--
def fetch_batch(db_path, limit, already_done):
    """messages JOIN message_bodies WHERE embed_gen IS NULL AND deleted_at IS NULL.
    Full metadata preserved per owner directive. chunk columns via window fn."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT m.id, m.rfc822_message_id, m.sent_at, m.subject, m.snippet,
               m.message_type, m.thread_position, m.has_attachments,
               mb.body_text, mb.body_html,
               p.email_address AS sender_email, p.display_name AS sender_name,
               (SELECT COUNT(*) FROM messages m2
                 WHERE m2.conversation_id = m.conversation_id) AS conv_size,
               (SELECT COUNT(*) FROM messages m3
                 WHERE m3.conversation_id = m.conversation_id
                   AND m3.id <= m.id) AS conv_pos
        FROM messages m
        JOIN message_bodies mb ON mb.message_id = m.id
        LEFT JOIN participants p ON p.id = m.sender_id
        WHERE m.embed_gen IS NULL AND m.deleted_at IS NULL
        ORDER BY m.id
        LIMIT ?
    """, (limit,)).fetchall()
    for r in rows:
        already_done.append(dict(r))
    conn.close()
    return already_done

def build_document(r):
    """One canonical text document per message — html fallback to text."""
    body = r["body_text"] or strip_html(r["body_html"])
    if not body:
        return None, None
    parts = [f"From: {r['sender_name'] or ''} <{r['sender_email'] or 'unknown'}>",
             f"Date: {iso_to_z(r['sent_at']) or 'unknown'}",
             f"Subject: {r['subject'] or '(no subject)'}",
             f"Type: {r['message_type'] or 'email'}",
             "", body.strip()]
    doc = "\n".join(p for p in parts if p is not None)
    meta = {
        "message_id": normalize_message_id(r["rfc822_message_id"]) or
                      f"mv-row-{r['id']}",          # namespace map: bare RFC822 or internal fallback
        "source_identifier": f"msgvault:{r['id']}",
        "occurred_at": iso_to_z(r["sent_at"]),
        "author_name": r["sender_name"],
        "author_address": r["sender_email"],
        "subject": r["subject"],
        "message_type": r["message_type"],
        "thread_position": r["thread_position"],
        "conversation_size": r["conv_size"],
        "conversation_position": r["conv_pos"],
        "has_attachments": bool(r["has_attachments"]),
        "source": "msgvault",
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    return doc, meta

def chunk_text(text, max_chars=1500, overlap=150):
    """Suite-style chunking: paragraph-boundary preferred, hard split fallback."""
    if len(text) <= max_chars:
        return [text]
    chunks, start = [], 0
    while start < len(text):
        end = min(start + max_chars, len(text))
        if end < len(text):
            cut = text.rfind("\n\n", start, end)
            if cut == -1:
                cut = text.rfind("\n", start, end)
            if cut > start:
                end = cut
        chunks.append(text[start:end].strip())
        start = max(end - overlap, start + 1)
    return [c for c in chunks if c]

# --------------------------------------------------------------------LOAD--
def upsert_chunks(collection, msg_id, canonical, doc, meta, embed_fn=None):
    """Chroma upsert: deterministic ids, metadata per chunk, i-th chunk indexing
    mirrors suite convention (chunk_index, total_chunks)."""
    chunks = chunk_text(doc)
    ids, docs, metas = [], [], []
    total = len(chunks)
    for i, ch in enumerate(chunks):
        ids.append(stable_id(canonical) + ("" if total == 1 else f"-{i}"))
        m = dict(meta)
        m["chunk_index"] = i
        m["total_chunks"] = total
        docs.append(ch)
        metas.append(m)
    if embed_fn:
        vectors = embed_fn.embed_documents(docs)
        collection.upsert(ids=ids, documents=docs, metadatas=metas, embeddings=vectors)
    else:
        collection.upsert(ids=ids, documents=docs, metadatas=metas)
    return total

def stamp_embedded(db_path, row_ids, gen):
    conn = sqlite3.connect(db_path)
    conn.executemany("UPDATE messages SET embed_gen = ? WHERE id = ?",
                     [(gen, rid) for rid in row_ids])
    conn.commit()
    conn.close()

def get_or_make_gen(db_path):
    """Use the DB's max existing embed_gen, or start at 1 for a fresh archive."""
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT MAX(embed_gen) FROM messages").fetchone()
    conn.close()
    return (row[0] or 0) + 1 if row and row[0] else 1

# --------------------------------------------------------------------MAIN--
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.expanduser("~/.msgvault/msgvault.db"))
    ap.add_argument("--store", required=True, help="Chroma PersistentClient path")
    ap.add_argument("--collection", default="mirror_food_emails")
    ap.add_argument("--batch", type=int, default=200)
    ap.add_argument("--limit", type=int, default=0, help="max messages this run (0=all)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--heartbeats", action="store_true", help="per-chunk log lines (watchdog-safe)")
    ap.add_argument("--curfew", default=None, help="stop cleanly at HH:MM local time (stamp + exit; no restart)")
    ap.add_argument("--embed-model", default=None, help="sentence-transformers model name; omit = text-only store")
    a = ap.parse_args()

    import chromadb
    embed_fn = None
    if a.embed_model:
        from langchain_ollama import OllamaEmbeddings
        embed_fn = OllamaEmbeddings(model=a.embed_model)
        print(f"[EMBED] using {a.embed_model} via Ollama — same model as ships 2/3/5", flush=True)
    client = chromadb.PersistentClient(path=a.store)
    collection = client.get_or_create_collection(a.collection)

    done, processed, chunk_total, t0 = [], 0, 0, time.time()
    gen = get_or_make_gen(a.db)
    print(f"[START] msgvault ingest — gen={gen} batch={a.batch} "
          f"store={a.store} collection={a.collection} dry_run={a.dry_run}", flush=True)

    while True:
        if a.curfew:
            now = datetime.now().strftime("%H:%M")
            if now >= a.curfew:
                print(f"[CURFEW] {now} >= {a.curfew} — progress stamped, exiting cleanly. Resume anytime; watermark holds.", flush=True)
                break
        batch = fetch_batch(a.db, a.batch, done)
        if not batch:
            break
        stamped_ids = []
        for r in batch:
            doc, meta = build_document(r)
            if doc is None:
                # empty body: still stamp so it never re-queues
                stamped_ids.append(r["id"])
                continue
            canon = meta["message_id"]
            if a.dry_run:
                print(f"[DRY] row={r['id']} msgid={canon} from={r['sender_email']} "
                      f"date={meta['occurred_at']} len={len(doc)}", flush=True)
                stamped_ids.append(r["id"])
                continue
            n = upsert_chunks(collection, r["id"], canon, doc, meta, embed_fn)
            chunk_total += n
            stamped_ids.append(r["id"])
            if a.heartbeats:
                print(f"[WROTE] row={r['id']} chunks+={n} total_chunks={chunk_total}", flush=True)
            if a.limit and processed >= a.limit:
                break
        if not a.dry_run and stamped_ids:
            stamp_embedded(a.db, stamped_ids, gen)
        if a.dry_run:
            print("[DRY-DONE] dry-run inspects one batch only — nothing stamped, nothing looped", flush=True)
            break
        processed += len(stamped_ids)
        print(f"[BATCH] processed={processed} chunks={chunk_total} "
              f"elapsed={time.time()-t0:.0f}s", flush=True)
        if a.limit and processed >= a.limit:
            break

    print(f"[DONE] messages={processed} chunks={chunk_total} "
          f"elapsed={time.time()-t0:.0f}s dry_run={a.dry_run}", flush=True)

if __name__ == "__main__":
    main()
