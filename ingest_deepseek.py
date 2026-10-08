#!/usr/bin/env python3
# v5 — subprocess-isolated writes; upsert (idempotent); 3-strike skip; sidecar resume
import os, sys, json, logging, glob, subprocess, tempfile
import requests

HOME = os.path.expanduser("~")
FOOD = os.path.join(HOME, "Mirror-Food")
EXPORT_DIR = os.path.join(FOOD, "deepseek-export")
VECTOR_STORE = os.path.join(FOOD, "vector-store-v2")
LOG_FILE = os.path.join(FOOD, "ingest", "deepseek-ingest.log")
SIDECAR = os.path.join(FOOD, "ingest", "ingested_ids.json")
COLLECTION = "deepseek_conversations"
OLLAMA_URL = "http://localhost:11434"
EMBED_MODEL = "nomic-embed-text"
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 100
LOCK_FILE = os.path.join(FOOD, "ingest", ".ingest-lock")
CHILD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "add_child.py")
WRITE_TIMEOUT = 240

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE),
              logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("deepseek-ingest")

def acquire_lock():
    if os.path.exists(LOCK_FILE):
        pid = open(LOCK_FILE).read().strip()
        alive = False
        if pid.isdigit():
            try:
                os.kill(int(pid), 0)
                alive = True
            except OSError:
                alive = False
        if alive:
            logger.info("Another ingest (pid " + pid + ") is running. Exiting.")
            sys.exit(0)
        logger.warning("Stale lock from pid " + pid
                       + " — removing and continuing.")
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

def release_lock():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

def load_ingested():
    if os.path.exists(SIDECAR):
        try:
            return set(json.load(open(SIDECAR)))
        except Exception as e:
            logger.warning("Sidecar unreadable: " + str(e))
    return set()

def save_ingested(ids):
    json.dump(sorted(ids), open(SIDECAR, "w"))

def preflight():
    logger.info("PREFLIGHT: checking Ollama ...")
    try:
        r = requests.get(OLLAMA_URL + "/api/tags", timeout=10)
        r.raise_for_status()
        names = [m.get("name", "")
                 for m in r.json().get("models", [])]
        if not any(EMBED_MODEL in n for n in names):
            logger.error("PREFLIGHT FAIL: model missing")
            sys.exit(1)
        logger.info("PREFLIGHT OK: Ollama up, model available")
    except Exception as e:
        logger.error("PREFLIGHT FAIL: " + str(e))
        sys.exit(1)

def embed(text):
    r = requests.post(OLLAMA_URL + "/api/embeddings",
                      json={"model": EMBED_MODEL, "prompt": text},
                      timeout=120)
    r.raise_for_status()
    return r.json()["embedding"]

def chunk(text, size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    if len(text) <= size:
        return [text]
    out = []
    start = 0
    while start < len(text):
        end = start + size
        out.append(text[start:end])
        start = end - overlap
    return out

def walk_mapping(mapping, root_id="root"):
    ordered = []
    visited = set()
    stack = [root_id]
    while stack:
        node_id = stack.pop()
        if node_id in visited:
            continue
        visited.add(node_id)
        node = mapping.get(node_id)
        if not node:
            continue
        msg = node.get("message")
        if msg and msg.get("fragments"):
            parts = []
            for frag in msg["fragments"]:
                ftype = frag.get("type", "")
                fcontent = frag.get("content", "")
                if fcontent:
                    parts.append("[" + ftype + "] " + fcontent)
            if parts:
                ordered.append({
                    "id": msg.get("id", ""),
                    "model": msg.get("model", ""),
                    "inserted_at": msg.get("inserted_at", ""),
                    "text": "\n".join(parts),
                })
        children = node.get("children", [])
        for child in reversed(children):
            if child not in visited:
                stack.append(child)
    return ordered

def load_conversations(export_dir):
    convs = []
    for path in glob.glob(os.path.join(export_dir, "**", "*.json"),
                          recursive=True):
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as e:
            logger.warning("Could not parse " + path + ": " + str(e))
            continue
        if isinstance(data, list):
            for item in data:
                if (isinstance(item, dict) and "mapping" in item
                        and "id" in item):
                    convs.append(item)
        elif isinstance(data, dict):
            if "mapping" in data and "id" in data:
                convs.append(data)
    return convs

def isolated_write(ids, docs, metas, embs):
    payload = {"store": VECTOR_STORE, "collection": COLLECTION,
               "ids": ids, "docs": docs, "metas": metas,
               "embs": embs}
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json",
                                      delete=False)
    json.dump(payload, tmp)
    tmp.close()
    try:
        r = subprocess.run(
            ["/usr/local/Caskroom/miniconda/base/bin/python3",
             CHILD, tmp.name],
            capture_output=True, text=True, timeout=WRITE_TIMEOUT)
        ok = (r.returncode == 0 and "ADDED" in r.stdout)
        if not ok:
            logger.warning("Child write failed: "
                           + r.stderr[-200:])
        return ok
    except subprocess.TimeoutExpired:
        logger.warning("Child write TIMEOUT ("
                       + str(WRITE_TIMEOUT) + "s) — killed")
        return False
    finally:
        os.unlink(tmp.name)

def main():
    logger.info("=== INGEST v5 START (pid "
                + str(os.getpid()) + ") ===")
    acquire_lock()
    preflight()
    existing = load_ingested()
    logger.info("Sidecar says " + str(len(existing))
                + " already ingested")
    convs = load_conversations(EXPORT_DIR)
    logger.info("Found " + str(len(convs))
                + " conversations in export")
    todo = [c for c in convs if c.get("id", "") not in existing]
    logger.info("To ingest this run: " + str(len(todo)))
    written = 0
    failed = []
    for idx, conv in enumerate(todo):
        cid = conv.get("id", "")
        title = conv.get("title", "untitled")
        msgs = walk_mapping(conv.get("mapping", {}))
        if not msgs:
            existing.add(cid)
            save_ingested(existing)
            logger.info("[" + str(idx + 1) + "/"
                        + str(len(todo)) + "] SKIP empty — "
                        + title)
            continue
        full_text = "\n\n".join(
            "### " + (m["inserted_at"] or "") + " -- "
            + (m["model"] or "") + "\n" + m["text"]
            for m in msgs)
        logger.info("chunking " + str(len(full_text))
                    + " chars — " + title)
        pieces = chunk(full_text)
        logger.info("embedding " + str(len(pieces))
                    + " chunks — " + title)
        ids, docs, metas, embs = [], [], [], []
        for i, piece in enumerate(pieces):
            if i % 10 == 0:
                logger.info("chunk "
                            + str(i + 1) + "/"
                            + str(len(pieces))
                            + " — " + title)
            ids.append(cid + "::" + str(i))
            docs.append(piece)
            metas.append({
                "conversation_id": cid,
                "title": title,
                "chunk_index": i,
                "inserted_at": conv.get("inserted_at", ""),
                "updated_at": conv.get("updated_at", ""),
            })
            try:
                embs.append(embed(piece))
            except Exception as e:
                logger.warning("Embed fail " + cid
                               + " chunk " + str(i) + ": "
                               + str(e))
                embs.append(None)
        valid = [(i, d, m, e) for i, d, m, e
                 in zip(ids, docs, metas, embs)
                 if e is not None]
        if not valid:
            logger.info("[" + str(idx + 1) + "/"
                        + str(len(todo))
                        + "] SKIP no-embeddings — " + title)
            continue
        ok = isolated_write([v[0] for v in valid],
                            [v[1] for v in valid],
                            [v[2] for v in valid],
                            [v[3] for v in valid])
        if ok:
            written += 1
            existing.add(cid)
            save_ingested(existing)
            logger.info("[" + str(idx + 1) + "/"
                        + str(len(todo)) + "] WROTE ("
                        + str(len(valid)) + " chunks) — "
                        + title)
        else:
            failed.append((idx, cid, title))
            logger.warning("[" + str(idx + 1) + "/"
                           + str(len(todo))
                           + "] WRITE FAILED — " + title)
    # one retry pass for failures
    retried = 0
    if failed:
        logger.info("Retry pass: " + str(len(failed))
                    + " failed conversations")
        for idx, cid, title in failed:
            conv = next((c for c in todo
                         if c.get("id", "") == cid), None)
            if not conv:
                continue
            msgs = walk_mapping(conv.get("mapping", {}))
            full_text = "\n\n".join(
                "### " + (m["inserted_at"] or "") + " -- "
                + (m["model"] or "") + "\n" + m["text"]
                for m in msgs)
            logger.info("RETRY chunking "
                        + str(len(full_text)) + " chars — " + title)
            pieces = chunk(full_text)
            logger.info("RETRY embedding " + str(len(pieces))
                        + " chunks — " + title)
            ids, docs, metas, embs = [], [], [], []
            for i, piece in enumerate(pieces):
                if i % 10 == 0:
                    logger.info("RETRY chunk "
                                + str(i + 1) + "/"
                                + str(len(pieces)) + " — " + title)
                ids.append(cid + "::" + str(i))
                docs.append(piece)
                metas.append({"conversation_id": cid,
                              "title": title,
                              "chunk_index": i,
                              "inserted_at": conv.get(
                                  "inserted_at", ""),
                              "updated_at": conv.get(
                                  "updated_at", "")})
                try:
                    embs.append(embed(piece))
                except Exception:
                    embs.append(None)
            valid = [(i, d, m, e) for i, d, m, e
                     in zip(ids, docs, metas, embs)
                     if e is not None]
            if valid and isolated_write(
                    [v[0] for v in valid],
                    [v[1] for v in valid],
                    [v[2] for v in valid],
                    [v[3] for v in valid]):
                written += 1
                retried += 1
                existing.add(cid)
                save_ingested(existing)
                logger.info("RETRY OK — " + title)
            else:
                logger.warning("RETRY FAILED (final) — "
                               + title + " [" + cid + "]")
    logger.info("=== DONE === Written: " + str(written)
                + " | Retried-ok: " + str(retried)
                + " | Sidecar total: "
                + str(len(existing)))
    release_lock()

if __name__ == "__main__":
    main()
