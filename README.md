# A Mirror of My Becoming™ — Suite: Ingestion Tools

**Battle-tested, not beta-tested.**

A three-component ingest pipeline for building a personal RAG vector store from
exported conversation data — designed to survive the failures that kill normal
pipelines: crashes, kills, wedges, and silent stalls.

Built and run on consumer hardware (MacBook Air, Intel i5, 8GB RAM, no cloud).
Zero conversations lost across five session kills and nineteen watchdog
restarts. That night's corpus held a 4.6-million-character conversation
(5,163 chunks) and a 3.25-million-character conversation (3,615 chunks) —
the pipeline that spent three days wedging digested both in a single
unattended run.

## The Three Components (pick what you need — no hidden dependencies)

| Component | File | Guards against | Works alone? |
|-----------|------|----------------|--------------|
| **Ingest engine + sidecar checkpoint** | `ingest_deepseek.py` | Corruption, duplication, lost progress | Yes — idempotent upserts mean re-runs never duplicate; the sidecar resumes exactly where it stopped |
| **Watchdog** | `watchdog.sh` | Silent stalls and wedges | Yes — kills and relaunches any run whose log goes stale |
| **Child writer** | `add_child.py` | Store corruption on crash | Required by the engine; opens a fresh store connection per write |

Each component guards a different failure mode. Remove one and the others
degrade gracefully instead of fatally:

- No watchdog? You kill wedged runs manually. The sidecar still saves progress.
- No sidecar? Restarts start over. The upserts still prevent duplicates.

One known coupling, documented: the watchdog's staleness timeout was calibrated
to a script that logs every chunk. If your conversations are huge, keep the
per-chunk heartbeat logging (included) or raise the timeout.

## Why It Survived

The design assumes the pipeline will die. Not "might" — will. So:

1. **Writes are idempotent** — the same record written twice lands the same as once
2. **The checkpoint is written before the work it describes**
3. **Each write happens in a subprocess** — a wedge poisons one conversation, never the run
4. **The log tells the truth** — per-chunk progress lines, so a watchdog never
   kills a working process for being quiet

The failure story is published, with receipts:
**[The Cache Is Not the Corpus](https://qaevelyn.github.io/white-papers/the-cache-is-not-the-corpus/)** —
including the addendum where the wedge was diagnosed as a logging failure,
not a systems failure, and the author's own records were corrected for
counting restarts instead of completions. A record that hides its own errors
is publicity, not documentation.

## Quickstart

    # 1. Requirements: python3, ollama running with an embedding model, chromadb
    # 2. Configure paths at the top of ingest_deepseek.py (export dir, store path)
    # 3. Run
    python3 ingest_deepseek.py

    # Unattended: run the watchdog instead — it relaunches on stall
    bash watchdog.sh

## License

AGPL-3.0 — free for personal, educational, research, and non-commercial use.
Commercial use requires a license from the author. Free does not mean free to
exploit.

## Author

Evelyn — [qaevelyn.github.io](https://qaevelyn.github.io) —
sovereign AI builder, independent journalist. The corpus is public; the record
of its errors is too.

## Ship 7 — msgvault ingest adapter (ingest_msgvault.py)

Ship 7 of the A Mirror of My Becoming fleet. Reads msgvault.db (SQLite mail
archive, embed_gen watermark native), normalizes RFC 5322 message-ids,
preserves full metadata, feeds the canonical Chroma store with deterministic
ids + nomic-embed-text vectors (same model as ships 2/3/5 — no new downloads).

LAUNCH PATTERNS (owner schedule, 2026-09-30):
- Mon/Tue/Thu/Fri nights: nohup python3 ingest_msgvault.py --db ~/.msgvault/msgvault.db --store <store> --collection mirror_food_emails --embed-model nomic-embed-text --heartbeats --curfew 08:00 &
- Wed/weekend nights: same command WITHOUT --curfew (runs 24h).
- Never self-restarts. Progress stamped every batch; resume = relaunch.

Free does not mean free to exploit. If you build a product on this work, the author expects to be paid.
