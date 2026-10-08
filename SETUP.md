# Setup — Suite: Ingestion Tools (Scribe · Warden · Hand)

Three tools that build a personal RAG vector store from DeepSeek conversation
exports. Designed to survive crashes, kills, wedges, and silent stalls.

## What you need

1. Python 3
2. [Ollama](https://ollama.com) running locally, with an embedding model pulled:
   `ollama pull nomic-embed-text`
3. `pip install chromadb requests`
4. Your DeepSeek conversation export (the JSON files)

## Run Scribe (the ingest engine)

```bash
# Defaults work with zero edits — they create/use ~/Mirror-Food/:
python3 ingest_deepseek.py

# Or point everything at your own paths (the recommended way):
python3 ingest_deepseek.py \
  --export-dir ~/my-exports/deepseek \
  --store ~/my-vector-store \
  --collection my_conversations \
  --log-dir ~/my-logs
