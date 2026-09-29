#!/usr/bin/env python3
# v5 child — one isolated chroma write per invocation; exits when done
import sys, json
import chromadb
payload = json.load(open(sys.argv[1]))
client = chromadb.PersistentClient(path=payload["store"])
coll = client.get_or_create_collection(payload["collection"])
coll.upsert(ids=payload["ids"], documents=payload["docs"],
            metadatas=payload["metas"], embeddings=payload["embs"])
print("ADDED", len(payload["ids"]))
