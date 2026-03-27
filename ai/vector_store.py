"""
Vector Store for Node Knowledge Base
─────────────────────────────────────
Uses ChromaDB (in-process) + Ollama embeddings to index all Versor nodes.
Provides semantic search: given a user query, returns the most relevant nodes.
"""

import os
import httpx
import chromadb
from ai.node_knowledge import NODE_DOCUMENTS, build_embedding_texts, build_node_spec

_ollama_base = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
_ollama_model = os.environ.get("OLLAMA_MODEL", "llama3.2")

_client = None
_collection = None


def _get_embedding(text: str) -> list[float]:
    """Get embedding vector from Ollama."""
    resp = httpx.post(
        f"{_ollama_base}/api/embeddings",
        json={"model": _ollama_model, "prompt": text},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["embedding"]


def _get_embeddings_batch(texts: list[str]) -> list[list[float]]:
    """Get embeddings for multiple texts."""
    return [_get_embedding(t) for t in texts]


def init_store():
    """Initialize ChromaDB and index all nodes. Idempotent — safe to call multiple times."""
    global _client, _collection

    _client = chromadb.Client()

    # Delete and recreate to ensure fresh data
    try:
        _client.delete_collection("versor_nodes")
    except Exception:
        pass

    _collection = _client.create_collection(
        name="versor_nodes",
        metadata={"hnsw:space": "cosine"},
    )

    pairs = build_embedding_texts()
    ids = [p[0] for p in pairs]
    texts = [p[1] for p in pairs]

    # Get embeddings from Ollama
    try:
        embeddings = _get_embeddings_batch(texts)
        _collection.add(
            ids=ids,
            documents=texts,
            embeddings=embeddings,
        )
        print(f"[RAG] Indexed {len(ids)} nodes into vector store")
    except Exception as e:
        # Fallback: index without embeddings (ChromaDB will use its default)
        print(f"[RAG] Ollama embedding failed ({e}), using default embeddings")
        _collection.add(ids=ids, documents=texts)


def search_nodes(query: str, top_k: int = 10) -> list[dict]:
    """
    Hybrid search: semantic (vector) + keyword matching.
    Returns list of {id, name, score, spec} dicts.
    """
    global _collection
    if _collection is None:
        init_store()

    # Semantic search
    semantic_results = {}
    try:
        query_embedding = _get_embedding(query)
        results = _collection.query(
            query_embeddings=[query_embedding],
            n_results=min(top_k * 2, len(NODE_DOCUMENTS)),
        )
        if results and results["ids"] and results["ids"][0]:
            for i, node_id in enumerate(results["ids"][0]):
                dist = results["distances"][0][i] if results.get("distances") else 0
                semantic_results[node_id] = 1 - dist
    except Exception:
        pass

    # Keyword matching — boost nodes whose use_cases or description match query words
    query_lower = query.lower()
    query_words = set(query_lower.split())
    keyword_scores = {}
    for doc in NODE_DOCUMENTS:
        score = 0
        text = f"{doc['description']} {' '.join(doc['use_cases'])} {' '.join(doc['examples'])}".lower()
        # Exact phrase match in use_cases (strongest signal)
        for uc in doc["use_cases"]:
            if query_lower in uc.lower() or uc.lower() in query_lower:
                score += 5
        # Word overlap
        doc_words = set(text.split())
        overlap = query_words & doc_words
        score += len(overlap) * 0.5
        # Boost for key terms
        for term in ["sum", "add", "total", "average", "filter", "join", "group", "sort",
                     "label", "categorize", "rename", "select", "drop", "clean", "merge",
                     "multiply", "divide", "regression", "concat", "duplicate", "null",
                     "missing", "outlier", "pivot", "transpose", "cast", "date"]:
            if term in query_lower and term in text:
                score += 3
        if score > 0:
            keyword_scores[doc["id"]] = score

    # Combine scores: normalize and merge
    all_ids = set(semantic_results.keys()) | set(keyword_scores.keys())
    combined = {}
    max_sem = max(semantic_results.values()) if semantic_results else 1
    max_kw = max(keyword_scores.values()) if keyword_scores else 1
    for nid in all_ids:
        sem = semantic_results.get(nid, 0) / max_sem if max_sem > 0 else 0
        kw = keyword_scores.get(nid, 0) / max_kw if max_kw > 0 else 0
        combined[nid] = sem * 0.4 + kw * 0.6  # weight keyword matching higher

    # Sort by combined score
    ranked = sorted(combined.items(), key=lambda x: x[1], reverse=True)[:top_k]

    nodes = []
    for node_id, score in ranked:
        doc = next((d for d in NODE_DOCUMENTS if d["id"] == node_id), None)
        if doc:
            nodes.append({
                "id": node_id,
                "name": doc["name"],
                "category": doc["category"],
                "score": score,
                "spec": build_node_spec(node_id),
            })
    return nodes


def get_all_node_specs() -> str:
    """Return specs for ALL nodes (used as fallback)."""
    return "\n\n".join(build_node_spec(doc["id"]) for doc in NODE_DOCUMENTS)
