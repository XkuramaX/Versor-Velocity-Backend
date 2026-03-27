"""
AI Workflow Generator — Agentic RAG
────────────────────────────────────
Uses semantic retrieval (ChromaDB + Ollama embeddings) to find relevant nodes,
then a multi-step agent loop for feasibility → generation → validation.

When gaps are found, persists FeatureTicket rows for developer prioritisation.
"""

from __future__ import annotations

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import List, Optional
import json, io, os, httpx, polars as pl
from database import get_db
from models import FeatureTicket
from ai.agent import generate_workflow_agentic, refine_workflow_agentic
from ai.vector_store import init_store as init_vector_store

router = APIRouter(prefix="/ai", tags=["AI"])

_ollama_base = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
_ollama_tags = f"{_ollama_base}/api/tags"
_ollama_model = os.environ.get("OLLAMA_MODEL", "llama3")

_vector_store_ready = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _read_headers(raw: bytes) -> list[str]:
    try:
        return pl.read_csv(io.BytesIO(raw), n_rows=0).columns
    except Exception:
        return []


def _ensure_vector_store():
    global _vector_store_ready
    if not _vector_store_ready:
        try:
            init_vector_store()
            _vector_store_ready = True
            print("[RAG] Vector store initialized")
        except Exception as e:
            print(f"[RAG] Vector store init warning: {e}")


def _upsert_tickets(gaps: list[dict], use_case: str, db: Session) -> list[dict]:
    results = []
    for gap in gaps:
        title = gap.get("title", "")[:300]
        if not title:
            continue
        existing = db.query(FeatureTicket).filter(FeatureTicket.title == title).first()
        if existing:
            existing.vote_count += 1
            db.commit()
            db.refresh(existing)
            ticket = existing
        else:
            ticket = FeatureTicket(
                title=title,
                description=gap.get("description", ""),
                category=gap.get("category", "Other"),
                use_case=use_case[:2000],
                missing_capability=gap.get("missing_capability", ""),
            )
            db.add(ticket)
            db.commit()
            db.refresh(ticket)
        results.append(_ticket_dict(ticket))
    return results


def _ticket_dict(t: FeatureTicket) -> dict:
    return {
        "id": t.id,
        "title": t.title,
        "description": t.description,
        "category": t.category,
        "use_case": t.use_case,
        "missing_capability": getattr(t, "missing_capability", ""),
        "vote_count": t.vote_count,
        "status": t.status,
        "created_at": t.created_at.isoformat(),
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/generate-workflow")
async def generate_workflow(
    prompt: str = Form(...),
    input_files: List[UploadFile] = File(...),
    output_file: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
):
    """
    Agentic RAG workflow generation.
    1. Reads file schemas
    2. Retrieves relevant nodes via semantic search
    3. Assesses feasibility with retrieved context
    4. Generates workflow JSON
    5. Validates against node registry
    """
    _ensure_vector_store()

    # Read file schemas
    input_schemas = []
    for f in input_files:
        raw = await f.read()
        input_schemas.append({"name": f.filename, "columns": _read_headers(raw)})

    output_cols = []
    if output_file:
        raw_out = await output_file.read()
        output_cols = _read_headers(raw_out)

    # Run the agentic pipeline
    try:
        result = await generate_workflow_agentic(prompt, input_schemas, output_cols)
    except httpx.ConnectError:
        raise HTTPException(503, "Cannot reach Ollama. Run: ollama serve")
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"Ollama error: {e.response.text}")
    except Exception as e:
        raise HTTPException(422, f"AI generation failed: {e}")

    # Persist tickets for any gaps
    gaps = result.get("gaps", [])
    tickets = _upsert_tickets(gaps, use_case=prompt, db=db) if gaps else []

    return {
        "feasibility": result.get("feasibility", "none"),
        "reasoning": result.get("reasoning", ""),
        "workflow": result.get("workflow"),
        "gaps": gaps,
        "tickets": tickets,
        "feasible_steps": result.get("feasible_steps", []),
        "input_schemas": input_schemas,
        "output_schema": output_cols,
        "retrieved_nodes": result.get("retrieved_nodes", []),
    }


@router.post("/refine-workflow")
async def refine_workflow(
    prompt: str = Form(...),
    current_workflow: str = Form(...),
    input_schemas: str = Form("[]"),
):
    """Refine an existing workflow using agentic RAG."""
    _ensure_vector_store()

    try:
        schemas = json.loads(input_schemas)
    except json.JSONDecodeError:
        schemas = []

    try:
        wf = json.loads(current_workflow)
    except json.JSONDecodeError:
        raise HTTPException(400, "current_workflow is not valid JSON")

    try:
        result = await refine_workflow_agentic(prompt, wf, schemas)
    except httpx.ConnectError:
        raise HTTPException(503, "Cannot reach Ollama. Run: ollama serve")
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"Ollama error: {e.response.text}")
    except Exception as e:
        raise HTTPException(422, f"Refinement failed: {e}")

    return result


@router.get("/tickets")
async def list_tickets(
    status: Optional[str] = None,
    db: Session = Depends(get_db),
):
    q = db.query(FeatureTicket)
    if status:
        q = q.filter(FeatureTicket.status == status)
    tickets = q.order_by(FeatureTicket.vote_count.desc()).all()
    return [_ticket_dict(t) for t in tickets]


@router.post("/tickets/{ticket_id}/vote")
async def vote_ticket(ticket_id: int, db: Session = Depends(get_db)):
    t = db.query(FeatureTicket).filter(FeatureTicket.id == ticket_id).first()
    if not t:
        raise HTTPException(404, "Ticket not found")
    t.vote_count += 1
    db.commit()
    db.refresh(t)
    return _ticket_dict(t)


@router.patch("/tickets/{ticket_id}/status")
async def update_ticket_status(
    ticket_id: int,
    status: str = Form(...),
    db: Session = Depends(get_db),
):
    allowed = {"open", "in_progress", "done"}
    if status not in allowed:
        raise HTTPException(400, f"status must be one of {allowed}")
    t = db.query(FeatureTicket).filter(FeatureTicket.id == ticket_id).first()
    if not t:
        raise HTTPException(404, "Ticket not found")
    t.status = status
    db.commit()
    db.refresh(t)
    return _ticket_dict(t)


@router.get("/status")
async def ai_status():
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(_ollama_tags)
            models = [m["name"] for m in resp.json().get("models", [])]
        active = _ollama_model
        if models and active not in models:
            base = active.split(":")[0]
            matched = next((m for m in models if m.startswith(base)), None)
            active = matched or models[0]
        return {
            "ollama_running": True,
            "available_models": models,
            "configured_model": _ollama_model,
            "active_model": active,
            "model_ready": active in models,
            "rag_enabled": _vector_store_ready,
        }
    except Exception:
        return {
            "ollama_running": False,
            "available_models": [],
            "configured_model": _ollama_model,
            "active_model": _ollama_model,
            "model_ready": False,
            "rag_enabled": _vector_store_ready,
        }
