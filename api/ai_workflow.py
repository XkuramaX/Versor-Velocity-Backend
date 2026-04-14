"""
AI Workflow Generator — Planner → Validator → Compiler
──────────────────────────────────────────────────────
Stage A (Planner):   LLM outputs plain-text YAML steps — no JSON
Stage B (Validator): Python validates columns, types, fixes errors
Stage C (Compiler):  Python generates React Flow JSON deterministically
"""

from __future__ import annotations

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import List, Optional
import json, io, os, httpx, polars as pl
from database import get_db
from models import FeatureTicket
from ai.schema_inferrer import infer_schema
from ai.planner import plan, plan_refinement
from ai.validator import validate
from ai.compiler import compile_to_react_flow

router = APIRouter(prefix="/ai", tags=["AI"])

_ollama_base = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
_ollama_tags = f"{_ollama_base}/api/tags"
_ollama_model = os.environ.get("OLLAMA_MODEL", "qwen2.5-coder:7b")


def _read_file_schema(raw: bytes, filename: str) -> dict:
    schema = infer_schema(raw, filename)
    return {"name": filename, "schema": schema, "columns": list(schema.keys())}


def _upsert_tickets(gaps: list[dict], use_case: str, db: Session) -> list[dict]:
    results = []
    for gap in gaps:
        title = gap.get("title", gap.get("reason", ""))[:300]
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
                description=gap.get("description", gap.get("reason", "")),
                category=gap.get("category", "Other"),
                use_case=use_case[:2000],
                missing_capability=gap.get("missing_capability", gap.get("action", "")),
            )
            db.add(ticket)
            db.commit()
            db.refresh(ticket)
        results.append(_ticket_dict(ticket))
    return results


def _ticket_dict(t: FeatureTicket) -> dict:
    return {
        "id": t.id, "title": t.title, "description": t.description,
        "category": t.category, "use_case": t.use_case,
        "missing_capability": getattr(t, "missing_capability", ""),
        "vote_count": t.vote_count, "status": t.status,
        "created_at": t.created_at.isoformat(),
    }


@router.post("/generate-workflow")
async def generate_workflow(
    prompt: str = Form(...),
    input_files: List[UploadFile] = File(...),
    output_file: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
):
    """
    Planner → Validator → Compiler pipeline.
    """
    # 1. Read file schemas
    file_schemas = []
    for f in input_files:
        raw = await f.read()
        file_schemas.append(_read_file_schema(raw, f.filename))

    output_cols = []
    if output_file:
        raw_out = await output_file.read()
        output_cols = list(infer_schema(raw_out, output_file.filename).keys())

    # 2. Stage A — Planner: LLM outputs YAML steps
    try:
        planner_result = await plan(prompt, file_schemas)
    except httpx.ConnectError:
        raise HTTPException(503, "Cannot reach Ollama. Run: ollama serve")
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"Ollama error: {e.response.text}")
    except Exception as e:
        raise HTTPException(422, f"Planner failed: {e}")

    steps = planner_result.get("steps", [])
    unsupported = planner_result.get("unsupported", [])

    # File tickets for unsupported steps
    tickets = _upsert_tickets(unsupported, use_case=prompt, db=db) if unsupported else []

    # Determine feasibility
    if not steps and unsupported:
        feasibility = "none"
    elif unsupported:
        feasibility = "partial"
    else:
        feasibility = "full"

    # 3. Stage B — Validator: check columns, types, fix errors
    workflow = None
    validation_errors = []
    validation_warnings = []
    validation_fixes = []

    if steps:
        vresult = validate(steps, file_schemas)
        validation_errors = vresult.errors
        validation_warnings = vresult.warnings
        validation_fixes = vresult.fixes
        validated_steps = vresult.steps

        # 4. Stage C — Compiler: deterministic React Flow JSON
        if validated_steps:
            wf = compile_to_react_flow(validated_steps)
            workflow = {"nodes": wf["nodes"], "edges": wf["edges"]}

    # Build display steps
    feasible_steps = []
    for s in steps:
        action = s.get("action", "?")
        label = s.get("label", s.get("alias", action))
        feasible_steps.append(f"{label} ({action})")

    # Build gaps from unsupported
    gaps = []
    for u in unsupported:
        gaps.append({
            "title": u.get("reason", u.get("action", "Unknown feature")),
            "description": u.get("reason", "This operation is not supported by available nodes."),
            "category": "Other",
            "missing_capability": u.get("action", ""),
        })

    reasoning = f"{'All' if feasibility == 'full' else 'Some'} steps mapped to existing nodes."
    if validation_fixes:
        reasoning += f" Auto-fixed: {'; '.join(validation_fixes)}"
    if validation_errors:
        reasoning += f" Errors: {'; '.join(validation_errors)}"

    return {
        "feasibility": feasibility,
        "reasoning": reasoning,
        "workflow": workflow,
        "gaps": gaps,
        "tickets": tickets,
        "feasible_steps": feasible_steps,
        "input_schemas": [{"name": s["name"], "columns": s["columns"]} for s in file_schemas],
        "output_schema": output_cols,
        "validation_errors": validation_errors,
        "validation_warnings": validation_warnings,
        "validation_fixes": validation_fixes,
    }


@router.post("/refine-workflow")
async def refine_workflow(
    prompt: str = Form(...),
    current_workflow: str = Form(...),
    input_schemas: str = Form("[]"),
):
    try:
        schemas = json.loads(input_schemas)
    except json.JSONDecodeError:
        schemas = []

    try:
        wf = json.loads(current_workflow)
    except json.JSONDecodeError:
        raise HTTPException(400, "current_workflow is not valid JSON")

    # Extract current steps from workflow nodes
    current_steps = []
    for node in wf.get("nodes", []):
        data = node.get("data", {})
        current_steps.append({
            "action": data.get("nodeType", "upload_csv"),
            "config": data.get("config", {}),
            "alias": data.get("label", ""),
        })

    try:
        planner_result = await plan_refinement(prompt, current_steps, schemas)
    except httpx.ConnectError:
        raise HTTPException(503, "Cannot reach Ollama. Run: ollama serve")
    except Exception as e:
        raise HTTPException(422, f"Refinement failed: {e}")

    steps = planner_result.get("steps", [])
    if not steps:
        raise HTTPException(422, "Planner returned no steps")

    vresult = validate(steps, schemas)
    wf_out = compile_to_react_flow(vresult.steps)
    return {"workflow": {"nodes": wf_out["nodes"], "edges": wf_out["edges"]}}


# ── Ticket + Status endpoints (unchanged) ────────────────────────────────────

@router.get("/tickets")
async def list_tickets(status: Optional[str] = None, db: Session = Depends(get_db)):
    q = db.query(FeatureTicket)
    if status:
        q = q.filter(FeatureTicket.status == status)
    return [_ticket_dict(t) for t in q.order_by(FeatureTicket.vote_count.desc()).all()]

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
async def update_ticket_status(ticket_id: int, status: str = Form(...), db: Session = Depends(get_db)):
    if status not in {"open", "in_progress", "done"}:
        raise HTTPException(400, "status must be: open, in_progress, done")
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
            "ollama_running": True, "available_models": models,
            "configured_model": _ollama_model, "active_model": active,
            "model_ready": active in models, "architecture": "planner-validator-compiler",
        }
    except Exception:
        return {
            "ollama_running": False, "available_models": [],
            "configured_model": _ollama_model, "active_model": _ollama_model,
            "model_ready": False, "architecture": "planner-validator-compiler",
        }
