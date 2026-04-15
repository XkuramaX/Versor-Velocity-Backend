"""
Scheduler API
─────────────
- Schedule CRUD (cron expression, enable/disable)
- Watched folder file manager (upload, list, delete, download)
- Webhook trigger (supports multiple input nodes via watched folder)
- Authenticated node-data API for external consumers
"""

from fastapi import APIRouter, HTTPException, Body, Query, UploadFile, File, Form, Depends, Header
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime
import json, os, shutil

from database import get_db
from models import Workflow, WorkflowPermission, WorkflowRun, WorkflowVersion, WorkflowSchedule
from scheduler.headless_runner import run_workflow_headless, get_workflow_folder, list_upload_nodes
from scheduler.cron_service import compute_next_run

router = APIRouter(prefix="/scheduler", tags=["Scheduler"])


# ── Auth helper ───────────────────────────────────────────────────────────────

def _verify_token(token: str) -> dict:
    """Verify a JWT from the auth service. Returns payload or raises 401."""
    import os
    from jose import JWTError, jwt
    secret = os.getenv("AUTH_SECRET_KEY", "your-secret-key-change-in-production")
    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
        return payload
    except (JWTError, Exception):
        raise HTTPException(401, "Invalid or expired token")


def _check_workflow_access(db: Session, workflow_id: str, user_id: int, min_level: str = "viewer"):
    """Check user has at least min_level access. Returns the Workflow or raises 403."""
    wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(404, "Workflow not found")

    # Creator has full access
    if wf.creator_id == user_id:
        return wf

    perm = db.query(WorkflowPermission).filter(
        WorkflowPermission.workflow_id == workflow_id,
        WorkflowPermission.user_id == user_id,
    ).first()

    level_order = {"viewer": 0, "runner": 1, "editor": 2, "creator": 3}
    user_level = level_order.get(perm.permission_level.value, -1) if perm else -1
    required = level_order.get(min_level, 0)

    if user_level < required:
        raise HTTPException(403, f"Requires at least '{min_level}' access")
    return wf


def _log_run(db: Session, workflow_id: str, trigger_type: str, triggered_by: str, result: dict):
    """Log a headless run to WorkflowRun (shared with UI runner)."""
    # Find latest version
    latest_version = db.query(WorkflowVersion).filter(
        WorkflowVersion.workflow_id == workflow_id
    ).order_by(WorkflowVersion.version_number.desc()).first()

    saved_nodes = []
    for nr in result.get("node_results", []):
        if nr.get("status") == "success" and nr.get("backend_id"):
            saved_nodes.append({
                "node_id": nr["node_id"],
                "backend_node_id": nr["backend_id"],
                "label": nr.get("label", ""),
            })

    run = WorkflowRun(
        workflow_id=workflow_id,
        version_id=latest_version.id if latest_version else None,
        run_by=f"{trigger_type}:{triggered_by}",
        status=result["status"].upper(),
        saved_nodes=json.dumps(saved_nodes),
    )
    db.add(run)

    # Update workflow last_run
    wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if wf:
        wf.last_run = result["status"].upper()
        wf.updated_at = datetime.utcnow()

    db.commit()
    db.refresh(run)
    return run


# ── Schedule CRUD ─────────────────────────────────────────────────────────────

@router.post("/workflows/{workflow_id}/schedule")
async def create_or_update_schedule(
    workflow_id: str,
    cron_expression: str = Body(...),
    enabled: bool = Body(True),
    timezone: str = Body("UTC"),
    created_by: str = Body("admin"),
    db: Session = Depends(get_db),
):
    wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if not wf:
        raise HTTPException(404, "Workflow not found")

    try:
        next_run = compute_next_run(cron_expression)
    except Exception:
        raise HTTPException(400, f"Invalid cron expression: {cron_expression}")

    existing = db.query(WorkflowSchedule).filter(WorkflowSchedule.workflow_id == workflow_id).first()
    if existing:
        existing.cron_expression = cron_expression
        existing.enabled = 1 if enabled else 0
        existing.timezone = timezone
        existing.next_run_at = next_run
        existing.updated_at = datetime.utcnow()
    else:
        sched = WorkflowSchedule(
            workflow_id=workflow_id,
            cron_expression=cron_expression,
            enabled=1 if enabled else 0,
            timezone=timezone,
            created_by=created_by,
            next_run_at=next_run,
        )
        db.add(sched)

    db.commit()
    return {"status": "scheduled", "cron": cron_expression, "next_run": next_run.isoformat(), "enabled": enabled}


@router.get("/workflows/{workflow_id}/schedule")
async def get_schedule(workflow_id: str, db: Session = Depends(get_db)):
    sched = db.query(WorkflowSchedule).filter(WorkflowSchedule.workflow_id == workflow_id).first()
    if not sched:
        return {"scheduled": False}
    return {
        "scheduled": True,
        "cron_expression": sched.cron_expression,
        "enabled": sched.enabled == 1,
        "timezone": sched.timezone,
        "next_run_at": sched.next_run_at.isoformat() if sched.next_run_at else None,
        "last_run_at": sched.last_run_at.isoformat() if sched.last_run_at else None,
        "created_by": sched.created_by,
    }


@router.delete("/workflows/{workflow_id}/schedule")
async def delete_schedule(workflow_id: str, db: Session = Depends(get_db)):
    sched = db.query(WorkflowSchedule).filter(WorkflowSchedule.workflow_id == workflow_id).first()
    if sched:
        db.delete(sched)
        db.commit()
    return {"status": "deleted"}


@router.patch("/workflows/{workflow_id}/schedule/toggle")
async def toggle_schedule(workflow_id: str, enabled: bool = Body(...), db: Session = Depends(get_db)):
    sched = db.query(WorkflowSchedule).filter(WorkflowSchedule.workflow_id == workflow_id).first()
    if not sched:
        raise HTTPException(404, "No schedule found")
    sched.enabled = 1 if enabled else 0
    if enabled and sched.cron_expression:
        sched.next_run_at = compute_next_run(sched.cron_expression)
    db.commit()
    return {"enabled": enabled}


# ── Watched Folder File Manager ───────────────────────────────────────────────

@router.post("/workflows/{workflow_id}/files/upload")
async def upload_file_to_folder(workflow_id: str, file: UploadFile = File(...)):
    folder = get_workflow_folder(workflow_id)
    file_path = os.path.join(folder, file.filename)
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    size = os.path.getsize(file_path)
    return {"filename": file.filename, "size_bytes": size, "size_mb": round(size / 1024 / 1024, 2)}


@router.get("/workflows/{workflow_id}/files")
async def list_folder_files(workflow_id: str, db: Session = Depends(get_db)):
    """List watched folder files + which upload nodes they map to."""
    folder = get_workflow_folder(workflow_id)

    # Get upload nodes from saved workflow
    wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    upload_nodes = list_upload_nodes(wf.workflow_data) if wf and wf.workflow_data else []

    files = []
    if os.path.exists(folder):
        for fname in sorted(os.listdir(folder)):
            fpath = os.path.join(folder, fname)
            if os.path.isfile(fpath):
                stat = os.stat(fpath)
                # Find which upload node this file maps to
                matched_node = None
                fname_lower = fname.lower()
                fname_stem = os.path.splitext(fname)[0].lower()
                for un in upload_nodes:
                    un_label = (un["label"] or "").strip().lower()
                    un_stem = os.path.splitext(un_label)[0].lower()
                    if un_label == fname_lower or (un_stem and un_stem == fname_stem):
                        matched_node = un["label"]
                        break

                files.append({
                    "filename": fname,
                    "size_bytes": stat.st_size,
                    "size_mb": round(stat.st_size / 1024 / 1024, 2),
                    "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "matched_node": matched_node,
                })

    return {
        "workflow_id": workflow_id,
        "folder": folder,
        "files": files,
        "upload_nodes": upload_nodes,
    }


@router.delete("/workflows/{workflow_id}/files/{filename}")
async def delete_folder_file(workflow_id: str, filename: str):
    folder = get_workflow_folder(workflow_id)
    fpath = os.path.join(folder, filename)
    if not os.path.exists(fpath):
        raise HTTPException(404, "File not found")
    os.remove(fpath)
    return {"status": "deleted", "filename": filename}


@router.get("/workflows/{workflow_id}/files/{filename}/download")
async def download_folder_file(workflow_id: str, filename: str):
    folder = get_workflow_folder(workflow_id)
    fpath = os.path.join(folder, filename)
    if not os.path.exists(fpath):
        raise HTTPException(404, "File not found")
    return FileResponse(fpath, filename=filename)


# ── Webhook Trigger ───────────────────────────────────────────────────────────

@router.post("/workflows/{workflow_id}/trigger")
async def trigger_workflow(
    workflow_id: str,
    triggered_by: str = Form("webhook"),
    node_label: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
):
    """Trigger a workflow run.

    For multiple input nodes: upload files to the Data Files tab beforehand,
    naming each file to match the upload node's label.

    Optionally attach a single file here — if node_label is provided, the file
    is saved with that name; otherwise it keeps its original filename.
    """
    wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if not wf or not wf.workflow_data:
        raise HTTPException(404, "Workflow not found or has no data")

    # Save attached file to watched folder
    if file:
        folder = get_workflow_folder(workflow_id)
        save_name = node_label if node_label else file.filename
        fpath = os.path.join(folder, save_name)
        with open(fpath, "wb") as f:
            shutil.copyfileobj(file.file, f)

    # Execute
    result = run_workflow_headless(workflow_id, wf.workflow_data)

    # Log to WorkflowRun
    trigger_type = "webhook" if triggered_by == "webhook" else "manual"
    run = _log_run(db, workflow_id, trigger_type, triggered_by, result)

    return {
        "run_id": run.id,
        "status": result["status"],
        "error": result.get("error"),
        "node_results": result.get("node_results", []),
    }


# ── Upload Nodes Info ─────────────────────────────────────────────────────────

@router.get("/workflows/{workflow_id}/upload-nodes")
async def get_upload_nodes(workflow_id: str, db: Session = Depends(get_db)):
    """List all upload nodes in a workflow so the user knows which files to provide."""
    wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if not wf or not wf.workflow_data:
        raise HTTPException(404, "Workflow not found")
    return {"upload_nodes": list_upload_nodes(wf.workflow_data)}


# ── Webhook URL helper ────────────────────────────────────────────────────────

@router.get("/workflows/{workflow_id}/webhook-url")
async def get_webhook_url(workflow_id: str, db: Session = Depends(get_db)):
    wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    upload_nodes = list_upload_nodes(wf.workflow_data) if wf and wf.workflow_data else []

    base = f"/scheduler/workflows/{workflow_id}/trigger"
    examples = {
        "url": base,
        "method": "POST",
        "example_simple": f'curl -X POST http://localhost:80{base} -H "Content-Type: application/json" -d \'{{"triggered_by": "airflow"}}\'',
    }

    if len(upload_nodes) == 1:
        lbl = upload_nodes[0]["label"]
        examples["example_with_file"] = (
            f'curl -X POST http://localhost:80{base} '
            f'-F "triggered_by=pipeline" -F "node_label={lbl}" -F "file=@{lbl}"'
        )
    elif len(upload_nodes) > 1:
        examples["note"] = (
            "This workflow has multiple input nodes. Upload files to the Data Files tab "
            "with filenames matching each upload node's label, then trigger without a file."
        )
        examples["upload_nodes"] = [n["label"] for n in upload_nodes]

    return examples


# ── Authenticated Node Data API ───────────────────────────────────────────────

@router.get("/workflows/{workflow_id}/nodes/{node_frontend_id}/data")
async def get_node_data_authenticated(
    workflow_id: str,
    node_frontend_id: str,
    format: str = Query("json", description="json or csv"),
    limit: int = Query(100, description="Max rows for JSON preview"),
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """Get a node's output data, authenticated via Bearer token.

    The user must have at least 'viewer' access to the workflow.
    node_frontend_id is the React Flow node ID (e.g. 'upload_csv_1234567890').
    """
    # Auth
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    token = authorization.split(" ", 1)[1]
    payload = _verify_token(token)
    user_id = int(payload.get("sub", 0))

    # Access check
    wf = _check_workflow_access(db, workflow_id, user_id, "viewer")

    if not wf.workflow_data:
        raise HTTPException(404, "Workflow has no data")

    # Find the node's backend_node_id from the saved workflow
    try:
        wf_data = json.loads(wf.workflow_data)
    except json.JSONDecodeError:
        raise HTTPException(500, "Invalid workflow data")

    node = next((n for n in wf_data.get("nodes", []) if n["id"] == node_frontend_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_frontend_id}' not found in workflow")

    backend_id = node.get("data", {}).get("backendNodeId")
    if not backend_id:
        raise HTTPException(404, "Node has not been executed yet — no data available")

    # Get data from engine
    from controllers.DataframeController import DataframeController
    engine = DataframeController()

    if backend_id not in engine.registry:
        raise HTTPException(404, "Node data expired or not in memory. Re-run the workflow first.")

    if format == "csv":
        buffer = engine.export_node_to_buffer(backend_id, "csv")
        return StreamingResponse(
            buffer,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={backend_id}.csv"},
        )

    # JSON preview
    metadata = engine.get_node_metadata(backend_id, n_preview=limit)
    return {
        "workflow_id": workflow_id,
        "node_id": node_frontend_id,
        "backend_node_id": backend_id,
        "label": node.get("data", {}).get("label", ""),
        "data": metadata,
    }
