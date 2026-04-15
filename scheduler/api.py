"""
Scheduler API
─────────────
- Schedule CRUD (cron expression, enable/disable)
- Watched folder file manager (upload, list, delete, download)
- Webhook trigger (run workflow on demand)
- Run history
"""

from fastapi import APIRouter, HTTPException, Body, Query, UploadFile, File, Depends
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime
import json, os, shutil

from database import get_db
from models import Workflow, WorkflowSchedule, ScheduleRunLog
from scheduler.headless_runner import run_workflow_headless, get_workflow_folder
from scheduler.cron_service import compute_next_run

router = APIRouter(prefix="/scheduler", tags=["Scheduler"])


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
    """Create or update a cron schedule for a workflow."""
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
async def upload_file_to_folder(
    workflow_id: str,
    file: UploadFile = File(...),
):
    """Upload a file to the workflow's watched folder."""
    folder = get_workflow_folder(workflow_id)
    file_path = os.path.join(folder, file.filename)
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    size = os.path.getsize(file_path)
    return {
        "filename": file.filename,
        "size_bytes": size,
        "size_mb": round(size / 1024 / 1024, 2),
        "path": file_path,
    }


@router.get("/workflows/{workflow_id}/files")
async def list_folder_files(workflow_id: str):
    """List all files in the workflow's watched folder."""
    folder = get_workflow_folder(workflow_id)
    files = []
    if os.path.exists(folder):
        for fname in sorted(os.listdir(folder)):
            fpath = os.path.join(folder, fname)
            if os.path.isfile(fpath):
                stat = os.stat(fpath)
                files.append({
                    "filename": fname,
                    "size_bytes": stat.st_size,
                    "size_mb": round(stat.st_size / 1024 / 1024, 2),
                    "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                })
    return {"workflow_id": workflow_id, "folder": folder, "files": files}


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
    triggered_by: str = Body("webhook"),
    file: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
):
    """
    Trigger a workflow run on demand.
    Optionally upload a file that will be placed in the watched folder before execution.
    """
    wf = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if not wf or not wf.workflow_data:
        raise HTTPException(404, "Workflow not found or has no data")

    # If a file is provided, save it to the watched folder
    if file:
        folder = get_workflow_folder(workflow_id)
        fpath = os.path.join(folder, file.filename)
        with open(fpath, "wb") as f:
            shutil.copyfileobj(file.file, f)

    # Log the run
    log = ScheduleRunLog(
        workflow_id=workflow_id,
        trigger_type="webhook" if triggered_by == "webhook" else "manual",
        triggered_by=triggered_by,
        status="running",
    )
    db.add(log)
    db.commit()
    db.refresh(log)

    # Execute
    try:
        result = run_workflow_headless(workflow_id, wf.workflow_data)
        log.status = result["status"]
        log.error_message = result.get("error")
        log.node_results = json.dumps(result.get("node_results", []))
    except Exception as e:
        log.status = "error"
        log.error_message = str(e)

    log.completed_at = datetime.utcnow()
    db.commit()

    return {
        "run_id": log.id,
        "status": log.status,
        "error": log.error_message,
        "node_results": json.loads(log.node_results) if log.node_results else [],
    }


# ── Run History ───────────────────────────────────────────────────────────────

@router.get("/workflows/{workflow_id}/runs")
async def get_run_history(
    workflow_id: str,
    limit: int = Query(20),
    db: Session = Depends(get_db),
):
    logs = db.query(ScheduleRunLog).filter(
        ScheduleRunLog.workflow_id == workflow_id
    ).order_by(ScheduleRunLog.started_at.desc()).limit(limit).all()

    return [{
        "id": l.id,
        "trigger_type": l.trigger_type,
        "triggered_by": l.triggered_by,
        "started_at": l.started_at.isoformat() if l.started_at else None,
        "completed_at": l.completed_at.isoformat() if l.completed_at else None,
        "status": l.status,
        "error": l.error_message,
        "duration_seconds": (l.completed_at - l.started_at).total_seconds() if l.completed_at and l.started_at else None,
    } for l in logs]


# ── Webhook URL helper ────────────────────────────────────────────────────────

@router.get("/workflows/{workflow_id}/webhook-url")
async def get_webhook_url(workflow_id: str):
    return {
        "url": f"/scheduler/workflows/{workflow_id}/trigger",
        "method": "POST",
        "example_curl": f'curl -X POST http://localhost:80/scheduler/workflows/{workflow_id}/trigger -H "Content-Type: application/json" -d \'{{"triggered_by": "external_system"}}\'',
        "example_with_file": f'curl -X POST http://localhost:80/scheduler/workflows/{workflow_id}/trigger -F "triggered_by=external" -F "file=@data.csv"',
    }
