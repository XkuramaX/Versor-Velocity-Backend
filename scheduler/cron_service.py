"""
Cron Scheduler Service
──────────────────────
Background asyncio task that:
1. Every 30 seconds, checks all enabled schedules
2. If a schedule is due (next_run_at <= now), runs the workflow headlessly
3. Computes the next run time from the cron expression
4. Logs results to WorkflowRun (shared with UI runner)
"""

import asyncio
import json
from datetime import datetime, timezone
from croniter import croniter
from sqlalchemy.orm import Session
from database import SessionLocal
from models import WorkflowSchedule, WorkflowRun, WorkflowVersion, Workflow
from scheduler.headless_runner import run_workflow_headless

CHECK_INTERVAL = 30  # seconds


def compute_next_run(cron_expr: str, after: datetime = None) -> datetime:
    if after is None:
        after = datetime.now(timezone.utc)
    elif after.tzinfo is None:
        after = after.replace(tzinfo=timezone.utc)
    cron = croniter(cron_expr, after)
    return cron.get_next(datetime).replace(tzinfo=None)


def _run_scheduled_workflow(schedule: WorkflowSchedule, db: Session):
    """Execute a single scheduled workflow and log to WorkflowRun."""
    wf = db.query(Workflow).filter(Workflow.id == schedule.workflow_id).first()
    if not wf or not wf.workflow_data:
        print(f"[Scheduler] Workflow {schedule.workflow_id} not found or has no saved data — skipping")
        schedule.last_run_at = datetime.utcnow()
        schedule.next_run_at = compute_next_run(schedule.cron_expression)
        db.commit()
        return

    result = run_workflow_headless(schedule.workflow_id, wf.workflow_data)

    # Log to WorkflowRun
    latest_version = db.query(WorkflowVersion).filter(
        WorkflowVersion.workflow_id == schedule.workflow_id
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
        workflow_id=schedule.workflow_id,
        version_id=latest_version.id if latest_version else None,
        run_by="cron:scheduler",
        status=result["status"].upper(),
        saved_nodes=json.dumps(saved_nodes),
    )
    db.add(run)

    # Update workflow last_run
    wf.last_run = result["status"].upper()
    wf.updated_at = datetime.utcnow()

    # Update schedule timestamps
    schedule.last_run_at = datetime.utcnow()
    schedule.next_run_at = compute_next_run(schedule.cron_expression)
    db.commit()

    status_emoji = "✅" if result["status"] == "success" else "❌"
    print(f"[Scheduler] {status_emoji} Workflow {schedule.workflow_id} — {result['status']}")
    if result.get("error"):
        print(f"[Scheduler]   Error: {result['error']}")


async def scheduler_loop():
    """Main scheduler loop — runs as a background asyncio task."""
    print("[Scheduler] Started. Checking every 30s.")
    while True:
        try:
            db = SessionLocal()
            now = datetime.utcnow()

            schedules = db.query(WorkflowSchedule).filter(
                WorkflowSchedule.enabled == 1
            ).all()

            for sched in schedules:
                if sched.next_run_at is None:
                    sched.next_run_at = compute_next_run(sched.cron_expression)
                    db.commit()
                    continue

                if sched.next_run_at <= now:
                    print(f"[Scheduler] Running workflow {sched.workflow_id} (cron: {sched.cron_expression})")
                    try:
                        # Run in thread pool to avoid blocking the event loop
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, _run_scheduled_workflow, sched, db)
                    except Exception as e:
                        print(f"[Scheduler] Error running {sched.workflow_id}: {e}")

            db.close()
        except Exception as e:
            print(f"[Scheduler] Loop error: {e}")

        await asyncio.sleep(CHECK_INTERVAL)
