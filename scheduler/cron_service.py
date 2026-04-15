"""
Cron Scheduler Service
──────────────────────
Background asyncio task that:
1. Every 30 seconds, checks all enabled schedules
2. If a schedule is due (next_run_at <= now), runs the workflow headlessly
3. Computes the next run time from the cron expression
4. Logs results to ScheduleRunLog
"""

import asyncio
import json
from datetime import datetime, timezone
from croniter import croniter
from sqlalchemy.orm import Session
from database import SessionLocal
from models import WorkflowSchedule, ScheduleRunLog, Workflow
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
    """Execute a single scheduled workflow and log the result."""
    log = ScheduleRunLog(
        workflow_id=schedule.workflow_id,
        trigger_type="cron",
        triggered_by="scheduler",
        status="running",
    )
    db.add(log)
    db.commit()
    db.refresh(log)

    wf = db.query(Workflow).filter(Workflow.id == schedule.workflow_id).first()
    if not wf or not wf.workflow_data:
        log.status = "error"
        log.error_message = "Workflow not found or has no data"
        log.completed_at = datetime.utcnow()
        db.commit()
        return

    try:
        result = run_workflow_headless(schedule.workflow_id, wf.workflow_data)
        log.status = result["status"]
        log.error_message = result.get("error")
        log.node_results = json.dumps(result.get("node_results", []))
    except Exception as e:
        log.status = "error"
        log.error_message = str(e)

    log.completed_at = datetime.utcnow()
    schedule.last_run_at = datetime.utcnow()
    schedule.next_run_at = compute_next_run(schedule.cron_expression)
    db.commit()


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
                        _run_scheduled_workflow(sched, db)
                    except Exception as e:
                        print(f"[Scheduler] Error running {sched.workflow_id}: {e}")

            db.close()
        except Exception as e:
            print(f"[Scheduler] Loop error: {e}")

        await asyncio.sleep(CHECK_INTERVAL)
