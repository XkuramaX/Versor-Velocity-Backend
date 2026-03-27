from fastapi import APIRouter, HTTPException, Body, Query, UploadFile, File
from typing import Dict, List, Any, Optional, Union
import tempfile
import os
from tasks import (
    upload_csv_task, filter_node_task, transform_node_task,
    join_nodes_task, groupby_node_task
)
from celery.result import AsyncResult
from celery_config import celery_app

router = APIRouter(prefix="/async", tags=["Async Operations"])

@router.post("/upload_csv")
async def async_upload_csv(file: UploadFile = File(...)):
    """Queue CSV upload for async processing"""
    try:
        # Save file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        # Queue task
        task = upload_csv_task.delay(tmp_path, file.filename)
        
        return {
            "task_id": task.id,
            "status": "queued",
            "message": "CSV upload queued for processing"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/filter")
async def async_filter_node(
    parent_id: str = Query(...),
    column: str = Body(...),
    operator: str = Body(...),
    value: Union[str, int, float, List[Union[str, int, float]]] = Body(...)
):
    """Queue filter operation for async processing"""
    try:
        task = filter_node_task.delay(parent_id, column, operator, value)
        return {
            "task_id": task.id,
            "status": "queued",
            "message": "Filter operation queued"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/transform/{operation}")
async def async_transform(
    operation: str,
    parent_id: str = Query(...),
    params: Dict = Body(...)
):
    """Queue generic transform operation"""
    try:
        task = transform_node_task.delay(operation, parent_id, params)
        return {
            "task_id": task.id,
            "status": "queued",
            "message": f"{operation} queued for processing"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/join")
async def async_join(
    left_id: str = Query(...),
    right_id: str = Query(...),
    on: str = Body(...),
    how: str = Body("inner")
):
    """Queue join operation"""
    try:
        task = join_nodes_task.delay(left_id, right_id, on, how)
        return {
            "task_id": task.id,
            "status": "queued",
            "message": "Join operation queued"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/groupby")
async def async_groupby(
    parent_id: str = Query(...),
    group_cols: List[str] = Body(...),
    aggs: Dict[str, List[str]] = Body(...)
):
    """Queue groupby operation"""
    try:
        task = groupby_node_task.delay(parent_id, group_cols, aggs)
        return {
            "task_id": task.id,
            "status": "queued",
            "message": "GroupBy operation queued"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/task/{task_id}")
async def get_task_status(task_id: str):
    """Get status of async task"""
    try:
        task = AsyncResult(task_id, app=celery_app)
        
        if task.state == 'PENDING':
            return {
                "task_id": task_id,
                "status": "pending",
                "message": "Task is waiting in queue"
            }
        elif task.state == 'PROCESSING':
            return {
                "task_id": task_id,
                "status": "processing",
                "message": task.info.get('status', 'Processing'),
                "progress": task.info.get('progress', 0)
            }
        elif task.state == 'SUCCESS':
            result = task.result
            return {
                "task_id": task_id,
                "status": "completed",
                "result": result
            }
        elif task.state == 'FAILURE':
            return {
                "task_id": task_id,
                "status": "failed",
                "error": str(task.info)
            }
        else:
            return {
                "task_id": task_id,
                "status": task.state.lower(),
                "message": str(task.info)
            }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/task/{task_id}")
async def cancel_task(task_id: str):
    """Cancel a running task"""
    try:
        task = AsyncResult(task_id, app=celery_app)
        task.revoke(terminate=True)
        return {
            "task_id": task_id,
            "status": "cancelled",
            "message": "Task cancelled successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
