from celery_config import celery_app
from controllers.DataframeController import DataframeController
from controllers.WorkflowManager import WorkflowManager
import tempfile
import os

engine = DataframeController()
workflow = WorkflowManager(engine)

@celery_app.task(bind=True, name='tasks.upload_csv')
def upload_csv_task(self, file_path, filename):
    """Async CSV upload task"""
    try:
        self.update_state(state='PROCESSING', meta={'status': 'Uploading CSV', 'progress': 50})
        
        from fastapi import UploadFile
        with open(file_path, 'rb') as f:
            upload_file = UploadFile(filename=filename, file=f)
            res_id = engine.upload_csv(upload_file)
        
        workflow.nodes[res_id] = {
            "type": "upload_csv",
            "params": {"filename": filename},
            "parent": None,
            "children": []
        }
        
        metadata = engine.get_node_metadata(res_id)
        os.remove(file_path)  # Cleanup temp file
        
        return {'node_id': res_id, 'metadata': metadata, 'status': 'completed'}
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        return {'status': 'error', 'error': str(e)}

@celery_app.task(bind=True, name='tasks.filter_node')
def filter_node_task(self, parent_id, column, operator, value):
    """Async filter operation"""
    try:
        self.update_state(state='PROCESSING', meta={'status': 'Filtering data', 'progress': 50})
        
        if operator == "in":
            if not isinstance(value, list):
                value = [value]
            expression = f"pl.col('{column}').is_in({value})"
        elif operator in ["==", "!=", ">", "<", ">=", "<="]:
            expression = f"pl.col('{column}') {operator} {repr(value)}"
        elif operator == "contains":
            expression = f"pl.col('{column}').str.contains({repr(value)})"
        elif operator == "starts_with":
            expression = f"pl.col('{column}').str.starts_with({repr(value)})"
        elif operator == "ends_with":
            expression = f"pl.col('{column}').str.ends_with({repr(value)})"
        else:
            raise ValueError(f"Unsupported operator: {operator}")
        
        res_id = workflow.create_node("filter_data", {"expression": expression}, parent_id)
        metadata = engine.get_node_metadata(res_id)
        return {'node_id': res_id, 'metadata': metadata, 'status': 'completed'}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

@celery_app.task(bind=True, name='tasks.transform_node')
def transform_node_task(self, operation, parent_id, params):
    """Generic async transform operation"""
    try:
        self.update_state(state='PROCESSING', meta={'status': f'Applying {operation}', 'progress': 50})
        
        res_id = workflow.create_node(operation, params, parent_id)
        metadata = engine.get_node_metadata(res_id)
        return {'node_id': res_id, 'metadata': metadata, 'status': 'completed'}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

@celery_app.task(bind=True, name='tasks.join_nodes')
def join_nodes_task(self, left_id, right_id, on, how):
    """Async join operation"""
    try:
        self.update_state(state='PROCESSING', meta={'status': 'Joining datasets', 'progress': 50})
        
        params = {"right_id": right_id, "on": on, "how": how}
        res_id = workflow.create_node("join_nodes", params, parent_id=left_id)
        metadata = engine.get_node_metadata(res_id)
        return {'node_id': res_id, 'metadata': metadata, 'status': 'completed'}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

@celery_app.task(bind=True, name='tasks.groupby_node')
def groupby_node_task(self, parent_id, group_cols, aggs):
    """Async groupby operation"""
    try:
        self.update_state(state='PROCESSING', meta={'status': 'Grouping data', 'progress': 50})
        
        res_id = workflow.create_node("group_by_agg", {"group_cols": group_cols, "aggs": aggs}, parent_id)
        metadata = engine.get_node_metadata(res_id)
        return {'node_id': res_id, 'metadata': metadata, 'status': 'completed'}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}
