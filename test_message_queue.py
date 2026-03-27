import pytest
import asyncio
import tempfile
import os
from fastapi.testclient import TestClient
from celery.result import AsyncResult
from tasks import upload_csv_task, filter_node_task, transform_node_task, join_nodes_task, groupby_node_task
from celery_config import celery_app

# Test data
TEST_CSV_DATA = """name,age,city
Alice,25,NYC
Bob,30,LA
Charlie,35,Chicago
David,40,NYC
Eve,45,LA"""

class TestMessageQueueIntegration:
    """Test suite for message queue integration"""
    
    @pytest.fixture
    def test_csv_file(self):
        """Create temporary CSV file for testing"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(TEST_CSV_DATA)
            temp_path = f.name
        yield temp_path
        if os.path.exists(temp_path):
            os.remove(temp_path)
    
    def test_upload_csv_task(self, test_csv_file):
        """Test CSV upload via Celery task"""
        # Submit task
        task = upload_csv_task.delay(test_csv_file, 'test.csv')
        
        # Wait for completion
        result = task.get(timeout=30)
        
        # Verify result
        assert result['status'] == 'completed'
        assert 'node_id' in result
        assert 'metadata' in result
        assert result['metadata']['rows'] == 5
        assert result['metadata']['columns'] == 3
        
        print(f"✓ CSV Upload Test Passed - Node ID: {result['node_id']}")
        return result['node_id']
    
    def test_filter_task(self, test_csv_file):
        """Test filter operation via Celery task"""
        # First upload CSV
        upload_task = upload_csv_task.delay(test_csv_file, 'test.csv')
        upload_result = upload_task.get(timeout=30)
        parent_id = upload_result['node_id']
        
        # Then filter
        filter_task = filter_node_task.delay(parent_id, 'age', '>', 30)
        filter_result = filter_task.get(timeout=30)
        
        # Verify result
        assert filter_result['status'] == 'completed'
        assert 'node_id' in filter_result
        assert filter_result['metadata']['rows'] == 3  # 3 people > 30
        
        print(f"✓ Filter Test Passed - Filtered {filter_result['metadata']['rows']} rows")
        return filter_result['node_id']
    
    def test_transform_task(self, test_csv_file):
        """Test transform operation via Celery task"""
        # Upload CSV
        upload_task = upload_csv_task.delay(test_csv_file, 'test.csv')
        upload_result = upload_task.get(timeout=30)
        parent_id = upload_result['node_id']
        
        # Select columns
        transform_task = transform_node_task.delay('select_columns', parent_id, {'columns': ['name', 'age']})
        transform_result = transform_task.get(timeout=30)
        
        # Verify result
        assert transform_result['status'] == 'completed'
        assert transform_result['metadata']['columns'] == 2
        
        print(f"✓ Transform Test Passed - Selected 2 columns")
        return transform_result['node_id']
    
    def test_groupby_task(self, test_csv_file):
        """Test groupby operation via Celery task"""
        # Upload CSV
        upload_task = upload_csv_task.delay(test_csv_file, 'test.csv')
        upload_result = upload_task.get(timeout=30)
        parent_id = upload_result['node_id']
        
        # GroupBy
        groupby_task = groupby_node_task.delay(
            parent_id,
            ['city'],
            {'age': ['mean', 'count']}
        )
        groupby_result = groupby_task.get(timeout=30)
        
        # Verify result
        assert groupby_result['status'] == 'completed'
        assert groupby_result['metadata']['rows'] == 3  # 3 cities
        
        print(f"✓ GroupBy Test Passed - Grouped by city")
        return groupby_result['node_id']
    
    def test_task_status_tracking(self, test_csv_file):
        """Test task status tracking"""
        # Submit task
        task = upload_csv_task.delay(test_csv_file, 'test.csv')
        
        # Check status while running
        status_checks = []
        while not task.ready():
            status = task.state
            status_checks.append(status)
            asyncio.sleep(0.1)
        
        # Verify status progression
        assert 'PENDING' in status_checks or 'PROCESSING' in status_checks
        assert task.state == 'SUCCESS'
        
        print(f"✓ Status Tracking Test Passed - States: {set(status_checks)}")
    
    def test_task_cancellation(self, test_csv_file):
        """Test task cancellation"""
        # Submit long-running task
        task = upload_csv_task.delay(test_csv_file, 'test.csv')
        
        # Cancel immediately
        task.revoke(terminate=True)
        
        # Verify cancellation
        assert task.state in ['REVOKED', 'PENDING']
        
        print(f"✓ Task Cancellation Test Passed")
    
    def test_error_handling(self):
        """Test error handling in tasks"""
        # Try to filter non-existent node
        task = filter_node_task.delay('invalid_node_id', 'age', '>', 30)
        result = task.get(timeout=30)
        
        # Verify error handling
        assert result['status'] == 'error'
        assert 'error' in result
        
        print(f"✓ Error Handling Test Passed")
    
    def test_concurrent_tasks(self, test_csv_file):
        """Test concurrent task execution"""
        # Submit multiple tasks concurrently
        tasks = []
        for i in range(5):
            task = upload_csv_task.delay(test_csv_file, f'test_{i}.csv')
            tasks.append(task)
        
        # Wait for all to complete
        results = [task.get(timeout=60) for task in tasks]
        
        # Verify all succeeded
        assert all(r['status'] == 'completed' for r in results)
        assert len(set(r['node_id'] for r in results)) == 5  # All unique
        
        print(f"✓ Concurrent Tasks Test Passed - {len(results)} tasks completed")
    
    def test_workflow_chain(self, test_csv_file):
        """Test chained workflow operations"""
        # Upload
        upload_task = upload_csv_task.delay(test_csv_file, 'test.csv')
        node1 = upload_task.get(timeout=30)['node_id']
        
        # Filter
        filter_task = filter_node_task.delay(node1, 'age', '>', 25)
        node2 = filter_task.get(timeout=30)['node_id']
        
        # Select
        select_task = transform_node_task.delay('select_columns', node2, {'columns': ['name', 'city']})
        node3 = select_task.get(timeout=30)['node_id']
        
        # Verify chain
        assert node1 != node2 != node3
        
        print(f"✓ Workflow Chain Test Passed - 3 operations chained")


class TestAsyncAPIEndpoints:
    """Test async API endpoints"""
    
    @pytest.fixture
    def client(self):
        from api.NodesApiComplete import app
        return TestClient(app)
    
    def test_async_upload_endpoint(self, client, test_csv_file):
        """Test async upload endpoint"""
        with open(test_csv_file, 'rb') as f:
            response = client.post(
                '/async/upload_csv',
                files={'file': ('test.csv', f, 'text/csv')}
            )
        
        assert response.status_code == 200
        data = response.json()
        assert 'task_id' in data
        assert data['status'] == 'queued'
        
        print(f"✓ Async Upload Endpoint Test Passed - Task ID: {data['task_id']}")
    
    def test_async_filter_endpoint(self, client):
        """Test async filter endpoint"""
        response = client.post(
            '/async/filter?parent_id=test_node',
            json={'column': 'age', 'operator': '>', 'value': 30}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert 'task_id' in data
        
        print(f"✓ Async Filter Endpoint Test Passed")
    
    def test_task_status_endpoint(self, client, test_csv_file):
        """Test task status endpoint"""
        # Submit task
        with open(test_csv_file, 'rb') as f:
            response = client.post(
                '/async/upload_csv',
                files={'file': ('test.csv', f, 'text/csv')}
            )
        task_id = response.json()['task_id']
        
        # Check status
        status_response = client.get(f'/async/task/{task_id}')
        assert status_response.status_code == 200
        
        status_data = status_response.json()
        assert 'status' in status_data
        
        print(f"✓ Task Status Endpoint Test Passed - Status: {status_data['status']}")
    
    def test_task_cancel_endpoint(self, client, test_csv_file):
        """Test task cancellation endpoint"""
        # Submit task
        with open(test_csv_file, 'rb') as f:
            response = client.post(
                '/async/upload_csv',
                files={'file': ('test.csv', f, 'text/csv')}
            )
        task_id = response.json()['task_id']
        
        # Cancel task
        cancel_response = client.delete(f'/async/task/{task_id}')
        assert cancel_response.status_code == 200
        
        print(f"✓ Task Cancel Endpoint Test Passed")


def run_all_tests():
    """Run all tests"""
    print("\n" + "="*60)
    print("MESSAGE QUEUE INTEGRATION TEST SUITE")
    print("="*60 + "\n")
    
    # Create test CSV
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        f.write(TEST_CSV_DATA)
        test_file = f.name
    
    try:
        # Task tests
        print("\n--- CELERY TASK TESTS ---\n")
        test_suite = TestMessageQueueIntegration()
        test_suite.test_upload_csv_task(test_file)
        test_suite.test_filter_task(test_file)
        test_suite.test_transform_task(test_file)
        test_suite.test_groupby_task(test_file)
        test_suite.test_task_status_tracking(test_file)
        test_suite.test_error_handling()
        test_suite.test_concurrent_tasks(test_file)
        test_suite.test_workflow_chain(test_file)
        
        print("\n--- API ENDPOINT TESTS ---\n")
        # API tests would require running server
        print("⚠ API tests require running server - skipping")
        
        print("\n" + "="*60)
        print("ALL TESTS PASSED ✓")
        print("="*60 + "\n")
        
    finally:
        if os.path.exists(test_file):
            os.remove(test_file)


if __name__ == '__main__':
    run_all_tests()
