#!/usr/bin/env python3
"""
Simple Message Queue Integration Test
Run this to verify message queue is working correctly
"""

import sys
import time
import tempfile
import os

# Test CSV data
TEST_CSV = """name,age,salary,department
Alice,25,50000,Engineering
Bob,30,60000,Sales
Charlie,35,70000,Engineering
David,40,80000,Sales
Eve,45,90000,Engineering"""

def test_celery_connection():
    """Test 1: Verify Celery can connect to Redis"""
    print("\n[Test 1] Testing Celery connection to Redis...")
    try:
        from celery_config import celery_app
        
        # Ping workers
        inspect = celery_app.control.inspect()
        active = inspect.active()
        
        if active:
            print(f"✓ Connected to Celery workers: {list(active.keys())}")
            return True
        else:
            print("✗ No active Celery workers found")
            print("  Start workers with: celery -A tasks worker --loglevel=info")
            return False
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False

def test_upload_csv():
    """Test 2: Upload CSV file"""
    print("\n[Test 2] Testing CSV upload task...")
    try:
        from tasks import upload_csv_task
        
        # Create temp file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(TEST_CSV)
            temp_path = f.name
        
        # Submit task
        print(f"  Submitting upload task...")
        task = upload_csv_task.delay(temp_path, 'test.csv')
        print(f"  Task ID: {task.id}")
        
        # Wait for result
        print(f"  Waiting for completion...")
        result = task.get(timeout=30)
        
        # Verify
        if result['status'] == 'completed':
            print(f"✓ Upload successful!")
            print(f"  Node ID: {result['node_id']}")
            print(f"  Rows: {result['metadata']['rows']}")
            print(f"  Columns: {result['metadata']['columns']}")
            return result['node_id']
        else:
            print(f"✗ Upload failed: {result.get('error')}")
            return None
            
    except Exception as e:
        print(f"✗ Test failed: {e}")
        return None
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

def test_filter_operation(node_id):
    """Test 3: Filter operation"""
    print("\n[Test 3] Testing filter operation...")
    try:
        from tasks import filter_node_task
        
        # Submit filter task
        print(f"  Filtering age > 30...")
        task = filter_node_task.delay(node_id, 'age', '>', 30)
        print(f"  Task ID: {task.id}")
        
        # Wait for result
        result = task.get(timeout=30)
        
        if result['status'] == 'completed':
            print(f"✓ Filter successful!")
            print(f"  Node ID: {result['node_id']}")
            print(f"  Filtered rows: {result['metadata']['rows']}")
            return result['node_id']
        else:
            print(f"✗ Filter failed: {result.get('error')}")
            return None
            
    except Exception as e:
        print(f"✗ Test failed: {e}")
        return None

def test_groupby_operation(node_id):
    """Test 4: GroupBy operation"""
    print("\n[Test 4] Testing groupby operation...")
    try:
        from tasks import groupby_node_task
        
        # Submit groupby task
        print(f"  Grouping by department...")
        task = groupby_node_task.delay(
            node_id,
            ['department'],
            {'salary': ['mean', 'count']}
        )
        print(f"  Task ID: {task.id}")
        
        # Wait for result
        result = task.get(timeout=30)
        
        if result['status'] == 'completed':
            print(f"✓ GroupBy successful!")
            print(f"  Node ID: {result['node_id']}")
            print(f"  Groups: {result['metadata']['rows']}")
            return result['node_id']
        else:
            print(f"✗ GroupBy failed: {result.get('error')}")
            return None
            
    except Exception as e:
        print(f"✗ Test failed: {e}")
        return None

def test_task_status():
    """Test 5: Task status tracking"""
    print("\n[Test 5] Testing task status tracking...")
    try:
        from tasks import upload_csv_task
        from celery.result import AsyncResult
        
        # Create temp file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(TEST_CSV)
            temp_path = f.name
        
        # Submit task
        task = upload_csv_task.delay(temp_path, 'test.csv')
        
        # Track status
        states = []
        while not task.ready():
            state = task.state
            if state not in states:
                states.append(state)
                print(f"  Status: {state}")
            time.sleep(0.1)
        
        print(f"  Final status: {task.state}")
        print(f"✓ Status tracking working! States: {' → '.join(states + [task.state])}")
        
        os.remove(temp_path)
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False

def main():
    """Run all integration tests"""
    print("="*60)
    print("MESSAGE QUEUE INTEGRATION TEST")
    print("="*60)
    
    # Test 1: Connection
    if not test_celery_connection():
        print("\n❌ FAILED: Cannot connect to Celery workers")
        print("\nMake sure:")
        print("  1. Redis is running: redis-server")
        print("  2. Celery worker is running: celery -A tasks worker --loglevel=info")
        sys.exit(1)
    
    # Test 2: Upload
    node_id = test_upload_csv()
    if not node_id:
        print("\n❌ FAILED: CSV upload test failed")
        sys.exit(1)
    
    # Test 3: Filter
    filtered_node = test_filter_operation(node_id)
    if not filtered_node:
        print("\n❌ FAILED: Filter test failed")
        sys.exit(1)
    
    # Test 4: GroupBy
    grouped_node = test_groupby_operation(node_id)
    if not grouped_node:
        print("\n❌ FAILED: GroupBy test failed")
        sys.exit(1)
    
    # Test 5: Status tracking
    if not test_task_status():
        print("\n❌ FAILED: Status tracking test failed")
        sys.exit(1)
    
    # All tests passed
    print("\n" + "="*60)
    print("✅ ALL TESTS PASSED!")
    print("="*60)
    print("\nMessage queue integration is working correctly!")
    print("\nYou can now:")
    print("  1. Use async API endpoints: /async/*")
    print("  2. Monitor tasks at: http://localhost:5555 (Flower)")
    print("  3. Scale workers: celery -A tasks worker --concurrency=8")
    print()

if __name__ == '__main__':
    main()
