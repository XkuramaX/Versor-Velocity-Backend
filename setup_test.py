import polars as pl
import redis

try:
    # Test Polars
    df = pl.DataFrame({"status": ["Versor Engine Active"]})
    print(df)
    
    # Test Redis (Ensure your Redis server is running!)
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.set('system_check', 'connected')
    print(f"Redis Status: {r.get('system_check').decode('utf-8')}")
    
except Exception as e:
    print(f"Setup Error: {e}")