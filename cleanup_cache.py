#!/usr/bin/env python3
"""
Cleanup utility for Versor cache directory.
Deletes Parquet files older than 24 hours.
Run as cron job: 0 */6 * * * /path/to/cleanup_cache.py
"""

import os
import time
from pathlib import Path

CACHE_DIR = "./cache"
MAX_AGE_HOURS = 24

def cleanup_old_files():
    cutoff_time = time.time() - (MAX_AGE_HOURS * 3600)
    deleted_count = 0
    
    if not os.path.exists(CACHE_DIR):
        print(f"Cache directory {CACHE_DIR} does not exist")
        return
    
    for filename in os.listdir(CACHE_DIR):
        if filename.endswith('.parquet'):
            file_path = os.path.join(CACHE_DIR, filename)
            if os.path.getmtime(file_path) < cutoff_time:
                os.remove(file_path)
                deleted_count += 1
                print(f"Deleted: {filename}")
    
    print(f"Cleanup complete. Deleted {deleted_count} files.")

if __name__ == "__main__":
    cleanup_old_files()
