from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Optional
import time

class InputValidationMiddleware(BaseHTTPMiddleware):
    """Middleware for input validation and file size limits"""
    
    MAX_FILE_SIZE = 1024 * 1024 * 1024  # 1GB
    MAX_JSON_SIZE = 10 * 1024 * 1024   # 10MB
    ALLOWED_FILE_TYPES = {
        'text/csv',
        'application/vnd.ms-excel',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/vnd.oasis.opendocument.spreadsheet'
    }
    
    async def dispatch(self, request: Request, call_next):
        # Validate content length for file uploads
        if request.url.path.startswith('/nodes/io/upload'):
            content_length = request.headers.get('content-length')
            if content_length:
                content_length = int(content_length)
                if content_length > self.MAX_FILE_SIZE:
                    raise HTTPException(
                        status_code=413,
                        detail=f"File too large. Maximum size is {self.MAX_FILE_SIZE / 1024 / 1024}MB"
                    )
        
        # Validate JSON body size
        elif request.method in ['POST', 'PUT', 'PATCH']:
            content_length = request.headers.get('content-length')
            if content_length:
                content_length = int(content_length)
                if content_length > self.MAX_JSON_SIZE:
                    raise HTTPException(
                        status_code=413,
                        detail=f"Request body too large. Maximum size is {self.MAX_JSON_SIZE / 1024 / 1024}MB"
                    )
        
        response = await call_next(request)
        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Simple rate limiting middleware"""
    
    def __init__(self, app, requests_per_minute: int = 100):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.request_counts = {}  # {ip: [(timestamp, count)]}
        self.window_size = 60  # 1 minute in seconds
    
    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host
        current_time = time.time()
        
        # Clean old entries
        if client_ip in self.request_counts:
            self.request_counts[client_ip] = [
                (ts, count) for ts, count in self.request_counts[client_ip]
                if current_time - ts < self.window_size
            ]
        
        # Count requests in current window
        if client_ip not in self.request_counts:
            self.request_counts[client_ip] = []
        
        request_count = sum(count for _, count in self.request_counts[client_ip])
        
        if request_count >= self.requests_per_minute:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Maximum {self.requests_per_minute} requests per minute."
            )
        
        # Add current request
        self.request_counts[client_ip].append((current_time, 1))
        
        response = await call_next(request)
        return response
