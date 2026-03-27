from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from auth_middleware import get_current_user, require_permission

app = FastAPI(title="Versor Backend Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Example protected endpoint
@app.post("/nodes/io/upload_csv")
async def upload_csv(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    # Check if user has write permission
    if "write" not in current_user.get("permissions", []):
        raise HTTPException(status_code=403, detail="Write permission required")
    
    # Your existing upload logic here
    # ...
    return {"node_id": "source_abc123", "metadata": {...}}

# Admin only endpoint
@app.get("/admin/users")
async def get_all_users(current_user: dict = Depends(get_current_user)):
    if "admin" not in current_user.get("permissions", []):
        raise HTTPException(status_code=403, detail="Admin permission required")
    
    # Admin functionality
    return {"users": []}

# Public endpoint (no auth required)
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)