import uvicorn
from api.NodesApiComplete import app

if __name__ == "__main__":
    print("Attempting to start Versor Backend...")
    uvicorn.run("api.NodesApiComplete:app", host="0.0.0.0", port=8000, reload=True, timeout_keep_alive=60)