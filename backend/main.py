from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import redis
import json

app = FastAPI(title="TickStream Alpha API")

# for cross origin sharing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # front-end URLs
    allow_methods=["*"],
    allow_headers=["*"],
)

# cache connection
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

@app.get("/")
def read_root():
    return {"status": "online", "message": "TickStream API is pulling from Redis"}

@app.get("/snapshot/{ticker}")
def get_ticker_snapshot(ticker: str):
    """Fetch the latest signal and features for a specific ticker"""
    key = f"live:{ticker.upper()}"
    data = r.get(key)
    
    if not data:
        raise HTTPException(status_code=404, detail=f"Ticker {ticker} not found in speed layer")
    
    return json.loads(data)

@app.get("/all-signals")
def get_all_signals():
    """Scan Redis for all live ticker signals"""
    keys = r.keys("live:*")
    results = []
    for key in keys:
        results.append(json.loads(r.get(key)))
    return results

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)