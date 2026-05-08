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
r = redis.Redis(host='localhost', port=6379, db=0)

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

@app.get("/macro/vitals")
def get_macro_vitals():
    """Fetch all latest global macro indicators from Redis"""
    keys = r.keys("indicator:*") 
    vitals = {}
    for key in keys:
        data = r.get(key)
        if data:
            indicator_name = key.decode().split(":")[1]
            vitals[indicator_name] = json.loads(data)
    return vitals

@app.get("/news/recent")
def get_recent_news():
    try:
        news_list = r.lrange("recent_news", 0, 9)
        formatted_news = []
        for n in news_list:
            formatted_news.append(json.loads(n.decode('utf-8')))
            
        return formatted_news
    except Exception as e:
        print(f"❌ Redis Parsing Error: {e}")
        return []

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)