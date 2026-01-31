import asyncio
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI
import glob
import json
import os
from pathlib import Path
import sys
import traceback
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi import HTTPException

from analysis_scripts.analysis import Analysis

print(">>> [MINIMAL STARTUP] main.py loaded")
##print(">>> [MINIMAL STARTUP] Working directory:", os.getcwd())
#print(">>> [MINIMAL STARTUP] Python path first 3:", sys.path[:3])
#print(">>> [MINIMAL STARTUP] SUPABASE_SERVICE_ROLE_KEY present:", bool(os.getenv("SUPABASE_SERVICE_ROLE_KEY")))

app = FastAPI()
# os.makedirs("logfiles", exist_ok=True)

# Mount the static folder
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/favicon.ico")
async def favicon():
    file_path = Path("./static/favicon.ico")
    if not file_path.is_file():
        print(f"[FAVICON] File not found at {file_path.resolve()}")
        raise HTTPException(status_code=404, detail="Not Found")
    return FileResponse(file_path)      
        
@app.get("/")
async def root():
    return {"message": "Scraper alive – /scrapego to run, /scrapetime for last run"}

print(">>> [MINIMAL STARTUP] FastAPI app created – about to start Uvicorn")

# Development mode
if __name__ == "__main__":
    print("=== RUNNING IN DEVELOPMENT MODE ===")
    try:
        analysis = Analysis()
        analysis.run_analysis()
        print("=== DEVELOPMENT SCRAPE COMPLETED ===")
    except Exception as e:
        print(f"=== DEVELOPMENT SCRAPE FAILED: {e} ===")