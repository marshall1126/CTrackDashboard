# app.py
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
import json
import os
import glob
from datetime import datetime, timedelta, timezone

import analysis_scripts.constants as constants
from analysis_scripts.database.neon_manager import NeonManager, NeonConnectionMode

app = FastAPI()

# Mount static files (add this once after app = FastAPI())
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    return {"message": "CTrack AI Analysis alive"}

@app.get("/api/analysis_last_updt")
async def api_analysis_last_updt():
    db_manager =  NeonManager(NeonConnectionMode.POOLER)
    db_manager.db_connect()
    ok, records = db_manager.db_select(constants.TableNames.TBL_ANALYSIS_LAST_UPDT, limit=20, order_by='last_updt', order_dir="DESC")
    db_manager.db_close()
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to fetch records")
    # Convert datetime objects to strings for JSON serialization
    serialized = []
    for r in records:
        serialized.append({
            "idx": r["idx"],
            "last_updt": r["last_updt"].isoformat() if r["last_updt"] else None,
            "count": r["count"]
        })
    return JSONResponse(content=serialized)