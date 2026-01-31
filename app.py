# app.py
from fastapi import FastAPI, HTTPException
from WebData.webreader import readtheweb
from datetime import datetime, timezone
import json
import os
import glob
from datetime import datetime, timedelta

app = FastAPI()

# Persistent file on Railway's disk (survives deploys & restarts)
LAST_RUN_FILE = "/app/last_run.json"   # Railway mounts /app as persistent

# ────────────────────────────── ADD TO app.py ──────────────────────────────


def cleanup_old_logfiles(keep_days: int = 7):
    """
    Deletes all files in logfiles/ that are older than `keep_days`.
    Runs automatically once per day when Railway cron triggers /scrapego.
    """
    log_dir = "logfiles"
    if not os.path.isdir(log_dir):
        print("[CLEANUP] logfiles directory not found – nothing to clean")
        return

    cutoff = datetime.now() - timedelta(days=keep_days)
    pattern = os.path.join(log_dir, "*.log")
    deleted_count = 0

    for filepath in glob.glob(pattern):
        try:
            # Get file modification time
            mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
            if mtime < cutoff:
                os.remove(filepath)
                # Also show in Railway logs
                print(f"[CLEANUP] Deleted old log ({keep_days}+ days): {os.path.basename(filepath)}")
                deleted_count += 1
        except Exception as e:
            print(f"[CLEANUP] Error deleting {filepath}: {e}")

    if deleted_count == 0:
        print(f"[CLEANUP] No log files older than {keep_days} days found")
    else:
        print(f"[CLEANUP] Successfully deleted {deleted_count} old log file(s)")
# ─────────────────────────────────────────────────────────────────────────────


def load_last_run() -> str | None:
    if os.path.exists(LAST_RUN_FILE):
        with open(LAST_RUN_FILE, "r") as f:
            data = json.load(f)
            return data.get("last_run")
    return None


def save_last_run():
    datetime.utcnow().isoformat() + "Z"   →   datetime.now(timezone.utc).isoformat()
    with open(LAST_RUN_FILE, "w") as f:
        json.dump(data, f)


@app.get("/")
async def root():
    return {"message": "Scraper alive – /scrapego to run, /scrapetime for last run"}


@app.get("/scrapego")
async def scrapego():
    print(f"[{datetime.utcnow().isoformat()}] Scrape triggered")
    try:
        result = await readtheweb()
        save_last_run()   # ← this is the only new line you need
        cleanup_old_logfiles()
        return {
            "status": "success",
            "result": result,
            "triggered_at": datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        print(f"Scrape failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scrapetime")
async def scrapetime():
    last = load_last_run()
    if last:
        return {
            "last_successful_run_utc": last,
            "message": "This is when /scrapego last finished successfully"
        }
    else:
        return {
            "last_successful_run_utc": None,
            "message": "No successful run recorded yet"
        }