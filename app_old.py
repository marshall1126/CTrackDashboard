from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from aiojobs import create_scheduler
from WebData.webreadercron import WebReaderScheduler
import asyncio
import threading
import schedule

app = FastAPI()

scheduler_instance = WebReaderScheduler()

async def run_scheduler():
    """Run the schedule loop in a background thread."""
    while scheduler_instance.running:
        schedule.run_pending()
        await asyncio.sleep(60)  # Check every minute

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize the scheduler
    scheduler = await create_scheduler()
    scheduler_instance.running = True
    loop = asyncio.get_event_loop()
    threading.Thread(target=lambda: loop.run_until_complete(run_scheduler()), daemon=True).start()
    yield
    # Shutdown: Stop the scheduler
    await scheduler_instance.stopreader()

app = FastAPI(lifespan=lifespan)

@app.get("/scrapego")
async def trigger_scrapego():
    try:
        print("trigger_scrapego")
        result = await scheduler_instance.startreader()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

#if __name__ == "__main__":
#    import uvicorn
#    uvicorn.run(app, host="0.0.0.0", port=5000)