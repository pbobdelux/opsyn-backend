import asyncio
import logging
from fastapi import FastAPI

# Import the existing sync scheduler
from services.sync_scheduler import run_scheduler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global task reference
scheduler_task = None

# Create FastAPI app
app = FastAPI(title="Opsyn Sync Worker")

@app.on_event("startup")
async def startup_event():
    """
    Start the sync scheduler as a background task on app startup.
    """
    global scheduler_task
    logger.info("[worker_web] Starting sync scheduler in background...")
    scheduler_task = asyncio.create_task(run_scheduler())

@app.on_event("shutdown")
async def shutdown_event():
    """
    Cancel the scheduler task on app shutdown.
    """
    global scheduler_task
    logger.info("[worker_web] Shutting down sync scheduler...")
    if scheduler_task and not scheduler_task.done():
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            logger.info("[worker_web] Sync scheduler cancelled")

@app.get("/health")
async def health():
    """
    Health check endpoint for Railway.
    """
    return {"ok": True, "service": "opsyn-sync-worker"}

@app.get("/")
async def root():
    """
    Root endpoint.
    """
    return {"message": "Opsyn Sync Worker is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
