import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

# Import the existing sync scheduler
from services.sync_scheduler import run_scheduler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global task reference
scheduler_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage app lifecycle: start scheduler on startup, cancel on shutdown.
    """
    global scheduler_task

    logger.info("[worker_web] Starting sync scheduler in background...")
    # Start the scheduler as a background task
    scheduler_task = asyncio.create_task(run_scheduler())

    yield

    logger.info("[worker_web] Shutting down sync scheduler...")
    # Cancel the scheduler task on shutdown
    if scheduler_task and not scheduler_task.done():
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            logger.info("[worker_web] Sync scheduler cancelled")

# Create FastAPI app with lifespan context manager
app = FastAPI(title="Opsyn Sync Worker", lifespan=lifespan)

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
