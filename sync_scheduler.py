import asyncio
from services.sync_scheduler import worker_main

if __name__ == "__main__":
    print("=== SYNC WORKER STARTED (NEW VERSION) ===")
    print("Calling worker_main() with asyncio.run()...")
    asyncio.run(worker_main())