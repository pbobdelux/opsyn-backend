import asyncio
from services.sync_scheduler import main

if __name__ == "__main__":
    print("=== SYNC WORKER STARTED (NEW VERSION) ===")
    print("Calling main() with asyncio.run()...")
    main()