import asyncio
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from server.integrations.mongodb import MongoDBClient
from server.workers.parsing_worker import get_parsing_worker

async def main():
    await MongoDBClient.get_database()
    worker = get_parsing_worker()
    await worker.start()

if __name__ == "__main__":
    asyncio.run(main())
