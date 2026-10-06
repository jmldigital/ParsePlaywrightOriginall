import asyncio
import logging

from .models import ItemError, SearchResult

log = logging.getLogger(__name__)


async def database_call(function, *args):
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # Finish an in-flight SQLite operation before releasing the process lock.
        await task
        raise


class Runner:
    def __init__(self, store, scraper, settings):
        self.store, self.scraper, self.settings = store, scraper, settings
        self.tasks = []

    async def start(self):
        self.tasks = [
            asyncio.create_task(self.worker(), name=f"worker-{i}")
            for i in range(self.settings.global_concurrency)
        ]
        self.tasks.append(asyncio.create_task(self.cleaner(), name="cleanup"))

    async def stop(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.scraper.close()

    def healthy(self):
        return bool(self.tasks) and all(not t.done() for t in self.tasks)

    async def worker(self):
        while True:
            item = await database_call(self.store.claim)
            if item is None:
                await asyncio.sleep(0.1)
                continue
            try:
                async with asyncio.timeout(self.settings.item_timeout_seconds):
                    result = await self.scraper.search(
                        item["brand"], item["oem"], item["max_days"]
                    )
            except TimeoutError:
                result = SearchResult(
                    status="error",
                    error=ItemError(
                        code="SOURCE_TIMEOUT", message="Source processing timed out"
                    ),
                )
            except Exception:
                log.exception(
                    "Position failed: job=%s ordinal=%s",
                    item["job_id"],
                    item["ordinal"],
                )
                result = SearchResult(
                    status="error",
                    error=ItemError(
                        code="SOURCE_ERROR", message="Source processing failed"
                    ),
                )
            # Database failures must stop this worker, not silently lose a result.
            # Health becomes unhealthy; restart recovers the running item.
            await database_call(self.store.finish, item, result)

    async def cleaner(self):
        while True:
            await database_call(self.store.cleanup)
            await asyncio.sleep(self.settings.cleanup_interval_seconds)
