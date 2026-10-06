import asyncio
import secrets
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from filelock import FileLock

from .models import Accepted, JobRequest, Progress, Results
from .settings import Settings
from .store import Conflict, QueueFull, Store
from .worker import Runner


def create_app(settings=None, scraper=None):
    settings = settings or Settings()
    store = Store(
        settings.database_path, settings.retention_hours, settings.max_active_jobs
    )
    bearer = HTTPBearer(auto_error=False)

    async def authorize(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ):
        if credentials is None or not secrets.compare_digest(
            credentials.credentials.encode(),
            settings.api_token.get_secret_value().encode(),
        ):
            raise HTTPException(
                401, "Invalid bearer token", headers={"WWW-Authenticate": "Bearer"}
            )

    @asynccontextmanager
    async def lifespan(app):
        settings.database_path.parent.mkdir(parents=True, exist_ok=True)
        lock = FileLock(str(settings.database_path) + ".lock", timeout=0)
        lock.acquire()  # Prevent duplicate workers / recovery from another process.
        runner = None
        try:
            await asyncio.to_thread(store.initialize)
            provider = scraper
            if provider is None:
                from .stparts import StpartsScraper

                provider = StpartsScraper(settings)
            runner = Runner(store, provider, settings)
            app.state.runner = runner
            await runner.start()
            yield
        finally:
            try:
                if runner:
                    await runner.stop()
            finally:
                try:
                    await asyncio.to_thread(store.close)
                finally:
                    lock.release()

    app = FastAPI(title="Stparts Price API", version="0.1.0", lifespan=lifespan)

    @app.get("/health", include_in_schema=False)
    async def health(response: Response):
        healthy = app.state.runner.healthy()
        response.status_code = 200 if healthy else 503
        return {"status": "ok" if healthy else "unhealthy"}

    @app.post(
        "/jobs",
        status_code=202,
        response_model=Accepted,
        dependencies=[Depends(authorize)],
    )
    async def submit(request: JobRequest):
        if not app.state.runner.healthy():
            raise HTTPException(503, "Workers unavailable")
        try:
            return await asyncio.to_thread(store.create, request)
        except Conflict:
            raise HTTPException(
                409, "batch_id already exists with different content"
            ) from None
        except QueueFull:
            raise HTTPException(
                429, "Job queue is full", headers={"Retry-After": "60"}
            ) from None

    @app.get(
        "/jobs/{job_id}", response_model=Progress, dependencies=[Depends(authorize)]
    )
    async def progress(job_id: str):
        job = await asyncio.to_thread(store.get, job_id)
        if job is None:
            raise HTTPException(404, "Job not found or expired")
        return job

    @app.get(
        "/jobs/{job_id}/results",
        response_model=Results,
        dependencies=[Depends(authorize)],
    )
    async def results(job_id: str):
        job = await asyncio.to_thread(store.get, job_id, True)
        if job is None:
            raise HTTPException(404, "Job not found or expired")
        return job

    return app
