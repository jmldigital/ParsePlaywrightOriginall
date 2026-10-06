"""Verify the actual Uvicorn entry point without querying the source."""

import os
import secrets
import subprocess
import sys
import time

import httpx

token = secrets.token_urlsafe(32)
env = dict(os.environ, API_TOKEN=token)
process = subprocess.Popen(
    [
        sys.executable,
        "-m",
        "uvicorn",
        "price_api.app:create_app",
        "--factory",
        "--host",
        "127.0.0.1",
        "--port",
        "8000",
    ],
    env=env,
)
try:
    with httpx.Client(
        base_url="http://127.0.0.1:8000", timeout=5, trust_env=False
    ) as client:
        for _ in range(100):
            try:
                if client.get("/health").status_code == 200:
                    break
            except httpx.TransportError:
                pass
            time.sleep(0.1)
        else:
            raise AssertionError("Server did not become healthy")
        assert client.post("/jobs", json={}).status_code == 401
        headers = {"Authorization": f"Bearer {token}"}
        assert (
            client.post(
                "/jobs", json={"batch_id": "empty", "parts": []}, headers=headers
            ).status_code
            == 422
        )
        assert client.get("/jobs/missing", headers=headers).status_code == 404
        assert client.get("/jobs/missing/results", headers=headers).status_code == 404
        assert client.get("/openapi.json").status_code == 200
    print("Runtime smoke passed: health, bearer auth, validation, routing, OpenAPI")
finally:
    process.terminate()
    process.wait(timeout=20)
