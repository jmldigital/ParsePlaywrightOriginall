import asyncio
import sqlite3
import time

from fastapi.testclient import TestClient

from price_api.app import create_app
from price_api.models import SearchResult
from price_api.settings import Settings

TOKEN = "test-token-longer-than-24-characters"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


class FakeScraper:
    def __init__(self, delay=0.002):
        self.delay = delay
        self.active = 0
        self.peak = 0
        self.by_batch = {}
        self.batch_peak = {}
        self.seen = []

    async def search(self, brand, oem, days):
        self.active += 1
        self.peak = max(self.active, self.peak)
        self.by_batch[brand] = self.by_batch.get(brand, 0) + 1
        self.batch_peak[brand] = max(
            self.batch_peak.get(brand, 0), self.by_batch[brand]
        )
        self.seen.append((brand, oem, days))
        try:
            await asyncio.sleep(self.delay)
            if oem == "FAIL":
                raise RuntimeError("isolated error")
            if oem == "SLOW":
                await asyncio.sleep(1)
            if oem == "NONE":
                return SearchResult(status="not_found")
            return SearchResult(status="found", price="123.40", delivery_days=1)
        finally:
            self.active -= 1
            self.by_batch[brand] -= 1

    async def close(self):
        pass


def make_client(tmp_path, scraper=None, **options):
    settings = Settings(
        _env_file=None,
        api_token=TOKEN,
        database_path=tmp_path / "jobs.sqlite3",
        **options,
    )
    return TestClient(create_app(settings, scraper or FakeScraper()))


def batch(name="batch", count=1):
    return {
        "batch_id": name,
        "parts": [{"id": str(i), "brand": name, "oem": str(i)} for i in range(count)],
    }


def wait_done(client, job_id):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        response = client.get(f"/jobs/{job_id}", headers=HEADERS)
        assert response.status_code == 200
        if response.json()["status"] == "completed":
            return response.json()
        time.sleep(0.05)
    raise AssertionError("Job did not complete")


def test_auth_validation_and_idempotency(tmp_path):
    with make_client(tmp_path) as client:
        assert client.post("/jobs", json=batch()).status_code == 401
        assert (
            client.get(
                "/jobs/unknown", headers={"Authorization": "Bearer bad"}
            ).status_code
            == 401
        )
        assert client.get("/jobs/unknown/results").status_code == 401
        for request in [
            batch(count=501),
            batch(count=0),
            {**batch(), "max_delivery_days": -1},
            {**batch(), "price": "42"},
            {**batch(), "parts": batch(count=1)["parts"] * 2},
        ]:
            assert (
                client.post("/jobs", json=request, headers=HEADERS).status_code == 422
            )
        a = client.post("/jobs", json=batch(), headers=HEADERS)
        assert a.status_code == 202
        b = client.post("/jobs", json=batch(), headers=HEADERS)
        assert a.json()["job_id"] == b.json()["job_id"]
        assert (
            client.post("/jobs", json=batch(count=2), headers=HEADERS).status_code
            == 409
        )
        assert client.get("/jobs/unknown", headers=HEADERS).status_code == 404
        schema = client.get("/openapi.json").json()
        assert schema["paths"]["/jobs"]["post"]["security"]


def test_isolation_timeout_results_and_order(tmp_path):
    scraper = FakeScraper()
    request = batch(count=4)
    for part, oem in zip(request["parts"], ["OK", "NONE", "FAIL", "SLOW"], strict=True):
        part["oem"] = oem
    with make_client(tmp_path, scraper, item_timeout_seconds=0.05) as client:
        job = client.post("/jobs", json=request, headers=HEADERS).json()["job_id"]
        progress = wait_done(client, job)
        assert (
            progress["found"],
            progress["not_found"],
            progress["errors"],
            progress["processed"],
        ) == (1, 1, 2, 4)
        result = client.get(f"/jobs/{job}/results", headers=HEADERS).json()
        assert result["max_delivery_days"] == 2
        assert result["expires_at"].endswith("Z")
        assert [p["id"] for p in result["parts"]] == ["0", "1", "2", "3"]
        assert [p["status"] for p in result["parts"]] == [
            "found",
            "not_found",
            "error",
            "error",
        ]
        assert result["parts"][0]["price"] == "123.40"
        assert result["parts"][3]["error"]["code"] == "SOURCE_TIMEOUT"
        assert result["parts"][1]["price"] is None
        assert all(p["parsed_at"].endswith("Z") for p in result["parts"])
        assert all(days == 2 for _, _, days in scraper.seen)


def test_three_batches_1500_positions_shared_limits(tmp_path, monkeypatch):
    # Exercise scheduling at scale independently of host fsync latency.
    # File-backed durability, recovery and TTL are covered in separate tests.
    connect = sqlite3.connect
    monkeypatch.setattr(
        "price_api.store.sqlite3.connect",
        lambda path, **kwargs: connect(":memory:", **kwargs),
    )
    scraper = FakeScraper(delay=0.02)
    with make_client(tmp_path, scraper, global_concurrency=4) as client:
        ids = [
            client.post("/jobs", json=batch(name, 500), headers=HEADERS).json()[
                "job_id"
            ]
            for name in ("a", "b", "c")
        ]
        for job in ids:
            assert wait_done(client, job)["processed"] == 500
            assert (
                len(client.get(f"/jobs/{job}/results", headers=HEADERS).json()["parts"])
                == 500
            )
        assert len(scraper.seen) == 1500
        assert len(set(scraper.seen)) == 1500
        assert 2 < scraper.peak <= 4
        assert all(value <= 2 for value in scraper.batch_peak.values())
        assert {x[0] for x in scraper.seen[:100]} == {"a", "b", "c"}


def test_restart_keeps_completed_and_resumes_pending(tmp_path):
    class Slow(FakeScraper):
        async def search(self, *args):
            if int(args[1]) >= 2:
                await asyncio.sleep(60)
            return await super().search(*args)

    request = batch(count=50)
    with make_client(tmp_path, Slow()) as client:
        job = client.post("/jobs", json=request, headers=HEADERS).json()["job_id"]
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            before = client.get(f"/jobs/{job}/results", headers=HEADERS).json()["parts"]
            if len(before) >= 2:
                break
            time.sleep(0.05)
        assert 0 < len(before) < 50
    scraper = FakeScraper()
    with make_client(tmp_path, scraper) as client:
        assert wait_done(client, job)["processed"] == 50
        after = client.get(f"/jobs/{job}/results", headers=HEADERS).json()["parts"]
        by_id = {p["id"]: p for p in after}
        assert all(by_id[p["id"]] == p for p in before)
        assert not ({p["oem"] for p in before} & {oem for _, oem, _ in scraper.seen})


def test_ids_are_preserved_exactly(tmp_path):
    request = batch()
    request["batch_id"] = " batch-id "
    request["parts"][0]["id"] = " position-01 "
    with make_client(tmp_path) as client:
        accepted = client.post("/jobs", json=request, headers=HEADERS).json()
        assert accepted["batch_id"] == " batch-id "
        wait_done(client, accepted["job_id"])
        result = client.get(
            f"/jobs/{accepted['job_id']}/results", headers=HEADERS
        ).json()
        assert result["parts"][0]["id"] == " position-01 "
