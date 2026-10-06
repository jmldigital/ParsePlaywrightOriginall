import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from price_api.models import JobRequest, SearchResult
from price_api.store import QueueFull, Store


def request(name="batch", count=3):
    return JobRequest(
        batch_id=name,
        parts=[dict(id=str(i), brand="Toyota", oem=str(i)) for i in range(count)],
    )


def test_atomic_idempotency_and_claims(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    store.initialize()
    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs = list(pool.map(lambda _: store.create(request()), range(20)))
        assert len({j["job_id"] for j in jobs}) == 1
        claimed = [i for i in pool.map(lambda _: store.claim(), range(20)) if i]
    assert len(claimed) == 2
    assert len({i["ordinal"] for i in claimed}) == 2


def test_recovery_retention_and_queue_limit(tmp_path):
    store = Store(tmp_path / "db.sqlite", max_active_jobs=1)
    store.initialize()
    job = store.create(request(count=2))["job_id"]
    with pytest.raises(QueueFull):
        store.create(request("other"))
    a, b = store.claim(), store.claim()
    store.finish(a, SearchResult(status="not_found"))
    store.initialize()  # Simulate startup after crash while b was running.
    recovered = store.claim()
    assert recovered["ordinal"] == b["ordinal"]
    store.finish(recovered, SearchResult(status="found", price="1.20", delivery_days=0))
    with store.connection() as db:
        row = db.execute("SELECT completed,expires FROM jobs").fetchone()
        assert row["expires"] - row["completed"] == 50 * 3600
        db.execute("UPDATE jobs SET expires=?", (time.time() - 1,))
    assert store.get(job) is None
    store.create(request("active"))
    store.cleanup()
    with store.connection() as db:
        assert (
            db.execute("SELECT count(*) FROM items WHERE job_id=?", (job,)).fetchone()[
                0
            ]
            == 0
        )
        assert db.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1
