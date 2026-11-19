"""规模与参数上限测试：大批次、海量历史冲突、增量重判、changed_limit、原子性。"""

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app import core, db
from app.main import app

client = TestClient(app, raise_server_exceptions=False)

# 故意把单条 SQL 的绑定参数上限压得很低，证明实现不依赖大参数列表
LOW_LIMIT = 50


@pytest.fixture(autouse=True)
def clean_tables():
    conn = db.get_conn()
    conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, LOW_LIMIT)
    with db.lock():
        conn.execute("DELETE FROM observations")
        conn.execute("DELETE FROM calibration_points")
    yield
    conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 250000)


def post_obs(items, limit=None):
    params = {} if limit is None else {"changed_limit": limit}
    r = client.post("/observations", json={"observations": items}, params=params)
    assert r.status_code == 200, r.text
    return r.json()


def post_cal(items, limit=None):
    params = {} if limit is None else {"changed_limit": limit}
    r = client.post("/calibrations", json={"calibrations": items}, params=params)
    assert r.status_code == 200, r.text
    return r.json()


def get_obs(obs_id):
    return client.get(f"/observations/{obs_id}").json()


# ---------------------------------------------------------------- 大批次

def test_large_observation_batch_beyond_sql_limit():
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 0},
        {"boot_segment": "A", "counter": 1000, "utc_ms": 1000},
    ])
    items = [
        {"obs_id": f"o{i:04d}", "boot_segment": "A", "counter": i, "tide_level": i / 10}
        for i in range(400)  # 远超 50 个绑定参数
    ]
    resp = post_obs(items)
    assert resp["changed_count"] == 400
    assert len(resp["changed"]) == 400
    assert get_obs("o0399")["utc_ms"] == 399
    assert get_obs("o0399")["status"] == "ok"


def test_large_calibration_batch_beyond_sql_limit():
    items = [
        {"boot_segment": "A", "counter": i, "utc_ms": i * 10}
        for i in range(400)
    ]
    assert post_cal(items)["changed_count"] == 0  # 尚无观测
    resp = post_obs([
        {"obs_id": "mid", "boot_segment": "A", "counter": 200, "tide_level": 1.0},
    ])
    assert resp["changed_count"] == 1
    assert get_obs("mid")["utc_ms"] == 2000


def test_many_historical_conflicts_then_one_more_does_not_500():
    # 300 个 UTC 毫秒，每个毫秒被两个不同开机段占用 -> 600 条历史冲突
    cals = []
    obs = []
    for i in range(300):
        ms = i * 10
        for seg in ("A", "B"):
            cals.append({"boot_segment": seg, "counter": i, "utc_ms": ms})
            obs.append({
                "obs_id": f"{seg}{i:04d}", "boot_segment": seg,
                "counter": i, "tide_level": 1.0,
            })
    post_cal(cals)
    post_obs(obs)

    # 再多制造一条冲突观测；单条 SQL 若拼接历史毫秒会超限
    post_cal([{"boot_segment": "C", "counter": 0, "utc_ms": 0}])
    resp = post_obs([
        {"obs_id": "C0000", "boot_segment": "C", "counter": 0, "tide_level": 1.0},
    ])
    # A0/B0 本已是 conflict，状态未变；只有新观测 C0000 状态变化
    assert resp["changed_count"] == 1
    assert {c["obs_id"] for c in resp["changed"]} == {"C0000"}
    for oid in ("A0000", "B0000", "C0000"):
        body = get_obs(oid)
        assert body["status"] == "conflict"
        assert "冲突" in body["reason"]
    # 冲突原因按整批结果更新，包含全部第三方开机段
    assert "C" in get_obs("A0000")["reason"]
    assert "A" in get_obs("C0000")["reason"] and "B" in get_obs("C0000")["reason"]


def test_calibration_recalc_beyond_sql_limit_with_conflicts():
    # 大段观测 + 大量跨段冲突，校时点更正触发重算
    cals = [{"boot_segment": "A", "counter": i, "utc_ms": i * 10} for i in range(300)]
    cals += [{"boot_segment": "B", "counter": i, "utc_ms": i * 10} for i in range(300)]
    post_cal(cals)
    obs = [
        {"obs_id": f"A{i:04d}", "boot_segment": "A", "counter": i, "tide_level": 1.0}
        for i in range(300)
    ] + [
        {"obs_id": f"B{i:04d}", "boot_segment": "B", "counter": i, "tide_level": 1.0}
        for i in range(300)
    ]
    post_obs(obs)
    # B 段整体平移，解除全部冲突。一次更正影响 300 个毫秒/600 条观测
    resp = post_cal(
        [{"boot_segment": "B", "counter": i, "utc_ms": i * 10 + 100000}
         for i in range(300)]
    )
    assert resp["changed_count"] == 600
    assert get_obs("A0000")["status"] == "ok"
    assert get_obs("B0000")["utc_ms"] == 100000


# ---------------------------------------------------------------- 增量重判

def test_moved_observation_updates_remaining_conflict_reason():
    # 1000 毫秒由 A/B/C 三段共享
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "A", "counter": 1, "utc_ms": 2000},
        {"boot_segment": "B", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "C", "counter": 0, "utc_ms": 1000},
    ])
    post_obs([
        {"obs_id": "a1", "boot_segment": "A", "counter": 0, "tide_level": 1.0},
        {"obs_id": "a2", "boot_segment": "A", "counter": 1, "tide_level": 2.0},
        {"obs_id": "b1", "boot_segment": "B", "counter": 0, "tide_level": 3.0},
        {"obs_id": "c1", "boot_segment": "C", "counter": 0, "tide_level": 4.0},
    ])
    assert get_obs("b1")["status"] == "conflict"
    assert "A" in get_obs("b1")["reason"]

    # 把 a1 移到新的 UTC 毫秒 2000：原 1000 仍由 B、C 冲突
    resp = post_obs([
        {"obs_id": "a1", "boot_segment": "A", "counter": 1, "tide_level": 1.0},
    ])
    assert resp["changed_count"] == 1  # 只有 a1 的 UTC 变了
    assert "a1" in {c["obs_id"] for c in resp["changed"]}

    # 剩余冲突方状态不变，但原因按整批结果更新（不再提到 A）
    b1 = get_obs("b1")
    c1 = get_obs("c1")
    assert b1["status"] == "conflict" and c1["status"] == "conflict"
    assert "A" not in b1["reason"] and "C" in b1["reason"]
    assert "A" not in c1["reason"] and "B" in c1["reason"]
    # 新时刻 2000 与同段 a2 共毫秒，不算冲突
    assert get_obs("a1")["status"] == "ok"
    assert get_obs("a1")["utc_ms"] == 2000
    # a2 状态未变，不在变更清单
    assert "a2" not in {c["obs_id"] for c in resp["changed"]}


def test_moved_observation_does_not_touch_unrelated_conflicts():
    # 无关历史冲突：X/Y 段共享 9000 毫秒
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "A", "counter": 1, "utc_ms": 2000},
        {"boot_segment": "X", "counter": 0, "utc_ms": 9000},
        {"boot_segment": "Y", "counter": 0, "utc_ms": 9000},
    ])
    post_obs([
        {"obs_id": "a1", "boot_segment": "A", "counter": 0, "tide_level": 1.0},
        {"obs_id": "x1", "boot_segment": "X", "counter": 0, "tide_level": 2.0},
        {"obs_id": "y1", "boot_segment": "Y", "counter": 0, "tide_level": 3.0},
    ])
    t = TestClient(app)
    with db.lock():
        # 记录 UPDATE 语句，确认没有扫描/改写无关冲突
        seen = []
        conn = db.get_conn()
        conn.set_trace_callback(seen.append)
    try:
        resp = t.post("/observations", json={"observations": [
            {"obs_id": "a1", "boot_segment": "A", "counter": 1, "tide_level": 1.0},
        ]})
    finally:
        with db.lock():
            db.get_conn().set_trace_callback(None)
    assert resp.status_code == 200
    updates = [s for s in seen if s.lstrip().upper().startswith("UPDATE")]
    # 只允许更新 a1 本身；x1/y1 的冲突不应被重写
    assert all("x1" not in s and "y1" not in s for s in updates)
    assert get_obs("x1")["status"] == "conflict"
    assert get_obs("y1")["status"] == "conflict"


# ---------------------------------------------------------------- changed_limit

def test_changed_limit_omitted_returns_all():
    post_cal([{"boot_segment": "A", "counter": 0, "utc_ms": 0},
              {"boot_segment": "A", "counter": 10, "utc_ms": 10}])
    items = [
        {"obs_id": f"o{i}", "boot_segment": "A", "counter": i, "tide_level": 1.0}
        for i in range(8)
    ]
    resp = post_obs(items)
    assert resp["changed_count"] == 8
    assert [c["obs_id"] for c in resp["changed"]] == [f"o{i}" for i in range(8)]


def test_changed_limit_truncates_in_id_order_but_count_is_total():
    post_cal([{"boot_segment": "A", "counter": 0, "utc_ms": 0},
              {"boot_segment": "A", "counter": 10, "utc_ms": 10}])
    items = [
        {"obs_id": f"o{i}", "boot_segment": "A", "counter": i, "tide_level": 1.0}
        for i in range(8)
    ]
    resp = post_obs(items, limit=3)
    assert resp["changed_count"] == 8
    assert [c["obs_id"] for c in resp["changed"]] == ["o0", "o1", "o2"]

    # 校时点接口同样支持 changed_limit；10ms 的校时点改动了，o0 恰好命中
    # 计数 0 的校时点（UTC 不变），其余 7 条观测的 UTC 变化
    resp = post_cal([{"boot_segment": "A", "counter": 10, "utc_ms": 20}], limit=2)
    assert resp["changed_count"] == 7
    assert [c["obs_id"] for c in resp["changed"]] == ["o1", "o2"]


def test_changed_limit_zero_returns_only_total():
    post_cal([{"boot_segment": "A", "counter": 0, "utc_ms": 0}])
    resp = post_obs([
        {"obs_id": "z1", "boot_segment": "A", "counter": 0, "tide_level": 1.0},
        {"obs_id": "z2", "boot_segment": "A", "counter": 0, "tide_level": 2.0},
    ], limit=0)
    assert resp == {"changed_count": 2, "changed": []}


# ---------------------------------------------------------------- 原子性

def test_write_failure_keeps_last_committed_state(monkeypatch):
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "A", "counter": 10, "utc_ms": 2000},
    ])
    post_obs([{"obs_id": "keep", "boot_segment": "A", "counter": 5, "tide_level": 1.5}])
    before = get_obs("keep")
    assert before["utc_ms"] == 1500 and before["status"] == "ok"

    def boom(*args, **kwargs):
        raise RuntimeError("simulated conflict refresh failure")

    monkeypatch.setattr(core, "resolve_conflicts_for", boom)
    r = client.post("/observations", json={"observations": [
        {"obs_id": "keep", "boot_segment": "A", "counter": 10, "tide_level": 9.9},
    ]})
    assert r.status_code == 500

    # 原文、校时点、供数都保留在上次提交状态
    after = get_obs("keep")
    assert (after["counter"], after["tide_level"]) == (5, 1.5)
    assert after["utc_ms"] == 1500 and after["status"] == "ok"
    assert client.get("/tide-levels", params={"start_ms": 0, "end_ms": 99999}).json()["count"] == 1


def test_recalc_failure_rolls_back_calibration_batch(monkeypatch):
    post_cal([{"boot_segment": "A", "counter": 0, "utc_ms": 1000},
              {"boot_segment": "A", "counter": 10, "utc_ms": 2000}])
    post_obs([{"obs_id": "keep", "boot_segment": "A", "counter": 5, "tide_level": 1.5}])

    def boom(*args, **kwargs):
        raise RuntimeError("simulated recalc failure")

    monkeypatch.setattr(core, "recalc_segment", boom)
    r = client.post("/calibrations", json={"calibrations": [
        {"boot_segment": "A", "counter": 10, "utc_ms": 9000},
    ]})
    assert r.status_code == 500

    # 校时点未落库，观测仍按旧校时换算
    assert get_obs("keep")["utc_ms"] == 1500
    post_obs([{"obs_id": "probe", "boot_segment": "A", "counter": 10, "tide_level": 2.0}])
    assert get_obs("probe")["utc_ms"] == 2000
