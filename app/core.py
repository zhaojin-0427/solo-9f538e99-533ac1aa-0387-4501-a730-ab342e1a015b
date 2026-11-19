"""校时换算、批量写入、重算与冲突检测的核心逻辑。

状态机（observations.status）：
  ok                   已校时且无冲突，供分析
  pending_calibration  计数落在本段校时区间外，保留原文、待校时
  conflict             不同开机段落在同一 UTC 毫秒，冲突观测均不供分析

所有函数都假定运行在 db.transaction 包裹的同一事务里，
因此“原文 + 校时依据 + 分析供数”同次生效。

参数上限
--------
SQLite 对单条语句可绑定的参数个数有上限（不同的编译/运行配置从 999
到十几万不等）。本模块绝不把「批次大小」或「受影响观测数」直接拼进
一条 SQL：读取、快照都按 ``_param_chunk`` 分块；冲突判定按受影响的
UTC 毫秒分块查询。因此即使历史冲突数量接近上限，再多产生一条冲突
也只会多处理一个毫秒，而不会拼出超限语句。

冲突判定是增量的：一次提交只重判本次写入/重算触及的 UTC 毫秒
（观测的原时刻与新时刻、校时点变化导致 UTC 改变的观测时刻），
不扫描也不改写无关的历史冲突。
"""

from __future__ import annotations

import bisect
import sqlite3
from typing import Iterable, Sequence

OK = "ok"
PENDING = "pending_calibration"
CONFLICT = "conflict"

# 分块大小的保守上限。实际大小还会参考当前连接允许的绑定参数数，
# 这样即便运行环境把 SQLITE_LIMIT_VARIABLE_NUMBER 调得很低也能工作。
PARAM_CHUNK = 500


def _chunks(values: Sequence, size: int) -> Iterable[Sequence]:
    """把序列切成不超过 size 的块，保证单条 SQL 不超绑定参数上限。"""
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _param_chunk(conn: sqlite3.Connection) -> int:
    """当前连接单条 IN 语句可安全绑定的参数个数（至少 1）。"""
    limit = 999  # 老版本 SQLite / 老 Python 的默认值
    getlimit = getattr(conn, "getlimit", None)
    if getlimit is not None:
        try:
            limit = conn.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)
        except (sqlite3.Error, AttributeError):
            pass
    # 留两个参数余量，避免与语句中的其他占位符叠加后超限
    return max(1, min(PARAM_CHUNK, limit - 2))


# ---------------------------------------------------------------- 校时换算

def load_points(conn: sqlite3.Connection, segment: str) -> list[tuple[int, int]]:
    """某开机段的全部校时点，按计数升序：[(counter, utc_ms), ...]"""
    rows = conn.execute(
        "SELECT counter, utc_ms FROM calibration_points "
        "WHERE boot_segment = ? ORDER BY counter",
        (segment,),
    ).fetchall()
    return [(r["counter"], r["utc_ms"]) for r in rows]


def compute_utc(
    points: list[tuple[int, int]], counter: int
) -> tuple[int, tuple[int, int], tuple[int, int]] | None:
    """相邻校时点间线性换算并向下取整。

    返回 (utc_ms, 下界点, 上界点)；计数落在校时区间外返回 None（待校时）。
    恰好命中某个校时点时直接取该点 UTC（上下界相同）。
    """
    if not points:
        return None
    counters = [p[0] for p in points]
    i = bisect.bisect_left(counters, counter)
    if i < len(points) and counters[i] == counter:
        p = points[i]
        return p[1], p, p
    if i == 0 or i == len(points):
        return None
    (c1, t1), (c2, t2) = points[i - 1], points[i]
    # 段内 UTC 单调不倒退已由写入方校验，故各项均非负，// 即向下取整
    utc_ms = t1 + (counter - c1) * (t2 - t1) // (c2 - c1)
    return utc_ms, (c1, t1), (c2, t2)


def _pending_reason(segment: str, counter: int, points: list[tuple[int, int]]) -> str:
    if not points:
        return f"开机段 '{segment}' 尚无校时点，待校时"
    return (
        f"计数 {counter} 超出开机段 '{segment}' 的校时区间 "
        f"[{points[0][0]}, {points[-1][0]}]，待校时"
    )


# ---------------------------------------------------------------- 重算与冲突

def recalc_segment(
    conn: sqlite3.Connection, segment: str
) -> tuple[set[str], set[int]]:
    """按当前校时点重算某开机段全部观测。

    只改写真有变化的行：UTC、状态、原因、校时依据都与原值一致时跳过。
    原本是 ``conflict`` 且 UTC 未变的行会保留冲突状态，交由调用方的
    增量冲突判定按整批结果处理，避免误把未受影响的历史冲突恢复为 ok。

    返回 (时刻或状态变化的 obs_id 集合, 需要重判冲突的 UTC 毫秒集合)。
    """
    points = load_points(conn, segment)
    rows = conn.execute(
        "SELECT obs_id, counter, utc_ms, status, reason, "
        "cal_lo_counter, cal_lo_utc_ms, cal_hi_counter, cal_hi_utc_ms "
        "FROM observations WHERE boot_segment = ?",
        (segment,),
    ).fetchall()

    pending_updates: list[tuple] = []
    resolved_updates: list[tuple] = []
    changed: set[str] = set()
    affected_ms: set[int] = set()

    for row in rows:
        obs_id = row["obs_id"]
        result = compute_utc(points, row["counter"])
        if result is None:
            new_utc = None
            new_status = PENDING
            new_reason = _pending_reason(segment, row["counter"], points)
            new_basis = (None, None, None, None)
        else:
            new_utc, (c_lo, t_lo), (c_hi, t_hi) = result
            new_basis = (c_lo, t_lo, c_hi, t_hi)
            if row["status"] == CONFLICT and row["utc_ms"] == new_utc:
                # UTC 没动：冲突与否要按整批结果统一重判，先原样保留
                new_status = CONFLICT
                new_reason = row["reason"]
            else:
                new_status = OK
                new_reason = None

        old_tuple = (
            row["utc_ms"], row["status"], row["reason"],
            row["cal_lo_counter"], row["cal_lo_utc_ms"],
            row["cal_hi_counter"], row["cal_hi_utc_ms"],
        )
        new_tuple = (new_utc, new_status, new_reason, *new_basis)
        if new_tuple == old_tuple:
            continue

        if new_status == PENDING:
            pending_updates.append((new_reason, obs_id))
        else:
            resolved_updates.append((new_utc, new_status, new_reason, *new_basis, obs_id))

        old_utc = row["utc_ms"]
        if new_utc != old_utc or new_status != row["status"]:
            changed.add(obs_id)
        if old_utc is not None and old_utc != new_utc:
            affected_ms.add(old_utc)
        if new_utc is not None and new_utc != old_utc:
            affected_ms.add(new_utc)

    if pending_updates:
        conn.executemany(
            "UPDATE observations SET utc_ms = NULL, status = ?, reason = ?, "
            "cal_lo_counter = NULL, cal_lo_utc_ms = NULL, "
            "cal_hi_counter = NULL, cal_hi_utc_ms = NULL "
            "WHERE obs_id = ?",
            pending_updates,
        )
    if resolved_updates:
        conn.executemany(
            "UPDATE observations SET utc_ms = ?, status = ?, reason = ?, "
            "cal_lo_counter = ?, cal_lo_utc_ms = ?, "
            "cal_hi_counter = ?, cal_hi_utc_ms = ? "
            "WHERE obs_id = ?",
            resolved_updates,
        )
    return changed, affected_ms


def conflict_ms_set(conn: sqlite3.Connection) -> set[int]:
    """被 >=2 个不同开机段的观测占用的 UTC 毫秒集合（诊断/测试用）。"""
    rows = conn.execute(
        "SELECT utc_ms FROM observations WHERE utc_ms IS NOT NULL "
        "GROUP BY utc_ms HAVING COUNT(DISTINCT boot_segment) > 1"
    ).fetchall()
    return {r["utc_ms"] for r in rows}


def resolve_conflicts_for(
    conn: sqlite3.Connection, ms_set: set[int]
) -> set[str]:
    """只重判给定 UTC 毫秒上的冲突，返回状态发生变化的 obs_id。

    每个毫秒单独成组：>=2 个不同开机段则整组置 ``conflict`` 并写入
    最新的冲突原因；只剩一个开机段则把原 ``conflict`` 恢复为 ``ok``。
    同段多条观测共用同一毫秒不算冲突。

    查询按 ``_param_chunk`` 分块，因此受影响的毫秒数可以远超单条 SQL
    的绑定参数上限；未传入的毫秒（无关的历史冲突）完全不会被扫描或改写。
    """
    changed: set[str] = set()
    if not ms_set:
        return changed

    chunk_size = _param_chunk(conn)
    for chunk in _chunks(sorted(ms_set), chunk_size):
        ph = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT obs_id, utc_ms, boot_segment, status, reason "
            f"FROM observations WHERE utc_ms IN ({ph})",
            tuple(chunk),
        ).fetchall()

        by_ms: dict[int, list[sqlite3.Row]] = {}
        for r in rows:
            by_ms.setdefault(r["utc_ms"], []).append(r)

        for ms, group in by_ms.items():
            segments = {g["boot_segment"] for g in group}
            if len(segments) > 1:
                for g in group:
                    others = "、".join(sorted(segments - {g["boot_segment"]}))
                    reason = f"UTC 毫秒 {ms} 与开机段 {others} 的观测冲突，不供分析"
                    if g["status"] != CONFLICT:
                        conn.execute(
                            "UPDATE observations SET status = ?, reason = ? WHERE obs_id = ?",
                            (CONFLICT, reason, g["obs_id"]),
                        )
                        changed.add(g["obs_id"])
                    elif g["reason"] != reason:
                        # 冲突原因随整批结果更新
                        conn.execute(
                            "UPDATE observations SET reason = ? WHERE obs_id = ?",
                            (reason, g["obs_id"]),
                        )
            else:
                for g in group:
                    if g["status"] == CONFLICT:
                        conn.execute(
                            "UPDATE observations SET status = ?, reason = NULL WHERE obs_id = ?",
                            (OK, g["obs_id"]),
                        )
                        changed.add(g["obs_id"])
    return changed


def resolve_conflicts(conn: sqlite3.Connection) -> None:
    """全库刷新冲突（仅供维护/兼容；请求路径使用增量判定）。"""
    resolve_conflicts_for(conn, conflict_ms_set(conn))


# ---------------------------------------------------------------- 变更快照

def fetch_states(conn: sqlite3.Connection, obs_ids: Sequence[str]) -> list[dict]:
    """读取给定观测的当前 (obs_id, utc_ms, status)，按 obs_id 升序。

    ``obs_ids`` 可以任意大：查询按绑定参数上限分块。
    """
    if not obs_ids:
        return []
    states: list[dict] = []
    chunk_size = _param_chunk(conn)
    for chunk in _chunks(list(obs_ids), chunk_size):
        ph = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT obs_id, utc_ms, status FROM observations "
            f"WHERE obs_id IN ({ph})",
            tuple(chunk),
        ).fetchall()
        states.extend(
            {"obs_id": r["obs_id"], "utc_ms": r["utc_ms"], "status": r["status"]}
            for r in rows
        )
    states.sort(key=lambda s: s["obs_id"])
    return states


def _load_states(
    conn: sqlite3.Connection, obs_ids: Sequence[str]
) -> dict[str, tuple[int | None, str]]:
    """obs_id -> (utc_ms, status)，按绑定参数上限分块读取。"""
    if not obs_ids:
        return {}
    out: dict[str, tuple[int | None, str]] = {}
    chunk_size = _param_chunk(conn)
    for chunk in _chunks(list(obs_ids), chunk_size):
        ph = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT obs_id, utc_ms, status FROM observations "
            f"WHERE obs_id IN ({ph})",
            tuple(chunk),
        ).fetchall()
        for r in rows:
            out[r["obs_id"]] = (r["utc_ms"], r["status"])
    return out


# ---------------------------------------------------------------- 批量写入

def upsert_observations(
    conn: sqlite3.Connection, items: list[dict]
) -> list[str]:
    """分批写入观测（同 ID 覆盖原文并重算），返回时刻/状态变化的 obs_id（升序）。

    新观测或被更正观测可能改变原时刻/新时刻的冲突归属，因此只对这两类
    UTC 毫秒做增量冲突重判，并把受波及的观测一并纳入变更清单。
    """
    if not items:
        return []

    # 同批同 ID 后者覆盖前者
    computed: dict[str, dict] = {}
    for it in items:
        computed[it["obs_id"]] = it
    ids = list(computed)

    points_by_seg = {
        seg: load_points(conn, seg)
        for seg in {it["boot_segment"] for it in computed.values()}
    }
    before = _load_states(conn, ids)

    pending_rows: list[tuple] = []
    resolved_rows: list[tuple] = []
    affected_ms: set[int] = set()

    for obs_id, it in computed.items():
        old = before.get(obs_id)
        if old is not None and old[0] is not None:
            affected_ms.add(old[0])  # 原时刻：覆盖后需要重判

        result = compute_utc(points_by_seg[it["boot_segment"]], it["counter"])
        if result is None:
            reason = _pending_reason(
                it["boot_segment"], it["counter"], points_by_seg[it["boot_segment"]]
            )
            pending_rows.append((
                it["obs_id"], it["boot_segment"], it["counter"], it["tide_level"],
                PENDING, reason,
            ))
        else:
            utc_ms, (c_lo, t_lo), (c_hi, t_hi) = result
            resolved_rows.append((
                it["obs_id"], it["boot_segment"], it["counter"], it["tide_level"],
                utc_ms, OK, c_lo, t_lo, c_hi, t_hi,
            ))
            affected_ms.add(utc_ms)  # 新时刻：可能卷入冲突

    if pending_rows:
        conn.executemany(
            "INSERT INTO observations (obs_id, boot_segment, counter, tide_level, "
            "utc_ms, status, reason) VALUES (?,?,?,?,NULL,?,?) "
            "ON CONFLICT(obs_id) DO UPDATE SET "
            "boot_segment=excluded.boot_segment, counter=excluded.counter, "
            "tide_level=excluded.tide_level, utc_ms=NULL, "
            "status=excluded.status, reason=excluded.reason, "
            "cal_lo_counter=NULL, cal_lo_utc_ms=NULL, "
            "cal_hi_counter=NULL, cal_hi_utc_ms=NULL",
            pending_rows,
        )
    if resolved_rows:
        conn.executemany(
            "INSERT INTO observations (obs_id, boot_segment, counter, tide_level, "
            "utc_ms, status, reason, cal_lo_counter, cal_lo_utc_ms, "
            "cal_hi_counter, cal_hi_utc_ms) VALUES (?,?,?,?,?,?,NULL,?,?,?,?) "
            "ON CONFLICT(obs_id) DO UPDATE SET "
            "boot_segment=excluded.boot_segment, counter=excluded.counter, "
            "tide_level=excluded.tide_level, utc_ms=excluded.utc_ms, "
            "status=excluded.status, reason=NULL, "
            "cal_lo_counter=excluded.cal_lo_counter, "
            "cal_lo_utc_ms=excluded.cal_lo_utc_ms, "
            "cal_hi_counter=excluded.cal_hi_counter, "
            "cal_hi_utc_ms=excluded.cal_hi_utc_ms",
            resolved_rows,
        )

    resolve_changed = resolve_conflicts_for(conn, affected_ms)
    after = _load_states(conn, ids)
    # 本批观测按写入前后的最终状态判定，避免“写入时临时置 ok、随后又被
    # 判为冲突”这类中间态造成假变更；其他被卷入/解除冲突的观测
    # 由增量冲突判定直接给出（它们的状态确实发生了翻转）。
    changed = resolve_changed - set(ids)
    for obs_id in ids:
        if before.get(obs_id) != after.get(obs_id):
            changed.add(obs_id)
    return sorted(changed)


class MonotonicViolation(Exception):
    """校时点使段内 UTC 倒退；violations 携带明细。"""

    def __init__(self, violations: list[dict]):
        super().__init__("calibration points make UTC go backwards within a segment")
        self.violations = violations


def upsert_calibrations(
    conn: sqlite3.Connection, items: list[dict]
) -> list[str]:
    """分批写入校时点（同段同计数替换旧值），重算受影响观测。

    合并新旧校时点后，任一开机段内 UTC 随计数倒退则抛 MonotonicViolation，
    由调用方回滚整批，旧数据照常供数。
    """
    if not items:
        return []
    segments = {it["boot_segment"] for it in items}
    conn.executemany(
        "INSERT INTO calibration_points (boot_segment, counter, utc_ms) "
        "VALUES (?,?,?) "
        "ON CONFLICT(boot_segment, counter) DO UPDATE SET utc_ms=excluded.utc_ms",
        [(it["boot_segment"], it["counter"], it["utc_ms"]) for it in items],
    )

    violations: list[dict] = []
    for seg in sorted(segments):
        points = load_points(conn, seg)
        for (c1, t1), (c2, t2) in zip(points, points[1:]):
            if t2 < t1:
                violations.append({
                    "boot_segment": seg,
                    "counter": c2, "utc_ms": t2,
                    "prev_counter": c1, "prev_utc_ms": t1,
                })
    if violations:
        raise MonotonicViolation(violations)

    changed: set[str] = set()
    affected_ms: set[int] = set()
    for seg in sorted(segments):
        seg_changed, seg_ms = recalc_segment(conn, seg)
        changed |= seg_changed
        affected_ms |= seg_ms

    changed |= resolve_conflicts_for(conn, affected_ms)
    return sorted(changed)
