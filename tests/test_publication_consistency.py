#!/usr/bin/env python3 -I
"""离线一致性测试：交错发布 + 中断恢复（单条命令，80 秒内完成）。

运行方式：

    python3 -I tests/test_publication_consistency.py

测试完全离线、仅用标准库与自带夹具，通过子进程 + 故障注入环境变量
（RECON_FAULT / RECON_FAULT_ACTION）驱动真实的多进程交错，而不是
线程内打桩。覆盖四个保证：

  1. 慢旧求解：新输入批次生效、另一轮重跑已发布后，旧任务在提交时被
     围栏拒绝（superseded），绝不会把旧结果发成当前版；求解期间接入
     和另一轮重跑都能正常完成。
  2. 相同输入的并发重跑收敛到同一个已发布版本（unchanged，不新增版本）。
  3. 发布中途硬停机（os._exit）：current 仍指向完整旧版本，查询可读；
     遗留 staged 预留与临时目录在下次打开时被回收。
  4. 恢复后的增量重跑只复用当前版本血缘，绝不沿用 staged/错版结果，
     且再次中断后仍能恢复出完整版本。

退出码 0 表示全部断言通过，否则非零。
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from recon.fixtures import build_fixture, RECEIVER_MAP, FIXTURE_FILES
from recon.pipeline import Pipeline, IngestionReport
from recon.query import QueryService
from recon.storage import Lake

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLI = os.path.join(REPO_ROOT, "recon_cli.py")
TIMEOUT = 80.0

_failures = []
_checks = 0


def check(cond, label):
    global _checks
    _checks += 1
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {label}")
    if not cond:
        _failures.append(label)


def section(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def run_cli(lake, *args, env_extra=None, expect_exit=0):
    env = dict(os.environ)
    env["PYTHONPATH"] = ""  # -I already isolates; keep env explicit
    if env_extra:
        env.update(env_extra)
    proc = subprocess.run(
        [sys.executable, "-I", CLI, "--lake", lake, *args],
        capture_output=True, text=True, env=env, timeout=TIMEOUT)
    ok = proc.returncode == expect_exit
    payload = None
    if proc.stdout.strip():
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError:
            payload = None
    return proc.returncode, proc.stderr.strip(), payload, proc.stdout


def ingest_all_fixture(pipe, src, run_id="init"):
    for source, items in FIXTURE_FILES.items():
        for partition, rel in items:
            pipe.ingest(source, partition, os.path.join(src, rel),
                        run_id, IngestionReport(run_id))


def _raw_current_version_name(lake):
    """Read the current pointer without opening a Lake (no auto-recovery)."""
    link = os.path.join(lake, "current")
    if os.path.islink(link):
        target = os.readlink(link)
    elif os.path.exists(link):
        with open(link, "r", encoding="utf-8") as f:
            target = f.read().strip()
    else:
        return None
    return os.path.basename(target) if target else None


def current_version_name(lake):
    lake_obj = Lake(lake)
    vdir = lake_obj.current_version_dir()
    return os.path.basename(vdir) if vdir else None


def query_summary(lake):
    return QueryService(Lake(lake)).summary()


def gamma_status(lake):
    q = QueryService(Lake(lake))
    groups = [g for g in q.list_groups() if "N3" in g["nights"]]
    if not groups:
        return None
    return q.get_group(groups[0]["group_id"])["status"]


def main():
    t0 = time.time()
    work = tempfile.mkdtemp(prefix="recon_consistency_")
    src = build_fixture(os.path.join(work, "inputs"))
    lake = os.path.join(work, "lake")
    print(f"临时数据湖: {lake}")

    # ---------------------------------------------------------------- #
    section("场景 0：初始接入与首版发布")
    # ---------------------------------------------------------------- #
    pipe = Pipeline(Lake(lake), RECEIVER_MAP)
    ingest_all_fixture(pipe, src)
    code, err, rep, _ = run_cli(lake, "rerun")
    check(code == 0 and rep and rep["outcome"] == "ok",
          f"首版发布成功（outcome=ok, v{rep['version_id'] if rep else '?'}）")
    v1 = rep["version_id"] if rep else None
    check(gamma_status(lake) == "pending", "首版中 GAMMA(N3) 为 pending（设备合计缺失）")
    check(query_summary(lake)["group_total"] == 3, "首版包含完整 3 个核对组")

    # ---------------------------------------------------------------- #
    section("场景 1：慢旧求解 vs 新输入批次 + 另一轮重跑（发布围栏）")
    # ---------------------------------------------------------------- #
    fixed = os.path.join(src, "device", "fixes", "night=N3", "part.csv")
    env_slow = {"RECON_FAULT": "before_solve",
                "RECON_FAULT_ACTION": "sleep:3"}
    t_start = time.time()
    proc_old = subprocess.Popen(
        [sys.executable, "-I", CLI, "--lake", lake, "rerun"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, env={**os.environ, **env_slow})
    time.sleep(1.0)  # 让旧任务进入长时间求解

    # 求解期间接入新批次（必须不被阻塞）
    t_ing = time.time()
    code, err, rep_ing, _ = run_cli(
        lake, "ingest", "--source", "device", "--partition", "N3",
        "--file", fixed)
    ing_elapsed = time.time() - t_ing
    check(code == 0 and rep_ing and rep_ing["status"] == "accepted",
          f"慢求解期间接入 N3 修正批次成功（耗时 {ing_elapsed:.2f}s，未被阻塞）")

    # 另一轮重跑基于新输入完成发布
    code, err, rep_fresh, _ = run_cli(
        lake, "rerun", "--changed", "device=N3")
    check(code == 0 and rep_fresh and rep_fresh["outcome"] == "ok",
          f"新一轮重跑基于新输入发布 v{rep_fresh['version_id'] if rep_fresh else '?'}")
    v_fresh = rep_fresh["version_id"] if rep_fresh else None
    check(current_version_name(lake) == f"v{v_fresh}",
          "当前版本已切换为新一轮重跑产物")
    check(gamma_status(lake) == "feasible",
          "当前版本中 GAMMA 已随新输入变为 feasible")

    # 等旧任务结束：它必须被围栏拒绝，不能覆盖当前版
    out_old, err_old = proc_old.communicate(timeout=TIMEOUT)
    total = time.time() - t_start
    try:
        rep_old = json.loads(out_old)
    except json.JSONDecodeError:
        rep_old = {}
    check(proc_old.returncode == 0,
          f"旧慢任务正常退出（exit=0，总耗时 {total:.2f}s）")
    check(rep_old.get("outcome") == "superseded",
          f"旧任务发布被围栏拒绝（outcome=superseded，detail: "
          f"{rep_old.get('detail', '')[:60]}...）")
    check(current_version_name(lake) == f"v{v_fresh}",
          "旧任务结束后当前版本仍是新版，未被旧结果覆盖")
    check(gamma_status(lake) == "feasible",
          "当前版本业务结果保持新输入口径（GAMMA=feasible）")

    # 相同输入再跑一轮：幂等 no-op，不产生新版本
    code, _, rep_same, _ = run_cli(lake, "rerun")
    check(rep_same and rep_same["outcome"] == "unchanged"
          and rep_same["version_id"] == v_fresh,
          f"相同输入重跑收敛到已发布版本（unchanged, v{rep_same['version_id'] if rep_same else '?'}）")
    check(sorted(os.listdir(os.path.join(lake, "versions"))) ==
          [f"v{v1}", f"v{v_fresh}"],
          "版本目录无多余版本（无错版、无重复版）")

    # ---------------------------------------------------------------- #
    section("场景 2：发布中途硬停机（after_tree_write 崩溃）")
    # ---------------------------------------------------------------- #
    before = current_version_name(lake)
    # make a real (business-neutral: DELTA stays isolated) input change so
    # the rerun genuinely enters the publication phase instead of no-op'ing
    n4_variant = os.path.join(work, "device_n4_variant.csv")
    with open(n4_variant, "wb") as f:
        f.write(("night,receiver_code,occupied_seconds\n"
                 "N4,DEV-A,5100\n").encode("gb18030"))
    code, _, _, _ = run_cli(
        lake, "ingest", "--source", "device", "--partition", "N4",
        "--file", n4_variant)
    check(code == 0, "崩溃前接入 N4 变体批次（触发一次真实新版本发布）")

    env_crash = {"RECON_FAULT": "before_swap",
                 "RECON_FAULT_ACTION": "exit"}
    code, err, _, _ = run_cli(lake, "rerun", env_extra=env_crash,
                              expect_exit=91)
    check(code == 91, "发布中途进程硬停机（版本树已完整落盘、指针切换前）")
    check(_raw_current_version_name(lake) == before,
          f"停机后 current 仍指向上一完整版本 {before}（读者看不到半发布）")

    # 直接用 sqlite3 检查，避免打开 Lake 触发自动恢复
    import sqlite3
    conn = sqlite3.connect(os.path.join(lake, "state.db"))
    conn.row_factory = sqlite3.Row
    staged = [dict(r) for r in conn.execute(
        "SELECT * FROM publications WHERE state='staged'")]
    check(len(staged) == 1, "崩溃留下 1 条 staged 预留（尚未标记 current）")
    staged_vid = staged[0]["version_id"] if staged else None
    check(staged_vid is not None and os.path.isfile(os.path.join(
            lake, "versions", f"v{staged_vid}", "current.json")),
          "staged 版本目录自身完整（含 current.json），只是未对读者生效")
    conn.close()

    # 打开新的 Lake 句柄即触发恢复；查询仍读到完整、可读的旧版本
    q = QueryService(Lake(lake))
    snap = q.snapshot()
    check(snap.get("published") and snap.get("group_count") == 3
          and len(q.query_pairs()) > 0,
          "停机后查询仍读到完整版本（清单与配对结果均可读）")

    leftovers = [n for n in os.listdir(os.path.join(lake, "versions"))
                 if n.startswith(".stage-")]
    check(not leftovers, "无半发布临时目录残留（原子改名 + 启动清理）")

    # 另一种中断位置：指针已原子切到完整新版、台账提交前停机。
    # 读者此刻读到的已是完整新版；恢复逻辑负责把台账提升为 current。
    n4_variant_b = os.path.join(work, "device_n4_variant_b.csv")
    with open(n4_variant_b, "wb") as f:
        f.write(("night,receiver_code,occupied_seconds\n"
                 "N4,DEV-A,5150\n").encode("gb18030"))
    run_cli(lake, "ingest", "--source", "device", "--partition", "N4",
            "--file", n4_variant_b)
    env_crash_b = {"RECON_FAULT": "after_tree_write",
                   "RECON_FAULT_ACTION": "exit"}
    code, _, _, _ = run_cli(lake, "rerun", env_extra=env_crash_b,
                            expect_exit=91)
    check(code == 91, "第二次硬停机（指针已切换、台账提交前）")
    qb = QueryService(Lake(lake))
    check(qb.snapshot().get("published") and len(qb.query_pairs()) > 0,
          "指针切换后停机：读者看到的新版本自身完整可读")

    # ---------------------------------------------------------------- #
    section("场景 3：恢复发布 + 恢复后增量重跑不沿用错版")
    # ---------------------------------------------------------------- #
    # 重新打开即恢复：staged 与 current 指针不符 -> 判为 abandoned
    code, _, rep_rec, _ = run_cli(lake, "rerun")
    check(rep_rec and rep_rec["outcome"] in ("ok", "unchanged"),
          f"恢复重跑完成（outcome={rep_rec['outcome'] if rep_rec else '?'}）")
    conn = sqlite3.connect(os.path.join(lake, "state.db"))
    conn.row_factory = sqlite3.Row
    states = {r["version_id"]: r["state"] for r in conn.execute(
        "SELECT version_id, state FROM publications")}
    conn.close()
    check(all(s in ("current", "abandoned") for s in states.values()),
          "恢复后 publications 台账无悬挂 staged，状态自洽")
    cur_name = current_version_name(lake)
    check(states[int(cur_name[1:])] == "current",
          f"台账 current 与指针 {cur_name} 一致")
    check(gamma_status(lake) == "feasible",
          "恢复后业务结果正确（GAMMA=feasible，未回退错版）")

    # 增量重跑：只触碰 N4 隔离组，GAMMA 必须复用当前版本血缘而非 staged 行
    code, _, rep_inc, _ = run_cli(
        lake, "rerun", "--changed", "device=N4")
    check(code == 0 and rep_inc,
          f"恢复后增量重跑可执行（outcome={rep_inc['outcome'] if rep_inc else '?'}）")
    check(gamma_status(lake) == "feasible",
          "增量重跑未沿用错版/中断版结果（GAMMA 仍为 feasible）")
    q2 = QueryService(Lake(lake))
    check(q2.summary()["group_total"] == 3
          and q2.snapshot()["group_count"] == 3,
          "增量重跑后当前版本仍包含完整 3 个组")

    # 再制造一次中断（这次在写版本树之前），验证可重复恢复。
    # 先制造一次真实输入变化，保证 rerun 进入发布阶段。
    n4_variant2 = os.path.join(work, "device_n4_variant2.csv")
    with open(n4_variant2, "wb") as f:
        f.write(("night,receiver_code,occupied_seconds\n"
                 "N4,DEV-A,5200\n").encode("gb18030"))
    code, _, _, _ = run_cli(
        lake, "ingest", "--source", "device", "--partition", "N4",
        "--file", n4_variant2)
    check(code == 0, "第二次中断前接入新的 N4 变体批次")
    env_crash2 = {"RECON_FAULT": "after_reserve", "RECON_FAULT_ACTION": "exit"}
    code, _, _, _ = run_cli(lake, "rerun", env_extra=env_crash2,
                            expect_exit=91)
    check(code == 91, "第二次中断（staged 预留已持久化、版本树写入前）成功模拟")
    # 预留已提交：直接查 sqlite 应能看到 staged 行，而读者仍只读旧 current
    conn = sqlite3.connect(os.path.join(lake, "state.db"))
    conn.row_factory = sqlite3.Row
    pending = [dict(r) for r in conn.execute(
        "SELECT version_id FROM publications WHERE state='staged'")]
    conn.close()
    check(len(pending) == 1, "中断后 staged 预留已持久化（等待恢复裁决）")
    check(QueryService(Lake(lake)).snapshot().get("published"),
          "第二次中断后当前完整版本依然可读")
    code, _, rep_rec2, _ = run_cli(lake, "rerun")
    check(code == 0 and rep_rec2,
          f"再次恢复成功（outcome={rep_rec2['outcome'] if rep_rec2 else '?'}）")
    check(QueryService(Lake(lake)).summary()["group_total"] == 3,
          "再次恢复后版本完整、组齐全")

    # 历史版本保留各自血缘行是正常的；关键是复用查询只能看到 current 行：
    # 每个当前版本组经 group_version() 解析出的版本号必须等于当前发布版本。
    lake_obj = Lake(lake)
    current_vid = int(current_version_name(lake)[1:])
    lineage_ok = all(
        (meta := lake_obj.group_version(g["group_id"])) is not None
        and meta["version_id"] == current_vid
        for g in QueryService(lake_obj).list_groups())
    check(lineage_ok,
          "每个当前版本组的复用血缘都指向当前发布版本（staged/错版行不可见）")

    elapsed = time.time() - t0
    print("\n" + "-" * 72)
    if _failures:
        print(f"失败 {len(_failures)} 项: {_failures}")
        print(f"RESULT: FAIL（{_checks} 项断言，耗时 {elapsed:.1f}s）")
        shutil.rmtree(work, ignore_errors=True)
        return 1
    print(f"RESULT: ALL PASSED — {_checks} 项断言，耗时 {elapsed:.1f}s"
          f"（限制 {TIMEOUT:.0f}s）")
    print(f"临时目录（已清理）: {work}")
    shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
