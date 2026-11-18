#!/usr/bin/env python3
"""端到端自测：在本机模拟 Compose 拓扑（管理服务 + 两个独立持久化节点）。

覆盖:
  1. 发布与双节点同步、整列表原子生效、实答一致
  2. 单节点失联：另一节点继续应答；状态显示待补，绝不误报 synced
  3. 节点恢复后自动补齐到最新修订
  4. 迟到的旧修订推送被节点拒绝(409)，节点/同步结论不回退；
     迟到的旧/高版本上报都只能进 claim，不能改管理端观察值
  4b. 交错/乱序下发: 跳号接受、随后旧号拒绝
  4c. 旧卷快照恢复: 实际服务版本倒退时撤销同步结论、补发最新整份地址，
     只有两端实答与现行修订一致才重新报 synced；恢复期间发布不被旧上报误判
  5. 并发发布：修订严格递增、无重号
  6. 节点重启：独立 SQLite 持久化，服务版本不丢
  7. 非法输入校验

用法: python scripts/selftest.py [python可执行路径]
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.argv[1] if len(sys.argv) > 1 else sys.executable

DOMAIN = "device.example.internal"
MGMT_PORT = 18000
NODE_SPECS = [
    {"id": "node-a", "udp": 10053, "http": 18053},
    {"id": "node-b", "udp": 10054, "http": 18054},
]

PASS, FAIL = 0, 0
procs: dict[str, subprocess.Popen] = {}
tmpdir: Path | None = None


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def http(method: str, path: str, body: dict | None = None, port: int = MGMT_PORT):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data,
        headers={"Content-Type": "application/json"}, method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def dns_answers(port: int, domain: str = DOMAIN, timeout: float = 3.0) -> set[str]:
    from dnslib import DNSRecord
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(timeout)
    try:
        s.sendto(DNSRecord.question(domain, qtype="A").pack(), ("127.0.0.1", port))
        data, _ = s.recvfrom(65535)
    finally:
        s.close()
    reply = DNSRecord.parse(data)
    return {str(rr.rdata) for rr in reply.rr}


def wait_status(cond, timeout: float = 20.0, interval: float = 0.5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        _, last = http("GET", "/status")
        if cond(last):
            return last
        time.sleep(interval)
    return last


def view_of(st: dict, node_id: str) -> dict:
    return next(n for n in st["nodes"] if n["node_id"] == node_id)


def start_node(spec: dict) -> subprocess.Popen:
    assert tmpdir is not None
    data_dir = tmpdir / spec["id"]
    data_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env.update({
        "NODE_ID": spec["id"],
        "NODE_DB_PATH": str(data_dir / "node.db"),
        "MGMT_BASE_URL": f"http://127.0.0.1:{MGMT_PORT}",
        "UDP_PORT": str(spec["udp"]),
        "HTTP_PORT": str(spec["http"]),
        "HEARTBEAT_INTERVAL": "2",
    })
    p = subprocess.Popen(
        [PY, "-m", "dnsnode.node"], cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    procs[spec["id"]] = p
    # Fail fast on a bind conflict rather than silently talking to a stale
    # process that still holds the port.
    time.sleep(0.6)
    if p.poll() is not None:
        raise RuntimeError(
            f"{spec['id']} exited at startup:\n{p.stdout.read() if p.stdout else ''}"
        )
    return p


def start_mgmt() -> subprocess.Popen:
    assert tmpdir is not None
    env = dict(os.environ)
    nodes_json = json.dumps([
        {"id": s["id"],
         "control_url": f"http://127.0.0.1:{s['http']}",
         "dns_host": "127.0.0.1",
         "dns_port": s["udp"]} for s in NODE_SPECS
    ])
    env.update({
        "MANAGED_DOMAIN": DOMAIN,
        "MGMT_DB_PATH": str(tmpdir / "management.db"),
        "OFFLINE_AFTER_SECONDS": "5",
        "RECONCILE_INTERVAL": "1",
        "NODES": nodes_json,
    })
    p = subprocess.Popen(
        [PY, "-m", "uvicorn", "management.app:app",
         "--host", "127.0.0.1", "--port", str(MGMT_PORT)],
        cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    procs["mgmt"] = p
    return p


def wait_http(url_port: int, path: str, timeout: float = 15.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{url_port}{path}", timeout=2
            ) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.4)
    return False


def _port_in_use(port: int, udp: bool = True) -> bool:
    fam = socket.AF_INET
    typ = socket.SOCK_DGRAM if udp else socket.SOCK_STREAM
    s = socket.socket(fam, typ)
    try:
        s.bind(("127.0.0.1", port))
    except OSError:
        return True
    else:
        return False
    finally:
        s.close()


def stop(name: str) -> None:
    p = procs.pop(name, None)
    if not p or p.poll() is not None:
        return
    p.terminate()
    try:
        p.wait(timeout=8)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=5)
    # The process must fully release its UDP/HTTP ports before a snapshot or
    # volume restore, otherwise a restarted node loses the bind while the old
    # process keeps answering from the deleted file (a harness-only race).
    spec = next((s for s in NODE_SPECS if s["id"] == name), None)
    if spec:
        deadline = time.time() + 8
        while time.time() < deadline and (
            _port_in_use(spec["udp"], udp=True)
            or _port_in_use(spec["http"], udp=False)
        ):
            time.sleep(0.1)


def teardown() -> None:
    for name in list(procs):
        stop(name)


# --------------------------------------------------------------------------- #
# Test scenarios
# --------------------------------------------------------------------------- #
def scenario_basic() -> None:
    print("\n=== 1. 发布/同步/整列表生效/实答一致 ===")
    code, body = http("POST", "/publishes", {"addresses": ["10.0.0.1", "10.0.0.2"]})
    check("首次发布返回201且修订号=1", code == 201 and body["revision"] == 1, str(body))

    st = wait_status(lambda s: s["overall_status"] == "synced")
    check("双节点同步完成 overall=synced", st["overall_status"] == "synced", json.dumps(st))
    check("两节点 served_revision 均为1",
          all(n["served_revision"] == 1 for n in st["nodes"]), json.dumps(st["nodes"]))

    a = dns_answers(NODE_SPECS[0]["udp"])
    b = dns_answers(NODE_SPECS[1]["udp"])
    check("节点A 实答=整列表", a == {"10.0.0.1", "10.0.0.2"}, str(a))
    check("节点B 实答=整列表", b == {"10.0.0.1", "10.0.0.2"}, str(b))
    check("两端实际答复一致", a == b)

    code, ver = http("GET", "/verify")
    check("/verify 全部匹配最新修订", code == 200 and ver["all_nodes_match"], json.dumps(ver))

    # 第二次发布：整列表替换，旧地址必须全部消失
    code, body = http("POST", "/publishes", {"addresses": ["10.0.0.3"]})
    check("第二次发布修订号=2(严格递增)", code == 201 and body["revision"] == 2, str(body))
    wait_status(lambda s: s["overall_status"] == "synced")
    a2 = dns_answers(NODE_SPECS[0]["udp"])
    b2 = dns_answers(NODE_SPECS[1]["udp"])
    check("整列表替换: 新地址生效", a2 == {"10.0.0.3"}, str(a2))
    check("整列表替换: 旧地址全部消失", "10.0.0.1" not in a2 and "10.0.0.2" not in a2, str(a2))
    check("替换后两端仍一致", a2 == b2)


def scenario_outage() -> None:
    print("\n=== 2. 单节点失联: 另一节点继续应答, 不误报 synced ===")
    stop("node-b")
    time.sleep(1)

    code, body = http("POST", "/publishes", {"addresses": ["10.0.1.10", "10.0.1.11"]})
    rev3 = body["revision"]
    check("失联期间发布成功, 修订号=3", code == 201 and rev3 == 3, str(body))

    # 存活节点应在数个对账周期内追上
    deadline = time.time() + 15
    st = None
    while time.time() < deadline:
        _, st = http("GET", "/status")
        na = next(n for n in st["nodes"] if n["node_id"] == "node-a")
        if na["served_revision"] == 3:
            break
        time.sleep(0.5)
    na = next(n for n in st["nodes"] if n["node_id"] == "node-a")
    nb = next(n for n in st["nodes"] if n["node_id"] == "node-b")
    check("存活节点A 追上修订3", na["served_revision"] == 3, json.dumps(na))
    check("存活节点A 继续提供 DNS 实答",
          dns_answers(NODE_SPECS[0]["udp"]) == {"10.0.1.10", "10.0.1.11"})

    # 等待离线判定窗口
    time.sleep(6)
    _, st = http("GET", "/status")
    nb = next(n for n in st["nodes"] if n["node_id"] == "node-b")
    check("失联节点B 标记 offline", st["overall_status"] == "degraded-node-offline"
          and not nb["online"], json.dumps(st))
    check("synced=false, 绝不误报同步完成", st["synced"] is False)
    check("待补状态: node-b waiting-node-recovery",
          nb["catch_up_status"] == "waiting-node-recovery", json.dumps(nb))
    check("pending_nodes 包含 node-b", "node-b" in st["pending_nodes"])

    code, ver = http("GET", "/verify")
    check("失联期间 /verify 明确不通过(不假装一致)",
          ver["all_nodes_match"] is False
          and any(not n["matches_latest_revision"] for n in ver["nodes"]),
          json.dumps(ver))


def scenario_recovery() -> None:
    print("\n=== 3. 节点恢复后自动补齐 ===")
    start_node(NODE_SPECS[1])
    check("节点B 控制面恢复", wait_http(NODE_SPECS[1]["http"], "/health"))
    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=25)
    nb = next(n for n in st["nodes"] if n["node_id"] == "node-b")
    check("恢复后 node-b 自动补齐到修订3",
          st["overall_status"] == "synced" and nb["served_revision"] == 3, json.dumps(st))
    check("补齐后两端实答一致",
          dns_answers(NODE_SPECS[0]["udp"]) == dns_answers(NODE_SPECS[1]["udp"]))


def scenario_stale() -> None:
    print("\n=== 4. 迟到旧修订被拒(409), 不回退 ===")
    # 直接向节点重放一个旧 revision=1 的发布
    spec = NODE_SPECS[0]
    req = urllib.request.Request(
        f"http://127.0.0.1:{spec['http']}/publish",
        data=json.dumps({"revision": 1, "domain": DOMAIN,
                         "addresses": ["1.2.3.4"]}).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=5)
        code, raw = 0, {}
    except urllib.error.HTTPError as e:
        code, raw = e.code, json.loads(e.read())
    check("旧修订推送被节点 409 拒绝", code == 409 and raw["status"] == "rejected", str(raw))
    check("节点仍服务当前修订(不回退)", raw.get("served_revision") == 3, str(raw))
    check("节点实答未被旧数据污染",
          dns_answers(spec["udp"]) == {"10.0.1.10", "10.0.1.11"})

    # 管理端状态同样不回退
    _, st = http("GET", "/status")
    check("管理端同步结论仍为 synced", st["overall_status"] == "synced", json.dumps(st))

    # 模拟迟到的旧上报（节点自身的 claim，可记录但不能回滚管理端观察值）
    code, _ = http("POST", "/internal/nodes/node-a/report", {"served_revision": 1})
    _, st = http("GET", "/status")
    na = view_of(st, "node-a")
    check("迟到旧上报不回退管理端观察的 served 版本",
          code == 200 and na["served_revision"] == 3, json.dumps(na))
    check("旧上报仅体现在 reported_revision",
          na["reported_revision"] == 1, json.dumps(na))
    # claim 与观察值矛盾时先撤销同步结论，待下一轮实答核对后恢复
    check("矛盾 claim 立即撤销同步结论(宁可保守)", st["synced"] is False, json.dumps(na))
    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=10)
    check("实答核对通过后同步结论自动恢复",
          st["overall_status"] == "synced" and view_of(st, "node-a")["rollback_suspected"] is False,
          json.dumps(st))

    # 反向风险：旧进程迟到的“较高版本”上报也不能凭一句话抬高观察值
    code, _ = http("POST", "/internal/nodes/node-a/report", {"served_revision": 999})
    _, st = http("GET", "/status")
    na = view_of(st, "node-a")
    check("迟到高版本上报不能凭 claim 抬高 served/verified",
          code == 200 and na["served_revision"] == 3
          and na["verified_revision"] == 3 and na["reported_revision"] == 999,
          json.dumps(na))
    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=10)
    check("高 claim 经实答核对后不影响 synced", st["synced"] is True, json.dumps(st))


def scenario_interleave() -> None:
    print("\n=== 4b. 交错/乱序下发: 跳号接受、随后旧号拒绝 ===")
    spec = NODE_SPECS[0]
    _, st0 = http("GET", "/status")
    base = st0["latest_revision"]["revision"]

    def direct_push(rev: int, addr: str):
        req = urllib.request.Request(
            f"http://127.0.0.1:{spec['http']}/publish",
            data=json.dumps({"revision": rev, "domain": DOMAIN,
                             "addresses": [addr]}).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    # 乱序先到一个跳号的较新修订 -> 必须接受
    code, body = direct_push(base + 3, "192.0.2.20")
    check("跳号的较新修订(base+3)被接受", code == 200
          and body["served_revision"] == base + 3, str(body))
    check("节点实答立即切换为跳号修订数据",
          dns_answers(spec["udp"]) == {"192.0.2.20"})

    # 迟到的中间修订(base+1) -> 必须 409, 数据不回退
    code, body = direct_push(base + 1, "192.0.2.19")
    check("迟到的中间修订被 409 拒绝", code == 409
          and body["status"] == "rejected", str(body))
    check("拒绝后实答不回退", dns_answers(spec["udp"]) == {"192.0.2.20"})

    # 同号重放 -> 同样拒绝(幂等保护)
    code, _ = direct_push(base + 3, "192.0.2.99")
    check("同号重放被 409 拒绝", code == 409)
    check("重放不改变实答", dns_answers(spec["udp"]) == {"192.0.2.20"})

    # 管理端连续发布: 落后的 node-b 逐版追上; node-a 对旧版持续 409,
    # 直到管理端修订号超过 base+3 后两节点重新收敛。
    last_rev = base
    for i in range(1, 5):
        code, body = http("POST", "/publishes",
                          {"addresses": [f"198.51.100.{i}"]})
        check(f"管理端第{i}次发布成功(修订 {body['revision']})", code == 201)
        last_rev = body["revision"]
    check("管理端修订号已超过注入的跳号修订", last_rev == base + 4)
    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=30)
    check("交错后双节点最终收敛到管理端最新修订",
          all(n["served_revision"] == last_rev for n in st["nodes"]),
          json.dumps(st["nodes"]))
    check("收敛后两端实答一致",
          dns_answers(NODE_SPECS[0]["udp"]) == dns_answers(NODE_SPECS[1]["udp"])
          == {"198.51.100.4"})


def scenario_snapshot_rollback() -> None:
    print("\n=== 4c. 旧卷快照恢复: 识别版本倒退、撤销同步、补发整列表 ===")
    spec = NODE_SPECS[1]                       # 对 node-b 做快照恢复
    node_dir = tmpdir / spec["id"]
    snapshot_dir = tmpdir / f"{spec['id']}-snapshot"
    _, st0 = http("GET", "/status")
    base = st0["latest_revision"]["revision"]

    # 1) 在节点停服（卷静止）时打一个旧快照（当前版本 base）
    stop(spec["id"])
    time.sleep(0.5)
    if snapshot_dir.exists():
        shutil.rmtree(snapshot_dir)
    shutil.copytree(node_dir, snapshot_dir)
    start_node(spec)
    check("打快照后节点B 重新就绪", wait_http(spec["http"], "/health"))
    wait_status(lambda s: s["overall_status"] == "synced", timeout=15)

    # 2) 随后制造两个更新的修订
    code, body = http("POST", "/publishes", {"addresses": ["203.0.113.1"]})
    rev_new1 = body["revision"]
    check("快照后第1次新发布成功", code == 201 and rev_new1 == base + 1, str(body))
    code, body = http("POST", "/publishes", {"addresses": ["203.0.113.2"]})
    rev_new2 = body["revision"]
    check("快照后第2次新发布成功(恢复期间仍可发布)", code == 201 and rev_new2 == base + 2,
          str(body))
    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=25)
    check("发布后双节点正常同步",
          all(n["served_revision"] == rev_new2 for n in st["nodes"]),
          json.dumps(st["nodes"]))

    # 3) 停节点并用旧快照覆盖其持久化卷 -> 实际服务版本倒退到 base
    stop(spec["id"])
    time.sleep(0.5)
    shutil.rmtree(node_dir)
    shutil.copytree(snapshot_dir, node_dir)

    # 4) 旧进程迟到的“高版本”上报不能掩盖恢复节点的旧数据
    code, _ = http("POST", f"/internal/nodes/{spec['id']}/report",
                   {"served_revision": rev_new2})
    check("失联节点的旧上报仍被接收(刷新 claim)", code == 200)
    _, st = http("GET", "/status")
    nb = view_of(st, spec["id"])
    check("失联期间高 claim 不产生任何实答证据(verified 被清零, 不抬升)",
          nb["reported_revision"] == rev_new2 and nb["verified_revision"] == 0
          and nb["caught_up"] is False,
          json.dumps(nb))
    start_node(spec)
    check("恢复节点B 控制面起来", wait_http(spec["http"], "/health"))

    # 节点自身当前进程的 /health 是确定性的回滚实据（高 claim 被戳穿）
    import urllib.request as _ur
    with _ur.urlopen(f"http://127.0.0.1:{spec['http']}/health", timeout=3) as _r:
        health = json.loads(_r.read())
    check("节点当前进程如实报告倒退的旧版本(实据)",
          int(health["revision"]) < rev_new2, json.dumps(health))
    check("节点B 实答确为旧快照数据(版本倒退属实)",
          dns_answers(spec["udp"]) != {"203.0.113.2"},
          str(dns_answers(spec["udp"])))
    check("另一节点A 继续供答最新修订",
          dns_answers(NODE_SPECS[0]["udp"]) == {"203.0.113.2"})

    # 管理端必须经实据撤销同步结论（回滚窗口内可见，随后自动补齐）
    deadline = time.time() + 12
    saw_withdrawn, saw_rollback_flag = False, False
    while time.time() < deadline:
        _, st = http("GET", "/status")
        nb = view_of(st, spec["id"])
        if not st["synced"] and spec["id"] in st["pending_nodes"]:
            saw_withdrawn = True
        if nb["rollback_suspected"]:
            saw_rollback_flag = True
        if st["overall_status"] == "synced":
            break
        time.sleep(0.2)
    check("管理端撤销同步结论并将 node-b 列入待补", saw_withdrawn)
    check("回滚期间置 rollback_suspected 标记", saw_rollback_flag)
    code, ver = http("GET", "/verify")
    check("/verify 始终以两端实答为依据(接口可用)", code == 200, json.dumps(ver))

    # 5) 后台对账必须识别倒退并补发“最新整份地址”，直到实答一致才重新同步
    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=30)
    nb = view_of(st, spec["id"])
    check("恢复节点自动补齐到最新修订",
          st["overall_status"] == "synced"
          and nb["served_revision"] == rev_new2
          and nb["verified_revision"] == rev_new2
          and nb["rollback_suspected"] is False,
          json.dumps(nb))
    check("补齐后两端实答一致且为最新整列表",
          dns_answers(NODE_SPECS[0]["udp"])
          == dns_answers(NODE_SPECS[1]["udp"]) == {"203.0.113.2"})

    # 6) 恢复期间的新发布同样不能被旧上报误判生效：再来一次发布验证
    code, body = http("POST", "/publishes", {"addresses": ["203.0.113.3"]})
    rev_new3 = body["revision"]
    check("恢复后新发布修订继续递增", code == 201 and rev_new3 == rev_new2 + 1, str(body))
    # 注入一个针对新修订的“旧版本”claim，管理端必须以实答为准
    http("POST", f"/internal/nodes/{spec['id']}/report",
         {"served_revision": rev_new2})
    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=25)
    nb = view_of(st, spec["id"])
    check("新发布以实答收敛, 旧 claim 不影响同步判定",
          st["overall_status"] == "synced" and nb["verified_revision"] == rev_new3,
          json.dumps(nb))
    check("新发布两端实答一致",
          dns_answers(NODE_SPECS[0]["udp"])
          == dns_answers(NODE_SPECS[1]["udp"]) == {"203.0.113.3"})


def scenario_concurrent() -> None:
    print("\n=== 5. 并发发布: 修订严格递增无重号 ===")
    import concurrent.futures
    payloads = [[f"172.16.0.{i}"] for i in range(1, 9)]
    # 基线必须在并发发布之前取，否则拿到的是风暴后的最新修订号
    _, st0 = http("GET", "/status")
    base = st0["latest_revision"]["revision"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(
            lambda p: http("POST", "/publishes", {"addresses": p}), payloads
        ))
    codes = [c for c, _ in results]
    revs = sorted(b["revision"] for _, b in results)
    check("8 个并发发布全部 201", all(c == 201 for c in codes), str(codes))
    check("修订号唯一且连续递增(base+1..base+8)",
          revs == list(range(base + 1, base + 9)), str(revs))

    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=30)
    check("并发风暴后最终双节点收敛到最新修订",
          st["overall_status"] == "synced"
          and all(n["served_revision"] == base + 8 for n in st["nodes"]), json.dumps(st))
    check("收敛后两端实答一致",
          dns_answers(NODE_SPECS[0]["udp"]) == dns_answers(NODE_SPECS[1]["udp"]))


def scenario_persistence() -> None:
    print("\n=== 6. 节点重启: 独立 SQLite 持久化 ===")
    for spec in NODE_SPECS:
        stop(spec["id"])
    for spec in NODE_SPECS:
        start_node(spec)
    for spec in NODE_SPECS:
        check(f"{spec['id']} 重启后健康", wait_http(spec["http"], "/health", timeout=15))
    # 节点未收到任何新发布也应凭本地库继续应答最新数据
    time.sleep(2)
    a = dns_answers(NODE_SPECS[0]["udp"])
    b = dns_answers(NODE_SPECS[1]["udp"])
    _, st0 = http("GET", "/status")
    expected = set(st0["latest_revision"]["addresses"])
    check("重启后节点A 凭持久化继续应答", a == expected, str(a))
    check("重启后节点B 凭持久化继续应答", b == expected, str(b))
    st = wait_status(lambda s: s["overall_status"] == "synced", timeout=20)
    check("心跳恢复后管理端重新判定 synced",
          st["overall_status"] == "synced", json.dumps(st))


def scenario_validation() -> None:
    print("\n=== 7. 输入校验 ===")
    code, _ = http("POST", "/publishes", {"addresses": []})
    check("空地址列表拒绝", code == 422, str(code))
    code, _ = http("POST", "/publishes", {"addresses": ["10.0.0.999"]})
    check("非法 IPv4 拒绝", code == 422, str(code))
    code, _ = http("POST", "/publishes", {"addresses": ["10.0.0.1", "10.0.0.1"]})
    check("重复地址拒绝", code == 422, str(code))


def main() -> int:
    global tmpdir
    tmpdir = Path(tempfile.mkdtemp(prefix="dns-pub-test-"))
    print(f"临时数据目录: {tmpdir}")
    try:
        start_mgmt()
        if not wait_http(MGMT_PORT, "/status", timeout=20):
            print("管理服务启动失败")
            if procs.get("mgmt"):
                print(procs["mgmt"].stdout.read(4000))
            return 2
        for spec in NODE_SPECS:
            start_node(spec)
            if not wait_http(spec["http"], "/health", timeout=15):
                print(f"{spec['id']} 启动失败")
                return 2

        scenario_basic()
        scenario_outage()
        scenario_recovery()
        scenario_stale()
        scenario_interleave()
        scenario_snapshot_rollback()
        scenario_concurrent()
        scenario_persistence()
        scenario_validation()
    finally:
        teardown()

    print(f"\n==== 结果: {PASS} 通过, {FAIL} 失败 ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
