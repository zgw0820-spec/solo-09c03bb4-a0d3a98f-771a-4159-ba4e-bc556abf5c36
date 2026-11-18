"""FastAPI management service for the single-domain A-record publisher.

Responsibilities
----------------
* Accept address-list submissions (Swagger / OpenAPI at ``/docs``) and turn
  each one into a new strictly increasing revision in SQLite.
* Push every revision to both independent DNS nodes and keep reconciling
  until each node serves the newest revision.
* Detect a node that has been restored from an old volume snapshot: its real
  served revision can move *backwards*.  Management observes the node itself
  (live ``/health`` reads and real UDP DNS probes) instead of trusting
  fire-and-forget reports, withdraws any earlier "synced" conclusion on
  rollback and re-pushes the whole latest address list.
* Report per-node claimed / observed / verified revision, online/offline
  state and catch-up status.  The aggregate conclusion can only read
  "synced" while *both* nodes' real DNS answers freshly match the current
  revision.
* Verify the latest revision against the real UDP DNS answers of both nodes.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

from dnslib import QTYPE, RCODE, DNSRecord
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from . import db

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
DOMAIN = os.environ.get("MANAGED_DOMAIN", "device.example.internal").rstrip(".")

def _load_nodes() -> list[dict[str, Any]]:
    raw = os.environ.get(
        "NODES",
        '[{"id":"node-a","control_url":"http://127.0.0.1:8053",'
        '"dns_host":"127.0.0.1","dns_port":10053},'
        '{"id":"node-b","control_url":"http://127.0.0.1:8054",'
        '"dns_host":"127.0.0.1","dns_port":10054}]',
    )
    nodes = json.loads(raw)
    for n in nodes:
        n.setdefault("dns_port", 5353)
    return nodes


NODES: list[dict[str, Any]] = _load_nodes()
NODE_IDS = [n["id"] for n in NODES]
RECONCILE_INTERVAL = float(os.environ.get("RECONCILE_INTERVAL", "3"))
PUSH_TIMEOUT = float(os.environ.get("PUSH_TIMEOUT", "4"))
# A DNS-based verification is treated as evidence only for this many seconds.
# After it goes stale the node must be re-probed before "synced" is reported.
VERIFY_FRESHNESS = float(os.environ.get("VERIFY_FRESHNESS_SECONDS", "15"))


# --------------------------------------------------------------------------- #
# Schemas
# --------------------------------------------------------------------------- #
class PublishRequest(BaseModel):
    addresses: list[str] = Field(
        ...,
        min_length=1,
        description="Complete A-record address list. The whole list replaces "
                    "the previous one atomically on every node.",
        examples=[["10.10.1.11", "10.10.1.12"]],
    )

    @field_validator("addresses")
    @classmethod
    def _valid_ips(cls, v: list[str]) -> list[str]:
        seen: set[str] = set()
        for ip in v:
            parts = ip.split(".")
            if len(parts) != 4 or not all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
                raise ValueError(f"{ip!r} is not a valid IPv4 address")
            if ip in seen:
                raise ValueError(f"duplicate address {ip!r}")
            seen.add(ip)
        return v


class ReportRequest(BaseModel):
    served_revision: int = Field(..., ge=0)


# --------------------------------------------------------------------------- #
# Blocking node interaction (executed in worker threads)
# --------------------------------------------------------------------------- #
def _http_post(url: str, payload: dict, timeout: float = PUSH_TIMEOUT) -> tuple[int, dict]:
    req = urlrequest.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urlerror.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read() or b"{}")
        except Exception:
            return exc.code, {}
    except Exception as exc:
        raise ConnectionError(str(exc)) from exc


def _http_get(url: str, timeout: float = PUSH_TIMEOUT) -> dict:
    try:
        with urlrequest.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read() or b"{}")
    except Exception as exc:
        raise ConnectionError(str(exc)) from exc


def push_to_node(node: dict, revision: int, domain: str, addresses: list[str]) -> dict:
    """Push one (whole-list) revision to one node.

    A 409 means the node's *current* process already serves a newer
    revision; its real served revision is then fetched directly instead of
    being inferred from any report.
    """
    try:
        code, body = _http_post(
            f"{node['control_url'].rstrip('/')}/publish",
            {"revision": revision, "domain": domain, "addresses": addresses},
        )
    except ConnectionError as exc:
        return {"ok": False, "status": "unreachable", "detail": str(exc)}

    if code == 200:
        return {"ok": True, "status": "applied",
                "served_revision": body.get("served_revision", revision)}
    if code == 409:
        try:
            health = _http_get(f"{node['control_url'].rstrip('/')}/health")
            return {"ok": True, "status": "already-newer",
                    "served_revision": int(health.get("revision", revision))}
        except ConnectionError as exc:
            return {"ok": False, "status": "unreachable", "detail": str(exc)}
    return {"ok": False, "status": f"http-{code}", "detail": body}


def _dns_query(host: str, port: int, domain: str, timeout: float = 3.0) -> dict:
    import socket

    question = DNSRecord.question(domain, qtype="A")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        sock.sendto(question.pack(), (host, int(port)))
        data, _ = sock.recvfrom(65535)
    finally:
        sock.close()
    reply = DNSRecord.parse(data)
    answers = []
    for rr in reply.rr:
        if QTYPE[rr.rtype] == "A":
            answers.append(str(rr.rdata))
    return {
        "rcode": RCODE[reply.header.rcode],
        "aa": bool(reply.header.aa),
        "answers": sorted(answers),
    }


# --------------------------------------------------------------------------- #
# Evidence-based per-node observation / reconciliation
# --------------------------------------------------------------------------- #
def _observe_and_reconcile_node(node: dict, rev: dict) -> dict[str, Any]:
    """Blocking worker: gather live evidence for one node and catch it up.

    Order of operations, all in this worker thread:
      1. read the node's current /health (its own current process/database);
      2. send a real UDP DNS query and compare against the target revision;
      3. push the whole latest list whenever the real answer does not match;
      4. probe DNS again after a push;
      5. persist every finding through db helpers.
    """
    node_id = node["id"]
    target = rev["revision"]
    expected = sorted(rev["addresses"])
    result: dict[str, Any] = {
        "node_id": node_id, "target_revision": target,
        "health_revision": None, "push": None, "rollback": False,
    }

    # 1) Live control-plane evidence from the node itself.
    health_rev: int | None = None
    try:
        health = _http_get(f"{node['control_url'].rstrip('/')}/health")
        health_rev = int(health.get("revision", 0))
        result["health_revision"] = health_rev
        with db.get_conn() as conn:
            if db.set_observed(conn, node_id, health_rev):
                result["rollback"] = True
    except ConnectionError as exc:
        result["health_error"] = str(exc)

    # 2) Real DNS evidence against the current revision.
    def probe() -> tuple[dict | None, str | None]:
        try:
            answer = _dns_query(node["dns_host"], node["dns_port"], rev["domain"])
            if not answer["aa"]:
                return answer, f"non-authoritative answer (AA=0, rcode={answer['rcode']})"
            if answer["answers"] != expected:
                return answer, (
                    f"dns answers {answer['answers']} do not match revision "
                    f"{target} {expected}"
                )
            return answer, None
        except Exception as exc:  # unreachable / timeout / parse failure
            return None, f"dns probe failed: {exc}"

    answer, mismatch = probe()

    # 3) Any mismatch (stale answers, wrong data, unreachable DNS while the
    #    control plane is reachable) requires a full-list push.
    if mismatch is not None and health_rev is not None:
        outcome = push_to_node(node, target, rev["domain"], rev["addresses"])
        result["push"] = outcome
        if outcome.get("ok") and outcome.get("served_revision") is not None:
            pushed_rev = int(outcome["served_revision"])
            with db.get_conn() as conn:
                if db.set_observed(conn, node_id, pushed_rev):
                    result["rollback"] = True
            # 4) Re-probe the real answers; never trust the push response alone.
            answer, mismatch = probe()

    # 5) Persist verification evidence.
    with db.get_conn() as conn:
        if mismatch is None and answer is not None:
            db.set_verified(conn, node_id, target, answer["answers"])
            result["verified"] = True
            result["answers"] = answer["answers"]
        else:
            db.set_verify_failed(conn, node_id, mismatch or "no answer")
            result["verified"] = False
            result["detail"] = mismatch
    return result


_reconcile_lock = asyncio.Lock()
_reconcile_wakeup = asyncio.Event()


async def reconcile_once() -> dict[str, Any]:
    """Observe every node against the latest revision and push when behind.

    Whether a node needs a push is decided from *live* evidence (health +
    real DNS answers), never from a stored counter.  A snapshot-restored
    node therefore cannot be skipped as "already synced".
    """
    async with _reconcile_lock:
        with db.get_conn() as conn:
            latest = db.latest_revision(conn)
            if latest is None:
                return {"skipped": "no revision yet"}
            rev = db.row_to_revision_dict(latest)

        loop = asyncio.get_running_loop()
        outcomes = await asyncio.gather(
            *(loop.run_in_executor(None, _observe_and_reconcile_node, node, rev)
              for node in NODES),
            return_exceptions=True,
        )
        results: dict[str, Any] = {}
        for node, outcome in zip(NODES, outcomes):
            if isinstance(outcome, Exception):
                outcome = {"node_id": node["id"], "verified": False,
                           "detail": f"observe error: {outcome!r}"}
                with db.get_conn() as conn:
                    db.set_verify_failed(conn, node["id"], str(outcome))
            results[node["id"]] = outcome
        return {"revision": rev["revision"], "results": results}


async def reconciler_loop() -> None:
    while True:
        try:
            await reconcile_once()
        except Exception as exc:  # never let the loop die
            print(f"[mgmt] reconcile error: {exc!r}")
        try:
            await asyncio.wait_for(_reconcile_wakeup.wait(), timeout=RECONCILE_INTERVAL)
            _reconcile_wakeup.clear()
        except asyncio.TimeoutError:
            pass


def _request_reconcile() -> None:
    """Wake the background reconciler immediately (best effort)."""
    try:
        _reconcile_wakeup.set()
    except RuntimeError:
        pass


# --------------------------------------------------------------------------- #
# Application
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    with db.get_conn() as conn:
        db.ensure_expected_nodes(conn, NODE_IDS)
    task = asyncio.create_task(reconciler_loop())
    yield
    task.cancel()


app = FastAPI(
    title="单域名 A 记录发布 API",
    description="机房设备管理域名 A 记录发布：递增修订、双权威节点同步、实答核对、"
                "快照回滚识别与自动补齐。",
    version="1.1.0",
    lifespan=lifespan,
)


@app.post("/publishes", status_code=201, summary="提交新的地址列表（生成新修订）")
async def create_publish(req: PublishRequest, background: BackgroundTasks) -> dict:
    # BEGIN IMMEDIATE serialises concurrent submissions; revisions stay
    # strictly increasing even under parallel POSTs.
    with db.revision_tx() as conn:
        revision = db.insert_revision(conn, DOMAIN, req.addresses)
        row = db.latest_revision(conn)
        rev = db.row_to_revision_dict(row)

    # Immediate push attempt; the background reconciler guarantees retries.
    background.add_task(reconcile_once)
    _request_reconcile()
    return {
        "revision": revision,
        "domain": rev["domain"],
        "addresses": rev["addresses"],
        "message": "修订已创建，正在向各节点下发；可通过 /status 查看同步进度。",
    }


def _node_view(node: dict, st, target: int, now: float) -> dict:
    served = st["served_revision"] if st else 0
    reported = st["reported_revision"] if st else 0
    verified_rev = st["verified_revision"] if st else 0
    verified_at = st["verified_at"] if st else 0
    last_seen = st["last_seen"] if st else 0
    rollback = bool(st["rollback_suspected"]) if st else False
    online = (now - last_seen) <= db.OFFLINE_AFTER if last_seen > 0 else False
    fresh = (now - verified_at) <= VERIFY_FRESHNESS if verified_at > 0 else False
    # Caught-up requires *fresh real DNS evidence* for the current revision.
    caught_up = (
        target > 0 and verified_rev == target and fresh and not rollback
    )
    if caught_up:
        catch_up_status = "served"
    elif not online:
        catch_up_status = "waiting-node-recovery"
    elif rollback:
        catch_up_status = "rollback-repushing"
    else:
        catch_up_status = "pending-retry"
    return {
        "node_id": node["id"],
        "online": online,
        "served_revision": served,
        "reported_revision": reported,
        "verified_revision": verified_rev,
        "verification_fresh": fresh,
        "rollback_suspected": rollback,
        "target_revision": target,
        "last_seen_seconds_ago": round(now - last_seen, 1) if last_seen else None,
        "last_verify_error": st["last_verify_error"] if st else "",
        "caught_up": caught_up,
        "catch_up_status": catch_up_status,
    }


@app.get("/status", summary="查看最新修订与各节点服务版本/待补状态")
async def status() -> dict:
    now = time.time()
    with db.get_conn() as conn:
        latest = db.latest_revision(conn)
        rows = db.all_node_state(conn)
        states = {r["node_id"]: r for r in rows}

    latest_rev = db.row_to_revision_dict(latest) if latest else None
    target = latest_rev["revision"] if latest_rev else 0

    node_views: list[dict] = []
    offline_ids, pending_ids = [], []
    for node in NODES:
        view = _node_view(node, states.get(node["id"]), target, now)
        if not view["online"]:
            offline_ids.append(node["id"])
        if not view["caught_up"]:
            pending_ids.append(node["id"])
        node_views.append(view)

    if latest_rev is None:
        overall = "no-revision-yet"
    elif offline_ids:
        # A node is missing: the other node keeps answering, but the pair is
        # NOT fully synced. Never report success on assumptions.
        overall = "degraded-node-offline"
    elif pending_ids:
        overall = "syncing"
    else:
        overall = "synced"

    return {
        "domain": DOMAIN,
        "latest_revision": latest_rev,
        "overall_status": overall,
        "synced": overall == "synced",
        "offline_nodes": offline_ids,
        "pending_nodes": pending_ids,
        "nodes": node_views,
    }


@app.get("/revisions", summary="修订历史")
async def revisions() -> list[dict]:
    with db.get_conn() as conn:
        return [db.row_to_revision_dict(r) for r in db.list_revisions(conn)]


@app.get("/verify", summary="以两端实际 DNS 答复核对最新修订")
async def verify() -> dict:
    with db.get_conn() as conn:
        latest = db.latest_revision(conn)
        if latest is None:
            raise HTTPException(409, "尚无修订，无法核对")
        rev = db.row_to_revision_dict(latest)

    loop = asyncio.get_running_loop()
    findings = []
    all_match = True
    for node in NODES:
        try:
            answer = await loop.run_in_executor(
                None, _dns_query, node["dns_host"], node["dns_port"], rev["domain"]
            )
            match = answer["answers"] == sorted(rev["addresses"]) and answer["aa"]
            # Persist the fresh evidence so /status reflects this probe too.
            with db.get_conn() as conn:
                if match:
                    db.set_verified(conn, node["id"], rev["revision"], answer["answers"])
                else:
                    db.set_verify_failed(
                        conn, node["id"],
                        f"answers {answer['answers']} != {sorted(rev['addresses'])}",
                    )
        except Exception as exc:
            answer = {"error": str(exc)}
            match = False
            with db.get_conn() as conn:
                db.set_verify_failed(conn, node["id"], str(exc))
        all_match &= match
        findings.append({"node_id": node["id"], **answer,
                         "matches_latest_revision": match})
    return {
        "domain": rev["domain"],
        "expected_revision": rev["revision"],
        "expected_addresses": sorted(rev["addresses"]),
        "all_nodes_match": all_match,
        "nodes": findings,
    }


@app.post("/reconcile", summary="手动触发一次对账下发")
async def manual_reconcile() -> dict:
    return await reconcile_once()


# --------------------------------------------------------------------------- #
# Internal endpoints used by the DNS nodes themselves
# --------------------------------------------------------------------------- #
@app.post("/internal/nodes/{node_id}/heartbeat")
async def heartbeat(node_id: str, req: ReportRequest) -> dict:
    if node_id not in NODE_IDS:
        raise HTTPException(404, "unknown node")
    with db.get_conn() as conn:
        # record_claim also refreshes last_seen, marking the node online.
        rollback = db.record_claim(conn, node_id, req.served_revision)
    if rollback:
        # A restored node is claiming an older revision: withdraw any earlier
        # sync conclusion immediately and re-push the whole latest list.
        _request_reconcile()
    return {"ok": True, "rollback_suspected": rollback}


@app.post("/internal/nodes/{node_id}/report")
async def report(node_id: str, req: ReportRequest) -> dict:
    if node_id not in NODE_IDS:
        raise HTTPException(404, "unknown node")
    with db.get_conn() as conn:
        rollback = db.record_claim(conn, node_id, req.served_revision)
    _request_reconcile()
    return {"ok": True, "rollback_suspected": rollback}
