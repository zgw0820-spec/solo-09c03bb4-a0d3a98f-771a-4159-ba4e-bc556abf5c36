# 单域名 A 记录发布系统（FastAPI + SQLite + dnslib）

机房设备管理域名的 A 记录发布：管理服务生成**严格递增修订**，向两台**独立持久化**的
UDP 权威 DNS 节点整列表下发；运维以两端**实际 DNS 答复**核对一致后再安排切流。

## 架构

```
                 ┌──────────────────────────────┐
  Swagger /docs  │  mgmt (FastAPI + SQLite/WAL)  │
  POST /publishes│  修订表 / 节点状态 / 对账循环   │
                 └───────────────┬──────────────┘
                HTTP /publish    │     ▲ heartbeat / report
              ┌──────────────────┴─────┴──────────────────┐
              ▼                                             ▼
     ┌─────────────────┐                           ┌─────────────────┐
     │ node-a (dnslib) │◄────── UDP A 查询 ───────►│ node-b (dnslib) │
     │ 独立 SQLite 卷   │                           │ 独立 SQLite 卷   │
     └─────────────────┘                           └─────────────────┘
        10053/udp (宿主)                               10054/udp (宿主)
```

* **修订递增**：每次 `POST /publishes` 在 `BEGIN IMMEDIATE` 事务内写入，
  SQLite 保留锁串行化并发请求，修订号用 AUTOINCREMENT，绝不重号/乱序。
* **只接受较新修订**：节点对普通推送 `revision <= 已服务修订` 一律 409 拒绝，
  迟到旧推送无法让节点回退。
* **快照恢复可识别、可回退**：节点卷若被旧快照替换，其实际服务修订会**倒退**。
  管理端不再盲信上报，而是把节点“自称版本”(`reported_revision`) 与自己
  **亲自取得的实据**（节点 `/health`、真实 UDP 答复）分开：自称版本只能触发
  `rollback_suspected` 怀疑，不能抬高管理端观察值；管理端经实据确认倒退后，
  撤销既有同步结论并补发**最新整份地址**。旧进程迟到的高版本上报无法掩盖
  恢复节点的旧数据，恢复期的新发布也不会被旧上报误判为已生效。
* **以实答为唯一同步依据**：只有节点真实 UDP 答复与现行修订一致（且证据新鲜）
  才记 `verified_revision` 并可报 `synced`；探测失败/超时会使既有验证失效，
  节点立即回到待补。地址列表在节点侧单事务整行替换，应答要么全旧、要么全新。
* **失联不误报**：`/status` 区分 `synced / syncing / degraded-node-offline`；
  单节点失联时另一节点继续应答，待补节点显示 `waiting-node-recovery`
  （回滚补发中为 `rollback-repushing`），`synced` 恒为 false；恢复后后台
  对账循环自动补齐。
* **实答核对**：`GET /verify` 与 `scripts/verify_dns.py` 都直接发 UDP 查询，
  以两端真实答复比对最新修订，而不是只看控制面状态。

## 启动（Docker Compose）

```bash
docker compose up -d --build
# Swagger:
open http://localhost:8000/docs
```

发布与核对：

```bash
curl -X POST http://localhost:8000/publishes -H 'Content-Type: application/json' \
  -d '{"addresses":["10.10.1.11","10.10.1.12"]}'
curl http://localhost:8000/status
curl http://localhost:8000/verify

# 运维侧直接核对两端 UDP 实答（切流前必做）
python scripts/verify_dns.py device.example.internal 127.0.0.1:10053 127.0.0.1:10054
```

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/publishes` | 提交完整地址列表，生成新修订（201） |
| GET  | `/status` | 最新修订、各节点在线/自称版本/观察版本/实答验证版本、回滚怀疑与待补状态、总体结论 |
| GET  | `/verify` | 用两端实际 UDP DNS 答复核对最新修订 |
| GET  | `/revisions` | 修订历史 |
| POST | `/reconcile` | 手动触发一次对账（实答探测 + 必要时整列表补发） |
| GET  | `/docs` | Swagger UI |

`/status` 中每个节点的关键字段：

| 字段 | 含义 |
| --- | --- |
| `reported_revision` | 节点心跳/上报的**自称**版本（允许变小，仅作线索，不参与同步结论） |
| `served_revision` | 管理端经 `/health`、推送应答等**实时实据**观察到的版本（确认回滚时可下降） |
| `verified_revision` / `verification_fresh` | 最近一次真实 UDP 答复命中的修订及证据是否仍新鲜 |
| `rollback_suspected` | 自称/实据显示版本倒退，正在补发整列表 |
| `caught_up` / `catch_up_status` | 是否凭新鲜实答追上现行修订；`served` / `rollback-repushing` / `pending-retry` / `waiting-node-recovery` |

## 本地无 Docker 自测

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python scripts/selftest.py
```

自测覆盖：发布同步与实答一致、单节点失联（不误报）、恢复补齐、
迟到旧修订 409 拒绝、迟到旧/高版本上报均不能改动观察值、交错乱序下发、
**旧卷快照恢复（版本倒退识别 → 撤销同步 → 整列表补发 → 实答一致才重新同步、
恢复期发布不被旧上报误判）**、并发发布修订连续、节点重启持久化、输入校验。
