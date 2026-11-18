"""SQLite persistence for the management service.

Concurrency model
-----------------
Every revision insert runs inside a ``BEGIN IMMEDIATE`` transaction, so two
concurrent POST requests are serialised by SQLite's reserved lock and the
autoincrement-style revision number can never be duplicated or reordered.

Rollback-safe node state
------------------------
A node's volume can be restored from an old snapshot, so the revision it
actually serves may go *backwards*.  The state of a node is therefore split
into three independent numbers:

* ``reported_revision`` -- what the node itself claims in heartbeats/reports.
  Any value is accepted (including decreases) so a restored node's low claim
  is visible; a claim lower than the observed revision raises
  ``rollback_suspected``.  Claims alone can never move the observed state
  upwards, so a late high-revision report from a stale old process cannot
  mask a recovered node's old data.
* ``served_revision`` -- the revision management has itself *observed* the
  node serving (live ``/health`` reads, push responses, DNS probes).  This
  value may decrease when live evidence confirms a rollback.
* ``verified_revision`` / ``verified_at`` -- the last revision whose real UDP
  DNS answers matched the management revision, and when that check happened.
  A node counts as caught up only while this is fresh and equals the latest
  revision; nothing else can produce a "synced" conclusion.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterable

DB_PATH = os.environ.get("MGMT_DB_PATH", "/data/management.db")
# A node that has not reported within this many seconds is considered offline.
OFFLINE_AFTER = float(os.environ.get("OFFLINE_AFTER_SECONDS", "15"))

_init_lock = threading.Lock()
_initialised = False

# Columns added after the first release; old management volumes are migrated
# in-place with ALTER TABLE.
_EXTRA_COLUMNS: dict[str, str] = {
    "reported_revision": "INTEGER NOT NULL DEFAULT 0",
    "verified_revision": "INTEGER NOT NULL DEFAULT 0",
    "verified_at": "REAL NOT NULL DEFAULT 0",
    "rollback_suspected": "INTEGER NOT NULL DEFAULT 0",
    "last_verify_error": "TEXT NOT NULL DEFAULT ''",
    "last_answers": "TEXT NOT NULL DEFAULT ''",
}


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_conn():
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    global _initialised
    with _init_lock:
        if _initialised:
            return
        with get_conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS revisions (
                    revision      INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain        TEXT NOT NULL,
                    addresses     TEXT NOT NULL,
                    created_at    REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS node_state (
                    node_id              TEXT PRIMARY KEY,
                    served_revision      INTEGER NOT NULL DEFAULT 0,
                    reported_revision    INTEGER NOT NULL DEFAULT 0,
                    verified_revision    INTEGER NOT NULL DEFAULT 0,
                    verified_at          REAL NOT NULL DEFAULT 0,
                    rollback_suspected   INTEGER NOT NULL DEFAULT 0,
                    last_verify_error    TEXT NOT NULL DEFAULT '',
                    last_answers         TEXT NOT NULL DEFAULT '',
                    last_seen            REAL NOT NULL DEFAULT 0,
                    target_revision      INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            # Migrate pre-rollback-aware management databases.
            existing = {r["name"] for r in conn.execute("PRAGMA table_info(node_state)")}
            for name, decl in _EXTRA_COLUMNS.items():
                if name not in existing:
                    conn.execute(f"ALTER TABLE node_state ADD COLUMN {name} {decl}")
        _initialised = True


@contextmanager
def revision_tx():
    """Serialise writers with a reserved lock for the whole transaction."""
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def insert_revision(conn: sqlite3.Connection, domain: str, addresses: list[str]) -> int:
    cur = conn.execute(
        "INSERT INTO revisions(domain, addresses, created_at) VALUES (?, ?, ?)",
        (domain, json.dumps(addresses), time.time()),
    )
    rev = int(cur.lastrowid)
    # Every known node now has this revision as its target. A new revision
    # automatically invalidates any earlier verification (verified_revision
    # can no longer equal the latest revision until real answers prove it).
    conn.execute("UPDATE node_state SET target_revision = ?", (rev,))
    return rev


def latest_revision(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT revision, domain, addresses, created_at "
        "FROM revisions ORDER BY revision DESC LIMIT 1"
    ).fetchone()


def get_revision(conn: sqlite3.Connection, revision: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT revision, domain, addresses, created_at "
        "FROM revisions WHERE revision = ?",
        (revision,),
    ).fetchone()


def list_revisions(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(
        conn.execute("SELECT revision, domain, addresses, created_at "
                     "FROM revisions ORDER BY revision DESC")
    )


def get_node_state(conn: sqlite3.Connection, node_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM node_state WHERE node_id = ?", (node_id,)
    ).fetchone()


def record_claim(conn: sqlite3.Connection, node_id: str, claimed: int) -> bool:
    """Record a node's own claim (heartbeat or /report).

    The claim is stored verbatim in ``reported_revision`` (decreases allowed)
    and refreshes ``last_seen``.  A claim below the management-observed
    revision raises the ``rollback_suspected`` flag, but *never* changes the
    observed revision by itself: only management's own live probes may do
    that, so a stale high-revision report from an old process cannot mask a
    recovered node's old data.

    Returns True when the claim indicates a possible rollback.
    """
    now = time.time()
    conn.execute(
        """
        INSERT INTO node_state(node_id, reported_revision, last_seen)
        VALUES (?, ?, ?)
        ON CONFLICT(node_id) DO UPDATE SET
            reported_revision  = excluded.reported_revision,
            last_seen          = excluded.last_seen,
            rollback_suspected = CASE
                WHEN excluded.reported_revision < node_state.served_revision
                THEN 1 ELSE node_state.rollback_suspected END
        """,
        (node_id, int(claimed), now),
    )
    row = get_node_state(conn, node_id)
    return bool(row and row["rollback_suspected"])


def set_observed(conn: sqlite3.Connection, node_id: str, revision: int) -> bool:
    """Set the served revision from management's own live evidence.

    Evidence sources are direct ``/health`` reads and push responses, i.e.
    the node's current process talking about its current database.  Unlike a
    fire-and-forget report, this may confirm a rollback and therefore move
    the value downwards.  Returns True when a decrease (rollback) was seen.
    """
    row = get_node_state(conn, node_id)
    previous = row["served_revision"] if row else 0
    conn.execute(
        """
        INSERT INTO node_state(node_id, served_revision)
        VALUES (?, ?)
        ON CONFLICT(node_id) DO UPDATE SET
            served_revision    = excluded.served_revision,
            rollback_suspected = CASE
                WHEN excluded.served_revision < node_state.served_revision
                THEN 1 ELSE node_state.rollback_suspected END
        """,
        (node_id, int(revision)),
    )
    return int(revision) < int(previous)


def set_verified(
    conn: sqlite3.Connection, node_id: str, revision: int, answers: list[str]
) -> None:
    """Record that real UDP DNS answers prove the node serves ``revision``."""
    conn.execute(
        """
        UPDATE node_state
        SET verified_revision  = ?,
            verified_at        = ?,
            last_answers       = ?,
            last_verify_error  = '',
            rollback_suspected = 0,
            served_revision    = MAX(served_revision, ?)
        WHERE node_id = ?
        """,
        (int(revision), time.time(), json.dumps(sorted(answers)),
         int(revision), node_id),
    )


def set_verify_failed(conn: sqlite3.Connection, node_id: str, error: str) -> None:
    """Invalidate the node's current verification after a failed probe.

    A timeout, unreachable node or mismatching answer means management no
    longer holds *fresh successful* DNS evidence for this node, so the old
    verification must stop counting as "caught up".  The observed
    ``served_revision`` is kept (it was obtained from live evidence and may
    still be informative), but ``verified_revision`` / ``verified_at`` are
    cleared and the node is treated as pending until the next good probe.
    """
    conn.execute(
        """
        UPDATE node_state
        SET verified_revision = 0,
            verified_at       = 0,
            last_verify_error = ?
        WHERE node_id = ?
        """,
        (str(error)[:500], node_id),
    )


def all_node_state(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT node_id, served_revision, reported_revision, "
        "verified_revision, verified_at, rollback_suspected, "
        "last_verify_error, last_answers, last_seen, target_revision "
        "FROM node_state ORDER BY node_id"
    ))


def ensure_expected_nodes(conn: sqlite3.Connection, node_ids: Iterable[str]) -> None:
    for node_id in node_ids:
        conn.execute(
            "INSERT OR IGNORE INTO node_state(node_id) VALUES (?)", (node_id,)
        )


def row_to_revision_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "revision": row["revision"],
        "domain": row["domain"],
        "addresses": json.loads(row["addresses"]),
        "created_at": row["created_at"],
    }
