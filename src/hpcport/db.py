"""Global SQLite state store for HPCPort. The daemon is the sole writer.

One shared connection protected by an RLock, in WAL mode. Helper methods return
plain dicts; JSON columns are serialized by the owning services.
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import closing, contextmanager
from pathlib import Path

SCHEMA_VERSION = 3

PROJECT_RETIREMENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS retired_projects (
  name TEXT PRIMARY KEY,
  retired_at REAL NOT NULL
);
"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
  project TEXT NOT NULL,
  batch TEXT NOT NULL,
  cluster TEXT NOT NULL,
  workdir TEXT NOT NULL,
  tag TEXT NOT NULL DEFAULT '',
  desc TEXT NOT NULL,
  fail_alerted INTEGER NOT NULL DEFAULT 0,
  done_notified INTEGER NOT NULL DEFAULT 0,
  created REAL NOT NULL,
  PRIMARY KEY (project, batch)
);

CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project TEXT NOT NULL,
  batch TEXT NOT NULL,
  submit_no INTEGER NOT NULL,
  cluster TEXT NOT NULL,
  workdir TEXT NOT NULL,
  script TEXT NOT NULL,
  partition TEXT,
  resources TEXT,
  slurm_id TEXT,
  state TEXT NOT NULL,
  slurm_state TEXT,
  exit_code TEXT,
  source TEXT,
  misses INTEGER NOT NULL DEFAULT 0,
  changed REAL,
  error TEXT,
  t_register REAL NOT NULL,
  t_submit REAL,
  t_start REAL,
  t_end REAL,
  approval INTEGER,
  core_hours REAL,
  gpu_hours REAL,
  cores REAL,
  gpus REAL,
  run_s REAL,
  cancel_by TEXT,
  cancel_at REAL,
  cancel_note TEXT,
  dispatch_token TEXT,
  dispatch_attempted INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS jobs_proj_batch ON jobs(project, batch);
CREATE INDEX IF NOT EXISTS jobs_cluster_state ON jobs(cluster, state);
CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state);

CREATE TABLE IF NOT EXISTS ops (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project TEXT NOT NULL,
  kind TEXT NOT NULL,                 -- exec | transfer
  cluster TEXT NOT NULL,
  spec TEXT NOT NULL,                 -- JSON
  dir TEXT,
  limit_s REAL NOT NULL,
  state TEXT NOT NULL,                -- running | ended | lost
  result TEXT,                        -- JSON
  cleaned INTEGER NOT NULL DEFAULT 0,
  stop_by TEXT,
  started REAL NOT NULL,
  ended REAL
);
CREATE INDEX IF NOT EXISTS ops_proj_state ON ops(project, state);

CREATE TABLE IF NOT EXISTS approvals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project TEXT,                       -- NULL for global preset edits without a project
  kind TEXT NOT NULL,                 -- submit | permission | proposal
  summary TEXT NOT NULL,
  detail TEXT NOT NULL,               -- JSON
  state TEXT NOT NULL,                -- pending | approved | rejected | cancelled
  comment TEXT,
  created REAL NOT NULL,
  decided REAL
);
CREATE INDEX IF NOT EXISTS approvals_state ON approvals(state);

CREATE TABLE IF NOT EXISTS permission_grants (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project TEXT NOT NULL,
  tool TEXT NOT NULL,
  input_sha256 TEXT NOT NULL,
  approval INTEGER NOT NULL,
  created REAL NOT NULL,
  used REAL
);

CREATE TABLE IF NOT EXISTS proposals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project TEXT,
  doc TEXT NOT NULL,                  -- cluster_preset:<cluster> | project_config:<project>
  origin TEXT NOT NULL,               -- agent | user | detected
  patch TEXT,                         -- JSON
  sha256 TEXT NOT NULL,
  base_version INTEGER,
  version INTEGER NOT NULL,
  summary TEXT NOT NULL,
  state TEXT NOT NULL,                -- pending | approved | rejected | superseded | stale | cancelled
  approval INTEGER,
  comment TEXT,
  created REAL NOT NULL,
  decided REAL
);

CREATE TABLE IF NOT EXISTS waits (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project TEXT NOT NULL,
  session_key TEXT NOT NULL,
  groups TEXT NOT NULL,               -- JSON: [{"items": ["b001" | "op:12"], "priority": 0|1|2}]
  fail_frac REAL NOT NULL,
  timeout_at REAL NOT NULL,
  timeout_h REAL NOT NULL,
  note TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL,                -- active | fired | cancelled | expired
  result TEXT,                        -- JSON once fired
  created REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS waits_state ON waits(state);

CREATE TABLE IF NOT EXISTS mcp_tasks (
  task_id TEXT PRIMARY KEY,
  project TEXT NOT NULL,
  wait_id INTEGER NOT NULL,
  status TEXT NOT NULL,               -- working | completed | failed | cancelled
  status_message TEXT,
  result_json TEXT,
  poll_interval_ms INTEGER NOT NULL DEFAULT 15000,
  ttl_ms INTEGER NOT NULL DEFAULT 604800000,
  created REAL NOT NULL,
  updated REAL NOT NULL,
  expires_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS notices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project TEXT NOT NULL,
  reason TEXT NOT NULL,
  subject TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL,                 -- JSON
  created REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS notices_proj ON notices(project, id);

CREATE TABLE IF NOT EXISTS session_cursors (
  session_key TEXT NOT NULL,
  project TEXT NOT NULL,
  last_notice_id INTEGER NOT NULL DEFAULT 0,
  updated REAL NOT NULL,
  PRIMARY KEY (session_key, project)
);

CREATE TABLE IF NOT EXISTS unregistered (
  cluster TEXT NOT NULL,
  slurm_id TEXT NOT NULL,
  project TEXT NOT NULL,
  workdir TEXT NOT NULL,
  first_seen REAL NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (cluster, slurm_id)
);

CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL NOT NULL,
  project TEXT,
  session_key TEXT,
  tool TEXT NOT NULL,
  args TEXT,
  result TEXT
);
CREATE INDEX IF NOT EXISTS audit_proj_id ON audit(project, id);

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);
""" + PROJECT_RETIREMENTS_SCHEMA


def read_retired_project_names(path: Path) -> set[str]:
    """Read the registry for commands that run outside the daemon."""
    if not path.is_file():
        return set()
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as con:
        if con.execute("PRAGMA user_version").fetchone()[0] < 3:
            return set()
        return {row[0] for row in con.execute("SELECT name FROM retired_projects")}


def add_column(table: str, column: str):
    def step(con):
        cols = {r[1] for r in con.execute("PRAGMA table_info(%s)" % table)}
        if cols and column.split()[0] not in cols:
            con.execute("ALTER TABLE %s ADD COLUMN %s" % (table, column))
    return step


def _init_done_notified(con):
    con.execute(
        "UPDATE batches SET done_notified = 1 "
        "WHERE EXISTS ("
        "  SELECT 1 FROM jobs j WHERE j.project = batches.project AND j.batch = batches.batch"
        ") AND NOT EXISTS ("
        "  SELECT 1 FROM jobs j WHERE j.project = batches.project AND j.batch = batches.batch "
        "  AND j.state IN ('unapproved', 'held', 'queued', 'running')"
        ")"
    )


MIGRATIONS: dict[int, list] = {
    1: [
        add_column("batches", "done_notified INTEGER NOT NULL DEFAULT 0"),
        add_column("jobs", "dispatch_token TEXT"),
        add_column("jobs", "dispatch_attempted INTEGER NOT NULL DEFAULT 0"),
        _init_done_notified,
    ],
    2: [PROJECT_RETIREMENTS_SCHEMA],
}


class DB:
    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._con = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._con.row_factory = sqlite3.Row
        self._migrate(path)
        self._con.execute("PRAGMA journal_mode=WAL")
        self._con.execute("PRAGMA synchronous=NORMAL")
        self._con.executescript(SCHEMA)
        self._con.execute("PRAGMA user_version = %d" % SCHEMA_VERSION)

    def _migrate(self, path) -> None:
        version = self._con.execute("PRAGMA user_version").fetchone()[0]
        exists = self._con.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'jobs'").fetchone()
        if not exists or version == SCHEMA_VERSION:
            return
        if version > SCHEMA_VERSION or any(v not in MIGRATIONS
                                           for v in range(version, SCHEMA_VERSION)):
            raise RuntimeError("%s has schema version %d, expected %d"
                               % (path, version, SCHEMA_VERSION))
        self._con.execute("BEGIN")
        try:
            for v in range(version, SCHEMA_VERSION):
                for step in MIGRATIONS[v]:
                    step(self._con) if callable(step) else self._con.execute(step)
            self._con.execute("PRAGMA user_version = %d" % SCHEMA_VERSION)
            self._con.execute("COMMIT")
        except Exception:
            self._con.execute("ROLLBACK")
            raise

    def close(self) -> None:
        with self._lock:
            self._con.close()

    @contextmanager
    def transaction(self):
        with self._lock:
            self._con.execute("BEGIN")
            with self._con:
                yield

    def all(self, sql: str, args: tuple = ()) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self._con.execute(sql, args).fetchall()]

    def one(self, sql: str, args: tuple = ()) -> dict | None:
        rows = self.all(sql, args)
        return rows[0] if rows else None

    def run(self, sql: str, args: tuple = ()) -> int:
        with self._lock:
            return self._con.execute(sql, args).lastrowid

    def insert(self, table: str, **values) -> int:
        cols = ", ".join(values)
        marks = ", ".join("?" for _ in values)
        return self.run("INSERT INTO %s (%s) VALUES (%s)" % (table, cols, marks),
                        tuple(values.values()))

    def update(self, table: str, where: dict, **values) -> None:
        sets = ", ".join("%s = ?" % k for k in values)
        cond = " AND ".join("%s = ?" % k for k in where)
        self.run("UPDATE %s SET %s WHERE %s" % (table, sets, cond),
                 tuple(values.values()) + tuple(where.values()))

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.one("SELECT value FROM meta WHERE key = ?", (key,))
        return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        self.run("INSERT INTO meta (key, value) VALUES (?, ?) "
                 "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))

    def delete_meta(self, key: str) -> None:
        self.run("DELETE FROM meta WHERE key = ?", (key,))

    def next_counter(self, key: str) -> int:
        with self._lock:
            row = self._con.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            n = int(row["value"]) + 1 if row else 1
            self._con.execute("INSERT INTO meta (key, value) VALUES (?, ?) "
                              "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                              (key, str(n)))
            return n
