"""
db_users.py — v18
==================
DuckDB-backed user management, session logging, and role definitions.

Roles:
  admin   — hardcoded AQBADMIN only; full DB, any-to-any matching
  posital — DB-stored; data restricted to POSITAL families; target = Posital
  nidec   — DB-stored; data restricted to NIDEC families;   target = Nidec
"""

import os, hashlib, datetime, duckdb
from typing import Optional

_HERE   = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_HERE, "data", "encoder_crossref.duckdb")

# ── Hardcoded admin (never in DB) ─────────────────────────────────────────────
ADMIN_USER = "AQBADMIN"
ADMIN_PASS = "aqbadmin2024"

# ── Role-based source/target definitions ──────────────────────────────────────
POSITAL_SOURCE_FAMILIES = {
    "Kubler": ["K58I_shaft", "K58I-PR_shaft", "5000"],
    "Sick":   ["DUS60E", "DFS60B"],
    "Lika":   ["MC58", "CX58"],
}
POSITAL_TARGET_MFR = "Posital"

NIDEC_SOURCE_FAMILIES = {
    "Kubler": ["K58I_shaft", "K58I-PR_shaft", "K80I"],
    "Sick":   ["DFS60B", "DFS60-S", "DBS60"],
    "Baumer": ["EIL580-S", "EIL580-I", "HOG86"],
    "EPC":    ["858S", "702S", "HS35"],
}
NIDEC_TARGET_MFR = "Nidec"

VALID_ROLES = ["posital", "nidec"]

# ── Password hashing ──────────────────────────────────────────────────────────
def _hash(pw: str) -> str:
    return hashlib.sha256(pw.strip().encode()).hexdigest()

# ── Table init ────────────────────────────────────────────────────────────────
def init_user_tables():
    con = duckdb.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id       VARCHAR PRIMARY KEY,
            password_hash VARCHAR NOT NULL,
            role          VARCHAR DEFAULT 'posital',
            search_limit  INTEGER DEFAULT 50,
            is_active     BOOLEAN DEFAULT TRUE,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes         VARCHAR DEFAULT ''
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS login_log (
            user_id    VARCHAR,
            action     VARCHAR,
            session_id VARCHAR,
            ts         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS search_log (
            user_id      VARCHAR,
            session_id   VARCHAR,
            part_number  VARCHAR,
            source_mfr   VARCHAR,
            target_mfr   VARCHAR,
            match_tier   VARCHAR,
            top_match_pn VARCHAR,
            top_score    DOUBLE,
            num_results  INTEGER,
            ts           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.close()

# ── Auth ──────────────────────────────────────────────────────────────────────
def authenticate(user_id: str, password: str) -> Optional[dict]:
    uid = user_id.strip().upper()
    if uid == ADMIN_USER:
        if password.strip() == ADMIN_PASS:
            return {"user_id": uid, "role": "admin", "search_limit": 999999,
                    "is_active": True, "is_admin": True}
        return None
    try:
        con = duckdb.connect(DB_PATH)
        row = con.execute(
            "SELECT user_id, role, search_limit, is_active FROM users "
            "WHERE user_id=? AND password_hash=?",
            [uid, _hash(password)]
        ).fetchone()
        con.close()
        if row and row[3]:
            return {"user_id": row[0], "role": row[1], "search_limit": row[2],
                    "is_active": True, "is_admin": False}
    except Exception:
        pass
    return None

# ── User management ───────────────────────────────────────────────────────────
def get_all_users() -> list:
    try:
        con = duckdb.connect(DB_PATH)
        rows = con.execute(
            "SELECT user_id, role, search_limit, is_active, created_at, notes "
            "FROM users ORDER BY created_at DESC"
        ).fetchall()
        con.close()
        return [{"user_id": r[0], "role": r[1], "search_limit": r[2],
                 "is_active": r[3], "created_at": r[4], "notes": r[5]}
                for r in rows]
    except Exception:
        return []

def create_user(user_id: str, password: str, role: str = "posital",
                search_limit: int = 50, notes: str = "") -> tuple:
    uid = user_id.strip().upper()
    if not uid or not password.strip():
        return False, "User ID and password are required."
    if uid == ADMIN_USER:
        return False, "Cannot create a user with that ID."
    if role not in VALID_ROLES:
        return False, f"Invalid role. Must be one of: {', '.join(VALID_ROLES)}"
    try:
        con = duckdb.connect(DB_PATH)
        if con.execute("SELECT user_id FROM users WHERE user_id=?", [uid]).fetchone():
            con.close(); return False, f"User '{uid}' already exists."
        con.execute(
            "INSERT INTO users (user_id, password_hash, role, search_limit, is_active, notes) "
            "VALUES (?,?,?,?,TRUE,?)",
            [uid, _hash(password), role, search_limit, notes]
        )
        con.close()
        return True, f"User '{uid}' created successfully."
    except Exception as e:
        return False, str(e)

def update_user(user_id: str, search_limit: int = None, is_active: bool = None,
                password: str = None, notes: str = None) -> tuple:
    uid = user_id.strip().upper()
    try:
        con = duckdb.connect(DB_PATH)
        if search_limit is not None:
            con.execute("UPDATE users SET search_limit=? WHERE user_id=?", [search_limit, uid])
        if is_active is not None:
            con.execute("UPDATE users SET is_active=? WHERE user_id=?", [is_active, uid])
        if password and password.strip():
            con.execute("UPDATE users SET password_hash=? WHERE user_id=?", [_hash(password), uid])
        if notes is not None:
            con.execute("UPDATE users SET notes=? WHERE user_id=?", [notes, uid])
        con.close()
        return True, "Updated."
    except Exception as e:
        return False, str(e)

def delete_user(user_id: str) -> tuple:
    uid = user_id.strip().upper()
    try:
        con = duckdb.connect(DB_PATH)
        con.execute("DELETE FROM users WHERE user_id=?", [uid])
        con.close()
        return True, f"User '{uid}' deleted."
    except Exception as e:
        return False, str(e)

def get_search_count(user_id: str) -> int:
    try:
        con = duckdb.connect(DB_PATH)
        n = con.execute(
            "SELECT COUNT(*) FROM search_log WHERE user_id=?", [user_id]
        ).fetchone()[0]
        con.close()
        return n
    except Exception:
        return 0

# ── Logging ───────────────────────────────────────────────────────────────────
def log_login(user_id: str, action: str, session_id: str):
    try:
        con = duckdb.connect(DB_PATH)
        con.execute(
            "INSERT INTO login_log (user_id, action, session_id) VALUES (?,?,?)",
            [user_id, action, session_id]
        )
        con.close()
    except Exception:
        pass

def log_search(user_id: str, session_id: str, part_number: str,
               source_mfr: str, target_mfr: str, match_tier: str,
               top_match_pn: str, top_score: float, num_results: int):
    """top_score must be stored as fraction (0.0–1.0), not percentage."""
    try:
        con = duckdb.connect(DB_PATH)
        con.execute(
            "INSERT INTO search_log "
            "(user_id, session_id, part_number, source_mfr, target_mfr, "
            "match_tier, top_match_pn, top_score, num_results) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            [user_id, session_id, part_number, source_mfr, target_mfr,
             match_tier, top_match_pn, float(top_score) / 100.0 if top_score > 1.0 else float(top_score),
             num_results]
        )
        con.close()
    except Exception as e:
        import traceback
        print(f"[log_search ERROR] {e}\n{traceback.format_exc()}")

# ── Analytics ─────────────────────────────────────────────────────────────────
def get_platform_stats() -> dict:
    try:
        con = duckdb.connect(DB_PATH)
        total_users  = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        active_users = con.execute("SELECT COUNT(*) FROM users WHERE is_active=TRUE").fetchone()[0]
        total_searches = con.execute("SELECT COUNT(*) FROM search_log").fetchone()[0]
        today = datetime.date.today().isoformat()
        searches_today = con.execute(
            "SELECT COUNT(*) FROM search_log WHERE ts::DATE=?", [today]
        ).fetchone()[0]
        top_pn = con.execute(
            "SELECT part_number, COUNT(*) c FROM search_log "
            "GROUP BY part_number ORDER BY c DESC LIMIT 1"
        ).fetchone()
        con.close()
        return {"total_users": total_users, "active_users": active_users,
                "total_searches": total_searches, "searches_today": searches_today,
                "top_part": top_pn[0] if top_pn else "—"}
    except Exception:
        return {"total_users": 0, "active_users": 0, "total_searches": 0,
                "searches_today": 0, "top_part": "—"}

def get_user_summary() -> list:
    """Per-user analytics: searches, sessions, last activity."""
    try:
        con = duckdb.connect(DB_PATH)
        rows = con.execute("""
            SELECT
                u.user_id, u.role, u.search_limit, u.is_active,
                u.created_at, u.notes,
                COUNT(DISTINCT s.session_id) AS total_sessions,
                COUNT(s.user_id)             AS total_searches,
                MAX(s.ts)                    AS last_search,
                MAX(l.ts)                    AS last_login
            FROM users u
            LEFT JOIN search_log s ON s.user_id = u.user_id
            LEFT JOIN login_log  l ON l.user_id = u.user_id AND l.action='login'
            GROUP BY u.user_id, u.role, u.search_limit, u.is_active,
                     u.created_at, u.notes
            ORDER BY last_login DESC NULLS LAST
        """).fetchall()
        con.close()
        keys = ["user_id","role","search_limit","is_active","created_at","notes",
                "total_sessions","total_searches","last_search","last_login"]
        return [dict(zip(keys, r)) for r in rows]
    except Exception:
        return []

def get_user_search_history(user_id: str) -> list:
    try:
        con = duckdb.connect(DB_PATH)
        rows = con.execute("""
            SELECT ts, part_number, source_mfr, target_mfr,
                   match_tier, top_match_pn, top_score, num_results
            FROM search_log WHERE user_id=? ORDER BY ts DESC
        """, [user_id]).fetchall()
        con.close()
        keys = ["ts","part_number","source_mfr","target_mfr",
                "match_tier","top_match_pn","top_score","num_results"]
        return [dict(zip(keys, r)) for r in rows]
    except Exception:
        return []

def get_daily_search_counts(days: int = 14) -> list:
    """Returns list of (date_str, count) for the last N days."""
    try:
        con = duckdb.connect(DB_PATH)
        rows = con.execute(f"""
            SELECT ts::DATE AS day, COUNT(*) AS cnt
            FROM search_log
            WHERE ts >= CURRENT_DATE - INTERVAL '{days} days'
            GROUP BY day ORDER BY day
        """).fetchall()
        con.close()
        return [(str(r[0]), r[1]) for r in rows]
    except Exception:
        return []

def get_role_breakdown() -> dict:
    """Returns dict of role → search count."""
    try:
        con = duckdb.connect(DB_PATH)
        rows = con.execute("""
            SELECT u.role, COUNT(s.user_id) AS cnt
            FROM users u
            LEFT JOIN search_log s ON s.user_id = u.user_id
            GROUP BY u.role
        """).fetchall()
        con.close()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}

def get_login_sessions() -> list:
    """Returns login events with timestamps for session timeline."""
    try:
        con = duckdb.connect(DB_PATH)
        rows = con.execute("""
            SELECT l.user_id, l.action, l.ts, u.role
            FROM login_log l
            LEFT JOIN users u ON u.user_id = l.user_id
            ORDER BY l.ts DESC LIMIT 200
        """).fetchall()
        con.close()
        return [{"user_id": r[0], "action": r[1], "ts": r[2], "role": r[3]}
                for r in rows]
    except Exception:
        return []