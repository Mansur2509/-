import sqlite3
import hashlib
from datetime import datetime, timedelta

DB_PATH = "sov.db"


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            tg_id         INTEGER PRIMARY KEY,
            full_name     TEXT NOT NULL,
            group_name    TEXT NOT NULL,
            gender        TEXT NOT NULL CHECK(gender IN ('М','Ж')),
            lang          TEXT DEFAULT 'ru',
            rating        REAL DEFAULT 0,
            experience    INTEGER DEFAULT 0,
            streak        INTEGER DEFAULT 0,
            notes         TEXT DEFAULT '',
            points        INTEGER DEFAULT 0,
            ban_type      TEXT DEFAULT 'none' CHECK(ban_type IN ('none','temp','full')),
            ban_until     TEXT DEFAULT NULL,
            agreed        INTEGER DEFAULT 0,
            last_seen_ann INTEGER DEFAULT 0,
            photo_file_id TEXT DEFAULT NULL,
            referred_by   INTEGER DEFAULT NULL,
            referral_count INTEGER DEFAULT 0,
            registered_at TEXT DEFAULT (datetime('now'))
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            title          TEXT NOT NULL,
            description    TEXT DEFAULT '',
            event_date     TEXT NOT NULL,
            event_time     TEXT DEFAULT '',
            location       TEXT DEFAULT '',
            duration       TEXT DEFAULT '',
            meeting_point  TEXT DEFAULT '',
            total_slots    INTEGER NOT NULL,
            male_slots     INTEGER DEFAULT 0,
            female_slots   INTEGER DEFAULT 0,
            gender_strict  INTEGER DEFAULT 0,
            photo_file_id  TEXT DEFAULT NULL,
            is_active      INTEGER DEFAULT 1,
            created_at     TEXT DEFAULT (datetime('now'))
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS applications (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id   INTEGER NOT NULL REFERENCES events(id),
            tg_id      INTEGER NOT NULL REFERENCES users(tg_id),
            status     TEXT DEFAULT 'pending' CHECK(status IN ('pending','selected','rejected')),
            attended   INTEGER DEFAULT 0,
            applied_at TEXT DEFAULT (datetime('now')),
            UNIQUE(event_id, tg_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS ratings (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER NOT NULL REFERENCES events(id),
            tg_id    INTEGER NOT NULL REFERENCES users(tg_id),
            score    REAL NOT NULL,
            comment  TEXT DEFAULT '',
            rated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(event_id, tg_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS point_history (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id    INTEGER NOT NULL REFERENCES users(tg_id),
            delta    INTEGER NOT NULL,
            reason   TEXT DEFAULT '',
            given_at TEXT DEFAULT (datetime('now'))
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS announcements (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            text       TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS event_proposals (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id          INTEGER NOT NULL REFERENCES users(tg_id),
            vol_count      TEXT NOT NULL,
            location       TEXT NOT NULL,
            event_date     TEXT NOT NULL,
            duration       TEXT NOT NULL,
            tasks          TEXT NOT NULL,
            gender_need    TEXT NOT NULL,
            organizer      TEXT NOT NULL,
            admin_approved TEXT NOT NULL,
            comment        TEXT DEFAULT '',
            status         TEXT DEFAULT 'pending',
            created_at     TEXT DEFAULT (datetime('now'))
        )
    """)

    # Карточки участника (сертификаты за ивент)
    c.execute("""
        CREATE TABLE IF NOT EXISTS cards (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id      INTEGER NOT NULL REFERENCES users(tg_id),
            event_id   INTEGER NOT NULL REFERENCES events(id),
            issued_at  TEXT DEFAULT (datetime('now')),
            UNIQUE(tg_id, event_id)
        )
    """)

    # QR-коды для подтверждения присутствия
    c.execute("""
        CREATE TABLE IF NOT EXISTS qr_tokens (
            token      TEXT PRIMARY KEY,
            event_id   INTEGER NOT NULL REFERENCES events(id),
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # Rate limiting
    c.execute("""
        CREATE TABLE IF NOT EXISTS rate_limit (
            tg_id  INTEGER NOT NULL,
            action TEXT NOT NULL,
            ts     TEXT NOT NULL,
            PRIMARY KEY (tg_id, action, ts)
        )
    """)

    # Безопасная миграция
    migrations = [
        ("users", "lang",           "TEXT DEFAULT 'ru'"),
        ("users", "streak",         "INTEGER DEFAULT 0"),
        ("users", "photo_file_id",  "TEXT DEFAULT NULL"),
        ("users", "referred_by",    "INTEGER DEFAULT NULL"),
        ("users", "referral_count", "INTEGER DEFAULT 0"),
        ("users", "agreed",         "INTEGER DEFAULT 0"),
        ("users", "last_seen_ann",  "INTEGER DEFAULT 0"),
        ("users", "points",         "INTEGER DEFAULT 0"),
        ("users", "ban_type",       "TEXT DEFAULT 'none'"),
        ("users", "ban_until",      "TEXT DEFAULT NULL"),
        ("events", "event_time",    "TEXT DEFAULT ''"),
        ("events", "location",      "TEXT DEFAULT ''"),
        ("events", "duration",      "TEXT DEFAULT ''"),
        ("events", "meeting_point", "TEXT DEFAULT ''"),
        ("events", "gender_strict", "INTEGER DEFAULT 0"),
        ("events", "photo_file_id", "TEXT DEFAULT NULL"),
        ("applications", "attended","INTEGER DEFAULT 0"),
    ]
    for table, col, definition in migrations:
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {definition}")
        except Exception:
            pass

    conn.commit()
    conn.close()


# ─── RATE LIMITING ───────────────────────────────────────────────────────────

def check_rate_limit(tg_id: int, action: str, max_calls: int, window_seconds: int) -> bool:
    conn = get_conn()
    cutoff = (datetime.now() - timedelta(seconds=window_seconds)).isoformat()
    conn.execute("DELETE FROM rate_limit WHERE tg_id=? AND action=? AND ts<?", (tg_id, action, cutoff))
    count = conn.execute("SELECT COUNT(*) FROM rate_limit WHERE tg_id=? AND action=?", (tg_id, action)).fetchone()[0]
    if count >= max_calls:
        conn.close()
        return False
    conn.execute("INSERT INTO rate_limit (tg_id, action, ts) VALUES (?,?,?)", (tg_id, action, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    return True


# ─── ПОЛЬЗОВАТЕЛИ ────────────────────────────────────────────────────────────

def user_exists(tg_id: int) -> bool:
    conn = get_conn()
    row = conn.execute("SELECT 1 FROM users WHERE tg_id=?", (tg_id,)).fetchone()
    conn.close()
    return row is not None


def register_user(tg_id: int, full_name: str, group_name: str, gender: str,
                  lang: str = "ru", referred_by: int = None):
    conn = get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO users (tg_id, full_name, group_name, gender, lang, referred_by) VALUES (?,?,?,?,?,?)",
        (tg_id, full_name, group_name, gender, lang, referred_by)
    )
    # Увеличиваем счётчик рефералов у пригласившего
    if referred_by:
        conn.execute("UPDATE users SET referral_count = referral_count + 1 WHERE tg_id=?", (referred_by,))
    conn.commit()
    conn.close()


def set_agreed(tg_id: int):
    conn = get_conn()
    conn.execute("UPDATE users SET agreed=1 WHERE tg_id=?", (tg_id,))
    conn.commit()
    conn.close()


def set_lang(tg_id: int, lang: str):
    conn = get_conn()
    conn.execute("UPDATE users SET lang=? WHERE tg_id=?", (lang, tg_id))
    conn.commit()
    conn.close()


def get_user_lang(tg_id: int) -> str:
    conn = get_conn()
    row = conn.execute("SELECT lang FROM users WHERE tg_id=?", (tg_id,)).fetchone()
    conn.close()
    return row["lang"] if row else "ru"


def get_user(tg_id: int):
    conn = get_conn()
    row = conn.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_users():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM users ORDER BY rating DESC, experience DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_user_notes(tg_id: int, notes: str):
    conn = get_conn()
    conn.execute("UPDATE users SET notes=? WHERE tg_id=?", (notes, tg_id))
    conn.commit()
    conn.close()


def set_user_photo(tg_id: int, file_id: str):
    conn = get_conn()
    conn.execute("UPDATE users SET photo_file_id=? WHERE tg_id=?", (file_id, tg_id))
    conn.commit()
    conn.close()


def delete_user(tg_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM applications WHERE tg_id=?", (tg_id,))
    conn.execute("DELETE FROM point_history WHERE tg_id=?", (tg_id,))
    conn.execute("DELETE FROM ratings WHERE tg_id=?", (tg_id,))
    conn.execute("DELETE FROM cards WHERE tg_id=?", (tg_id,))
    conn.execute("DELETE FROM users WHERE tg_id=?", (tg_id,))
    conn.commit()
    conn.close()


def get_top_users(limit=3):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM users WHERE ban_type='none' ORDER BY rating DESC, experience DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def is_banned(tg_id: int) -> tuple:
    user = get_user(tg_id)
    if not user:
        return False, ""
    if user["ban_type"] == "full":
        return True, "full"
    if user["ban_type"] == "temp" and user["ban_until"]:
        ban_until = datetime.fromisoformat(user["ban_until"])
        if datetime.now() < ban_until:
            return True, ban_until.strftime("%d.%m.%Y")
        conn = get_conn()
        conn.execute("UPDATE users SET ban_type='none', ban_until=NULL WHERE tg_id=?", (tg_id,))
        conn.commit()
        conn.close()
    return False, ""


def update_last_seen_ann(tg_id: int, ann_id: int):
    conn = get_conn()
    conn.execute("UPDATE users SET last_seen_ann=? WHERE tg_id=?", (ann_id, tg_id))
    conn.commit()
    conn.close()


def recalc_streak(tg_id: int):
    """Пересчитывает streak — кол-во ивентов подряд с attended=1."""
    conn = get_conn()
    rows = conn.execute(
        """SELECT a.attended FROM applications a
           JOIN events e ON a.event_id=e.id
           WHERE a.tg_id=? AND a.status='selected'
           ORDER BY e.event_date DESC""",
        (tg_id,)
    ).fetchall()
    streak = 0
    for r in rows:
        if r["attended"]:
            streak += 1
        else:
            break
    conn.execute("UPDATE users SET streak=? WHERE tg_id=?", (streak, tg_id))
    conn.commit()
    conn.close()
    return streak


# ─── ПОИНТЫ ──────────────────────────────────────────────────────────────────

def add_points(tg_id: int, delta: int, reason: str = "") -> dict:
    conn = get_conn()
    conn.execute("UPDATE users SET points = points + ? WHERE tg_id=?", (delta, tg_id))
    conn.execute("INSERT INTO point_history (tg_id, delta, reason) VALUES (?,?,?)", (tg_id, delta, reason))
    conn.commit()
    user = dict(conn.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,)).fetchone())
    new_points = user["points"]
    action = "none"
    if new_points >= 3:
        if user["ban_type"] == "temp":
            conn.execute("UPDATE users SET ban_type='full', ban_until=NULL, points=0 WHERE tg_id=?", (tg_id,))
            action = "full_ban"
        else:
            ban_until = (datetime.now() + timedelta(days=30)).isoformat()
            conn.execute("UPDATE users SET ban_type='temp', ban_until=?, points=0 WHERE tg_id=?", (ban_until, tg_id))
            action = "temp_ban"
        conn.commit()
    conn.close()
    return {"points": new_points, "action": action}


def get_point_history(tg_id: int):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM point_history WHERE tg_id=? ORDER BY given_at DESC LIMIT 10", (tg_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── БАНЫ ────────────────────────────────────────────────────────────────────

def ban_user(tg_id: int, ban_type: str, days: int = 30):
    conn = get_conn()
    if ban_type == "full":
        conn.execute("UPDATE users SET ban_type='full', ban_until=NULL WHERE tg_id=?", (tg_id,))
    else:
        ban_until = (datetime.now() + timedelta(days=days)).isoformat()
        conn.execute("UPDATE users SET ban_type='temp', ban_until=? WHERE tg_id=?", (ban_until, tg_id))
    conn.commit()
    conn.close()


def unban_user(tg_id: int):
    conn = get_conn()
    conn.execute("UPDATE users SET ban_type='none', ban_until=NULL, points=0 WHERE tg_id=?", (tg_id,))
    conn.commit()
    conn.close()


# ─── ОБЪЯВЛЕНИЯ ──────────────────────────────────────────────────────────────

def create_announcement(text: str) -> int:
    conn = get_conn()
    cur = conn.execute("INSERT INTO announcements (text) VALUES (?)", (text,))
    ann_id = cur.lastrowid
    conn.commit()
    conn.close()
    return ann_id


def get_announcements(limit=10, offset=0):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM announcements ORDER BY created_at DESC LIMIT ? OFFSET ?", (limit, offset)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_announcements_count() -> int:
    conn = get_conn()
    row = conn.execute("SELECT COUNT(*) as cnt FROM announcements").fetchone()
    conn.close()
    return row["cnt"]


def get_new_announcements_for_user(tg_id: int):
    user = get_user(tg_id)
    if not user:
        return []
    last_seen = user.get("last_seen_ann", 0) or 0
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM announcements WHERE id > ? ORDER BY id ASC", (last_seen,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── ИВЕНТЫ ──────────────────────────────────────────────────────────────────

def create_event(title, description, event_date, event_time, location, duration,
                 meeting_point, total_slots, male_slots, female_slots,
                 gender_strict=0, photo_file_id=None) -> int:
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO events
           (title, description, event_date, event_time, location, duration,
            meeting_point, total_slots, male_slots, female_slots, gender_strict, photo_file_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (title, description, event_date, event_time, location, duration,
         meeting_point, total_slots, male_slots, female_slots, gender_strict, photo_file_id)
    )
    eid = cur.lastrowid
    conn.commit()
    conn.close()
    return eid


def get_active_events():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM events WHERE is_active=1 ORDER BY event_date").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_events():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM events ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_event(event_id: int):
    conn = get_conn()
    row = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def close_event(event_id: int):
    conn = get_conn()
    conn.execute("UPDATE events SET is_active=0 WHERE id=?", (event_id,))
    conn.commit()
    conn.close()


def delete_event(event_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM applications WHERE event_id=?", (event_id,))
    conn.execute("DELETE FROM ratings WHERE event_id=?", (event_id,))
    conn.execute("DELETE FROM cards WHERE event_id=?", (event_id,))
    conn.execute("DELETE FROM qr_tokens WHERE event_id=?", (event_id,))
    conn.execute("DELETE FROM events WHERE id=?", (event_id,))
    conn.commit()
    conn.close()


def set_event_photo(event_id: int, file_id: str):
    conn = get_conn()
    conn.execute("UPDATE events SET photo_file_id=? WHERE id=?", (file_id, event_id))
    conn.commit()
    conn.close()


# ─── ЗАЯВКИ ──────────────────────────────────────────────────────────────────

def apply_to_event(event_id: int, tg_id: int) -> tuple:
    event = get_event(event_id)
    if not event:
        return False, "not_found"
    if event.get("gender_strict"):
        user = get_user(tg_id)
        m, f = event["male_slots"], event["female_slots"]
        if m > 0 and f == 0 and user["gender"] != "М":
            return False, "male_only"
        if f > 0 and m == 0 and user["gender"] != "Ж":
            return False, "female_only"
    try:
        conn = get_conn()
        conn.execute("INSERT INTO applications (event_id, tg_id) VALUES (?,?)", (event_id, tg_id))
        conn.commit()
        conn.close()
        return True, ""
    except Exception:
        return False, "already"


def cancel_application(event_id: int, tg_id: int):
    conn = get_conn()
    conn.execute(
        "DELETE FROM applications WHERE event_id=? AND tg_id=? AND status='pending'",
        (event_id, tg_id)
    )
    conn.commit()
    conn.close()


def get_applications(event_id: int):
    conn = get_conn()
    rows = conn.execute(
        """SELECT a.*, u.full_name, u.group_name, u.gender, u.rating, u.experience
           FROM applications a JOIN users u ON a.tg_id=u.tg_id
           WHERE a.event_id=? ORDER BY u.rating DESC, u.experience DESC""",
        (event_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def has_applied(event_id: int, tg_id: int):
    conn = get_conn()
    row = conn.execute(
        "SELECT status FROM applications WHERE event_id=? AND tg_id=?", (event_id, tg_id)
    ).fetchone()
    conn.close()
    return row["status"] if row else None


def get_user_events(tg_id: int):
    conn = get_conn()
    rows = conn.execute(
        """SELECT e.id, e.title, e.event_date, e.event_time, e.location, a.status, a.attended
           FROM applications a JOIN events e ON a.event_id=e.id
           WHERE a.tg_id=? ORDER BY e.event_date DESC""",
        (tg_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_selected_for_event(event_id: int):
    conn = get_conn()
    rows = conn.execute(
        """SELECT u.tg_id, u.full_name, u.group_name
           FROM applications a JOIN users u ON a.tg_id=u.tg_id
           WHERE a.event_id=? AND a.status='selected'""",
        (event_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def manually_add_to_event(event_id: int, tg_id: int) -> bool:
    """Принудительно добавить участника со статусом selected."""
    try:
        conn = get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO applications (event_id, tg_id, status) VALUES (?,?,'selected')",
            (event_id, tg_id)
        )
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False


def manually_remove_from_event(event_id: int, tg_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM applications WHERE event_id=? AND tg_id=?", (event_id, tg_id))
    conn.commit()
    conn.close()


# ─── ОЦЕНКИ ──────────────────────────────────────────────────────────────────

def add_rating(event_id: int, tg_id: int, score: float, comment: str = ""):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO ratings (event_id, tg_id, score, comment) VALUES (?,?,?,?)",
        (event_id, tg_id, score, comment)
    )
    row = conn.execute(
        "SELECT AVG(score) as avg_r, COUNT(*) as cnt FROM ratings WHERE tg_id=?", (tg_id,)
    ).fetchone()
    conn.execute(
        "UPDATE users SET rating=ROUND(?,2), experience=? WHERE tg_id=?",
        (row["avg_r"], row["cnt"], tg_id)
    )
    conn.commit()
    conn.close()


# ─── АВТОПОДБОР ──────────────────────────────────────────────────────────────

def auto_select(event_id: int) -> dict:
    event = get_event(event_id)
    if not event:
        return {"selected": [], "rejected": []}
    apps = get_applications(event_id)
    active_apps = [a for a in apps if not is_banned(a["tg_id"])[0]]
    males   = [a for a in active_apps if a["gender"] == "М"]
    females = [a for a in active_apps if a["gender"] == "Ж"]
    selected = []
    selected += males[:event["male_slots"]]
    selected += females[:event["female_slots"]]
    filled_ids = {s["tg_id"] for s in selected}
    remaining  = event["total_slots"] - len(selected)
    if remaining > 0:
        rest = sorted(
            [a for a in active_apps if a["tg_id"] not in filled_ids],
            key=lambda x: (-x["rating"], -x["experience"])
        )
        selected += rest[:remaining]
    selected_ids = {s["tg_id"] for s in selected}
    rejected = [a for a in apps if a["tg_id"] not in selected_ids]
    conn = get_conn()
    for s in selected:
        conn.execute("UPDATE applications SET status='selected' WHERE event_id=? AND tg_id=?", (event_id, s["tg_id"]))
    for r in rejected:
        conn.execute("UPDATE applications SET status='rejected' WHERE event_id=? AND tg_id=?", (event_id, r["tg_id"]))
    conn.commit()
    conn.close()
    return {"selected": selected, "rejected": rejected}


# ─── КАРТОЧКИ ────────────────────────────────────────────────────────────────

def issue_card(tg_id: int, event_id: int) -> bool:
    try:
        conn = get_conn()
        conn.execute("INSERT OR IGNORE INTO cards (tg_id, event_id) VALUES (?,?)", (tg_id, event_id))
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False


def get_user_cards(tg_id: int):
    conn = get_conn()
    rows = conn.execute(
        """SELECT c.*, e.title, e.event_date, e.photo_file_id
           FROM cards c JOIN events e ON c.event_id=e.id
           WHERE c.tg_id=? ORDER BY c.issued_at DESC""",
        (tg_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── QR КОДЫ ─────────────────────────────────────────────────────────────────

def generate_qr_token(event_id: int) -> str:
    """Генерирует уникальный токен для QR-кода ивента."""
    raw = f"sov_event_{event_id}_{datetime.now().isoformat()}"
    token = hashlib.sha256(raw.encode()).hexdigest()[:16]
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO qr_tokens (token, event_id) VALUES (?,?)",
        (token, event_id)
    )
    conn.commit()
    conn.close()
    return token


def get_event_by_qr_token(token: str):
    conn = get_conn()
    row = conn.execute("SELECT * FROM qr_tokens WHERE token=?", (token,)).fetchone()
    conn.close()
    return dict(row) if row else None


def confirm_attendance(event_id: int, tg_id: int) -> str:
    """Подтверждает присутствие. Возвращает: 'ok', 'already', 'not_selected'."""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM applications WHERE event_id=? AND tg_id=? AND status='selected'",
        (event_id, tg_id)
    ).fetchone()
    if not row:
        conn.close()
        return "not_selected"
    if row["attended"]:
        conn.close()
        return "already"
    conn.execute(
        "UPDATE applications SET attended=1 WHERE event_id=? AND tg_id=?", (event_id, tg_id)
    )
    conn.commit()
    conn.close()
    recalc_streak(tg_id)
    return "ok"


# ─── ПРЕДЛОЖЕНИЯ ─────────────────────────────────────────────────────────────

def create_proposal(tg_id, vol_count, location, event_date, duration,
                    tasks, gender_need, organizer, admin_approved, comment) -> int:
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO event_proposals
           (tg_id, vol_count, location, event_date, duration, tasks,
            gender_need, organizer, admin_approved, comment)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (tg_id, vol_count, location, event_date, duration,
         tasks, gender_need, organizer, admin_approved, comment)
    )
    pid = cur.lastrowid
    conn.commit()
    conn.close()
    return pid


def get_proposals(status="pending"):
    conn = get_conn()
    rows = conn.execute(
        """SELECT p.*, u.full_name FROM event_proposals p
           JOIN users u ON p.tg_id=u.tg_id
           WHERE p.status=? ORDER BY p.created_at DESC""",
        (status,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_proposal_status(pid: int, status: str):
    conn = get_conn()
    conn.execute("UPDATE event_proposals SET status=? WHERE id=?", (status, pid))
    conn.commit()
    conn.close()
