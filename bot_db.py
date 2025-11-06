import psycopg2
from config import DB_CONFIG, USERS_DB_CONFIG, ALT_DB_CONFIG
from contextvars import ContextVar

_active_db: ContextVar[str] = ContextVar("_active_db", default="main")

def set_active_db(alias: str):
    if alias not in ("main", "alt"):
        raise ValueError("active db must be 'main' or 'alt'")
    _active_db.set(alias)

class use_db:
    def __init__(self, alias: str):
        self.alias = alias
        self._token = None
    def __enter__(self):
        self._token = _active_db.set(self.alias)
    def __exit__(self, exc_type, exc, tb):
        _active_db.reset(self._token)

def get_connection():
    alias = _active_db.get()
    cfg = DB_CONFIG if alias == "main" else ALT_DB_CONFIG
    return psycopg2.connect(**cfg)


def get_regions():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT r.name
        FROM regions r
        JOIN data d ON r.id = d.region_id
        ORDER BY r.name;
    """)
    regions = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return regions


def get_partners():
    conn = get_connection()
    cursor = conn.cursor()

    partners = ["весь мир"]   
    
    cursor.execute("""
        SELECT name
        FROM country_groups
        WHERE parent_id IS NOT NULL
        AND name <> 'весь мир'
        ORDER BY name
    """)
    partners.extend(row[0] for row in cursor.fetchall())

    cursor.execute("""
        SELECT DISTINCT name_ru
        FROM countries
        ORDER BY name_ru
    """)
    partners.extend(row[0] for row in cursor.fetchall())

    cursor.close()
    conn.close()
    return partners


def get_years():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT year
        FROM data
        WHERE year > (
            SELECT MIN(year)
            FROM data)
        ORDER BY year;
    """,)
    years = [str(row[0]) for row in cursor.fetchall()]

    cursor.close()
    conn.close()
    return years


def get_categories():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM public.tn_ved_categories where parent_id is null;
    """)
    categories = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return categories


def get_subcategories(parent_name: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT sc.name
        FROM tn_ved_categories p
        JOIN tn_ved_categories sc ON sc.parent_id = p.id
        WHERE p.name = %s
        ORDER BY sc.name;
    """, (parent_name,))
    subcategories = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return subcategories


def get_users_connection():
    return psycopg2.connect(**USERS_DB_CONFIG)


def setup_users_tables():
    conn = get_users_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE,
            username TEXT UNIQUE,
            role TEXT CHECK (role IN ('admin', 'advanced', 'user')) NOT NULL DEFAULT 'user'
        );
    """)
    conn.commit()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS download_history (
            id SERIAL PRIMARY KEY,
            user_id INT,
            region TEXT,
            partner TEXT,
            year TEXT,
            downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()

    cursor.close()
    conn.close()


def register_user(telegram_id, username):
    conn = get_users_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO users (telegram_id, username)
        VALUES (%s, %s)
        ON CONFLICT (username) DO UPDATE
            SET telegram_id = EXCLUDED.telegram_id;
    """, (telegram_id, username))
    
    conn.commit()
    cursor.close()
    conn.close()


def get_user_role(telegram_id):
    conn = get_users_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT role
        FROM users
        WHERE telegram_id = %s;
    """, (telegram_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


async def change_user_role(username, new_role):
    conn = get_users_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users WHERE username = %s;", (username.lower(),))
    user = cursor.fetchone()

    if user:
        cursor.execute("""
            UPDATE users SET role = %s WHERE username = %s AND role != 'admin';
        """, (new_role, username.lower()))
        if cursor.rowcount == 0:
            reply = "Вы не можете изменить роль супер админа."
        else:
            reply = f"Роль пользователя @{username} успешно изменена на {new_role}."
    else:
        cursor.execute("""
            INSERT INTO users (telegram_id, username, role)
            VALUES (NULL, %s, %s);
        """, (username.lower(), new_role))
        reply = f"Пользователь @{username} добавлен с ролью {new_role}."

    conn.commit()
    cursor.close()
    conn.close()
    return reply


async def add_download_history(telegram_id, region, partner, year):
    conn = get_users_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id FROM users WHERE telegram_id = %s;
    """, (telegram_id,))
    user_id = cursor.fetchone()

    cursor.execute("""
        INSERT INTO download_history (user_id, region, partner, year)
        VALUES (%s, %s, %s, %s);
    """, (user_id[0], region, partner, year))
    conn.commit()
    cursor.close()
    conn.close()


async def get_download_history():
    conn = get_users_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT h.id, u.username, h.region, h.partner, h.year, h.downloaded_at
        FROM download_history h
        JOIN users u ON h.user_id = u.id  
        ORDER BY h.downloaded_at DESC
        LIMIT 10000;
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    reply = None
    if not rows:
        reply = "История скачиваний пуста."
        return

    return reply, rows