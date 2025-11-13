import psycopg2
from config import DB_CONFIG, USERS_DB_CONFIG
from contextvars import ContextVar


def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def tnved_exists(code: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 1
        FROM tn_veds
        WHERE code = %s
          AND digit IN (4, 6, 10)
        LIMIT 1;
    """, (code,))

    exists = cursor.fetchone() is not None

    cursor.close()
    conn.close()
    return exists

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
            username TEXT,
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

    username_norm = username if username is not None else None

    cursor.execute("""
        INSERT INTO users (telegram_id, username)
        VALUES (%s, %s)
        ON CONFLICT (telegram_id) DO UPDATE
            SET username = EXCLUDED.username;
    """, (telegram_id, username_norm))

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


async def change_user_role(telegram_id: int, new_role: str):
    conn = get_users_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT role, username
        FROM users
        WHERE telegram_id = %s;
    """, (telegram_id,))
    row = cursor.fetchone()

    if not row:
        reply = f"Пользователь с telegram_id={telegram_id} ещё ни разу не запускал бота."
    else:
        current_role, username = row

        if current_role == 'admin':
            reply = "Вы не можете изменить роль супер админа."
        else:
            cursor.execute("""
                UPDATE users
                SET role = %s
                WHERE telegram_id = %s AND role != 'admin';
            """, (new_role, telegram_id))
            reply = (
                f"Роль пользователя "
                f"{('@' + username) if username else f'id={telegram_id}'} "
                f"успешно изменена на {new_role}."
            )

    conn.commit()
    cursor.close()
    conn.close()
    return reply


async def add_download_history(telegram_id, partner, year):
    conn = get_users_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id FROM users WHERE telegram_id = %s;
    """, (telegram_id,))
    row = cursor.fetchone()
    user_id = row[0] if row else None

    cursor.execute("""
        INSERT INTO download_history (user_id, partner, year)
        VALUES (%s, %s, %s);
    """, (user_id, partner, year))
    conn.commit()
    cursor.close()
    conn.close()

async def get_download_history():
    conn = get_users_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT h.id, u.username, h.partner, h.year, h.downloaded_at
        FROM download_history h
        JOIN users u ON h.user_id = u.id  
        ORDER BY h.downloaded_at DESC
        LIMIT 100000;
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    reply = None
    if not rows:
        reply = "История скачиваний пуста."
        return

    return reply, rows

async def get_users_for_export():
    conn = get_users_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, telegram_id, username, role
        FROM users
        ORDER BY id;
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows
