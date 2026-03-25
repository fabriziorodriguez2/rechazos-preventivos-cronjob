import logging
import pymysql
import pymysql.cursors
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        db=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def execute_query(conn, sql, params=None):
    with conn.cursor() as cursor:
        cursor.execute(sql, params or ())
        return cursor.fetchall()


def execute_one(conn, sql, params=None):
    with conn.cursor() as cursor:
        cursor.execute(sql, params or ())
        return cursor.fetchone()


def execute_many(conn, sql, data):
    with conn.cursor() as cursor:
        cursor.executemany(sql, data)
        return cursor.rowcount


def commit(conn):
    conn.commit()


def rollback(conn):
    conn.rollback()


def close(conn):
    try:
        conn.close()
    except Exception:
        pass
