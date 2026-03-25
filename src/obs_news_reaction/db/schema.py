"""Database schema initialization and migrations."""

import sqlite3
from pathlib import Path

from obs_news_reaction.config import DB_PATH

_SCHEMA_SQL = Path(__file__).parent / "schema.sql"


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Create a database connection with row factory."""
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path | None = None) -> None:
    """Initialize the database schema."""
    conn = get_connection(db_path)
    try:
        sql = _SCHEMA_SQL.read_text()
        conn.executescript(sql)
        _migrate(conn)
    finally:
        conn.close()


def _migrate(conn: sqlite3.Connection) -> None:
    """Run any pending ALTER TABLE migrations."""
    # Future migrations go here as ALTER TABLE ADD COLUMN statements
    pass
