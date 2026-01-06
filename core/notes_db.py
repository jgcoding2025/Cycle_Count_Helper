from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone


@dataclass(frozen=True)
class NoteKey:
    session_id: str
    whs: str
    item: str
    batch_lot: str
    location: str


class NotesDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS notes (
                    session_id TEXT NOT NULL,
                    whs TEXT NOT NULL,
                    item TEXT NOT NULL,
                    batch_lot TEXT NOT NULL,
                    location TEXT NOT NULL,
                    user_notes TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (session_id, whs, item, batch_lot, location)
                );
                """
            )

    def read_notes_for_session(self, session_id: str) -> dict[NoteKey, tuple[str, str]]:
        """
        Returns mapping NoteKey -> (user_notes, updated_at_iso)
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT session_id, whs, item, batch_lot, location, user_notes, updated_at
                FROM notes
                WHERE session_id = ?
                """,
                (session_id,),
            )
            out: dict[NoteKey, tuple[str, str]] = {}
            for row in cur.fetchall():
                k = NoteKey(
                    session_id=row[0],
                    whs=row[1],
                    item=row[2],
                    batch_lot=row[3],
                    location=row[4],
                )
                out[k] = (row[5], row[6])
            return out

    def upsert_note(self, key: NoteKey, user_notes: str) -> str:
        """
        Inserts/updates a note. Returns updated_at ISO string.
        """
        updated_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO notes (session_id, whs, item, batch_lot, location, user_notes, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id, whs, item, batch_lot, location)
                DO UPDATE SET user_notes=excluded.user_notes, updated_at=excluded.updated_at
                """,
                (
                    key.session_id,
                    key.whs,
                    key.item,
                    key.batch_lot,
                    key.location,
                    user_notes,
                    updated_at,
                ),
            )
        return updated_at
