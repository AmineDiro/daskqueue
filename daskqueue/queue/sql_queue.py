import os
import sqlite3
from typing import Optional

from .base_queue import BaseQueue, Durability


class SQLQueue(BaseQueue):
    def __init__(
        self,
        durability: Durability,
        maxsize: Optional[int] = None,
        db_path: Optional[str] = None,
    ) -> None:
        super().__init__(durability, maxsize)

        if self.durability == Durability.DURABLE:
            self.check_path(db_path)
            self.db_path = db_path
        else:
            self.db_path = ":memory:"

        self.conn = self.setup_conn()

    @staticmethod
    def check_path(path: str) -> bool:
        """Checks for read and write access"""
        root_dir = os.path.basename(path)
        if path is None or not os.access(root_dir, os.R_OK | os.W_OK):
            raise ValueError("Please provide a valid path")

    def setup_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            """CREATE TABLE IF NOT EXISTS Queue
                (
                message BLOB NOT NULL,
                message_id TEXT NOT NULL,
                status INTEGER,
                in_time INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                lock_time INTEGER,
                done_time INTEGER
                )
        """
        )
