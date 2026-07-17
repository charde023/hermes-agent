"""queue_handoff의 로컬 멱등 이벤트·delivery receipt 저장소.

Conversation v1 서버는 같은 ``append_event``를 재호출하면 현재 delivery 상태를
돌려준다. 재시작 뒤에도 byte-identical event를 다시 보낼 수 있도록 profile별
Hermes state DB에 최초 이벤트를 보존한다. credential/token은 저장하지 않는다.
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from hermes_constants import get_hermes_home


_DELIVERY_STATES = {
    "enqueued",
    "pending",
    "claimed",
    "accepted",
    "completed",
    "error",
}


class ReceiptCollisionError(ValueError):
    """같은 idempotency key가 다른 요청에 재사용됐다."""


@dataclass(frozen=True)
class ReceiptRecord:
    idempotency_key: str
    request_hash: str
    protocol_version: str
    event: dict
    delivery_status: str


def default_receipt_db_path() -> Path:
    return get_hermes_home() / "state" / "queue_handoff_receipts.sqlite3"


class QueueHandoffReceiptStore:
    """작은 profile-local SQLite 저장소.

    ``reserve``는 BEGIN IMMEDIATE + PK로 동시 재시도도 하나의 이벤트만 선택한다.
    기존 row가 있으면 새로 만든 timestamp/event 대신 최초 event를 반환한다.
    """

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path is not None else default_receipt_db_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.path), timeout=30)
        con.row_factory = sqlite3.Row
        # 이 DB는 단건 reserve/update만 수행하므로 기본 rollback journal이면 충분하다.
        # 연결마다 WAL 전환을 시도하면 두 gateway turn의 최초 생성 race에서
        # ``PRAGMA journal_mode``끼리 충돌한다. BEGIN IMMEDIATE + busy_timeout으로
        # 필요한 직렬화만 보장한다.
        con.execute("PRAGMA busy_timeout=30000")
        return con

    def _init_schema(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS queue_handoff_receipts (
                    idempotency_key TEXT PRIMARY KEY,
                    request_hash TEXT NOT NULL,
                    protocol_version TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    delivery_status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
        # 메시지 본문을 포함하는 profile state다. POSIX에서는 owner-only로 고정하고,
        # chmod 의미가 제한적인 플랫폼에서는 실패를 무시한다.
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    @staticmethod
    def _record(row: sqlite3.Row) -> ReceiptRecord:
        return ReceiptRecord(
            idempotency_key=row["idempotency_key"],
            request_hash=row["request_hash"],
            protocol_version=row["protocol_version"],
            event=json.loads(row["event_json"]),
            delivery_status=row["delivery_status"],
        )

    def reserve(
        self,
        *,
        idempotency_key: str,
        request_hash: str,
        protocol_version: str,
        event: Mapping,
        initial_status: str,
    ) -> tuple[ReceiptRecord, bool]:
        if initial_status not in _DELIVERY_STATES:
            raise ValueError("invalid delivery status")
        event_json = json.dumps(
            dict(event), sort_keys=True, ensure_ascii=False, separators=(",", ":")
        )
        now = datetime.now(timezone.utc).isoformat()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT * FROM queue_handoff_receipts WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row is not None:
                if (
                    row["request_hash"] != request_hash
                    or row["protocol_version"] != protocol_version
                ):
                    raise ReceiptCollisionError(
                        "idempotency collision: key already belongs to another request"
                    )
                con.commit()
                return self._record(row), False
            con.execute(
                """
                INSERT INTO queue_handoff_receipts (
                    idempotency_key, request_hash, protocol_version, event_json,
                    delivery_status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    idempotency_key,
                    request_hash,
                    protocol_version,
                    event_json,
                    initial_status,
                    now,
                    now,
                ),
            )
            row = con.execute(
                "SELECT * FROM queue_handoff_receipts WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            con.commit()
            return self._record(row), True
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def get(self, idempotency_key: str) -> ReceiptRecord | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM queue_handoff_receipts WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        return self._record(row) if row is not None else None

    def update_status(self, idempotency_key: str, status: str) -> ReceiptRecord:
        if status not in _DELIVERY_STATES:
            raise ValueError("invalid delivery status")
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as con:
            changed = con.execute(
                """
                UPDATE queue_handoff_receipts
                   SET delivery_status = ?, updated_at = ?
                 WHERE idempotency_key = ?
                """,
                (status, now, idempotency_key),
            ).rowcount
            if changed != 1:
                raise KeyError(idempotency_key)
            row = con.execute(
                "SELECT * FROM queue_handoff_receipts WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        return self._record(row)
