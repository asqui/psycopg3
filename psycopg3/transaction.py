"""
Transaction context managers returned by Connection.transaction()
"""

# Copyright (C) 2020 The Psycopg Team

import logging
from types import TracebackType
from typing import Optional, Type, TYPE_CHECKING

from .pq import TransactionStatus

if TYPE_CHECKING:
    from .connection import Connection

_log = logging.getLogger(__name__)


class Transaction:
    def __init__(
        self, conn: "Connection", savepoint_name: str, force_rollback: bool
    ) -> None:
        self._conn = conn
        self._savepoint_name: Optional[bytes] = None
        if savepoint_name is not None:
            self._savepoint_name = savepoint_name.encode("ascii")
        self.force_rollback = force_rollback

        self._outer_transaction: bool

    @property
    def connection(self) -> "Connection":
        return self._conn

    @property
    def savepoint_name(self) -> Optional[str]:
        if self._savepoint_name is None:
            return None
        return self._savepoint_name.decode("ascii")

    def __enter__(self) -> None:
        with self._conn.lock:
            if self._conn.pgconn.transaction_status == TransactionStatus.IDLE:
                assert self._conn._savepoints is None
                self._conn._savepoints = []
                self._outer_transaction = True
                self._log_and_exec_command(b"BEGIN")
            else:
                if self._conn._savepoints is None:
                    self._conn._savepoints = []
                self._outer_transaction = False
                if self._savepoint_name is None:
                    self._savepoint_name = b"tx_savepoint_%i" % (
                        len(self._conn._savepoints) + 1
                    )

            if self._savepoint_name is not None:
                self._log_and_exec_command(
                    b"SAVEPOINT " + self._savepoint_name
                )
                self._conn._savepoints.append(self._savepoint_name)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        with self._conn.lock:
            if exc_type is None and not self.force_rollback:
                # Commit changes made in the transaction context
                if self._savepoint_name:
                    self._conn._savepoints.pop()
                    # TODO: Add test for this assert
                    # assert self._conn._savepoints.pop() == self._savepoint
                    self._log_and_exec_command(
                        b"RELEASE SAVEPOINT " + self._savepoint_name
                    )
                if self._outer_transaction:
                    # TODO: Add test for this assert
                    # assert len(self._conn._savepoints) == 0
                    self._log_and_exec_command(b"COMMIT")
                    self._conn._savepoints = None
            else:
                # Rollback changes made in the transaction context
                if self._savepoint_name:
                    self._conn._savepoints.pop()
                    # TODO: Add test for this assert
                    # assert self._conn._savepoints.pop() == self._savepoint
                    self._log_and_exec_command(
                        b"ROLLBACK TO SAVEPOINT " + self._savepoint_name
                    )
                if self._outer_transaction:
                    # TODO: Add test for this assert
                    # assert len(self._conn._savepoints) == 0
                    self._log_and_exec_command(b"ROLLBACK")
                    self._conn._savepoints = None

    def _log_and_exec_command(self, command: bytes) -> None:
        _log.debug(f"{self._conn}: {command.decode('ascii')}")
        self._conn._exec_command(command)
