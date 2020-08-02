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


class Rollback(Exception):
    """
    Exit the current Transaction context immediately and rollback any changes
    made within this context.

    If a transaction context is specified in the constructor, rollback
    enclosing transactions contexts up to and including the one specified.
    """

    def __init__(self, transaction: Optional["Transaction"] = None) -> None:
        self.transaction = transaction

    def __str__(self) -> str:
        return f"<Rollback({self.transaction})>"


class Transaction:
    def __init__(
        self,
        conn: "Connection",
        savepoint_name: Optional[str],
        force_rollback: bool,
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

    @property
    def rollback_exception(self) -> Rollback:
        return Rollback(self)

    def __enter__(self) -> "Transaction":
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
    ) -> bool:
        with self._conn.lock:
            if exc_type is None and not self.force_rollback:
                # Commit changes made in the transaction context
                if self._savepoint_name:
                    # TODO: Add test for these asserts
                    # assert self._conn._savepoints.pop() == self._savepoint
                    assert self._conn._savepoints is not None
                    self._conn._savepoints.pop()
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
                if exc_type is Rollback:
                    _log.debug(
                        f"{self._conn}: Explicit rollback from: ",
                        exc_info=True,
                    )

                if self._savepoint_name:
                    # TODO: Add test for these asserts
                    # assert self._conn._savepoints.pop() == self._savepoint
                    assert self._conn._savepoints is not None
                    self._conn._savepoints.pop()
                    self._log_and_exec_command(
                        b"ROLLBACK TO SAVEPOINT " + self._savepoint_name
                    )
                if self._outer_transaction:
                    # TODO: Add test for this assert
                    # assert len(self._conn._savepoints) == 0
                    self._log_and_exec_command(b"ROLLBACK")
                    self._conn._savepoints = None

                if isinstance(exc_val, Rollback):
                    if exc_val.transaction in (self, None):
                        return True  # Swallow the exception
        return False

    def _log_and_exec_command(self, command: bytes) -> None:
        _log.debug(f"{self._conn}: {command.decode('ascii')}")
        self._conn._exec_command(command)

    def __str__(self) -> str:
        return f"Transaction {self._savepoint_name!r} on" f" {self._conn}"
