"""
Transaction context managers returned by Connection.transaction()
"""

# Copyright (C) 2020 The Psycopg Team

# TODO: Should this module be _underscored since it's implementation detail?
from types import TracebackType
from typing import Optional, Type, TYPE_CHECKING

from .pq import TransactionStatus

if TYPE_CHECKING:
    from .connection import Connection


class Transaction:
    def __init__(self, conn: "Connection") -> None:
        self._conn = conn
        self._outer_transaction = False
        self._savepoint_name: Optional[bytes] = None

    @property
    def connection(self) -> "Connection":
        return self._conn

    def __enter__(self) -> None:
        with self._conn.lock:
            if self._conn.pgconn.transaction_status == TransactionStatus.IDLE:
                self._outer_transaction = True
                self._savepoint_name = None

                self._conn._exec_command(b"begin")
                assert len(self._conn._savepoints) == 0
                self._conn._savepoints.append(None)
            else:
                self._outer_transaction = False
                self._savepoint_name = b"tx_savepoint_%i" % (
                    len(self._conn._savepoints) + 1
                )
                self._conn._exec_command(b"savepoint " + self._savepoint_name)
                self._conn._savepoints.append(self._savepoint_name)

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        with self._conn.lock:
            if exc_type is None:
                if self._savepoint_name:
                    self._conn._savepoints.pop()
                    # TODO: Add test for this assert
                    # assert self._conn._savepoints.pop() == self._savepoint
                    self._conn._exec_command(
                        b"release savepoint " + self._savepoint_name
                    )
                if self._outer_transaction:
                    self._conn._savepoints.pop()
                    # TODO: Add test for this assert
                    # assert self._conn._savepoints.pop() is None
                    self._conn._exec_command(b"commit")
            else:
                if self._savepoint_name:
                    self._conn._savepoints.pop()
                    # TODO: Add test for this assert
                    # assert self._conn._savepoints.pop() == self._savepoint
                    self._conn._exec_command(
                        b"rollback to savepoint " + self._savepoint_name
                    )
                if self._outer_transaction:
                    self._conn._savepoints.pop()
                    # TODO: Add test for this assert
                    # assert self._conn._savepoints.pop() is None
                    self._conn._exec_command(b"rollback")
