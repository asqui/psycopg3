"""
Transaction context managers returned by Connection.transaction()
"""

# Copyright (C) 2020 The Psycopg Team

# TODO: Should this module be _underscored since it's implementation detail?
import weakref
from types import TracebackType
from typing import Optional, Type

from . import errors as e
from .pq import TransactionStatus


class Transaction:
    def __init__(self, conn: "Connection") -> None:
        self._conn = conn
        # self._original_autocommit: bool

    @property
    def connection(self) -> "Connection":
        return self._conn

    def __enter__(self):
        with self._conn.lock:
            if self._conn.pgconn.transaction_status == TransactionStatus.IDLE:
                self._outer_transaction = True
                self._savepoint = False

                self._original_autocommit = self._conn.autocommit
                if self._conn._autocommit:
                    self._conn._autocommit = False

                self._conn._start_query()
                assert len(self._conn._savepoints) == 0
                self._conn._savepoints.append(None)
            else:
                self._outer_transaction = False
                self._savepoint = "tx_savepoint_{}".format(
                    len([s for s in self._conn._savepoints if s is not None])
                    + 1
                )
                self.__exec(f"savepoint {self._savepoint}")
                self._conn._savepoints.append(self._savepoint)

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        with self._conn.lock:
            if exc_type is None:
                if self._savepoint:
                    self._conn._savepoints.pop()
                    # TODO: Add test for this assert
                    # assert self._conn._savepoints.pop() == self._savepoint
                    self.__exec(f"release savepoint {self._savepoint}")
                if self._outer_transaction:
                    self._conn._savepoints.pop()
                    # TODO: Add test for this assert
                    # assert self._conn._savepoints.pop() is None
                    self.__exec("commit")
            else:
                if self._savepoint:
                    self._conn._savepoints.pop()
                    # TODO: Add test for this assert
                    # assert self._conn._savepoints.pop() == self._savepoint
                    self.__exec(f"rollback to savepoint {self._savepoint}")
                if self._outer_transaction:
                    self._conn._savepoints.pop()
                    # TODO: Add test for this assert
                    # assert self._conn._savepoints.pop() is None
                    self.__exec("rollback")

            if self._outer_transaction:
                self._conn._autocommit = self._original_autocommit

    def __exec(self, command: str):
        self._conn._exec(command.encode("ascii"))
