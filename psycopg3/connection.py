"""
psycopg3 connection objects
"""

# Copyright (C) 2020 The Psycopg Team

import codecs
import logging
import asyncio
import threading
from typing import (
    cast,
    Any,
    Generator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    TYPE_CHECKING,
)

from . import pq
from . import exceptions as exc
from . import cursor
from .conninfo import make_conninfo
from .waiting import wait_select, wait_async, Wait, Ready

logger = logging.getLogger(__name__)

ConnectGen = Generator[Tuple[int, Wait], Ready, pq.PGconn]
QueryGen = Generator[Tuple[int, Wait], Ready, List[pq.PGresult]]
RV = TypeVar("RV")

if TYPE_CHECKING:
    from .adaptation import AdaptersMap, TypecastersMap


class BaseConnection:
    """
    Base class for different types of connections.

    Share common functionalities such as access to the wrapped PGconn, but
    allow different interfaces (sync/async).
    """

    def __init__(self, pgconn: pq.PGconn):
        self.pgconn = pgconn
        self.cursor_factory = cursor.BaseCursor
        self.adapters: AdaptersMap = {}
        self.casters: TypecastersMap = {}
        # name of the postgres encoding (in bytes)
        self.pgenc = b""

    def cursor(
        self, name: Optional[str] = None, binary: bool = False
    ) -> cursor.BaseCursor:
        if name is not None:
            raise NotImplementedError
        return self.cursor_factory(self, binary=binary)

    @property
    def codec(self) -> codecs.CodecInfo:
        # TODO: utf8 fastpath?
        pgenc = self.pgconn.parameter_status(b"client_encoding")
        if self.pgenc != pgenc:
            # for unknown encodings and SQL_ASCII be strict and use ascii
            pyenc = pq.py_codecs.get(pgenc.decode("ascii")) or "ascii"
            self._codec = codecs.lookup(pyenc)
            self.pgenc = pgenc
        return self._codec

    def encode(self, s: str) -> bytes:
        return self.codec.encode(s)[0]

    def decode(self, b: bytes) -> str:
        return self.codec.decode(b)[0]

    @classmethod
    def _connect_gen(cls, conninfo: str) -> ConnectGen:
        """
        Generator to create a database connection without blocking.

        Yield pairs (fileno, `Wait`) whenever an operation would block. The
        generator can be restarted sending the appropriate `Ready` state when
        the file descriptor is ready.
        """
        conn = pq.PGconn.connect_start(conninfo.encode("utf8"))
        logger.debug("connection started, status %s", conn.status.name)
        while 1:
            if conn.status == pq.ConnStatus.BAD:
                raise exc.OperationalError(
                    f"connection is bad: {pq.error_message(conn)}"
                )

            status = conn.connect_poll()
            logger.debug("connection polled, status %s", conn.status.name)
            if status == pq.PollingStatus.OK:
                break
            elif status == pq.PollingStatus.READING:
                yield conn.socket, Wait.R
            elif status == pq.PollingStatus.WRITING:
                yield conn.socket, Wait.W
            elif status == pq.PollingStatus.FAILED:
                raise exc.OperationalError(
                    f"connection failed: {pq.error_message(conn)}"
                )
            else:
                raise exc.InternalError(f"unexpected poll status: {status}")

        conn.nonblocking = 1
        return conn

    @classmethod
    def _exec_gen(cls, pgconn: pq.PGconn) -> QueryGen:
        """
        Generator returning query results without blocking.

        The query must have already been sent using `pgconn.send_query()` or
        similar. Flush the query and then return the result using nonblocking
        functions.

        Yield pairs (fileno, `Wait`) whenever an operation would block. The
        generator can be restarted sending the appropriate `Ready` state when
        the file descriptor is ready.

        Return the list of results returned by the database (whether success
        or error).
        """
        results: List[pq.PGresult] = []

        while 1:
            f = pgconn.flush()
            if f == 0:
                break

            ready = yield pgconn.socket, Wait.RW
            if ready is Ready.R:
                pgconn.consume_input()
            continue

        while 1:
            pgconn.consume_input()
            if pgconn.is_busy():
                ready = yield pgconn.socket, Wait.R
            res = pgconn.get_result()
            if res is None:
                break
            results.append(res)
            if res.status in (
                pq.ExecStatus.COPY_IN,
                pq.ExecStatus.COPY_OUT,
                pq.ExecStatus.COPY_BOTH,
            ):
                # After entering copy mode the libpq will create a phony result
                # for every request so let's break the endless loop.
                break

        return results


class Connection(BaseConnection):
    """
    Wrap a connection to the database.

    This class implements a DBAPI-compliant interface.
    """

    cursor_factory: Type[cursor.Cursor]

    def __init__(self, pgconn: pq.PGconn):
        super().__init__(pgconn)
        self.lock = threading.Lock()
        self.cursor_factory = cursor.Cursor

    @classmethod
    def connect(
        cls, conninfo: str, connection_factory: Any = None, **kwargs: Any
    ) -> "Connection":
        if connection_factory is not None:
            raise NotImplementedError()
        conninfo = make_conninfo(conninfo, **kwargs)
        gen = cls._connect_gen(conninfo)
        pgconn = cls.wait(gen)
        return cls(pgconn)

    def cursor(
        self, name: Optional[str] = None, binary: bool = False
    ) -> cursor.Cursor:
        cur = super().cursor(name, binary)
        return cast(cursor.Cursor, cur)

    def commit(self) -> None:
        self._exec_commit_rollback(b"commit")

    def rollback(self) -> None:
        self._exec_commit_rollback(b"rollback")

    def _exec_commit_rollback(self, command: bytes) -> None:
        with self.lock:
            status = self.pgconn.transaction_status
            if status == pq.TransactionStatus.IDLE:
                return

            self.pgconn.send_query(command)
            (pgres,) = self.wait(self._exec_gen(self.pgconn))
            if pgres.status != pq.ExecStatus.COMMAND_OK:
                raise exc.OperationalError(
                    f"error on {command.decode('utf8')}:"
                    f" {pq.error_message(pgres)}"
                )

    @classmethod
    def wait(cls, gen: Generator[Tuple[int, Wait], Ready, RV]) -> RV:
        return wait_select(gen)


class AsyncConnection(BaseConnection):
    """
    Wrap an asynchronous connection to the database.

    This class implements a DBAPI-inspired interface, with all the blocking
    methods implemented as coroutines.
    """

    cursor_factory: Type[cursor.AsyncCursor]

    def __init__(self, pgconn: pq.PGconn):
        super().__init__(pgconn)
        self.lock = asyncio.Lock()
        self.cursor_factory = cursor.AsyncCursor

    @classmethod
    async def connect(cls, conninfo: str, **kwargs: Any) -> "AsyncConnection":
        conninfo = make_conninfo(conninfo, **kwargs)
        gen = cls._connect_gen(conninfo)
        pgconn = await cls.wait(gen)
        return cls(pgconn)

    def cursor(
        self, name: Optional[str] = None, binary: bool = False
    ) -> cursor.AsyncCursor:
        cur = super().cursor(name, binary)
        return cast(cursor.AsyncCursor, cur)

    async def commit(self) -> None:
        await self._exec_commit_rollback(b"commit")

    async def rollback(self) -> None:
        await self._exec_commit_rollback(b"rollback")

    async def _exec_commit_rollback(self, command: bytes) -> None:
        async with self.lock:
            status = self.pgconn.transaction_status
            if status == pq.TransactionStatus.IDLE:
                return

            self.pgconn.send_query(command)
            (pgres,) = await self.wait(self._exec_gen(self.pgconn))
            if pgres.status != pq.ExecStatus.COMMAND_OK:
                raise exc.OperationalError(
                    f"error on {command.decode('utf8')}:"
                    f" {pq.error_message(pgres)}"
                )

    @classmethod
    async def wait(cls, gen: Generator[Tuple[int, Wait], Ready, RV]) -> RV:
        return await wait_async(gen)