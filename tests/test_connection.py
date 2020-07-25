import gc
import re
from contextlib import contextmanager, nullcontext

import pytest
import logging
import weakref

import psycopg3
from psycopg3 import Connection
from psycopg3.conninfo import conninfo_to_dict


@pytest.fixture
def temp_table(svcconn):
    """
    Creates a table called 'temp_table' for use in tests.

    NB: Should be specified as the first fixture in test method signatures in
    order to ensure that it gets cleaned up last, after any other connections
    used in the test are closed (so that they can't block dropping this table.)
    """
    svcconn.pgconn.exec_(b"drop table if exists temp_table")
    svcconn.pgconn.exec_(b"create table temp_table (id text primary key)")
    yield
    svcconn.pgconn.exec_(b"drop table temp_table")


def test_connect(dsn):
    conn = Connection.connect(dsn)
    assert conn.status == conn.ConnStatus.OK


def test_connect_str_subclass(dsn):
    class MyString(str):
        pass

    conn = Connection.connect(MyString(dsn))
    assert conn.status == conn.ConnStatus.OK


def test_connect_bad():
    with pytest.raises(psycopg3.OperationalError):
        Connection.connect("dbname=nosuchdb")


def test_close(conn):
    assert not conn.closed
    conn.close()
    assert conn.closed
    assert conn.status == conn.ConnStatus.BAD
    conn.close()
    assert conn.closed
    assert conn.status == conn.ConnStatus.BAD


def test_weakref(dsn):
    conn = psycopg3.connect(dsn)
    w = weakref.ref(conn)
    conn.close()
    del conn
    gc.collect()
    assert w() is None


def test_commit(temp_table, conn):
    conn.pgconn.exec_(b"begin")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    conn.pgconn.exec_(b"insert into temp_table values ('foo')")
    conn.commit()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    res = conn.pgconn.exec_(b"select id from temp_table where id = 'foo'")
    assert res.get_value(0, 0) == b"foo"

    conn.close()
    with pytest.raises(psycopg3.OperationalError):
        conn.commit()


def test_rollback(temp_table, conn):
    conn.pgconn.exec_(b"begin")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    conn.pgconn.exec_(b"insert into temp_table values ('foo')")
    conn.rollback()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    res = conn.pgconn.exec_(b"select id from temp_table where id = 'foo'")
    assert res.ntuples == 0

    conn.close()
    with pytest.raises(psycopg3.OperationalError):
        conn.rollback()


def test_auto_transaction(temp_table, conn):
    cur = conn.cursor()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE

    cur.execute("insert into temp_table values ('foo')")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS

    conn.commit()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    assert cur.execute("select * from temp_table").fetchone() == ("foo",)
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS


def test_auto_transaction_fail(temp_table, conn):
    cur = conn.cursor()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE

    cur.execute("insert into temp_table values ('foo')")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS

    with pytest.raises(psycopg3.DatabaseError):
        cur.execute("meh")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INERROR

    conn.commit()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    assert cur.execute("select * from temp_table").fetchone() is None
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS


def test_autocommit(conn):
    assert conn.autocommit is False
    conn.autocommit = True
    assert conn.autocommit
    cur = conn.cursor()
    assert cur.execute("select 1").fetchone() == (1,)
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE


def test_autocommit_connect(dsn):
    conn = Connection.connect(dsn, autocommit=True)
    assert conn.autocommit


def test_autocommit_intrans(conn):
    cur = conn.cursor()
    assert cur.execute("select 1").fetchone() == (1,)
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    with pytest.raises(psycopg3.ProgrammingError):
        conn.autocommit = True
    assert not conn.autocommit


def test_autocommit_inerror(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg3.DatabaseError):
        cur.execute("meh")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INERROR
    with pytest.raises(psycopg3.ProgrammingError):
        conn.autocommit = True
    assert not conn.autocommit


def test_autocommit_unknown(conn):
    conn.close()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.UNKNOWN
    with pytest.raises(psycopg3.ProgrammingError):
        conn.autocommit = True
    assert not conn.autocommit


def test_get_encoding(conn):
    (enc,) = conn.cursor().execute("show client_encoding").fetchone()
    assert enc == conn.client_encoding


def test_set_encoding(conn):
    newenc = "LATIN1" if conn.client_encoding != "LATIN1" else "UTF8"
    assert conn.client_encoding != newenc
    conn.client_encoding = newenc
    assert conn.client_encoding == newenc
    (enc,) = conn.cursor().execute("show client_encoding").fetchone()
    assert enc == newenc


@pytest.mark.parametrize(
    "enc, out, codec",
    [
        ("utf8", "UTF8", "utf-8"),
        ("utf-8", "UTF8", "utf-8"),
        ("utf_8", "UTF8", "utf-8"),
        ("eucjp", "EUC_JP", "euc_jp"),
        ("euc-jp", "EUC_JP", "euc_jp"),
    ],
)
def test_normalize_encoding(conn, enc, out, codec):
    conn.client_encoding = enc
    assert conn.client_encoding == out
    assert conn.codec.name == codec


@pytest.mark.parametrize(
    "enc, out, codec",
    [
        ("utf8", "UTF8", "utf-8"),
        ("utf-8", "UTF8", "utf-8"),
        ("utf_8", "UTF8", "utf-8"),
        ("eucjp", "EUC_JP", "euc_jp"),
        ("euc-jp", "EUC_JP", "euc_jp"),
    ],
)
def test_encoding_env_var(dsn, monkeypatch, enc, out, codec):
    monkeypatch.setenv("PGCLIENTENCODING", enc)
    conn = psycopg3.connect(dsn)
    assert conn.client_encoding == out
    assert conn.codec.name == codec


def test_set_encoding_unsupported(conn):
    conn.client_encoding = "EUC_TW"
    with pytest.raises(psycopg3.NotSupportedError):
        conn.cursor().execute("select 1")


def test_set_encoding_bad(conn):
    with pytest.raises(psycopg3.DatabaseError):
        conn.client_encoding = "WAT"


@pytest.mark.parametrize(
    "testdsn, kwargs, want",
    [
        ("", {}, ""),
        ("host=foo user=bar", {}, "host=foo user=bar"),
        ("host=foo", {"user": "baz"}, "host=foo user=baz"),
        (
            "host=foo port=5432",
            {"host": "qux", "user": "joe"},
            "host=qux user=joe port=5432",
        ),
        ("host=foo", {"user": None}, "host=foo"),
    ],
)
def test_connect_args(monkeypatch, pgconn, testdsn, kwargs, want):
    the_conninfo = None

    def fake_connect(conninfo):
        nonlocal the_conninfo
        the_conninfo = conninfo
        return pgconn
        yield

    monkeypatch.setattr(psycopg3.connection, "connect", fake_connect)
    psycopg3.Connection.connect(testdsn, **kwargs)
    assert conninfo_to_dict(the_conninfo) == conninfo_to_dict(want)


@pytest.mark.parametrize(
    "args, kwargs", [((), {}), (("", ""), {}), ((), {"nosuchparam": 42})],
)
def test_connect_badargs(monkeypatch, pgconn, args, kwargs):
    def fake_connect(conninfo):
        return pgconn
        yield

    monkeypatch.setattr(psycopg3.connection, "connect", fake_connect)
    with pytest.raises((TypeError, psycopg3.ProgrammingError)):
        psycopg3.Connection.connect(*args, **kwargs)


def test_broken_connection(conn):
    cur = conn.cursor()
    with pytest.raises(psycopg3.DatabaseError):
        cur.execute("select pg_terminate_backend(pg_backend_pid())")
    assert conn.closed


def test_notice_handlers(conn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg3")
    messages = []
    severities = []

    def cb1(diag):
        messages.append(diag.message_primary)

    def cb2(res):
        raise Exception("hello from cb2")

    conn.add_notice_handler(cb1)
    conn.add_notice_handler(cb2)
    conn.add_notice_handler("the wrong thing")
    conn.add_notice_handler(lambda diag: severities.append(diag.severity))

    conn.pgconn.exec_(b"set client_min_messages to notice")
    cur = conn.cursor()
    cur.execute(
        "do $$begin raise notice 'hello notice'; end$$ language plpgsql"
    )
    assert messages == ["hello notice"]
    assert severities == ["NOTICE"]

    assert len(caplog.records) == 2
    rec = caplog.records[0]
    assert rec.levelno == logging.ERROR
    assert "hello from cb2" in rec.message
    rec = caplog.records[1]
    assert rec.levelno == logging.ERROR
    assert "the wrong thing" in rec.message

    conn.remove_notice_handler(cb1)
    conn.remove_notice_handler("the wrong thing")
    cur.execute(
        "do $$begin raise warning 'hello warning'; end$$ language plpgsql"
    )
    assert len(caplog.records) == 3
    assert messages == ["hello notice"]
    assert severities == ["NOTICE", "WARNING"]

    with pytest.raises(ValueError):
        conn.remove_notice_handler(cb1)


def test_notify_handlers(conn):
    nots1 = []
    nots2 = []

    def cb1(n):
        nots1.append(n)

    conn.add_notify_handler(cb1)
    conn.add_notify_handler(lambda n: nots2.append(n))

    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("listen foo")
    cur.execute("notify foo, 'n1'")

    assert len(nots1) == 1
    n = nots1[0]
    assert n.channel == "foo"
    assert n.payload == "n1"
    assert n.pid == conn.pgconn.backend_pid

    assert len(nots2) == 1
    assert nots2[0] == nots1[0]

    conn.remove_notify_handler(cb1)
    cur.execute("notify foo, 'n2'")

    assert len(nots1) == 1
    assert len(nots2) == 2
    n = nots2[1]
    assert n.channel == "foo"
    assert n.payload == "n2"
    assert n.pid == conn.pgconn.backend_pid

    with pytest.raises(ValueError):
        conn.remove_notify_handler(cb1)


def test_transaction(conn):
    """Basic use of transaction() to BEGIN and COMMIT a transaction."""
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    with conn.transaction():
        assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE


def test_transaction_exposes_associated_connection(conn):
    """Transaction exposes it's connection as a read-only property."""
    tx = conn.transaction()
    assert tx.connection is conn
    with pytest.raises(AttributeError):
        tx.connection = conn


def test_transaction_is_not_kept_alive_by_connection(conn):
    """
    Since the Transaction has a reference back to the connection, ensure that
    the opposite is not also true, as we want to avoid reference cycles.
    """
    tx = conn.transaction()
    weak_tx = weakref.ref(tx)
    del tx
    gc.collect()
    assert weak_tx() is None


def test_transaction_begins_on_enter(conn):
    """Transaction does not begin until __enter__() is called."""
    tx = conn.transaction()
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    with tx:
        assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE


def test_transaction_commit_on_successful_exit(temp_table, conn):
    """Changes are committed on successful exit from the `with` block."""
    cur = conn.cursor()
    with conn.transaction():
        cur.execute("insert into temp_table values ('foo')")

    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    assert cur.execute("select * from temp_table").fetchone() == ("foo",)


def test_transaction_rollback_on_exception_exit(temp_table, conn):
    """Changes are rolled back if an exception escapes the `with` block."""
    cur = conn.cursor()
    with pytest.raises(ExpectedException):
        with conn.transaction():
            cur.execute("insert into temp_table values ('foo')")
            raise ExpectedException("This discards the insert")

    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    assert cur.execute("select * from temp_table").fetchone() is None


def test_transaction_prohibits_use_of_commit_rollback_autocommit(conn):
    """
    Within a Transaction block, it is forbidden to touch commit, rollback,
    or the autocommit setting on the connection, as this would interfere
    with the transaction scope being managed by the Transaction block.
    """
    conn.autocommit = False
    conn.commit()
    conn.rollback()

    with conn.transaction():
        message = re.escape(
            "can't change autocommit state when in Transaction context"
        )
        with pytest.raises(psycopg3.ProgrammingError, match=f"^{message}$"):
            conn.autocommit = False

        message = re.escape(
            "Explicit commit() forbidden within a Transaction context."
            " (Transaction will be automatically committed on successful exit"
            " from context.)"
        )
        with pytest.raises(psycopg3.ProgrammingError, match=f"^{message}$"):
            conn.commit()

        message = re.escape(
            "Explicit rollback() forbidden within a Transaction context. "
            "(Either raise Transaction.Rollback() or allow an exception to "
            "propagate out of the context.)"
        )
        with pytest.raises(psycopg3.ProgrammingError, match=f"^{message}$"):
            conn.rollback()

    conn.autocommit = False
    conn.commit()
    conn.rollback()


@contextmanager
def _transaction(conn, exception):
    """
    Context manager does things in a conn.transaction() and then optionally
    exits the context manager with an exception.
    """
    with pytest.raises(ExpectedException) if exception else nullcontext():
        with conn.transaction():
            yield
            if exception:
                raise ExpectedException()


@pytest.mark.parametrize("exception", [False, True])
@pytest.mark.parametrize("autocommit", [False, True])
def test_transaction_preserves_autocommit(conn, autocommit, exception):
    """
    Connection.autocommit value is False in Transaction block, but the original
    value is always restored after the block exits, both in successful exit and
    exception scenarios.
    """
    conn.autocommit = autocommit
    with _transaction(conn, exception):
        assert conn.autocommit is False
    assert conn.autocommit is autocommit


@pytest.mark.parametrize("exception", [False, True])
def test_transaction_autocommit_off_but_no_transaction_started(
    temp_table, conn, svcconn, exception
):
    """
    When connection has autocommit off but no transaction has been initiated
    before entering the Transaction context:
     * successful exit from the context will commit changes
     * exiting the context with an exception will discard changes
    """
    conn.autocommit = False
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    with _transaction(conn, exception):
        insert_row(conn, "new")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE
    assert conn.autocommit is False
    if exception:
        # Changes discarded
        assert_rows(conn, set())
        assert_rows(svcconn, set())
    else:
        # Changes committed
        assert_rows(conn, {"new"})
        assert_rows(svcconn, {"new"})


@pytest.mark.parametrize("exception", [False, True])
def test_transaction_autocommit_off_and_transaction_in_progress(
    temp_table, conn, svcconn, exception
):
    """
    When connection has autocommit off and a transaction is already in progress
    before entering the Transaction context:
     * successful exit from the context will leave changes made in the context
     * exiting the context with an exception will discard changes made within
       the context (but leave changes made prior to entering the context)
    In both cases, the original outer transaction is left running, and no
    changes are visible to an outside observer from another connection.
    """
    conn.autocommit = False
    insert_row(conn, "prior")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    with _transaction(conn, exception):
        insert_row(conn, "new")
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    assert conn.autocommit is False
    if exception:
        assert_rows(conn, {"prior"}, still_in_transaction=True)
    else:
        assert_rows(conn, {"prior", "new"}, still_in_transaction=True)
    # Nothing committed yet; changes not visible on another connection
    assert_rows(svcconn, set())


def test_transaction_nested_all_changes_persisted_on_successful_exit(
    temp_table, conn, svcconn
):
    """Changes from nested transaction contexts are all persisted on exit."""
    with conn.transaction():
        insert_row(conn, "outer-before")
        with conn.transaction():
            insert_row(conn, "inner")
        insert_row(conn, "outer-after")
    assert_rows(conn, {"outer-before", "inner", "outer-after"})
    assert_rows(svcconn, {"outer-before", "inner", "outer-after"})


def test_transaction_nested_all_changes_discarded_on_outer_exception(
    temp_table, conn, svcconn
):
    """
    Changes from nested transaction contexts are discarded when an exception
    raised in outer context escapes.
    """
    with pytest.raises(ExpectedException):
        with conn.transaction():
            insert_row(conn, "outer")
            with conn.transaction():
                insert_row(conn, "inner")
            raise ExpectedException()
    assert_rows(conn, set())
    assert_rows(svcconn, set())


def test_transaction_nested_all_changes_discarded_on_inner_exception(
    temp_table, conn, svcconn
):
    """
    Changes from nested transaction contexts are discarded when an exception
    raised in inner context escapes the outer context.
    """
    with pytest.raises(ExpectedException):
        with conn.transaction():
            insert_row(conn, "outer")
            with conn.transaction():
                insert_row(conn, "inner")
                raise ExpectedException()
    assert_rows(conn, set())
    assert_rows(svcconn, set())


def test_transaction_nested_inner_scope_exception_handled_in_outer_scope(
    temp_table, conn, svcconn
):
    """
    An exception escaping the inner transaction context causes changes made
    within that inner context to be discarded, but the error can then be
    handled in the outer context, allowing changes made in the outer context
    (both before, and after, the inner context) to be successfully committed.
    """
    with conn.transaction():
        insert_row(conn, "outer-before")
        with pytest.raises(ExpectedException):
            with conn.transaction():
                insert_row(conn, "inner")
                raise ExpectedException()
        insert_row(conn, "outer-after")
    assert_rows(conn, {"outer-before", "outer-after"})
    assert_rows(svcconn, {"outer-before", "outer-after"})


def insert_row(conn, value):
    conn.cursor().execute("INSERT INTO temp_table VALUES (%s)", (value,))


def assert_rows(conn, expected, still_in_transaction=False):
    if still_in_transaction:
        assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
    else:
        assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE

    rows = conn.cursor().execute("SELECT * FROM temp_table").fetchall()
    assert set(v for (v,) in rows) == expected


def assert_not_in_transaction(conn):
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE


def assert_in_transaction(conn):
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS


class ExpectedException(Exception):
    pass
