import logging

import pytest

import psycopg3
from psycopg3 import transaction


@pytest.fixture(autouse=True)
def test_table(svcconn):
    """
    Creates a table called 'test_table' for use in tests.
    """
    cur = svcconn.cursor()
    cur.execute("drop table if exists test_table")
    cur.execute("create table test_table (id text primary key)")
    yield
    cur.execute("drop table test_table")


def insert_row(conn, value):
    conn.cursor().execute("INSERT INTO test_table VALUES (%s)", (value,))


def assert_rows(conn, expected):
    rows = conn.cursor().execute("SELECT * FROM test_table").fetchall()
    assert set(v for (v,) in rows) == expected


def assert_not_in_transaction(conn):
    assert conn.pgconn.transaction_status == conn.TransactionStatus.IDLE


def assert_in_transaction(conn):
    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS


class ExpectedException(Exception):
    pass


def test_basic(conn):
    """Basic use of transaction() to BEGIN and COMMIT a transaction."""
    assert_not_in_transaction(conn)
    with conn.transaction():
        assert_in_transaction(conn)
    assert_not_in_transaction(conn)


def test_exposes_associated_connection(conn):
    """Transaction exposes its connection as a read-only property."""
    with conn.transaction() as tx:
        assert tx.connection is conn
        with pytest.raises(AttributeError):
            tx.connection = conn


def test_exposes_savepoint_name(conn):
    """Transaction exposes its savepoint name as a read-only property."""
    with conn.transaction(savepoint_name="foo") as tx:
        assert tx.savepoint_name == "foo"
        with pytest.raises(AttributeError):
            tx.savepoint_name = "bar"


def test_begins_on_enter(conn):
    """Transaction does not begin until __enter__() is called."""
    tx = conn.transaction()
    assert_not_in_transaction(conn)
    with tx:
        assert_in_transaction(conn)
    assert_not_in_transaction(conn)


def test_commit_on_successful_exit(conn):
    """Changes are committed on successful exit from the `with` block."""
    with conn.transaction():
        insert_row(conn, "foo")

    assert_not_in_transaction(conn)
    assert_rows(conn, {"foo"})


def test_rollback_on_exception_exit(conn):
    """Changes are rolled back if an exception escapes the `with` block."""
    with pytest.raises(ExpectedException):
        with conn.transaction():
            insert_row(conn, "foo")
            raise ExpectedException("This discards the insert")

    assert_not_in_transaction(conn)
    assert_rows(conn, set())


def test_prohibits_use_of_commit_rollback_autocommit(conn):
    """
    Within a Transaction block, it is forbidden to touch commit, rollback,
    or the autocommit setting on the connection, as this would interfere
    with the transaction scope being managed by the Transaction block.
    """
    conn.autocommit = False
    conn.commit()
    conn.rollback()

    with conn.transaction():
        with pytest.raises(psycopg3.ProgrammingError):
            conn.autocommit = False
        with pytest.raises(psycopg3.ProgrammingError):
            conn.commit()
        with pytest.raises(psycopg3.ProgrammingError):
            conn.rollback()

    conn.autocommit = False
    conn.commit()
    conn.rollback()


@pytest.mark.parametrize("autocommit", [False, True])
def test_preserves_autocommit(conn, autocommit):
    """
    Connection.autocommit is unchanged both during and after Transaction block.
    """
    conn.autocommit = autocommit
    with conn.transaction():
        assert conn.autocommit is autocommit
    assert conn.autocommit is autocommit


def test_autocommit_off_but_no_transaction_started_successful_exit(
    conn, svcconn
):
    """
    Scenario:
    * Connection has autocommit off but no transaction has been initiated
      before entering the Transaction context
    * Code exits Transaction context successfully

    Outcome:
    * Changes made within Transaction context are committed
    """
    conn.autocommit = False
    assert_not_in_transaction(conn)
    with conn.transaction():
        insert_row(conn, "new")
    assert_not_in_transaction(conn)

    # Changes committed
    assert_rows(conn, {"new"})
    assert_rows(svcconn, {"new"})


def test_autocommit_off_but_no_transaction_started_exception_exit(
    conn, svcconn
):
    """
    Scenario:
    * Connection has autocommit off but no transaction has been initiated
      before entering the Transaction context
    * Code exits Transaction context with an exception

    Outcome:
    * Changes made within Transaction context are discarded
    """
    conn.autocommit = False
    assert_not_in_transaction(conn)
    with pytest.raises(ExpectedException):
        with conn.transaction():
            insert_row(conn, "new")
            raise ExpectedException()
    assert_not_in_transaction(conn)

    # Changes discarded
    assert_rows(conn, set())
    assert_rows(svcconn, set())


def test_autocommit_off_and_transaction_in_progress_successful_exit(
    conn, svcconn
):
    """
    Scenario:
    * Connection has autocommit off but and a transaction is already in
      progress before entering the Transaction context
    * Code exits Transaction context successfully

    Outcome:
    * Changes made within Transaction context are left intact
    * Outer transaction is left running, and no changes are visible to an
      outside observer from another connection.
    """
    conn.autocommit = False
    insert_row(conn, "prior")
    assert_in_transaction(conn)
    with conn.transaction():
        insert_row(conn, "new")
    assert_in_transaction(conn)
    assert_rows(conn, {"prior", "new"})
    # Nothing committed yet; changes not visible on another connection
    assert_rows(svcconn, set())


def test_autocommit_off_and_transaction_in_progress_exception_exit(
    conn, svcconn
):
    """
    Scenario:
    * Connection has autocommit off but and a transaction is already in
      progress before entering the Transaction context
    * Code exits Transaction context with an exception

    Outcome:
    * Changes made before the Transaction context are left intact
    * Changes made within Transaction context are discarded
    * Outer transaction is left running, and no changes are visible to an
      outside observer from another connection.
    """
    conn.autocommit = False
    insert_row(conn, "prior")
    assert_in_transaction(conn)
    with pytest.raises(ExpectedException):
        with conn.transaction():
            insert_row(conn, "new")
            raise ExpectedException()
    assert_in_transaction(conn)
    assert_rows(conn, {"prior"})
    # Nothing committed yet; changes not visible on another connection
    assert_rows(svcconn, set())


def test_nested_all_changes_persisted_on_successful_exit(conn, svcconn):
    """Changes from nested transaction contexts are all persisted on exit."""
    with conn.transaction():
        insert_row(conn, "outer-before")
        with conn.transaction():
            insert_row(conn, "inner")
        insert_row(conn, "outer-after")
    assert_not_in_transaction(conn)
    assert_rows(conn, {"outer-before", "inner", "outer-after"})
    assert_rows(svcconn, {"outer-before", "inner", "outer-after"})


def test_nested_all_changes_discarded_on_outer_exception(conn, svcconn):
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
    assert_not_in_transaction(conn)
    assert_rows(conn, set())
    assert_rows(svcconn, set())


def test_nested_all_changes_discarded_on_inner_exception(conn, svcconn):
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
    assert_not_in_transaction(conn)
    assert_rows(conn, set())
    assert_rows(svcconn, set())


def test_nested_inner_scope_exception_handled_in_outer_scope(conn, svcconn):
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
    assert_not_in_transaction(conn)
    assert_rows(conn, {"outer-before", "outer-after"})
    assert_rows(svcconn, {"outer-before", "outer-after"})


def test_nested_three_levels_successful_exit(conn, svcconn):
    """Exercise management of more than one savepoint."""
    with conn.transaction():  # BEGIN
        insert_row(conn, "one")
        with conn.transaction():  # SAVEPOINT tx_savepoint_1
            insert_row(conn, "two")
            with conn.transaction():  # SAVEPOINT tx_savepoint_2
                insert_row(conn, "three")
    assert_not_in_transaction(conn)
    assert_rows(conn, {"one", "two", "three"})
    assert_rows(svcconn, {"one", "two", "three"})


def test_named_savepoints(conn, caplog):
    """
    Entering a transaction context will do one of these these things:
    1. Begin an outer transaction (if one isn't already in progress)
    2. Begin an outer transaction and create a savepoint (if one is named)
    3. Create a savepoint (if a transaction is already in progress)
       either using the name provided, or auto-generating a savepoint name.
    """
    with caplog.at_level(logging.DEBUG, logger=transaction._log.name):
        # Case 1
        with conn.transaction() as tx:
            assert tx.savepoint_name is None
            assert caplog.messages == [f"{conn}: BEGIN"]
            caplog.clear()
        assert caplog.messages == [f"{conn}: COMMIT"]
        caplog.clear()

        # Case 2
        with conn.transaction(savepoint_name="foo") as tx:
            assert tx.savepoint_name == "foo"
            assert caplog.messages == [
                f"{conn}: BEGIN",
                f"{conn}: SAVEPOINT foo",
            ]
            caplog.clear()

        # Case 3 (with savepoint name provided)
        with conn.transaction():
            caplog.clear()
            with conn.transaction(savepoint_name="bar") as tx:
                assert tx.savepoint_name == "bar"
                assert caplog.messages == [
                    f"{conn}: SAVEPOINT bar",
                ]
                caplog.clear()

        # Case 3 (with savepoint name auto-generated)
        with conn.transaction():
            caplog.clear()
            with conn.transaction() as tx:
                assert tx.savepoint_name == "tx_savepoint_1"
                assert caplog.messages == [
                    f"{conn}: SAVEPOINT tx_savepoint_1",
                ]
                caplog.clear()


def test_force_rollback_successful_exit(conn, svcconn):
    """
    Transaction started with the force_rollback option enabled discards all
    changes at the end of the context.
    """
    with conn.transaction(force_rollback=True):
        insert_row(conn, "foo")
    assert_rows(conn, set())
    assert_rows(svcconn, set())


def test_force_rollback_exception_exit(conn, svcconn):
    """
    Transaction started with the force_rollback option enabled discards all
    changes at the end of the context.
    """
    with pytest.raises(ExpectedException):
        with conn.transaction(force_rollback=True):
            insert_row(conn, "foo")
            raise ExpectedException()
    assert_rows(conn, set())
    assert_rows(svcconn, set())
