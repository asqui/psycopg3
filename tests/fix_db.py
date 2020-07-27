import os
import pytest

from psycopg3 import pq


def pytest_addoption(parser):
    parser.addoption(
        "--test-dsn",
        metavar="DSN",
        default=os.environ.get("PSYCOPG3_TEST_DSN") or None,
        help="Connection string to run database tests requiring a connection"
        " [you can also use the PSYCOPG3_TEST_DSN env var].",
    )


@pytest.fixture(scope="session")
def dsn(request):
    """Return the dsn used to connect to the `--test-dsn` database."""
    dsn = request.config.getoption("--test-dsn")
    if not dsn:
        pytest.skip("skipping test as no --test-dsn")
    return dsn


@pytest.fixture
def pgconn(dsn):
    """Return a PGconn connection open to `--test-dsn`."""
    conn = pq.PGconn.connect(dsn.encode("utf8"))
    if conn.status != pq.ConnStatus.OK:
        pytest.fail(
            f"bad connection: {conn.error_message.decode('utf8', 'replace')}"
        )
    yield conn
    conn.finish()


@pytest.fixture
def conn(dsn):
    """Return a `Connection` connected to the ``--test-dsn`` database."""
    from psycopg3 import Connection

    conn = Connection.connect(dsn)
    yield conn
    conn.close()


@pytest.fixture
async def aconn(dsn):
    """Return an `AsyncConnection` connected to the ``--test-dsn`` database."""
    from psycopg3 import AsyncConnection

    conn = await AsyncConnection.connect(dsn)
    yield conn
    await conn.close()


@pytest.fixture(scope="session")
def svcconn(dsn):
    """
    Return a session `Connection` connected to the ``--test-dsn`` database.
    """
    from psycopg3 import Connection

    conn = Connection.connect(dsn, autocommit=True)
    yield conn
    conn.close()


@pytest.fixture
def create_test_table(svcconn):
    """
    This fixture gives a factory for creating tables for use in tests.

    NB: Should be specified as the first fixture in test method signatures in
    order to ensure that it gets cleaned up last, after any other connections
    used in the test are closed (so that they can't block dropping the tables.)
    """
    tables = []

    def create(name: bytes, columns: bytes):
        assert name not in tables, f"Table {name} already created in this test"
        tables.append(name)
        svcconn.pgconn.exec_(b"drop table if exists %b" % name)
        svcconn.pgconn.exec_(b"create table %b (%b)" % (name, columns))

    yield create

    for name in tables:
        svcconn.pgconn.exec_(b"drop table %b" % name)
