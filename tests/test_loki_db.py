import pytest
from loki.loki_db import Database


"""
Using a Temporary Database for Testing
=======================================

1. Realism: You are testing with an actual database, ensuring that SQL
    operations are executed just as they would in production, potentially
    catching errors that mocks might miss.
2. Safety: Since the database is temporary and stored in memory, there's no
    risk of corrupting real data or accidentally persisting test data.
3. Performance: In-memory databases typically have very fast performance,
    making them ideal for unit tests.
4. Isolation: Each test can create its own temporary database, which
    disappears automatically after the test finishes, ensuring that tests are
    independent of one another.

How It Works:
-------------
- `temp_db` Fixture:
    - The `pytest.fixture` creates a temporary instance of the `Database` class
        connected to an in-memory database.
    - The temporary database is automatically cleaned up after the test.

- Database Creation and Operation:
    - During the test, you can create tables, insert, and retrieve data using
        a real connection to the temporary database.
    - This database only exists for the duration of the test and is
        automatically deleted afterward, ensuring that the test remains
        isolated and has no impact on the real environment.
"""


@pytest.fixture
def temp_db():
    # Uses memory for temporary storage
    db = Database(tempMem=True)
    db.attachTempDatabase("test_temp_db")
    yield db  # test instance delivery
    # The temporary database will be automatically discarded after the test


def test_get_version_tuple(temp_db: Database):
    # Check if the version tuple is correct
    assert temp_db.getVersionTuple() == (2, 2, 5, "release", "", "2019-03-15")


def test_get_version_string(temp_db: Database):
    # Check if the version string is correct
    assert temp_db.getVersionString() == "2.2.5 (2019-03-15)"


def test_get_database_driver_name(temp_db: Database):
    # Check if the database drive name
    assert temp_db.getDatabaseDriverName() == "SQLite"


def test_get_database_driver_version(temp_db: Database):
    # Check if the database drive version
    import apsw
    assert temp_db.getDatabaseDriverVersion() == apsw.sqlitelibversion()


def test_get_database_interface_name(temp_db: Database):
    # Check if the database interface name
    assert temp_db.getDatabaseInterfaceName() == "APSW"


def test_get_database_interface_version(temp_db: Database):
    # Check if the database interface version
    import apsw
    assert temp_db.getDatabaseInterfaceVersion() == apsw.apswversion()


def test_attach_temp_database_only(temp_db: Database):
    cursor = temp_db._db.cursor()
    # Create a table in the temporary database
    cursor.execute(
        "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)"
        )
    # Insert a value
    cursor.execute("INSERT INTO test_table (value) VALUES ('test_value')")
    # Retrieve the inserted value
    cursor.execute("SELECT value FROM test_table WHERE id = 1")
    result = cursor.fetchone()
    # Check if the inserted value was retrieved correctly
    assert result[0] == 'test_value'  # type: ignore

# NOTE: This test no works to memory database / only to file database
# def test_test_database_writeable(temp_db: Database):
#     assert temp_db.testDatabaseWriteable() is True


def test_create_database_objects(temp_db: Database):
    # Create a table in the temporary database
    temp_db.createDatabaseObjects(
        temp_db._schema["db"], "temp", list(temp_db._schema["db"].keys())
        )
    # Check if the table was created successfully
    cursor = temp_db._db.cursor()

    cursor.execute("SELECT * FROM setting")
    tables = cursor.fetchall()
    assert tables[0][0] == "schema"

    cursor.execute("SELECT * FROM grch_ucschg")
    tables = cursor.fetchall()
    assert tables[0][0] == 34

    cursor.execute("SELECT * FROM ldprofile")
    tables = cursor.fetchall()
    assert len(tables) == 0

# ldprofile


# @pytest.fixture
# def temp_db():
#     """
#     Fixture to create a temporary Database instance for testing.
#     This creates a temporary SQLite in-memory database.
#     """
#     db = Database(tempMem=True)  # Use in-memory database
#     db.createDatabaseTables(db._schema, "temp", list(db._schema["db"].keys()))
#     return db



# def test_checkTesting_no_setting(temp_db):
#     """
#     Test the _checkTesting method when no "testing" setting exists in the database.
#     """
#     temp_db.setDatabaseSetting("testing", None)  # Remove "testing" setting

#     result = temp_db._checkTesting()

#     # Assert that the setting is updated and method returns True
#     assert temp_db.getDatabaseSetting("testing") == "True"
#     assert result is True

# def test_checkTesting_matching_setting(temp_db):
#     """
#     Test the _checkTesting method when the "testing" setting matches the current testing state.
#     """
#     temp_db.setDatabaseSetting("testing", "1")  # Matching state

#     result = temp_db._checkTesting()

#     # Assert that the setting is updated and method returns True
#     assert result is True

# def test_checkTesting_non_matching_setting(temp_db):
#     """
#     Test the _checkTesting method when the "testing" setting does not match the current testing state.
#     """
#     temp_db.setDatabaseSetting("testing", "0")  # Non-matching state

#     result = temp_db._checkTesting()

#     # Assert that the setting is not updated and method returns False
#     assert result is False

"""
Test with file
"""


def test_attach_database_file():
    db = Database()

    # Teste com um arquivo de banco de dados real
    dbFile = 'loki.db'

    # Anexa o banco de dados real
    db.attachDatabaseFile(dbFile)

    # Verifica se o banco de dados foi anexado corretamente
    assert db._dbFile == dbFile
