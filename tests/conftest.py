import pytest
from ..loki.loki_db import Database

"""
Using a Memory Database for Testing
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


# Fixture for a clean in-memory database (no schema applied)
@pytest.fixture
def clean_memory_db():
    temp_db = Database(tempMem=True)
    yield temp_db
    temp_db._db.close()  # Close the connection after the test


# Fixture for an in-memory database with the schema loaded
@pytest.fixture
def schema_memory_db():
    temp_db = Database(tempMem=True)
    temp_db.createDatabaseObjects(
        temp_db._schema["db"], "temp", list(temp_db._schema["db"].keys())
    )
    # setting test parameters
    temp_db._dbFile = "temp"
    temp_db._is_test = True
    yield temp_db
    temp_db._db.close()


# Fixture for a file-based database
@pytest.fixture
def file_based_db():
    temp_db = Database(dbFile="tests/loki_test.db")
    yield temp_db
    temp_db._db.close()
