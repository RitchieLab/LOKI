# Tests to loki_db database connections methods


def test_loki_db_connection_with_file_based_db(file_based_db):
    # Test file db connection
    cursor = file_based_db._db.cursor()
    cursor.execute("SELECT * FROM setting")
    tables = cursor.fetchall()
    assert tables[0][0] == "schema"


def test_loki_db_connection_schema_memory_db(schema_memory_db):
    # Test Schema Memory db connection
    cursor = schema_memory_db._db.cursor()
    cursor.execute("SELECT * FROM setting")
    tables = cursor.fetchall()
    assert tables[0][0] == "schema"


def test_loki_db_connection_clean_memory_db(clean_memory_db):
    # Test Clean Memory db connection
    cursor = clean_memory_db._db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    assert (
        len(tables) == 0
    ), "No tables should exist in a clean memory database."  # noqa E501


def test_loki_db_connection_create_object(clean_memory_db):
    cursor = clean_memory_db._db.cursor()
    # Create a table in the temporary database
    cursor.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)")
    # Insert a value
    cursor.execute("INSERT INTO test_table (value) VALUES ('test_value')")
    # Retrieve the inserted value
    cursor.execute("SELECT value FROM test_table WHERE id = 1")
    result = cursor.fetchone()
    # Check if the inserted value was retrieved correctly
    assert result[0] == "test_value"  # type: ignore
