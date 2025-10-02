# Tests to loki_db interrogation methods


def test_get_version_tuple(clean_memory_db):
    # Check if the version tuple is correct
    assert clean_memory_db.getVersionTuple() == (
        2,
        2,
        5,
        "release",
        "",
        "2019-03-15",
    )  # noqa E501


def test_get_version_string(clean_memory_db):
    # Check if the version string is correct
    assert clean_memory_db.getVersionString() == "2.2.5 (2019-03-15)"


def test_get_database_driver_name(clean_memory_db):
    # Check if the database drive name
    assert clean_memory_db.getDatabaseDriverName() == "SQLite"


def test_get_database_driver_version(clean_memory_db):
    # Check if the database drive version
    import apsw

    assert (
        clean_memory_db.getDatabaseDriverVersion() == apsw.sqlitelibversion()
    )  # noqa E501


def test_get_database_interface_name(clean_memory_db):
    # Check if the database interface name
    assert clean_memory_db.getDatabaseInterfaceName() == "APSW"


def test_get_database_interface_version(clean_memory_db):
    # Check if the database interface version
    import apsw

    assert clean_memory_db.getDatabaseInterfaceVersion() == apsw.apswversion()
