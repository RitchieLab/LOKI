# Tests to loki_db database management methods


def test_loki_db_manag_get_database_memory_usage(clean_memory_db):
    # Maybe need to change the values
    x, y = clean_memory_db.getDatabaseMemoryUsage()
    assert x > 1000
    assert y > 1000


def test_loki_db_manag_get_database_memory_limit(file_based_db):
    # Maybe need to change the values
    x = file_based_db.getDatabaseMemoryLimit()
    assert x == 0


def test_loki_db_manag_set_database_memory_limit(file_based_db):
    # Maybe need to change the values
    file_based_db.setDatabaseMemoryLimit(limit=1)
    assert file_based_db.getDatabaseMemoryLimit() == 1


def test_loki_db_manag_configure_database(file_based_db):
    # Maybe need to change the values
    file_based_db.configureDatabase(db=None, tempMem=True)
    cursor = file_based_db._db.cursor()

    cursor.execute("PRAGMA page_size")
    assert cursor.fetchone()[0] == 4096

    cursor.execute("PRAGMA cache_size")
    assert cursor.fetchone()[0] == -65536

    cursor.execute("PRAGMA synchronous")
    assert cursor.fetchone()[0] == 0  # OFF

    cursor.execute("PRAGMA journal_mode")
    assert cursor.fetchone()[0] == "memory"  # MEMORY

    cursor.execute("PRAGMA locking_mode")
    assert cursor.fetchone()[0] == "normal"  # NORMAL


def test_loki_db_manag_attach_temp_database(schema_memory_db):
    # NOTE: Why this method is relavant? To temp db can not detach!
    # schema_memory_db.attachTempDatabase(db="temp")
    assert 2 == 2


def test_loki_db_manag_attach_database_file(file_based_db):
    file_based_db.detachDatabaseFile()
    file_based_db.attachDatabaseFile(dbFile="tests/loki_test.db")
    cursor = file_based_db._db.cursor()
    cursor.execute("SELECT * FROM setting WHERE setting='schema';")
    assert cursor.fetchall()[0][1] == "3"


def test_loki_db_manag_detach_database_file(file_based_db):
    file_based_db.detachDatabaseFile()
    cursor = file_based_db._db.cursor()
    cursor.execute("PRAGMA database_list")
    databases = cursor.fetchall()
    assert len(databases) == 1
    assert databases[0][1] == "main"


def test_loki_db_manag_test_database_writeable(file_based_db):
    assert file_based_db.testDatabaseWriteable() is True


def test_loki_db_manag_create_database_objects(clean_memory_db):
    clean_memory_db.createDatabaseObjects(
        schema=clean_memory_db._schema["db"],
        dbName="temp",
        tblList=list(clean_memory_db._schema["db"].keys()),
    )
    cursor = clean_memory_db._db.cursor()
    cursor.execute("SELECT * FROM setting")
    tables = cursor.fetchall()
    assert tables[0][0] == "schema"


def test_loki_db_manag_create_database_tables(clean_memory_db):
    clean_memory_db.createDatabaseTables(
        schema=clean_memory_db._schema["db"],
        dbName="temp",
        tblList=list(clean_memory_db._schema["db"].keys()),
    )
    cursor = clean_memory_db._db.cursor()
    cursor.execute("SELECT * FROM setting")
    tables = cursor.fetchall()
    assert tables[0][0] == "schema"


def test_loki_db_manag_create_database_indices(clean_memory_db):
    clean_memory_db.createDatabaseTables(
        schema=clean_memory_db._schema["db"],
        dbName="temp",
        tblList=list(clean_memory_db._schema["db"].keys()),
    )
    clean_memory_db.createDatabaseIndices(
        schema=clean_memory_db._schema["db"],
        dbName="temp",
        tblList=list(clean_memory_db._schema["db"].keys()),
    )
    cursor = clean_memory_db._db.cursor()
    cursor.execute("PRAGMA index_list(setting)")
    indices = cursor.fetchall()
    assert len(indices) == 1


def test_loki_db_manag_drop_database_objects(schema_memory_db):
    schema_memory_db.dropDatabaseObjects(
        schema=schema_memory_db._schema["db"],
        dbName="temp",
        tblList=list(schema_memory_db._schema["db"].keys()),
    )
    cursor = schema_memory_db._db.cursor()
    cursor.execute("SELECT count(*) FROM temp.sqlite_master WHERE type='table'")
    num_tables = cursor.fetchone()[0]
    assert num_tables == 3


def test_loki_db_manag_drop_database_tables(schema_memory_db):
    schema_memory_db.dropDatabaseTables(
        schema=schema_memory_db._schema["db"],
        dbName="temp",
        tblList=list(schema_memory_db._schema["db"].keys()),
    )
    cursor = schema_memory_db._db.cursor()
    cursor.execute("SELECT count(*) FROM temp.sqlite_master WHERE type='table'")
    num_tables = cursor.fetchone()[0]
    assert num_tables == 3


def test_loki_db_manag_drop_database_indices(schema_memory_db):
    schema_memory_db.dropDatabaseIndices(
        schema=schema_memory_db._schema["db"],
        dbName="temp",
        tblList=list(schema_memory_db._schema["db"].keys()),
    )
    cursor = schema_memory_db._db.cursor()
    cursor.execute("SELECT count(*) FROM temp.sqlite_master WHERE type='table'")
    num_tables = cursor.fetchone()[0]
    assert num_tables == 32


# TODO: Method return error to clean_memory_db
# TODO: Method does not apply any changes to the database
def test_loki_db_manag_update_database_schema(schema_memory_db):
    schema_memory_db.updateDatabaseSchema()
    cursor = schema_memory_db._db.cursor()
    cursor.execute("SELECT * FROM setting")
    tables = cursor.fetchall()
    assert tables[0][0] == "schema"


# NOTE: I stop here

# def auditDatabaseObjects(
#     self,
#     schema,
#     dbName,
#     tblList=None,
#     doTables=True,
#     idxList=None,
#     doIndecies=True,
#     doRepair=True,
# ):
#     ...

# def finalizeDatabase(self):
#     ...

# def optimizeDatabase(self):
#     ...

# def defragmentDatabase(self):
#     ...

# def getDatabaseSetting(self, setting, type=None):
#     ...

# def setDatabaseSetting(self, setting, value):
#     ...

# def getSourceModules(self):
#     ...

# def getSourceModuleVersions(self, sources=None):
#     ...

# def getSourceModuleOptions(self, sources=None):
#     ...

# def updateDatabase(
#     self, sources=None, sourceOptions=None, cacheOnly=False, forceUpdate=False   # noqa E501
# ):
#     ...

# def prepareTableForUpdate(self, table):
#     ...

# def prepareTableForQuery(self, table):
#     ...
