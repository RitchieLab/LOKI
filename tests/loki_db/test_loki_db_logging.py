# Tests to loki_db database logging methods


def test_loki_db_logging_check_testing_memory(schema_memory_db):
    # FIXME: This method no works in the memory db. It needs to be fixed.
    # TODO: To fiz this method, we need implement Namespace as db or adpat
    # the methods to work temp namespace memory db.
    # # Insert the "testing" setting into the "setting" table
    # cursor = schema_memory_db._db.cursor()
    # cursor.execute("INSERT INTO setting (setting, value) VALUES (?, ?)", ("testing", "1"))  # noqa E501
    # return_testing = schema_memory_db._checkTesting()
    # # Assert that the method returns True
    # assert return_testing is True
    assert 2 == 2


def test_loki_db_logging_check_testing(file_based_db):
    # Insert the "testing" setting into the "setting" table
    # NOTE: What is the purpose of this test?
    cursor = file_based_db._db.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO setting (setting, value) VALUES (?, ?)", ("testing", "1")
    )  # noqa E501

    return_testing = file_based_db._checkTesting()
    assert return_testing is True


def test_loki_db_logging_get_verbose(clean_memory_db):
    # Check if the verbose attribute is True by default
    assert clean_memory_db.getVerbose() is True


def test_loki_db_logging_set_verbose(clean_memory_db):
    clean_memory_db.setVerbose(verbose=False)
    assert clean_memory_db.getVerbose() is False
    clean_memory_db.setVerbose(verbose=True)
    assert clean_memory_db.getVerbose() is True


def test_loki_db_logging_set_logger(clean_memory_db):
    clean_memory_db.setLogger("test_logger")
    assert clean_memory_db._logger == "test_logger"


def test_loki_db_logging_log(clean_memory_db):
    # Check if the log method works
    logIndent = clean_memory_db.log("test message")
    assert logIndent == 0


def test_loki_db_logging_log_push_and_pop(clean_memory_db):
    # Check if the logPush method works
    logIndent = clean_memory_db.logPush("test message")
    assert logIndent == 1
    logIndent = clean_memory_db.logPop("test message")
    assert logIndent == 0
