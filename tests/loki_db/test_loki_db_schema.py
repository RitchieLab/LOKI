# Tests to loki_db database schema methods


def test_loki_db_create_objects(clean_memory_db):
    # The method createDatabaseTables is protected, so we need to
    # call it directly, and it call the createDatabaseObject method

    # Create all tables in the temporary database
    clean_memory_db.createDatabaseTables(
        clean_memory_db._schema["db"],
        "temp",
        list(clean_memory_db._schema["db"].keys()),
    )

    # Check if the 'setting' table was created and contains the expected data
    cursor = clean_memory_db._db.cursor()
    cursor.execute("SELECT * FROM setting")
    tables = cursor.fetchall()
    assert tables[0][0] == "schema"

    # Define the tables and expected first values to check
    table_checks = {
        "grch_ucschg": 34,
        "ldprofile": 0,
        "namespace": 0,
        "relationship": 0,
        "role": 0,
        "source": 0,
        "source_option": 0,
        "source_file": 0,
        "type": 0,
        "subtype": 0,
        "warning": 0,
        "snp_merge": 0,
        "snp_locus": 0,
        "snp_entrez_role": 0,
        "snp_biopolymer_role": 0,
        "biopolymer": 0,
        "biopolymer_name": 0,
        "biopolymer_name_name": 0,
        "biopolymer_region": 0,
        "biopolymer_zone": 0,
        "'group'": 0,  # 'group' is a reserved keyword, so it needs quotes
        "group_name": 0,
        "group_group": 0,
        "group_biopolymer": 0,
        "group_member_name": 0,
        "gwas": 0,
        "chain": 0,
        "chain_data": 0,
    }

    # Loop over the tables and check their content
    for table, expected_value in table_checks.items():
        cursor.execute(f"SELECT * FROM {table}")
        tables = cursor.fetchall()
        if expected_value == 0:
            assert len(tables) == 0  # No records expected
        else:
            assert tables[0][0] == expected_value  # Check first record value


# FIXME: How to test the index creation?
def test_create_database_indices(clean_memory_db):
    # Create all tables in the temporary database
    clean_memory_db.createDatabaseTables(
        clean_memory_db._schema["db"],
        "temp",
        list(clean_memory_db._schema["db"].keys()),
    )
    # Create all indices in the temporary database
    clean_memory_db.createDatabaseIndices(
        clean_memory_db._schema["db"],
        "temp",
        list(clean_memory_db._schema["db"].keys()),
    )

    cursor = clean_memory_db._db.cursor()

    # Check if the 'snp_merge' table was created successfully
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='snp_merge'"
    )  # noqa E501
    table_exists = cursor.fetchone()
    assert table_exists is None  # The snp_merge table should exist

    # Check if the index on 'snp_merge' was created successfully
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='snp_merge' AND name='snp_merge__merge_current'"
    )  # noqa E501
    index_exists = cursor.fetchone()
    assert index_exists is None  # snp_merge__merge_current idx should exist

    # Idx list to check
    # warning__source": "(source_id)
    # snp_merge__merge_current": "(rsMerged,rsCurrent)
    # snp_locus__rs_chr_pos": "(rs,chr,pos)
    # snp_locus__chr_pos_rs": "(chr,pos,rs)
    # snp_entrez_role__rs_entrez_role": "(rs,entrez_id,role_id)
    # snp_biopolymer_role__rs_biopolymer_role": "(rs,biopolymer_id,role_id)
    # snp_biopolymer_role__biopolymer_rs_role": "(biopolymer_id,rs,role_id)
    # biopolymer__type": "(type_id)
    # biopolymer__label_type": "(label,type_id)
    # biopolymer_name__name_namespace_biopolymer": "(name,namespace_id,biopolymer_id)  # noqa E501
    # biopolymer_region__ldprofile_chr_min": "(ldprofile_id,chr,posMin)
    # biopolymer_region__ldprofile_chr_max": "(ldprofile_id,chr,posMax)
    # biopolymer_zone__zone": "(chr,zone,biopolymer_id)
    # group_name__name_namespace_group": "(name,namespace_id,group_id)
    # group_name__source_name": "(source_id,name)
    # group_group__related": "(related_group_id,group_id)
    # group_biopolymer__biopolymer": "(biopolymer_id,group_id)
    # gwas__rs": "(rs)"
    # gwas__chr_pos": "(chr,pos)
    # chain__oldhg_newhg_chr": "(old_ucschg,new_ucschg,old_chr)
    # chain_data__end": "(chain_id,old_end)
