import os

# Old settings
# host = badger
# user = atf3
# pass = patO9FTPXUV0JonedaLe1COO
# name = ritchie_ensembl

db_host = "localhost"
db_user = "root"
db_pass = ""
db_name = "LOKI"

# PSU specific settings
db_host = "ritchiedb.rcc.psu.edu"
db_user = "loki_test"
db_name = "loki_test"
db_pass = "44NTx7NeEMyVrtdc"

# Get these from the environment if they exist
db_host = os.environ.get("DB_HOST", db_host)
db_user = os.environ.get("DB_USER", db_user)
db_pass = os.environ.get("DB_PASS", db_pass)
db_name = os.environ.get("DB_NAME", db_name)

