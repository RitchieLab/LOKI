import pytest

# import zipfile
# from unittest.mock import MagicMock, patch
from loki.loaders.loki_source_biogrid import Source_biogrid
from loki import loki_db


def test_get_version_string():
    assert Source_biogrid.getVersionString() == "2.1 (2022-04-13)"


@pytest.fixture
def database():
    # Aqui você pode inicializar o banco de dados em memória
    db = loki_db.Database(":memory:")  # Usando um banco de dados SQLite em memória
    # Carregar dados de teste se necessário
    return db


# @pytest.fixture
# def source_biogrid(database):
#     return Source_biogrid(database)  # Passar o banco de dados real para a classe

# def test_download(source_biogrid):
#     options = {}
#     path = "/some/path"
#     expected_files = [path + "/BIOGRID-ORGANISM-LATEST.tab2.zip"]

#     result = source_biogrid.download(options, path)

#     # Aqui, em vez de usar mocks, você verifica os resultados com base no banco de dados real
#     assert result == expected_files
