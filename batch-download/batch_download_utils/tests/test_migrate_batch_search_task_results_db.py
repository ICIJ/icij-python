import pytest
from batch_download_utils.migrate_batch_search_task_results_db import db_handling

def test_db_handling_postgres():
    assert (db_handling("jdbc:postgresql://postgres/foo?user=bar&password=baz")
            == "postgresql+asyncpg://postgres/foo?user=bar&password=baz")

def test_db_handling_sqlite():
    assert (db_handling("jdbc:sqlite:file:/path/to/db")
            == "sqlite+aiosqlite:////path/to/db")

def test_db_handling_mysql():
    assert (db_handling("jdbc:mysql://database/foo?user=bar&password=baz")
            == "mysql+aiomysql://database/foo?user=bar&password=baz")

def test_db_handling_unknown():
    with pytest.raises(ValueError):
        db_handling("jdbc:unknown://localhost/foo?user=bar&password=baz")

