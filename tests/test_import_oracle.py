from scripts import import_oracle
from unittest import mock
import pytest
import oracledb


def test_platforms(monkeypatch):
    """Test platforms. Need to add handling for Darwin"""
    platforms = {
        "linux": import_oracle.ev.LINUX_TNS,
        "win32": import_oracle.ev.WINDOWS_TNS,
    }
    for key, value in platforms.items():
        print(key, value)
        monkeypatch.setattr(import_oracle, "platform", f"{key}")
        assert import_oracle.test_platform() == platforms[key]


@mock.patch("oracledb.connect")
@mock.patch("psycopg2.connect")
def create_cxs(mock_oracle_connect, mock_pg_connect):
    """Test successful connections with mock values"""
    mock_oracle_connect.return_value = mock.Mock()
    mock_pg_connect.return_value = mock.Mock()
    oracle_cx, pg_cx = import_oracle.create_cxs
    assert oracle_cx is not None
    assert pg_cx is not None


@mock.patch("oracledb.connect")
@mock.patch("psycopg2.connect")
def test_create_cxs_failure(mock_oracle_connect, mock_pg_connect):
    """Test one db connection failing"""
    mock_oracle_connect.side_effect = oracledb.Error("Failed to connect to Oracle DB")
    mock_pg_connect.return_value = mock.Mock()

    with pytest.raises(Exception) as excinfo:
        oracle_cx, pg_cx = import_oracle.create_cxs()

    assert "Failed to connect to Oracle DB" in str(excinfo.value)


def test_get_pg_create_table_query_multicolumn():
    """Tests the create query string"""
    table_name = "test"
    columns = {
        "id": "<DbType DB_TYPE_VARCHAR>",
        "length": "<DbType DB_TYPE_VARCHAR>",
        "geom": "<DbType DB_TYPE_VARCHAR>",
    }

    expected_query = (
        "CREATE TABLE IF NOT EXISTS test (id VARCHAR, length VARCHAR, geom VARCHAR);"
    )
    actual_query = import_oracle.get_pg_create_table_query(
        table_name,
        columns,
    )
    assert expected_query == actual_query


def test_convert_pg_types_to_oracle_unmapped_key():
    """Tests what happens if an unexpected oracle type occurs"""
    oracle_type = "<DbType DB_TYPE_BINARY>"
    with pytest.raises(Exception) as excinfo:
        import_oracle.convert_pg_types_to_oracle(oracle_type)
    assert "Unmapped Oracle Type. Add type to oracle2pg dict." in str(excinfo.value)
