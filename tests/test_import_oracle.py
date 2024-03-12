from scripts import import_oracle
from unittest import mock
import pytest
import oracledb


platforms = {
    "linux": import_oracle.ev.LINUX_TNS,
    "win32": import_oracle.ev.WINDOWS_TNS,
}


def test_platforms(monkeypatch):
    for key, value in platforms.items():
        print(key, value)
        monkeypatch.setattr(import_oracle, "platform", f"{key}")
        assert import_oracle.test_platform() == platforms[key]


@mock.patch("oracledb.connect")
@mock.patch("psycopg2.connect")
def create_cxs(mock_oracle_connect, mock_pg_connect):
    mock_oracle_connect.return_value = mock.Mock()
    mock_pg_connect.return_value = mock.Mock()
    oracle_cx, pg_cx = import_oracle.create_cxs
    assert oracle_cx is not None
    assert pg_cx is not None


@mock.patch("oracledb.connect")
@mock.patch("psycopg2.connect")
def test_create_cxs_failure(mock_oracle_connect, mock_pg_connect):
    mock_oracle_connect.side_effect = oracledb.Error("Failed to connect to Oracle DB")
    mock_pg_connect.return_value = mock.Mock()

    with pytest.raises(Exception) as excinfo:
        oracle_cx, pg_cx = import_oracle.create_cxs()

    assert "Failed to connect to Oracle DB" in str(excinfo.value)
