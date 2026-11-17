import threading
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from dbt.adapters.clickhouse.connections import (
    _ACTIVE_QUERY_ID_ATTR,
    ClickHouseConnectionManager,
)
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtRuntimeError


def _make_manager_with_client(mock_client):
    conn = MagicMock()
    conn.name = 'test'
    conn.handle = mock_client

    manager = ClickHouseConnectionManager.__new__(ClickHouseConnectionManager)
    manager.get_thread_connection = MagicMock(return_value=conn)
    manager._add_query_comment = lambda s: s
    return manager


def _make_manager_and_conn(mock_client):
    # A SimpleNamespace, not a MagicMock: the binding check relies on
    # getattr(conn, _ACTIVE_QUERY_ID_ATTR, None) returning None when unset,
    # which a MagicMock would auto-create instead.
    conn = SimpleNamespace(name='test', handle=mock_client)

    manager = ClickHouseConnectionManager.__new__(ClickHouseConnectionManager)
    manager.get_thread_connection = MagicMock(return_value=conn)
    manager._add_query_comment = lambda s: s
    return manager, conn


class TestAdapterResponseQueryId:
    def test_query_id_passed_to_client_on_fetch(self):
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.result_set = [('val',)]
        mock_result.column_names = ['col']
        mock_client.query.return_value = mock_result

        response, _ = _make_manager_with_client(mock_client).execute('SELECT 1', fetch=True)

        _, kwargs = mock_client.query.call_args
        assert 'query_id' in kwargs
        assert kwargs['query_id'] == response.query_id

    def test_query_id_passed_to_client_on_command(self):
        mock_client = MagicMock()
        mock_client.command.return_value = None

        response, _ = _make_manager_with_client(mock_client).execute(
            'CREATE TABLE t (x Int32) ENGINE=Memory'
        )

        _, kwargs = mock_client.command.call_args
        assert 'query_id' in kwargs
        assert kwargs['query_id'] == response.query_id

    def test_query_id_is_valid_uuid(self):
        mock_client = MagicMock()
        mock_client.command.return_value = None

        response, _ = _make_manager_with_client(mock_client).execute('SELECT 1')

        # raises ValueError if not a valid UUID
        uuid.UUID(response.query_id)

    def test_query_id_unique_per_call(self):
        mock_client = MagicMock()
        mock_client.command.return_value = None

        manager = _make_manager_with_client(mock_client)
        r1, _ = manager.execute('SELECT 1')
        r2, _ = manager.execute('SELECT 1')

        assert r1.query_id != r2.query_id


class TestGetTableFromResponse:
    def test_returns_empty_table_when_no_column_metadata(self):
        # shape of a GRANT ... ON CLUSTER response from clickhouse-connect
        response = [['ch0', '9000', '0', '', '1', '0\nch1', '9000', '0', '', '0', '0']]

        table = ClickHouseConnectionManager.get_table_from_response(response, ())

        assert len(table.rows) == 0

    def test_fetch_on_cluster_grant_does_not_raise(self):
        mock_client = MagicMock()
        mock_client.query.return_value.result_set = [['ch0', '9000', '0', '', '0', '0']]
        mock_client.query.return_value.column_names = ()

        _, table = _make_manager_with_client(mock_client).execute(
            'GRANT ON CLUSTER c some_role TO some_user', fetch=True
        )

        assert len(table.rows) == 0


def test_reuse_connections_defaults_to_true():
    from dbt.adapters.clickhouse.credentials import ClickHouseCredentials

    creds = ClickHouseCredentials(host='localhost', schema='default')
    assert creds.reuse_connections is True


def _make_manager_with_reuse(reuse_connections: bool) -> ClickHouseConnectionManager:
    manager = ClickHouseConnectionManager.__new__(ClickHouseConnectionManager)
    manager.profile = MagicMock()
    manager.profile.credentials.reuse_connections = reuse_connections
    return manager


class TestReleaseRespectsReuseConnections:
    def test_release_is_a_noop_when_reuse_connections_is_true(self):
        manager = _make_manager_with_reuse(reuse_connections=True)
        with patch.object(SQLConnectionManager, 'release') as mock_super_release:
            manager.release()
        mock_super_release.assert_not_called()

    def test_release_calls_super_when_reuse_connections_is_false(self):
        manager = _make_manager_with_reuse(reuse_connections=False)
        with patch.object(SQLConnectionManager, 'release') as mock_super_release:
            manager.release()
        mock_super_release.assert_called_once()


class TestActiveQueryBinding:
    def test_execute_binds_query_id_for_the_duration_of_the_query(self):
        mock_client = MagicMock()
        manager, conn = _make_manager_and_conn(mock_client)
        seen = []

        def capture_binding(*args, **kwargs):
            seen.append(getattr(conn, _ACTIVE_QUERY_ID_ATTR, None))

        mock_client.command.side_effect = capture_binding

        response, _ = manager.execute('INSERT INTO t SELECT 1')

        assert seen == [response.query_id]
        assert getattr(conn, _ACTIVE_QUERY_ID_ATTR, None) is None

    def test_execute_clears_binding_when_query_raises(self):
        mock_client = MagicMock()
        mock_client.command.side_effect = RuntimeError('boom')
        manager, conn = _make_manager_and_conn(mock_client)

        with pytest.raises(DbtRuntimeError):
            manager.execute('INSERT INTO t SELECT 1')

        assert getattr(conn, _ACTIVE_QUERY_ID_ATTR, None) is None

    def test_add_query_binds_and_clears_query_id(self):
        mock_client = MagicMock()
        manager, conn = _make_manager_and_conn(mock_client)
        seen = []

        def capture_binding(*args, **kwargs):
            seen.append(getattr(conn, _ACTIVE_QUERY_ID_ATTR, None))

        mock_client.command.side_effect = capture_binding

        manager.add_query('INSERT INTO t SELECT 1')

        _, kwargs = mock_client.command.call_args
        assert kwargs['query_id'] is not None
        assert seen == [kwargs['query_id']]
        assert getattr(conn, _ACTIVE_QUERY_ID_ATTR, None) is None

    def test_add_query_clears_binding_when_query_raises(self):
        mock_client = MagicMock()
        mock_client.command.side_effect = RuntimeError('boom')
        manager, conn = _make_manager_and_conn(mock_client)

        with pytest.raises(DbtRuntimeError):
            manager.add_query('INSERT INTO t SELECT 1')

        assert getattr(conn, _ACTIVE_QUERY_ID_ATTR, None) is None


class TestCancel:
    def test_cancel_kills_the_bound_query_then_closes(self):
        mock_client = MagicMock()
        manager, conn = _make_manager_and_conn(mock_client)
        setattr(conn, _ACTIVE_QUERY_ID_ATTR, 'qid-123')

        manager.cancel(conn)

        mock_client.kill_query.assert_called_once_with('qid-123')
        mock_client.close.assert_called_once()
        assert getattr(conn, _ACTIVE_QUERY_ID_ATTR, None) is None

    def test_cancel_without_active_query_only_closes(self):
        mock_client = MagicMock()
        manager, conn = _make_manager_and_conn(mock_client)

        manager.cancel(conn)

        mock_client.kill_query.assert_not_called()
        mock_client.close.assert_called_once()

    def test_second_cancel_without_new_query_does_not_kill_again(self):
        # The binding is consumed by the first cancel, so a query started
        # later on the same connection can never be matched by a stale cancel.
        mock_client = MagicMock()
        manager, conn = _make_manager_and_conn(mock_client)
        setattr(conn, _ACTIVE_QUERY_ID_ATTR, 'qid-123')

        manager.cancel(conn)
        manager.cancel(conn)

        mock_client.kill_query.assert_called_once_with('qid-123')

    def test_cancel_only_kills_the_query_bound_to_that_connection(self):
        mock_client_a = MagicMock()
        mock_client_b = MagicMock()
        manager = ClickHouseConnectionManager.__new__(ClickHouseConnectionManager)
        conn_a = SimpleNamespace(name='model_a', handle=mock_client_a)
        conn_b = SimpleNamespace(name='model_b', handle=mock_client_b)
        setattr(conn_a, _ACTIVE_QUERY_ID_ATTR, 'qid-a')
        setattr(conn_b, _ACTIVE_QUERY_ID_ATTR, 'qid-b')

        manager.cancel(conn_a)

        mock_client_a.kill_query.assert_called_once_with('qid-a')
        mock_client_b.kill_query.assert_not_called()
        mock_client_b.close.assert_not_called()
        assert getattr(conn_b, _ACTIVE_QUERY_ID_ATTR, None) == 'qid-b'

    def test_cancel_closes_handle_even_when_kill_fails(self):
        mock_client = MagicMock()
        mock_client.kill_query.side_effect = RuntimeError('network down')
        manager, conn = _make_manager_and_conn(mock_client)
        setattr(conn, _ACTIVE_QUERY_ID_ATTR, 'qid-123')

        manager.cancel(conn)  # must not raise

        mock_client.close.assert_called_once()

    def test_cancel_without_handle_does_not_raise(self):
        manager, conn = _make_manager_and_conn(None)

        manager.cancel(conn)

    def test_cancel_from_another_thread_kills_the_in_flight_query(self):
        # Mirrors dbt's shutdown path: the model thread is blocked in
        # execute() while the main thread cancels its connection.
        mock_client = MagicMock()
        manager, conn = _make_manager_and_conn(mock_client)
        started = threading.Event()
        finish = threading.Event()

        def blocking_command(*args, **kwargs):
            started.set()
            finish.wait(10)
            raise RuntimeError('query was cancelled')

        mock_client.command.side_effect = blocking_command

        def run():
            try:
                manager.execute('INSERT INTO t SELECT 1')
            except DbtRuntimeError:
                pass

        thread = threading.Thread(target=run)
        thread.start()
        try:
            assert started.wait(5)
            manager.cancel(conn)
        finally:
            finish.set()
        thread.join(5)

        assert not thread.is_alive()
        _, kwargs = mock_client.command.call_args
        mock_client.kill_query.assert_called_once_with(kwargs['query_id'])
        assert getattr(conn, _ACTIVE_QUERY_ID_ATTR, None) is None
