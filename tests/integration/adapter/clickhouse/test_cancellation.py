import threading
import time

import pytest
from clickhouse_connect import get_client
from dbt.adapters.clickhouse.connections import _ACTIVE_QUERY_ID_ATTR
from dbt.tests.util import run_dbt, write_file

# Marker embedded in the slow model so its INSERT can be found in
# system.processes (and told apart from this test's own polling queries).
CANCEL_MARKER = 'cancel_integration_marker'

fast_model_sql = """
{{ config(materialized='table') }}
select 1 as id, 'original' as marker
"""

rebuilt_model_sql = """
{{ config(materialized='table') }}
select 2 as id, 'rebuilt' as marker
"""

# ~100s of server-side work: 1000 blocks of 100 rows at 0.1s sleep per block
# (the per-block sleep limit is 3s).
slow_model_sql = f"""
{{{{ config(materialized='table') }}}}
select count() as id, '{CANCEL_MARKER}' as marker
from numbers(100000)
where not ignore(sleepEachRow(0.001))
settings max_block_size = 100
"""

POLL_TIMEOUT = 60


def _ch_client(test_config):
    return get_client(
        host=test_config['host'],
        port=test_config['client_port'],
        username=test_config['user'],
        password=test_config['password'],
        secure=test_config['secure'],
    )


def _wait_for_model_insert(ch_client):
    """Wait until the slow model's INSERT is actually running server-side and
    return its query_id. The CREATE of the candidate table also embeds the
    model SQL, so only an `insert into` query matches; the polling query
    excludes itself via the system.processes filter."""
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        rows = ch_client.query(
            "SELECT query_id FROM system.processes "
            f"WHERE query LIKE '%{CANCEL_MARKER}%' AND query LIKE '%insert into%' "
            "AND query NOT LIKE '%system.processes%'"
        ).result_set
        if rows:
            return rows[0][0]
        time.sleep(0.2)
    raise AssertionError('model insert never appeared in system.processes')


def _find_connection(adapter, query_id):
    for conn in adapter.connections.thread_connections.values():
        if getattr(conn, _ACTIVE_QUERY_ID_ATTR, None) == query_id:
            return conn
    return None


class TestCancelRunningModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {"cancel_model.sql": fast_model_sql}

    def test_cancel_kills_server_query_and_preserves_target(self, project, test_config):
        # Establish the existing target table with its original data.
        results = run_dbt(["run"])
        assert results.results[0].status == 'success'

        # Replace the model with a slow one and run it in the background.
        write_file(slow_model_sql, project.project_root, "models", "cancel_model.sql")
        run_holder = {}

        def dbt_run():
            try:
                run_holder['result'] = run_dbt(["run"], expect_pass=False)
            except Exception as ex:  # surfaced by the assertion below
                run_holder['error'] = ex

        run_thread = threading.Thread(target=dbt_run)
        run_thread.start()

        ch_client = _ch_client(test_config)
        try:
            query_id = _wait_for_model_insert(ch_client)

            # The connection running the model must be the one carrying this
            # exact query_id: cancel() kills by binding, not by connection name.
            adapter = project.adapter
            deadline = time.time() + POLL_TIMEOUT
            conn = None
            while conn is None and time.time() < deadline:
                conn = _find_connection(adapter, query_id)
                if conn is None:
                    time.sleep(0.1)
            assert conn is not None, 'no dbt connection is bound to the running query_id'

            adapter.connections.cancel(conn)
            run_thread.join(120)

            # The server-side query is gone: closing the client was not what
            # stopped it, the KILL was.
            deadline = time.time() + 30
            while time.time() < deadline:
                rows = ch_client.query(
                    f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
                ).result_set
                if rows[0][0] == 0:
                    break
                time.sleep(0.2)
            else:
                raise AssertionError('killed query is still in system.processes')
        finally:
            ch_client.close()
            if run_thread.is_alive():
                run_thread.join(120)

        assert 'error' not in run_holder, f'dbt run raised unexpectedly: {run_holder.get("error")}'
        results = run_holder['result']
        assert results.results[0].status == 'error'

        # The cancel happened before the exchange, so the old target is
        # untouched and the candidate was never published.
        rows = project.run_sql(
            f"select id, marker from {project.test_schema}.cancel_model", fetch="all"
        )
        assert rows == [(1, 'original')]

        # The leftover candidate is dropped by the next run, which then
        # succeeds and publishes the new data.
        write_file(rebuilt_model_sql, project.project_root, "models", "cancel_model.sql")
        results = run_dbt(["run"])
        assert results.results[0].status == 'success'
        rows = project.run_sql(
            f"select id, marker from {project.test_schema}.cancel_model", fetch="all"
        )
        assert rows == [(2, 'rebuilt')]
        tables = {
            row[0]
            for row in project.run_sql(f"show tables from {project.test_schema}", fetch="all")
        }
        assert 'cancel_model__dbt_backup' not in tables
        assert 'cancel_model__dbt_intermediate' not in tables
