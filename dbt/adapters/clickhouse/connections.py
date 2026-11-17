import re
import time
import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, Tuple, Union

from dbt.adapters.clickhouse.dbclient import ChRetryableException, get_db_client
from dbt.adapters.clickhouse.logger import logger
from dbt.adapters.contracts.connection import AdapterResponse, Connection
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtRuntimeError

if TYPE_CHECKING:
    import agate

retryable_exceptions = [ChRetryableException]
ddl_re = re.compile(r'^\s*(CREATE|DROP|ALTER)\s', re.IGNORECASE)

# Attribute stashed on the dbt Connection holding the server-side query_id of
# the query currently executing on that connection. cancel() reads it to KILL
# exactly that query. It is set before the query starts and cleared as soon as
# it ends — successfully or not — so a stale binding can never match a query
# started later on the same connection.
_ACTIVE_QUERY_ID_ATTR = '_clickhouse_active_query_id'


class ClickHouseConnectionManager(SQLConnectionManager):
    """
    ClickHouse Connector connection manager.
    """

    TYPE = 'clickhouse'

    @classmethod
    def _set_active_query_id(cls, connection, query_id: str) -> None:
        setattr(connection, _ACTIVE_QUERY_ID_ATTR, query_id)

    @classmethod
    def _clear_active_query_id(cls, connection, query_id: str) -> None:
        # Only the query that set the binding may clear it.
        if getattr(connection, _ACTIVE_QUERY_ID_ATTR, None) == query_id:
            setattr(connection, _ACTIVE_QUERY_ID_ATTR, None)

    @classmethod
    def _pop_active_query_id(cls, connection):
        query_id = getattr(connection, _ACTIVE_QUERY_ID_ATTR, None)
        setattr(connection, _ACTIVE_QUERY_ID_ATTR, None)
        return query_id

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except Exception as exp:
            logger.debug('Error running SQL: {}', sql)
            if isinstance(exp, DbtRuntimeError):
                raise
            raise DbtRuntimeError('ClickHouse exception:  ' + str(exp)) from exp

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection
        credentials = cls.get_credentials(connection.credentials)

        def connect():
            return get_db_client(credentials)

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

    def cancel(self, connection):
        connection_name = connection.name
        logger.debug('Cancelling query \'{}\'', connection_name)
        # Pop the binding first: the kill below targets exactly the query that
        # was running when cancel started. A query started later on this
        # connection (e.g. a retry) gets a fresh binding and is never matched
        # by this cancel.
        query_id = self._pop_active_query_id(connection)
        handle = connection.handle
        if handle is not None:
            if query_id:
                # Closing the client does not stop server-side computation, so
                # kill the query explicitly first. Best-effort: the handle is
                # closed either way, and a failed kill must not abort the
                # cancellation of the remaining connections.
                try:
                    handle.kill_query(query_id)
                except Exception as ex:
                    logger.warning(
                        f'Failed to kill ClickHouse query {query_id} on \'{connection_name}\': {ex}'
                    )
            handle.close()
        logger.debug('Cancel query \'{}\'', connection_name)

    def release(self):
        # Default: keep the connection open (historical behavior). With
        # `reuse_connections: false`, fall through to the base release()
        # which closes the handle so the next model picks a fresh replica.
        if self.profile.credentials.reuse_connections:
            return
        super().release()

    @classmethod
    def get_table_from_response(cls, response, column_names) -> "agate.Table":
        """
        Build agate table from response.
        :param response: ClickHouse query result
        :param column_names: Table column names
        """
        from dbt_common.clients.agate_helper import empty_table, table_from_data_flat

        # clickhouse-connect returns DDL/DCL responses (e.g. GRANT ... ON CLUSTER) with no
        # column metadata; there is no table to build from them.
        if not column_names:
            return empty_table()

        data = []
        for row in response:
            data.append(dict(zip(column_names, row, strict=True)))

        return table_from_data_flat(data, column_names)

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False, limit: Optional[int] = None
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        # Don't try to fetch result of clustered DDL responses, we don't know what to do with them
        if fetch and ddl_re.match(sql):
            fetch = False

        sql = self._add_query_comment(sql)
        conn = self.get_thread_connection()
        client = conn.handle

        query_id = str(uuid.uuid4())
        self._set_active_query_id(conn, query_id)
        try:
            with self.exception_handler(sql):
                logger.debug(f'On {conn.name}: {sql}...')
                pre = time.time()
                if fetch:
                    query_result = client.query(sql, query_id=query_id)
                else:
                    query_result = client.command(sql, query_id=query_id)
                status = self.get_status(client)
                logger.debug(f'SQL status: {status} in {(time.time() - pre):.2f} seconds')
                if fetch:
                    table = self.get_table_from_response(
                        query_result.result_set, query_result.column_names
                    )
                else:
                    from dbt_common.clients.agate_helper import empty_table

                    table = empty_table()
                return AdapterResponse(_message=status, query_id=query_id), table
        finally:
            self._clear_active_query_id(conn, query_id)

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        sql = self._add_query_comment(sql)
        conn = self.get_thread_connection()
        client = conn.handle
        query_id = str(uuid.uuid4())
        self._set_active_query_id(conn, query_id)
        try:
            with self.exception_handler(sql):
                logger.debug(f'On {conn.name}: {sql}...')
                pre = time.time()
                client.command(sql, query_id=query_id)
                status = self.get_status(client)
                logger.debug(f'SQL status: {status} in {(time.time() - pre):0.2f} seconds')
                return conn, None
        finally:
            self._clear_active_query_id(conn, query_id)

    @classmethod
    def get_credentials(cls, credentials):
        """
        Returns ClickHouse credentials
        """
        return credentials

    @classmethod
    def get_status(cls, _):
        """
        Returns connection status
        """
        return 'OK'

    @classmethod
    def get_response(cls, _):
        return 'OK'

    def begin(self):
        pass

    def commit(self):
        pass

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        assert isinstance(type_code, int)
        return ''
