import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

raw_orders = """
{{ config(materialized='table', engine='MergeTree()', order_by=['order_id']) }}
select * from values (
    'order_id UInt32, customer_id String, ordered_at DateTime, amount Decimal(10, 2), updated_at DateTime',
    (1, 'alice', toDateTime('2024-01-10 10:00:00'), 100.00, toDateTime('2024-01-10 10:00:00')),
    (2, 'alice', toDateTime('2024-02-15 12:00:00'), 200.00, toDateTime('2024-02-15 12:00:00')),
    (3, 'bob',   toDateTime('2024-01-20 09:00:00'), 300.00, toDateTime('2024-01-20 09:00:00')),
    (4, 'carol', toDateTime('2024-03-01 08:00:00'), 400.00, toDateTime('2024-03-01 08:00:00')),
    (5, 'dave',  toDateTime('2024-01-05 07:00:00'), 500.00, toDateTime('2024-01-05 07:00:00'))
)
"""

dim_customers = """
{{ config(materialized='table', engine='MergeTree()', order_by=['customer_sk']) }}
select * from values (
    'customer_sk UInt32, customer_id String, valid_from DateTime, valid_to Nullable(DateTime), name String',
    (1, 'alice', toDateTime('2024-01-01 00:00:00'), NULL, 'Alice A'),
    (2, 'bob',   toDateTime('2024-01-01 00:00:00'), NULL, 'Bob'),
    (6, 'dave',  toDateTime('2024-01-01 00:00:00'), NULL, 'Dave')
)
"""

dim_customer_revisions = """
{{ config(materialized='table', engine='MergeTree()', order_by=['customer_id']) }}
select
    cast('' as String) as customer_id,
    toDateTime('2024-01-01 00:00:00') as valid_from,
    cast(null as Nullable(DateTime)) as valid_to
where 1 = 0
"""

fct_orders = """
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    order_by=['ordered_at', 'order_id']
) }}
{{ clickhouse_backfill_facts(
    raw_facts=ref('raw_orders'),
    dimension=ref('dim_customers'),
    revisions=ref('dim_customer_revisions'),
    fact_business_key='customer_id',
    fact_effective_at='ordered_at',
    dimension_business_key='customer_id',
    dimension_id='customer_sk',
    dimension_valid_from='valid_from',
    dimension_valid_to='valid_to',
    new_facts_filter="o.updated_at > (select coalesce(max(updated_at), toDateTime('1970-01-01 00:00:00')) from {target})"
) }}
"""

fct_orders_snapshot = """
{% snapshot fct_orders_snapshot %}
{{ config(unique_key='order_id', strategy='check', check_cols=['customer_sk']) }}
select * from {{ ref('fct_orders') }}
{% endsnapshot %}
"""

fct_orders_no_unique_key = """
{{ config(materialized='incremental') }}
{{ clickhouse_backfill_facts(
    raw_facts=ref('raw_orders'),
    dimension=ref('dim_customers'),
    revisions=ref('dim_customer_revisions'),
    fact_business_key='customer_id',
    fact_effective_at='ordered_at',
    dimension_business_key='customer_id',
    dimension_id='customer_sk',
    dimension_valid_from='valid_from',
    dimension_valid_to='valid_to'
) }}
"""

fct_orders_append = """
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='append') }}
{{ clickhouse_backfill_facts(
    raw_facts=ref('raw_orders'),
    dimension=ref('dim_customers'),
    revisions=ref('dim_customer_revisions'),
    fact_business_key='customer_id',
    fact_effective_at='ordered_at',
    dimension_business_key='customer_id',
    dimension_id='customer_sk',
    dimension_valid_from='valid_from',
    dimension_valid_to='valid_to'
) }}
"""

fct_orders_inserts_only = """
{{ config(materialized='incremental', unique_key='order_id', inserts_only=true) }}
{{ clickhouse_backfill_facts(
    raw_facts=ref('raw_orders'),
    dimension=ref('dim_customers'),
    revisions=ref('dim_customer_revisions'),
    fact_business_key='customer_id',
    fact_effective_at='ordered_at',
    dimension_business_key='customer_id',
    dimension_id='customer_sk',
    dimension_valid_from='valid_from',
    dimension_valid_to='valid_to'
) }}
"""


def backfill_dimension_and_facts(project):
    """Restate the dimension history and declare this round's revised ranges."""
    project.run_sql(
        "alter table dim_customers update valid_to = toDateTime('2024-02-01 00:00:00') "
        "where customer_sk = 1 settings mutations_sync = 2"
    )
    project.run_sql(
        "alter table dim_customers update valid_to = toDateTime('2024-01-15 00:00:00') "
        "where customer_sk = 2 settings mutations_sync = 2"
    )
    project.run_sql(
        "insert into dim_customers values "
        "(3, 'alice', toDateTime('2024-02-01 00:00:00'), NULL, 'Alice B'), "
        "(4, 'carol', toDateTime('2024-03-01 00:00:00'), NULL, 'Carol'), "
        "(5, 'bob', toDateTime('2024-01-25 00:00:00'), NULL, 'Bob B')"
    )
    project.run_sql(
        "insert into dim_customer_revisions values "
        "('alice', toDateTime('2024-01-01 00:00:00'), NULL), "
        "('bob', toDateTime('2024-01-15 00:00:00'), toDateTime('2024-01-25 00:00:00')), "
        "('carol', toDateTime('2024-03-01 00:00:00'), NULL)"
    )
    # Probe rows: a direct warehouse mutation must survive on untouched facts and
    # be overwritten by the raw value on re-fetched ones.
    project.run_sql(
        "alter table fct_orders update amount = 999 where order_id = 5 settings mutations_sync = 2"
    )
    project.run_sql(
        "alter table fct_orders update amount = 888 where order_id = 2 settings mutations_sync = 2"
    )
    # A newly arrived fact, picked up by the normal incremental filter.
    project.run_sql(
        "insert into raw_orders values "
        "(6, 'alice', toDateTime('2024-03-05 10:00:00'), 600.00, toDateTime('2024-03-05 10:00:00'))"
    )


def fetch_facts(project):
    return project.run_sql(
        "select order_id, customer_sk, cast(amount as Int64) as amount from fct_orders order by order_id",
        fetch="all",
    )


class TestIncrementalBackfill:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "raw_orders.sql": raw_orders,
            "dim_customers.sql": dim_customers,
            "dim_customer_revisions.sql": dim_customer_revisions,
            "fct_orders.sql": fct_orders,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"fct_orders_snapshot.sql": fct_orders_snapshot}

    def test_backfill_rewrites_dimension_fk(self, project, ch_test_version):
        if ch_test_version.startswith('22.3'):
            return  # lightweight deletes not supported in 22.3

        run_dbt()
        assert fetch_facts(project) == [
            (1, 1, 100),
            (2, 1, 200),
            (3, 2, 300),
            (4, None, 400),  # carol has no dimension row yet: no covering interval -> NULL
            (5, 6, 500),
        ]
        run_dbt(["snapshot"])

        backfill_dimension_and_facts(project)
        run_dbt(["run", "--select", "fct_orders"])

        assert fetch_facts(project) == [
            (1, 1, 100),  # still covered by version 1: rewritten to the same key
            (2, 3, 200),  # moved to the restated version 3; other fields re-fetched from raw
            (3, None, 300),  # bob's correction leaves 2024-01-20 uncovered: NULL
            (4, 4, 400),  # carol is now covered by version 4
            (5, 6, 999),  # dave was not revised: row not recomputed, mutation preserved
            (6, 3, 600),  # new fact joined against the corrected dimension
        ]

        # The corrections landed in the fact table itself, so a downstream snapshot
        # observes the foreign key changes.
        run_dbt(["snapshot"])
        snapshot_rows = project.run_sql(
            "select order_id, customer_sk, dbt_valid_to is null as is_current "
            "from fct_orders_snapshot order by order_id, dbt_valid_from",
            fetch="all",
        )
        assert snapshot_rows == [
            (1, 1, 1),
            (2, 1, 0),
            (2, 3, 1),
            (3, 2, 0),
            (3, None, 1),
            (4, None, 0),
            (4, 4, 1),
            (5, 6, 1),
            (6, 3, 1),
        ]

        # Re-running with the same revisions is a no-op: no duplicates, no new versions.
        run_dbt(["run", "--select", "fct_orders"])
        assert len(fetch_facts(project)) == 6
        run_dbt(["snapshot"])
        assert project.run_sql("select count() from fct_orders_snapshot", fetch="one")[0] == 9


class TestIncrementalBackfillDimensionValidation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "raw_orders.sql": raw_orders,
            "dim_customers.sql": dim_customers,
            "dim_customer_revisions.sql": dim_customer_revisions,
            "fct_orders.sql": fct_orders,
        }

    @pytest.fixture(autouse=True)
    def build_models(self, project, ch_test_version):
        if ch_test_version.startswith('22.3'):
            pytest.skip("lightweight deletes not supported in 22.3")
        run_dbt()

    def test_overlapping_dimension_intervals_rejected(self, project):
        project.run_sql(
            "insert into dim_customers values "
            "(7, 'dave', toDateTime('2024-01-03 00:00:00'), NULL, 'Dave overlap')"
        )
        _, logs = run_dbt_and_capture(["run", "--select", "fct_orders"], expect_pass=False)
        assert "Backfill validation failed" in logs
        assert "overlaps another version" in logs
        assert "dave" in logs

    def test_invalid_dimension_interval_rejected(self, project):
        project.run_sql(
            "insert into dim_customers values "
            "(8, 'dave', toDateTime('2024-02-01 00:00:00'), toDateTime('2024-01-01 00:00:00'), 'Dave')"
        )
        _, logs = run_dbt_and_capture(["run", "--select", "fct_orders"], expect_pass=False)
        assert "Backfill validation failed" in logs
        assert "invalid effective interval" in logs

    def test_invalid_revision_range_rejected(self, project):
        project.run_sql(
            "insert into dim_customer_revisions values "
            "('dave', toDateTime('2024-02-01 00:00:00'), toDateTime('2024-01-01 00:00:00'))"
        )
        _, logs = run_dbt_and_capture(["run", "--select", "fct_orders"], expect_pass=False)
        assert "Backfill validation failed" in logs
        assert "invalid effective range" in logs


class TestIncrementalBackfillConfigValidation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "raw_orders.sql": raw_orders,
            "dim_customers.sql": dim_customers,
            "dim_customer_revisions.sql": dim_customer_revisions,
            "fct_no_unique_key.sql": fct_orders_no_unique_key,
            "fct_append.sql": fct_orders_append,
            "fct_inserts_only.sql": fct_orders_inserts_only,
        }

    @pytest.fixture(autouse=True)
    def build_upstream(self, project, ch_test_version):
        if ch_test_version.startswith('22.3'):
            pytest.skip("lightweight deletes not supported in 22.3")
        run_dbt(["run", "--select", "raw_orders", "dim_customers", "dim_customer_revisions"])

    def test_unique_key_required(self, project):
        _, logs = run_dbt_and_capture(["run", "--select", "fct_no_unique_key"], expect_pass=False)
        assert "`backfill` mode requires a `unique_key`" in logs

    def test_append_strategy_rejected(self, project):
        _, logs = run_dbt_and_capture(["run", "--select", "fct_append"], expect_pass=False)
        assert "`backfill` mode requires a keyed incremental strategy" in logs

    def test_inserts_only_rejected(self, project):
        _, logs = run_dbt_and_capture(["run", "--select", "fct_inserts_only"], expect_pass=False)
        assert "`backfill` mode is not compatible with `inserts_only`" in logs
