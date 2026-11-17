import pytest
from dbt.tests.util import run_dbt

# Complete observation sets per batch. The snapshot source query always returns
# the *full* history known as of the current run for the keys it contains.
ROWS_V1 = [
    (1, "active", "2024-01-01 00:00:00"),
    (2, "active", "2024-01-01 00:00:00"),
]

ROWS_V2 = [
    (1, "active", "2024-01-01 00:00:00"),
    (1, "inactive", "2024-02-01 00:00:00"),
    (1, "active", "2024-03-01 00:00:00"),
    (2, "active", "2024-01-01 00:00:00"),
    # Consecutive same-valued observation for key 2: must not be dropped from
    # the input handling, but collapses into one continuous interval.
    (2, "active", "2024-02-01 00:00:00"),
    (3, "active", "2024-01-15 00:00:00"),
]

# Same shape as V2, but the 2024-02-01 observation for key 1 is corrected: it
# was "inactive", now it is "pending". The 2024-03-01 "active" value is restored
# as its own interval because it differs from the corrected middle value.
ROWS_V3 = [
    (1, "active", "2024-01-01 00:00:00"),
    (1, "pending", "2024-02-01 00:00:00"),
    (1, "active", "2024-03-01 00:00:00"),
    (2, "active", "2024-01-01 00:00:00"),
    (2, "active", "2024-02-01 00:00:00"),
    (3, "active", "2024-01-15 00:00:00"),
]

# Contradiction: key 1 at 2024-02-01 arrives with two different statuses.
ROWS_CONFLICT = [
    (1, "active", "2024-01-01 00:00:00"),
    (1, "inactive", "2024-02-01 00:00:00"),
    (1, "pending", "2024-02-01 00:00:00"),
    (2, "active", "2024-01-01 00:00:00"),
]

# Identical duplicates at the same effective point must be accepted, not rejected.
ROWS_DUPES = [
    (1, "active", "2024-01-01 00:00:00"),
    (1, "active", "2024-01-01 00:00:00"),
    (2, "active", "2024-01-01 00:00:00"),
]

SNAPSHOT = """
{% snapshot valid_time_snapshot %}
    {{ config(
        unique_key='id',
        strategy='valid_time',
        updated_at='effective_at',
    ) }}
    select * from {{ ref('seed') }}
{% endsnapshot %}
"""


class ValidTimeSnapshotBase:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"valid_time_snapshot.sql": SNAPSHOT}

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    @pytest.fixture(scope="class")
    def models(self):
        # A regular model stands in for the source system; each test re-creates
        # its contents with a different complete observation set.
        return {
            "seed.sql": ("{{ config(materialized='table') }}\nselect * from source_observations")
        }

    @staticmethod
    def _reseed(project, rows):
        relation = f"{project.test_schema}.source_observations"
        project.run_sql(f"drop table if exists {relation}")
        project.run_sql(
            f"create table {relation} "
            "(id UInt32, status String, effective_at DateTime) "
            "engine MergeTree order by (id, effective_at)"
        )
        values = ", ".join(
            f"({i}, '{status}', toDateTime('{effective_at}'))" for i, status, effective_at in rows
        )
        project.run_sql(f"insert into {relation} values {values}")
        # Rebuild the source model so the snapshot reads the new observation set.
        run_dbt(["run", "--select", "seed"])

    @staticmethod
    def _history(project):
        rows = project.run_sql(
            "select id, status, dbt_valid_from, dbt_valid_to, dbt_scd_id "
            f"from {project.test_schema}.valid_time_snapshot order by id, dbt_valid_from",
            fetch="all",
        )
        return [(r[0], r[1], str(r[2]), None if r[3] is None else str(r[3]), r[4]) for r in rows]

    def test_valid_time_history(self, project):
        self._reseed(project, ROWS_V1)
        run_dbt(["snapshot"])

        history = self._history(project)
        assert [(r[0], r[1], r[2], r[3]) for r in history] == [
            (1, "active", "2024-01-01 00:00:00", None),
            (2, "active", "2024-01-01 00:00:00", None),
        ]
        scd_id_v1 = {r[0]: r[4] for r in history}

        # Batch 2: backfill an intermediate change, restore the original value
        # afterwards, add a new key, and include a consecutive same-value pair.
        self._reseed(project, ROWS_V2)
        run_dbt(["snapshot"])

        history = self._history(project)
        assert [(r[0], r[1], r[2], r[3]) for r in history] == [
            (1, "active", "2024-01-01 00:00:00", "2024-02-01 00:00:00"),
            (1, "inactive", "2024-02-01 00:00:00", "2024-03-01 00:00:00"),
            (1, "active", "2024-03-01 00:00:00", None),
            # Consecutive same-value observations collapse into one interval.
            (2, "active", "2024-01-01 00:00:00", None),
            (3, "active", "2024-01-15 00:00:00", None),
        ]
        scd_id_v2 = {(r[0], r[2]): r[4] for r in history}

        # An unchanged starting point keeps its version id.
        assert scd_id_v2[(1, "2024-01-01 00:00:00")] == scd_id_v1[1]
        assert scd_id_v2[(2, "2024-01-01 00:00:00")] == scd_id_v1[2]
        # New effective points get fresh, deterministic version ids.
        assert scd_id_v2[(1, "2024-02-01 00:00:00")] != scd_id_v1[1]
        assert scd_id_v2[(1, "2024-03-01 00:00:00")] != scd_id_v1[1]

        # Re-running the same batch is a no-op, including stable version ids.
        run_dbt(["snapshot"])
        assert self._history(project) == history

        # Batch 3: correct the middle effective point. The intervals around it are
        # rebuilt, while unchanged starts keep their version ids.
        self._reseed(project, ROWS_V3)
        run_dbt(["snapshot"])

        history = self._history(project)
        assert [(r[0], r[1], r[2], r[3]) for r in history] == [
            (1, "active", "2024-01-01 00:00:00", "2024-02-01 00:00:00"),
            (1, "pending", "2024-02-01 00:00:00", "2024-03-01 00:00:00"),
            (1, "active", "2024-03-01 00:00:00", None),
            (2, "active", "2024-01-01 00:00:00", None),
            (3, "active", "2024-01-15 00:00:00", None),
        ]
        scd_id_v3 = {(r[0], r[2]): r[4] for r in history}
        assert scd_id_v3[(1, "2024-01-01 00:00:00")] == scd_id_v1[1]
        assert scd_id_v3[(1, "2024-03-01 00:00:00")] == scd_id_v2[(1, "2024-03-01 00:00:00")]
        assert scd_id_v3[(2, "2024-01-01 00:00:00")] == scd_id_v1[2]

    def test_contradiction_fails_and_preserves_history(self, project):
        self._reseed(project, ROWS_V1)
        run_dbt(["snapshot"])
        before = self._history(project)

        self._reseed(project, ROWS_CONFLICT)
        results = run_dbt(["snapshot"], expect_pass=False)
        assert results[0].status == "error"
        assert "contradiction" in results[0].message.lower()

        # The previously stored history is untouched.
        assert self._history(project) == before

        # A valid subsequent batch still applies cleanly.
        self._reseed(project, ROWS_V2)
        run_dbt(["snapshot"])
        assert len(self._history(project)) == 5

    def test_identical_duplicates_collapse(self, project):
        self._reseed(project, ROWS_DUPES)
        run_dbt(["snapshot"])
        history = self._history(project)
        assert [(r[0], r[1]) for r in history] == [(1, "active"), (2, "active")]

        # Idempotent on re-run.
        run_dbt(["snapshot"])
        assert self._history(project) == history


class TestSnapshotValidTime(ValidTimeSnapshotBase):
    pass


COMPOSITE_SNAPSHOT = """
{% snapshot valid_time_composite_snapshot %}
    {{ config(
        unique_key=['id', 'team_id'],
        strategy='valid_time',
        updated_at='effective_at',
    ) }}
    select * from {{ ref('seed') }}
{% endsnapshot %}
"""

COMPOSITE_ROWS_V1 = [
    (1, 10, "alice", "2024-01-01 00:00:00"),
    (1, 20, "bob", "2024-01-01 00:00:00"),
    (2, 10, "carol", "2024-01-01 00:00:00"),
]

COMPOSITE_ROWS_V2 = [
    (1, 10, "alice", "2024-01-01 00:00:00"),
    (1, 10, "alice2", "2024-02-01 00:00:00"),
    # Consecutive same value: must collapse into the single open interval.
    (1, 10, "alice2", "2024-03-01 00:00:00"),
    (1, 20, "bob", "2024-01-01 00:00:00"),
    (2, 10, "carol", "2024-01-01 00:00:00"),
]


class TestSnapshotValidTimeCompositeKey:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"valid_time_composite_snapshot.sql": COMPOSITE_SNAPSHOT}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "seed.sql": ("{{ config(materialized='table') }}\nselect * from source_observations")
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    @staticmethod
    def _reseed(project, rows):
        relation = f"{project.test_schema}.source_observations"
        project.run_sql(f"drop table if exists {relation}")
        project.run_sql(
            f"create table {relation} "
            "(id UInt32, team_id UInt32, name String, effective_at DateTime) "
            "engine MergeTree order by (id, team_id, effective_at)"
        )
        values = ", ".join(
            f"({i}, {team}, '{name}', toDateTime('{effective_at}'))"
            for i, team, name, effective_at in rows
        )
        project.run_sql(f"insert into {relation} values {values}")
        run_dbt(["run", "--select", "seed"])

    @staticmethod
    def _history(project):
        rows = project.run_sql(
            "select id, team_id, name, dbt_valid_from, dbt_valid_to, dbt_scd_id "
            f"from {project.test_schema}.valid_time_composite_snapshot "
            "order by id, team_id, dbt_valid_from",
            fetch="all",
        )
        return [
            (r[0], r[1], r[2], str(r[3]), None if r[4] is None else str(r[4]), r[5]) for r in rows
        ]

    def test_composite_key_backfill(self, project):
        self._reseed(project, COMPOSITE_ROWS_V1)
        run_dbt(["snapshot"])
        history = self._history(project)
        assert [(r[0], r[1], r[2], r[3], r[4]) for r in history] == [
            (1, 10, "alice", "2024-01-01 00:00:00", None),
            (1, 20, "bob", "2024-01-01 00:00:00", None),
            (2, 10, "carol", "2024-01-01 00:00:00", None),
        ]
        first_scd = {(r[0], r[1]): r[5] for r in history}

        self._reseed(project, COMPOSITE_ROWS_V2)
        run_dbt(["snapshot"])
        history = self._history(project)
        assert [(r[0], r[1], r[2], r[3], r[4]) for r in history] == [
            (1, 10, "alice", "2024-01-01 00:00:00", "2024-02-01 00:00:00"),
            (1, 10, "alice2", "2024-02-01 00:00:00", None),
            (1, 20, "bob", "2024-01-01 00:00:00", None),
            (2, 10, "carol", "2024-01-01 00:00:00", None),
        ]
        # Unchanged starts keep their version id; the new point gets a fresh one.
        later_scd = {(r[0], r[1], r[3]): r[5] for r in history}
        assert later_scd[(1, 10, "2024-01-01 00:00:00")] == first_scd[(1, 10)]
        assert later_scd[(1, 20, "2024-01-01 00:00:00")] == first_scd[(1, 20)]
        assert later_scd[(1, 10, "2024-02-01 00:00:00")] != first_scd[(1, 10)]

        # Re-running the same complete observation set changes nothing.
        run_dbt(["snapshot"])
        assert self._history(project) == history
