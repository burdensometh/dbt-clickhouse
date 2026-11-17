{#
    Valid-time (business effective-time) snapshot strategy.

    Unlike the standard ``timestamp`` / ``check`` strategies, which record when a
    change was *observed*, this strategy organizes history by a business effective
    timestamp and supports backfilling corrections for any point in the past.

    The model query must deliver the *complete* observation set known as of this
    run for every business key it contains: one row per business key and business
    effective timestamp, carrying the full attribute set. The strategy:

      * rejects a batch that contains contradictory rows for the same
        (business key, effective timestamp); identical duplicates collapse;
      * collapses consecutive equal-valued observations into left-closed,
        right-open ``[dbt_valid_from, dbt_valid_to)`` intervals;
      * rebuilds the history of every key present in the batch, reusing the
        existing ``dbt_scd_id`` whenever an interval keeps the same starting
        effective timestamp, while keys absent from the batch keep their history
        untouched.

    On failure (including a batch contradiction) the snapshot table is never
    swapped, so the previously stored history is preserved.
#}


{% macro snapshot_valid_time_strategy(node, snapshotted_rel, current_rel, model_config, target_exists) %}
    {%- set primary_key = config.get('unique_key') -%}
    {%- set effective_at = config.get('updated_at') -%}

    {%- if not effective_at -%}
        {{ exceptions.raise_compiler_error(
            "A snapshot configured with the valid_time strategy must specify an updated_at configuration "
            ~ "naming the business effective timestamp column."
        ) }}
    {%- endif -%}

    {#- Consumed by the generic snapshot materialization (type checks and column
        expansion); row_changed is only used by the timestamp/check staging macros. -#}
    {%- set row_changed_expr -%}
        ({{ snapshotted_rel }}.dbt_valid_from < {{ current_rel }}.{{ effective_at }})
    {%- endset -%}

    {%- set scd_args = api.Relation.scd_args(primary_key, effective_at) -%}
    {%- set scd_id_expr = snapshot_hash_arguments(scd_args) -%}

    {% do return({
        "unique_key": primary_key,
        "updated_at": effective_at,
        "effective_at": effective_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "invalidate_hard_deletes": false,
        "hard_deletes": "ignore"
    }) %}
{% endmacro %}


{#
    Business attribute columns: every source column except the business key
    column(s) and the effective timestamp column. These define whether two
    observations of the same key carry the same dimension state.
#}
{% macro clickhouse__valid_time_attr_columns(strategy, source_sql) %}
    {%- set source_columns = clickhouse__get_columns_in_query(source_sql) -%}
    {%- set key_columns = strategy.unique_key
        if clickhouse__valid_time_key_is_list(strategy.unique_key)
        else [strategy.unique_key] -%}
    {%- set excluded = [] -%}
    {%- for key in key_columns -%}
        {%- do excluded.append(key) -%}
    {%- endfor -%}
    {%- do excluded.append(strategy.effective_at) -%}

    {%- set attr_columns = [] -%}
    {%- for column in source_columns -%}
        {%- if column not in excluded -%}
            {%- do attr_columns.append(column) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(attr_columns) }}
{% endmacro %}


{#
    Hash over the full business attribute set. A constant is used when the source
    only consists of key + effective timestamp, so the hash expression stays valid.
#}
{% macro clickhouse__valid_time_attr_hash(strategy, source_sql) %}
    {%- set attr_columns = clickhouse__valid_time_attr_columns(strategy, source_sql) -%}
    {%- if attr_columns -%}
        {{ snapshot_hash_arguments(attr_columns) }}
    {%- else -%}
        {{ snapshot_hash_arguments(["''"]) }}
    {%- endif -%}
{% endmacro %}


{#
    A configured unique_key is either a column name or a list of column names.
    Mirrors the list-detection idiom used by the other ClickHouse macros.
#}
{% macro clickhouse__valid_time_key_is_list(unique_key) %}
    {{ return(unique_key is iterable and (unique_key is not string and unique_key is not mapping)) }}
{% endmacro %}


{% macro clickhouse__valid_time_partition_keys(strategy, prefix) %}
    {%- if clickhouse__valid_time_key_is_list(strategy.unique_key) -%}
        {%- for key in strategy.unique_key -%}
            {{ prefix }}.{{ key }}{%- if not loop.last %}, {% endif -%}
        {%- endfor -%}
    {%- else -%}
        {{ prefix }}.{{ strategy.unique_key }}
    {%- endif -%}
{% endmacro %}


{% macro clickhouse__valid_time_key_tuple(strategy, prefix) %}
    {%- if clickhouse__valid_time_key_is_list(strategy.unique_key) -%}
        ({%- for key in strategy.unique_key -%}
            {{ prefix }}.{{ key }}{%- if not loop.last %}, {% endif -%}
        {%- endfor -%})
    {%- else -%}
        ({{ prefix }}.{{ strategy.unique_key }})
    {%- endif -%}
{% endmacro %}


{#
    Deterministic version id for an interval starting at ``prefix.effective_at``.
    Equivalent to the generic snapshot scd hash over (business key, effective
    timestamp), but qualified so it stays resolvable after the bare effective
    timestamp column is projected out of the rebuilt CTE.
#}
{% macro clickhouse__valid_time_scd_id(strategy, prefix) %}
    {%- set scd_parts = [] -%}
    {%- if clickhouse__valid_time_key_is_list(strategy.unique_key) -%}
        {%- for key in strategy.unique_key -%}
            {%- do scd_parts.append(prefix ~ '.' ~ key) -%}
        {%- endfor -%}
    {%- else -%}
        {%- do scd_parts.append(prefix ~ '.' ~ strategy.unique_key) -%}
    {%- endif -%}
    {%- do scd_parts.append(prefix ~ '.' ~ strategy.effective_at) -%}
    {{ snapshot_hash_arguments(scd_parts) }}
{% endmacro %}


{#
    Key tuple as it appears in a staging table, where the materialization stores
    business keys in the dbt_unique_key* helper columns.
#}
{% macro clickhouse__valid_time_staging_key_tuple(unique_key, prefix) %}
    {%- if clickhouse__valid_time_key_is_list(unique_key) -%}
        ({%- for key in unique_key -%}
            {{ prefix }}.dbt_unique_key_{{ loop.index }}{%- if not loop.last %}, {% endif -%}
        {%- endfor -%})
    {%- else -%}
        ({{ prefix }}.dbt_unique_key)
    {%- endif -%}
{% endmacro %}


{#
    Bare key column names (no table prefix). Needed inside ClickHouse
    ``* except (...)`` lists, which only accept unqualified names.
#}
{% macro clickhouse__valid_time_key_names(strategy) %}
    {%- if clickhouse__valid_time_key_is_list(strategy.unique_key) -%}
        {%- for key in strategy.unique_key -%}
            {{ key }}{%- if not loop.last %}, {% endif -%}
        {%- endfor -%}
    {%- else -%}
        {{ strategy.unique_key }}
    {%- endif -%}
{% endmacro %}


{#
    Fails the run before any target table is touched when the same
    (business key, effective timestamp) arrives with contradictory attributes.
    Identical duplicates are allowed: they describe the same effective point and
    collapse downstream.
#}
{% macro clickhouse__valid_time_assert_no_contradictions(strategy, source_sql) %}
    {%- set conflict_sql -%}
        with snapshot_query as (
            {{ source_sql }}
        ),
        hashed as (
            select
                {{ clickhouse__valid_time_partition_keys(strategy, 'snapshot_query') }},
                {{ strategy.effective_at }} as dbt_effective_at,
                {{ clickhouse__valid_time_attr_hash(strategy, source_sql) }} as dbt_attr_hash
            from snapshot_query
        )
        select
            {{ clickhouse__valid_time_key_tuple(strategy, 'hashed') }} as dbt_conflict_key,
            dbt_effective_at,
            count() as dbt_row_count,
            uniqExact(dbt_attr_hash) as dbt_distinct_values
        from hashed
        group by
            {{ clickhouse__valid_time_partition_keys(strategy, 'hashed') }},
            dbt_effective_at
        having dbt_distinct_values > 1
        order by dbt_effective_at
        limit 10
    {%- endset -%}

    {%- set conflicts = run_query(conflict_sql) -%}
    {%- if conflicts.rows | length > 0 -%}
        {%- set details = [] -%}
        {%- for row in conflicts.rows -%}
            {%- do details.append(
                "key=" ~ row[0] ~ " effective_at=" ~ row[1]
                ~ " rows=" ~ row[2] ~ " distinct_values=" ~ row[3]
            ) -%}
        {%- endfor -%}
        {{ exceptions.raise_compiler_error(
            "valid_time snapshot contradiction: the batch contains different attribute sets for the same "
            ~ "business key and effective timestamp. Refusing to overwrite dimension history. Conflicting "
            ~ "(key, effective_at) points (up to 10): " ~ details | join('; ')
        ) }}
    {%- endif -%}
{% endmacro %}


{#
    Full valid-time history for every key present in the batch, expressed as
    left-closed/right-open intervals. Columns match the target snapshot table
    (plus dbt_change_type and the dbt_unique_key* helper columns, which the
    materialization strips out before the merge).
#}
{% macro clickhouse__snapshot_staging_table_valid_time(strategy, source_sql, target_relation, include_helpers=True) -%}
    {%- set attr_hash_expr = clickhouse__valid_time_attr_hash(strategy, source_sql) -%}
    {%- set effective_at = strategy.effective_at -%}

    with snapshot_query as (

        {{ source_sql }}

    ),

    hashed as (

        select
            *,
            {{ attr_hash_expr }} as dbt_attr_hash
        from snapshot_query

    ),

    {#- Keep one row per (key, effective point); identical duplicates collapse. -#}
    ranked as (

        select
            *,
            row_number() over (
                partition by {{ clickhouse__valid_time_partition_keys(strategy, 'hashed') }},
                             {{ effective_at }}
                order by {{ effective_at }}
            ) as dbt_row_num
        from hashed

    ),

    deduped as (

        select * from ranked where dbt_row_num = 1

    ),

    {#- Consecutive observations carrying the same attributes describe one
        continuous interval and must not produce extra history rows. -#}
    lagged as (

        select
            *,
            lagInFrame(dbt_attr_hash) over (
                partition by {{ clickhouse__valid_time_partition_keys(strategy, 'deduped') }}
                order by {{ effective_at }}
            ) as dbt_prev_attr_hash
        from deduped

    ),

    change_points as (

        select *
        from lagged
        where dbt_prev_attr_hash is null
           or dbt_prev_attr_hash != dbt_attr_hash

    ),

    source_intervals as (

        select
            *,
            leadInFrame(if(1 = 1, {{ effective_at }}, null), 1, null) over (
                partition by {{ clickhouse__valid_time_partition_keys(strategy, 'change_points') }}
                order by {{ effective_at }}
                rows between unbounded preceding and unbounded following
            ) as dbt_interval_valid_to
        from change_points

    )
    {%- if target_relation is not none -%}
    ,

    {#- Existing history, limited to keys this batch provides a full observation
        set for: only those keys are rebuilt. -#}
    target_history as (

        select *
        from {{ target_relation }}
        where {{ clickhouse__valid_time_key_tuple(strategy, target_relation) }} in (
            select {{ clickhouse__valid_time_key_tuple(strategy, 'source_intervals') }}
            from source_intervals
        )

    ),

    {#- Intervals whose start already exists keep their stored version id. -#}
    rebuilt_matched as (

        select
            source_intervals.* except (dbt_attr_hash, dbt_row_num, dbt_prev_attr_hash),
            source_intervals.{{ effective_at }} as dbt_updated_at,
            source_intervals.{{ effective_at }} as dbt_valid_from,
            source_intervals.dbt_interval_valid_to as dbt_valid_to,
            target_history.dbt_scd_id as dbt_scd_id
        from source_intervals
        inner join target_history
            on {{ clickhouse__valid_time_key_tuple(strategy, 'source_intervals') }}
             = {{ clickhouse__valid_time_key_tuple(strategy, 'target_history') }}
            and source_intervals.{{ effective_at }} = target_history.dbt_valid_from

    ),

    {#- Genuinely new effective points mint a deterministic version id. The
        anti-join keeps this branch free of NULL join results, whose default
        values otherwise collapse the hash expression to zero. -#}
    rebuilt_new as (

        select
            source_intervals.* except (dbt_attr_hash, dbt_row_num, dbt_prev_attr_hash),
            source_intervals.{{ effective_at }} as dbt_updated_at,
            source_intervals.{{ effective_at }} as dbt_valid_from,
            source_intervals.dbt_interval_valid_to as dbt_valid_to,
            {{ clickhouse__valid_time_scd_id(strategy, 'source_intervals') }} as dbt_scd_id
        from source_intervals
        left anti join target_history
            on {{ clickhouse__valid_time_key_tuple(strategy, 'source_intervals') }}
             = {{ clickhouse__valid_time_key_tuple(strategy, 'target_history') }}
            and source_intervals.{{ effective_at }} = target_history.dbt_valid_from

    ),

    rebuilt as (

        select * from rebuilt_matched
        union all
        select * from rebuilt_new

    )
    {%- else -%}
    ,

    rebuilt as (

        select
            source_intervals.* except (dbt_attr_hash, dbt_row_num, dbt_prev_attr_hash),
            {{ effective_at }} as dbt_updated_at,
            {{ effective_at }} as dbt_valid_from,
            dbt_interval_valid_to as dbt_valid_to,
            {{ clickhouse__valid_time_scd_id(strategy, 'source_intervals') }} as dbt_scd_id
        from source_intervals

    )
    {%- endif %}

    {%- if include_helpers -%}
    select
        'insert' as dbt_change_type,
        {#- Real business key columns (kept in the target) plus the dbt_unique_key*
            helper aliases the materialization strips after deriving insert cols. -#}
        {%- if clickhouse__valid_time_key_is_list(strategy.unique_key) -%}
            {%- for key in strategy.unique_key -%}
            rebuilt.{{ key }},
            rebuilt.{{ key }} as dbt_unique_key_{{ loop.index }},
            {%- endfor -%}
        {%- else -%}
        rebuilt.{{ strategy.unique_key }},
        rebuilt.{{ strategy.unique_key }} as dbt_unique_key,
        {%- endif %}
        rebuilt.* except ({{ clickhouse__valid_time_key_names(strategy) }},
                          {{ effective_at }},
                          dbt_interval_valid_to,
                          dbt_updated_at,
                          dbt_valid_from,
                          dbt_valid_to,
                          dbt_scd_id)
        ,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to,
        dbt_scd_id
    from rebuilt
    {%- else -%}
    select
        rebuilt.* except ({{ effective_at }},
                          dbt_interval_valid_to,
                          dbt_updated_at,
                          dbt_valid_from,
                          dbt_valid_to,
                          dbt_scd_id)
        ,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to,
        dbt_scd_id
    from rebuilt
    {%- endif %}
{%- endmacro %}


{#
    First-run build: apply the same collapse logic so the initially created table
    already has the interval shape every later run maintains. No helper columns
    (dbt_change_type / dbt_unique_key*) exist on a freshly created target.
#}
{% macro clickhouse__build_snapshot_table_valid_time(strategy, sql) %}
    {{ clickhouse__snapshot_staging_table_valid_time(strategy, sql, none, include_helpers=False) }}
{% endmacro %}


{#
    Merge a valid-time staging table into the target by rebuilding a full new
    table version: untouched keys are carried over verbatim, then the rebuilt
    histories from staging replace the previous rows of the batch's keys. The
    swap at the end (EXCHANGE TABLES or drop/rename) is what makes a failed run
    leave the original history in place.
#}
{% macro clickhouse__valid_time_snapshot_merge_sql(target, source, insert_cols) -%}
  {%- set insert_cols_csv = insert_cols | join(', ') -%}
  {%- set unique_key = config.get('unique_key') -%}
  {%- set key_tuple_source = clickhouse__valid_time_staging_key_tuple(unique_key, source) -%}
  {%- set key_tuple_target = clickhouse__valid_time_key_tuple(
        {'unique_key': unique_key}, target) -%}

  {%- set rebuild = target.derivative('__snapshot_valid_time_rebuild') -%}
  {#- A previous run that failed before the swap could leave this behind. -#}
  {% call statement('drop_stale_valid_time_rebuild_relation') %}
    drop table if exists {{ rebuild }} {{ on_cluster_clause(rebuild) }}
  {% endcall %}
  {% call statement('create_valid_time_rebuild_relation') %}
    create table if not exists {{ rebuild }} {{ on_cluster_clause(rebuild) }} as {{ target }}
  {% endcall %}

  {% call statement('insert_valid_time_untouched_keys') %}
    insert into {{ rebuild }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
      {{ target }}.{{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ target }}
    where {{ key_tuple_target }} not in (
        select distinct {{ key_tuple_source }} from {{ source }}
    )
  {% endcall %}

  {% call statement('insert_valid_time_rebuilt_keys') %}
    insert into {{ rebuild }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
      {{ source }}.{{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ source }}
    where {{ source }}.dbt_change_type = 'insert'
  {% endcall %}

  {% if target.can_exchange %}
    {% do exchange_tables_atomic(rebuild, target) %}
    {% call statement('drop_valid_time_rebuild_relation') %}
      drop table if exists {{ rebuild }} {{ on_cluster_clause(rebuild) }};
    {% endcall %}
  {% else %}
    {% call statement('drop_valid_time_target_relation') %}
      drop table if exists {{ target }} {{ on_cluster_clause(target) }};
    {% endcall %}
    {% call statement('rename_valid_time_rebuild_relation') %}
      rename table {{ rebuild }} to {{ target }};
    {% endcall %}
  {% endif %}

  {% do return ('select 1') %}
{% endmacro %}
