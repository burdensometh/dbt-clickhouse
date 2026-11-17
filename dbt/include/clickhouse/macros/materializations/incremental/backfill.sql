{#
  Backfill mode for local incremental fact models.

  When dimension history is corrected after the fact (late-arriving or restated
  SCD2 versions), the raw facts do not change, so a normal incremental run never
  reprocesses the already-warehoused rows and their dimension version foreign
  keys stay stale. `clickhouse_backfill_facts` builds the model SQL for an
  incremental fact model that, in addition to the newly arrived facts, re-fetches
  the old facts covered by this round's revised business keys / effective time
  ranges and rewrites their dimension foreign key by matching the fact's
  effective timestamp against the dimension's left-closed, right-open
  [valid_from, valid_to) intervals (NULL when no interval covers the fact).

  The corrected rows flow through the native incremental merge (keyed by
  `unique_key`), so only the affected facts are replaced in the target table,
  every other field and the fact identity are preserved, and downstream
  snapshots reading the fact table observe the foreign key changes.

  The dimension is validated before anything is published: rows with an invalid
  effective interval (null `valid_from`, null version id, or
  `valid_from >= valid_to`) and business keys with overlapping intervals abort
  the run with a compiler error.

  Minimal model:

      {{ config(materialized='incremental', unique_key='order_id') }}

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

  The `revisions` relation lists this round's revised business keys and the
  effective time ranges that must be re-derived; by default it is expected to
  expose the same column names as the dimension's business key / valid_from /
  valid_to (override with the `revisions_*` arguments). A null revisions
  `valid_to` re-derives everything from `valid_from` onwards.
#}

{%- macro clickhouse_backfill_facts(
    raw_facts,
    dimension,
    revisions,
    fact_business_key,
    fact_effective_at,
    dimension_business_key,
    dimension_id,
    dimension_valid_from,
    dimension_valid_to,
    fact_columns=none,
    revisions_business_key=none,
    revisions_valid_from=none,
    revisions_valid_to=none,
    new_facts_filter=none
) -%}
  {#- Flag the model as backfill-enabled so the incremental materialization enforces a keyed merge. -#}
  {%- do config(backfill=true) -%}
  {%- if not execute -%}
    {#- The parse-time render only collects the refs and the config flag; the real SQL is built at run time. -#}
    {{- return('select 1 as __backfill_placeholder__') -}}
  {%- endif -%}
  {%- if raw_facts is undefined or dimension is undefined or revisions is undefined -%}
    {%- do exceptions.raise_compiler_error(
      "clickhouse_backfill_facts: 'raw_facts', 'dimension' and 'revisions' relations are all required."
    ) -%}
  {%- endif -%}
  {%- if not fact_business_key or not fact_effective_at or not dimension_business_key
        or not dimension_id or not dimension_valid_from or not dimension_valid_to -%}
    {%- do exceptions.raise_compiler_error(
      "clickhouse_backfill_facts: 'fact_business_key', 'fact_effective_at', 'dimension_business_key', "
      ~ "'dimension_id', 'dimension_valid_from' and 'dimension_valid_to' must all be non-empty."
    ) -%}
  {%- endif -%}
  {%- set revisions_business_key = revisions_business_key or dimension_business_key -%}
  {%- set revisions_valid_from = revisions_valid_from or dimension_valid_from -%}
  {%- set revisions_valid_to = revisions_valid_to or dimension_valid_to -%}
  {%- if adapter.get_relation(raw_facts.database, raw_facts.schema, raw_facts.identifier) is none -%}
    {#- The raw facts relation does not exist yet (e.g. `dbt compile` before the upstream models
        were ever built). Emit a placeholder that still fails at run time, like any model whose
        upstream relation is missing. -#}
    {{- return('select 1 as __backfill_placeholder__ from ' ~ raw_facts | string ~ ' where 1 = 0') -}}
  {%- endif -%}
  {%- if fact_columns is none -%}
    {%- set fact_columns = adapter.get_columns_in_relation(raw_facts) | map(attribute='name') | list -%}
  {%- else -%}
    {%- set fact_columns = fact_columns | list -%}
  {%- endif -%}
  {%- if not fact_columns -%}
    {%- do exceptions.raise_compiler_error(
      "clickhouse_backfill_facts: could not read the columns of " ~ raw_facts ~ ". "
      ~ "Pass 'fact_columns' explicitly."
    ) -%}
  {%- endif -%}
  {#- The dimension foreign key column is always rewritten, never carried through. -#}
  {%- if dimension_id in fact_columns -%}
    {%- do fact_columns.remove(dimension_id) -%}
  {%- endif -%}
  {#- The fact identity must survive the rewrite: every simple unique_key column has to be selected. -#}
  {%- set unique_key = config.get('unique_key') -%}
  {%- if unique_key is string -%}
    {%- set unique_key_columns = unique_key.split(',') | map('trim') | list -%}
  {%- elif unique_key is iterable and unique_key is not mapping -%}
    {%- set unique_key_columns = unique_key | list -%}
  {%- else -%}
    {%- set unique_key_columns = [] -%}
  {%- endif -%}
  {%- if unique_key_columns -%}
    {%- for key_column in unique_key_columns -%}
      {%- if modules.re.match('^\w+$', key_column) and key_column not in fact_columns -%}
        {%- do exceptions.raise_compiler_error(
          "clickhouse_backfill_facts: the unique_key column '" ~ key_column ~ "' is not part of the "
          ~ "backfill output. Add it to 'fact_columns' so corrected facts keep their identity."
        ) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {#- Reject a broken dimension before anything is published. -#}
  {%- do clickhouse_backfill_validate_dimension(dimension, dimension_business_key, dimension_id, dimension_valid_from, dimension_valid_to) -%}
  {%- do clickhouse_backfill_validate_revision_ranges(revisions, revisions_business_key, revisions_valid_from, revisions_valid_to) -%}
  {%- set q_fact_bk = adapter.quote(fact_business_key) -%}
  {%- set q_fact_ts = adapter.quote(fact_effective_at) -%}
  {%- set q_dim_bk = adapter.quote(dimension_business_key) -%}
  {%- set q_dim_id = adapter.quote(dimension_id) -%}
  {%- set q_dim_from = adapter.quote(dimension_valid_from) -%}
  {%- set q_dim_to = adapter.quote(dimension_valid_to) -%}
  {%- set q_rev_bk = adapter.quote(revisions_business_key) -%}
  {%- set q_rev_from = adapter.quote(revisions_valid_from) -%}
  {%- set q_rev_to = adapter.quote(revisions_valid_to) -%}
  {%- set select_list = [] -%}
  {%- set output_columns = [] -%}
  {%- for column in fact_columns -%}
    {%- do select_list.append('o.' ~ adapter.quote(column) ~ ' as ' ~ adapter.quote(column)) -%}
    {%- do output_columns.append(adapter.quote(column)) -%}
  {%- endfor -%}
  {#- `join_use_nulls` defaults to 0 on ClickHouse, which fills non-matched join columns with
      default values instead of NULL. The marker column makes "no covering interval" explicit
      so the foreign key is NULL regardless of the server setting. -#}
  {%- do select_list.append('if(d.__bf_dim_match__ = 1, d.' ~ q_dim_id ~ ', null) as ' ~ q_dim_id) -%}
  {%- do output_columns.append(q_dim_id) -%}
  {%- set filter_sql = none -%}
  {%- if is_incremental() and new_facts_filter -%}
    {%- set filter_sql = new_facts_filter.replace('{target}', this | string) -%}
  {%- endif %}
with new_facts as (
    select
        {{ select_list | join(',\n        ') }}
    from {{ raw_facts }} o
    left join (select *, 1 as __bf_dim_match__ from {{ dimension }}) d
        on o.{{ q_fact_bk }} = d.{{ q_dim_bk }}
        and o.{{ q_fact_ts }} >= d.{{ q_dim_from }}
        and (d.{{ q_dim_to }} is null or o.{{ q_fact_ts }} < d.{{ q_dim_to }})
    {%- if filter_sql %}
    where {{ filter_sql }}
    {%- endif %}
)
{%- if is_incremental() and new_facts_filter %}
, revised_facts as (
    select distinct
        {{ select_list | join(',\n        ') }}
    from {{ raw_facts }} o
    inner join {{ revisions }} r
        on o.{{ q_fact_bk }} = r.{{ q_rev_bk }}
        and o.{{ q_fact_ts }} >= r.{{ q_rev_from }}
        and (r.{{ q_rev_to }} is null or o.{{ q_fact_ts }} < r.{{ q_rev_to }})
    left join (select *, 1 as __bf_dim_match__ from {{ dimension }}) d
        on o.{{ q_fact_bk }} = d.{{ q_dim_bk }}
        and o.{{ q_fact_ts }} >= d.{{ q_dim_from }}
        and (d.{{ q_dim_to }} is null or o.{{ q_fact_ts }} < d.{{ q_dim_to }})
    {#- Facts already picked up as new must not be selected twice. #}
    where not ({{ filter_sql }})
)
{%- endif %}
select {{ output_columns | join(', ') }} from new_facts
{%- if is_incremental() and new_facts_filter %}
union all
select {{ output_columns | join(', ') }} from revised_facts
{%- endif %}
{%- endmacro %}

{% macro clickhouse_backfill_validate_dimension(dimension, business_key, dimension_id, valid_from, valid_to) %}
  {%- if execute and adapter.get_relation(dimension.database, dimension.schema, dimension.identifier) is not none %}
    {%- set q_bk = adapter.quote(business_key) -%}
    {%- set q_id = adapter.quote(dimension_id) -%}
    {%- set q_from = adapter.quote(valid_from) -%}
    {%- set q_to = adapter.quote(valid_to) -%}
    {%- set invalid_intervals_sql -%}
        select count() as n
        from {{ dimension }}
        where {{ q_from }} is null
           or {{ q_id }} is null
           or ({{ q_to }} is not null and {{ q_from }} >= {{ q_to }})
    {%- endset -%}
    {%- set invalid_intervals = run_query(invalid_intervals_sql).rows[0][0] -%}
    {%- if invalid_intervals > 0 %}
      {%- set sample_sql -%}
          select {{ q_bk }}, {{ q_id }}, {{ q_from }}, {{ q_to }}
          from {{ dimension }}
          where {{ q_from }} is null
             or {{ q_id }} is null
             or ({{ q_to }} is not null and {{ q_from }} >= {{ q_to }})
          limit 5
      {%- endset -%}
      {% do exceptions.raise_compiler_error(
        "Backfill validation failed: dimension " ~ dimension ~ " has " ~ invalid_intervals
        ~ " row(s) with an invalid effective interval (null `" ~ valid_from ~ "`, null `" ~ dimension_id
        ~ "`, or `" ~ valid_from ~ "` >= `" ~ valid_to ~ "`). Fix the dimension history before "
        ~ "publishing. Offending rows (`" ~ business_key ~ "`, `" ~ dimension_id ~ "`, `" ~ valid_from
        ~ "`, `" ~ valid_to ~ "`, up to 5): "
        ~ clickhouse_backfill__format_samples(run_query(sample_sql).rows)
      ) %}
    {% endif %}
    {#- A fact can only map to one version per business key: intervals must not overlap.
        Every overlap has a row whose valid_from falls inside another row's interval. -#}
    {%- set overlaps_sql -%}
        select count() as n
        from {{ dimension }} a
        inner join {{ dimension }} b
            on a.{{ q_bk }} = b.{{ q_bk }}
            and a.{{ q_id }} != b.{{ q_id }}
            and b.{{ q_from }} <= a.{{ q_from }}
            and (b.{{ q_to }} is null or a.{{ q_from }} < b.{{ q_to }})
    {%- endset -%}
    {%- set overlaps = run_query(overlaps_sql).rows[0][0] -%}
    {%- if overlaps > 0 %}
      {%- set sample_sql -%}
          select a.{{ q_bk }} as business_key,
                 a.{{ q_from }} as valid_from, a.{{ q_to }} as valid_to,
                 b.{{ q_from }} as overlapping_from, b.{{ q_to }} as overlapping_to
          from {{ dimension }} a
          inner join {{ dimension }} b
              on a.{{ q_bk }} = b.{{ q_bk }}
              and a.{{ q_id }} != b.{{ q_id }}
              and b.{{ q_from }} <= a.{{ q_from }}
              and (b.{{ q_to }} is null or a.{{ q_from }} < b.{{ q_to }})
          limit 5
      {%- endset -%}
      {% do exceptions.raise_compiler_error(
        "Backfill validation failed: dimension " ~ dimension ~ " has " ~ overlaps
        ~ " row(s) whose effective interval overlaps another version of the same business key `"
        ~ business_key ~ "`. Fix the dimension history before publishing. "
        ~ "Offending rows (up to 5): "
        ~ clickhouse_backfill__format_samples(run_query(sample_sql).rows)
      ) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro clickhouse_backfill_validate_revision_ranges(revisions, business_key, valid_from, valid_to) %}
  {%- if execute and adapter.get_relation(revisions.database, revisions.schema, revisions.identifier) is not none %}
    {%- set q_from = adapter.quote(valid_from) -%}
    {%- set q_to = adapter.quote(valid_to) -%}
    {%- set invalid_ranges_sql -%}
        select count() as n
        from {{ revisions }}
        where {{ q_from }} is null
           or ({{ q_to }} is not null and {{ q_from }} >= {{ q_to }})
    {%- endset -%}
    {%- set invalid_ranges = run_query(invalid_ranges_sql).rows[0][0] -%}
    {%- if invalid_ranges > 0 %}
      {%- set sample_sql -%}
          select {{ adapter.quote(business_key) }}, {{ q_from }}, {{ q_to }}
          from {{ revisions }}
          where {{ q_from }} is null
             or ({{ q_to }} is not null and {{ q_from }} >= {{ q_to }})
          limit 5
      {%- endset -%}
      {% do exceptions.raise_compiler_error(
        "Backfill validation failed: revisions relation " ~ revisions ~ " has " ~ invalid_ranges
        ~ " row(s) with an invalid effective range (null `" ~ valid_from ~ "` or `" ~ valid_from
        ~ "` >= `" ~ valid_to ~ "`). Fix the revised ranges before publishing. "
        ~ "Offending rows (up to 5): "
        ~ clickhouse_backfill__format_samples(run_query(sample_sql).rows)
      ) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro clickhouse_backfill__format_samples(rows) -%}
  {%- set lines = [] -%}
  {%- for row in rows -%}
    {%- do lines.append('(' ~ row | map('string') | join(', ') ~ ')') -%}
  {%- endfor -%}
  {{- lines | join('; ') -}}
{% endmacro %}

{% macro clickhouse_backfill_validate_config(inserts_only, incremental_strategy, unique_key) %}
  {%- if inserts_only %}
    {% do exceptions.raise_compiler_error(
      "`backfill` mode is not compatible with `inserts_only`: corrections must be merged by "
      ~ "`unique_key`, not appended. Use `incremental_strategy='delete+insert'` (or `legacy`) instead."
    ) %}
  {% endif %}
  {%- if not unique_key %}
    {% do exceptions.raise_compiler_error(
      "`backfill` mode requires a `unique_key`: corrected facts must replace their warehoused "
      ~ "versions so downstream models and snapshots observe the updated foreign keys."
    ) %}
  {% endif %}
  {%- if incremental_strategy is not none and incremental_strategy not in ('legacy', 'delete_insert') %}
    {% do exceptions.raise_compiler_error(
      "`backfill` mode requires a keyed incremental strategy ('legacy' or 'delete+insert'), got '"
      ~ incremental_strategy ~ "'. Corrections have to be merged by `unique_key`; '"
      ~ incremental_strategy ~ "' cannot do that."
    ) %}
  {% endif %}
{% endmacro %}
