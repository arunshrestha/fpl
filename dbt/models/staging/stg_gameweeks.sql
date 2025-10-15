{{ config(materialized='table') }}

with src as (
    select * from {{ source('raw','bootstrap_static') }}
)

select
    (ev->>'id')::int                                 as id,
    ev->>'name'                                      as name,
    (ev->>'deadline_time')::timestamptz              as deadline_time,
    (ev->>'average_entry_score')::int                as average_entry_score,
    (ev->>'finished')::boolean                       as finished,
    (ev->>'data_checked')::boolean                   as data_checked,
    (ev->>'is_previous')::boolean                    as is_previous,
    (ev->>'is_current')::boolean                     as is_current,
    (ev->>'is_next')::boolean                        as is_next,
    (ev->>'deadline_time_epoch')::bigint             as deadline_time_epoch
from src
join lateral jsonb_array_elements((data->'events')) as ev on true