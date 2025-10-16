{{ config(materialized='table') }}

with src as (
    select * from {{ source('raw','bootstrap_static') }}
)

select
    (ev->>'id')::int                                 as gameweek_id,
    ev->>'name'                                      as gameweek_name,
    nullif(ev->>'deadline_time','')::timestamptz     as deadline_time,
    (ev->>'average_entry_score')::int                as average_entry_score,
    (ev->>'finished')::boolean                       as finished,
    (ev->>'data_checked')::boolean                   as data_checked,
    nullif(ev->>'highest_score','')::int             as highest_score,
    (ev->>'deadline_time_epoch')::bigint             as deadline_time_epoch,
    (ev->>'is_previous')::boolean                    as is_previous,
    (ev->>'is_current')::boolean                     as is_current,
    (ev->>'is_next')::boolean                        as is_next,
    (ev->>'ranked_count')::bigint                    as ranked_count,
    (ev->>'transfers_made')::bigint                  as transfers_made,
    nullif(ev->>'most_selected','')::int             as most_selected_player_id,
    nullif(ev->>'most_transferred_in','')::int       as most_transferred_in_player_id,
    nullif(ev->>'most_captained','')::int            as most_captained_player_id,
    nullif(ev->>'most_vice_captained','')::int       as most_vice_captained_player_id,
    nullif(ev->>'top_element','')::int               as top_player_id,
    ev->'top_element_info'                           as top_element_info,
    ev->'chip_plays'                                 as chip_plays
from src
join lateral jsonb_array_elements((data->'events')) as ev on true