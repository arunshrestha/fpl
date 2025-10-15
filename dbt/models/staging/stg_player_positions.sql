{{ config(materialized='table') }}

with src as (
    select * from {{ source('raw','bootstrap_static') }}
)

select
    (et->>'id')::int                       as id,
    et->>'singular_name'                   as player_position,
    et->>'singular_name_short'             as player_position_short,
    et->>'plural_name'                     as player_position_plural,
    et->>'plural_name_short'               as player_position_plural_short,
    (et->>'element_count')::int            as element_count
from src
join lateral jsonb_array_elements((data->'element_types')) as et on true