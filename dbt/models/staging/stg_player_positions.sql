{{ config(materialized='table') }}

with src as (
    select * from {{ source('raw','bootstrap_static') }}
)

select
    (et->>'id')::int                        as player_position_id,
    et->>'singular_name'                    as player_position_singular_name,
    et->>'singular_name_short'              as player_position_singular_name_short,
    et->>'plural_name'                      as player_position_plural_name,
    et->>'plural_name_short'                as player_position_plural_name_short,
    nullif(et->>'squad_select','')::int     as squad_select,
    nullif(et->>'squad_min_select','')::int as squad_min_select,
    nullif(et->>'squad_max_select','')::int as squad_max_select,
    nullif(et->>'squad_min_play','')::int   as squad_min_play,
    nullif(et->>'squad_max_play','')::int   as squad_max_play,
    (et->>'element_count')::int             as element_count
from src
join lateral jsonb_array_elements((data->'element_types')) as et on true