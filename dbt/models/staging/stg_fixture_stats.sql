{{ config(materialized='table') }}

with stats_flat as (
    select
        f.id as fixture_id,
        stat_elem->>'identifier' as stat_identifier,
        'h' as team_type,
        (stat_team_elem->>'element')::integer as player_id,
        (stat_team_elem->>'value')::integer as stat_value
    from {{ source('raw', 'fixtures') }} f
    join lateral jsonb_array_elements(f.stats) as stat_elem on true
    join lateral jsonb_array_elements(stat_elem->'h') as stat_team_elem on true

    union all

    select
        f.id as fixture_id,
        stat_elem->>'identifier' as stat_identifier,
        'a' as team_type,
        (stat_team_elem->>'element')::integer as player_id,
        (stat_team_elem->>'value')::integer as stat_value
    from {{ source('raw', 'fixtures') }} f
    join lateral jsonb_array_elements(f.stats) as stat_elem on true
    join lateral jsonb_array_elements(stat_elem->'a') as stat_team_elem on true
)

select
    fixture_id,
    stat_identifier,
    team_type,
    player_id,
    stat_value
from stats_flat