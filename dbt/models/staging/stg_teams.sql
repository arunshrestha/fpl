{{ config(materialized='table') }}

with src as (
    select * from {{ source('raw','bootstrap_static') }}
)

select
    (t->>'id')::int                                      as team_id,
    nullif(t->>'code','')::int                           as code,
    t->>'name'                                           as team_name,
    t->>'short_name'                                     as team_short_name,
    nullif(t->>'position','')::int                       as team_position,
    nullif(t->>'played','')::int                         as played,
    nullif(t->>'win','')::int                            as wins,
    nullif(t->>'draw','')::int                           as draws,
    nullif(t->>'loss','')::int                           as losses,
    nullif(t->>'points','')::int                         as points,
    nullif(t->>'strength','')::int                       as strength,
    nullif(t->>'pulse_id','')::int                       as pulse_id,
    nullif(t->>'strength_overall_home','')::int          as strength_overall_home,
    nullif(t->>'strength_overall_away','')::int          as strength_overall_away,
    nullif(t->>'strength_attack_home','')::int           as strength_attack_home,
    nullif(t->>'strength_attack_away','')::int           as strength_attack_away,
    nullif(t->>'strength_defence_home','')::int          as strength_defence_home,
    nullif(t->>'strength_defence_away','')::int          as strength_defence_away
from src
join lateral jsonb_array_elements((data->'teams')) as t on true