{{ config(materialized='table') }}

with src as (
    select * from {{ source('raw','bootstrap_static') }}
)

select
    (t->>'id')::int                                    as id,
    t->>'name'                                         as team_name,
    t->>'short_name'                                   as short_name,
    t->>'code'                                         as code,
    (t->>'team_division')::int                         as team_division,
    (t->>'strength')::int                              as strength,
    (t->>'strength_attack_home')::int                  as strength_attack_home,
    (t->>'strength_attack_away')::int                  as strength_attack_away,
    (t->>'strength_defence_home')::int                 as strength_defence_home,
    (t->>'strength_defence_away')::int                 as strength_defence_away,
    (t->>'pulse_id')::int                              as pulse_id
from src
join lateral jsonb_array_elements((data->'teams')) as t on true