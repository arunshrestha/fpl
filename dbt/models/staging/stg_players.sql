{{ config(materialized='table') }}

with src as (
    select * from {{ source('raw','bootstrap_static') }}
)

select
    (p->>'id')::int                                 as id,
    p->>'web_name'                                  as web_name,
    p->>'first_name'                                as first_name,
    p->>'second_name'                               as second_name,
    (p->>'team')::int                               as team_id,
    (p->>'element_type')::int                       as position_id,
    (p->>'now_cost')::int                            as now_cost,
    p->>'news'                                      as news,
    (p->>'minutes')::int                            as minutes,
    (p->>'total_points')::int                       as total_points,
    (p->>'goals_scored')::int                       as goals_scored,
    (p->>'assists')::int                            as assists,
    (p->>'clean_sheets')::int                       as clean_sheets,
    (p->>'goals_conceded')::int                     as goals_conceded,
    (p->>'own_goals')::int                          as own_goals,
    (p->>'penalties_saved')::int                    as penalties_saved,
    (p->>'penalties_missed')::int                   as penalties_missed,
    (p->>'yellow_cards')::int                       as yellow_cards,
    (p->>'red_cards')::int                          as red_cards,
    (p->>'saves')::int                              as saves,
    (p->>'minutes')::int                            as minutes_played -- duplicate safe alias
from src
join lateral jsonb_array_elements((data->'elements')) as p on true