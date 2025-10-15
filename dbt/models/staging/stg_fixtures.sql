{{ config(materialized='table') }}

with raw_fixtures as (
    select *
    from {{ source('raw', 'fixtures') }}
)

select
    id as fixture_id,
    "event" as gameweek_id,
    team_h as home_team_id,
    team_a as away_team_id,
    team_h_score as home_team_score,
    team_a_score as away_team_score,
    team_h_difficulty as home_team_difficulty,
    team_a_difficulty as away_team_difficulty
from raw_fixtures