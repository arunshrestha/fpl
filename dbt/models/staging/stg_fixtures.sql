{{ config(materialized='table') }}  -- creates a physical table

with raw_fixtures as (
    select *
    from {{ source('raw', 'fixtures') }}  -- references the source table
)

select
    id as fixture_id,
    "event" as gameweek_id,
    team_h as home_team_id,
    team_a as away_team_id,
    kickoff_time,
    now() as last_updated
from raw_fixtures
