with fixtures as (

    select *
    from {{ ref('stg_fixtures') }}

),

teams as (

    select *
    from {{ ref('stg_teams') }}

),

gameweeks as (

    select *
    from {{ ref('stg_gameweeks') }}

)

select

    f.fixture_id,
    f.gameweek_id,
    g.gameweek_name,

    f.home_team_id,
    ht.team_name as home_team_name,

    f.away_team_id,
    at.team_name as away_team_name,

    f.home_team_score,
    f.away_team_score,

    f.home_team_difficulty,
    f.away_team_difficulty,

    g.finished

from fixtures f

left join teams ht
    on f.home_team_id = ht.team_id

left join teams at
    on f.away_team_id = at.team_id

left join gameweeks g
    on f.gameweek_id = g.gameweek_id