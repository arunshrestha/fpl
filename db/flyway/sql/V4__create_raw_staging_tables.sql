-- raw.fixtures
CREATE TABLE IF NOT EXISTS ${RAW_STAGING_SCHEMA}.fixtures (
    id INTEGER PRIMARY KEY,             -- fixture id from FPL API
    "event" INTEGER,                    -- gameweek number FK to gameweeks.id
    team_h INTEGER NOT NULL,            -- FK to teams.id (home)
    team_a INTEGER NOT NULL,            -- FK to teams.id (away)
    team_h_difficulty INTEGER,          -- FPL rating of home difficulty
    team_a_difficulty INTEGER,          -- FPL rating of away difficulty
    kickoff_time TIMESTAMPTZ            -- kickoff timestamp (UTC)
);