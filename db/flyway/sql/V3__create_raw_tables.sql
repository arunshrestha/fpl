-- raw.fixtures
CREATE TABLE IF NOT EXISTS ${RAW_SCHEMA}.fixtures (
    code BIGINT,                              -- unique code for the fixture (FPL API)
    event INTEGER,                            -- gameweek number (FK to gameweeks)
    finished BOOLEAN,                         -- whether the fixture is finished
    finished_provisional BOOLEAN,             -- provisional finished status
    id BIGINT PRIMARY KEY,                    -- fixture id from FPL API
    kickoff_time TIMESTAMPTZ,                 -- kickoff timestamp (UTC)
    minutes INTEGER,                          -- minutes played in the fixture
    provisional_start_time BOOLEAN,           -- provisional start time (API returns bool, not timestamp)
    started BOOLEAN,                          -- whether the fixture has started
    team_a INTEGER,                           -- away team id (FK to teams)
    team_a_score FLOAT,                       -- away team score (API returns float)
    team_h INTEGER,                           -- home team id (FK to teams)
    team_h_score FLOAT,                       -- home team score (API returns float)
    stats JSONB,                              -- raw stats JSON from FPL API
    team_h_difficulty INTEGER,                -- FPL rating of home difficulty
    team_a_difficulty INTEGER,                -- FPL rating of away difficulty
    pulse_id BIGINT,                          -- pulse id (FPL API)
    fetched_at TIMESTAMPTZ DEFAULT NOW()      -- timestamp when data was fetched
);

-- raw.bootstrap_static
CREATE TABLE IF NOT EXISTS ${RAW_SCHEMA}.bootstrap_static (
    id BIGSERIAL PRIMARY KEY,         -- unique row id
    data JSONB NOT NULL,              -- full bootstrap-static payload
    fetched_at TIMESTAMPTZ DEFAULT NOW() -- when the payload was fetched
);
