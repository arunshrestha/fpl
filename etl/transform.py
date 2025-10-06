import pandas as pd
from typing import Dict, List
import logging

def transform_teams(teams_data: List[Dict]) -> pd.DataFrame:
    """Transforms raw teams data into a standardized DataFrame."""
    try:
        df = pd.DataFrame(teams_data)
        expected_cols = ['id', 'name', 'short_name', 'strength', 'code']
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None
        df = df[expected_cols]
        return df
    except Exception as e:
        logging.error(f"Error transforming teams: {e}")
        return pd.DataFrame()

def transform_gameweeks(events_data: List[Dict]) -> pd.DataFrame:
    try:
        df = pd.DataFrame(events_data)
        if "chip_plays" not in df.columns:
            df["chip_plays"] = [[] for _ in range(len(df))]
        chips_expanded = df["chip_plays"].apply(
            lambda x: {d["chip_name"]: d["num_played"] for d in x} if isinstance(x, list) else {}
        )
        df = df.join(pd.DataFrame(chips_expanded.tolist()))
        expected_cols = [
            'id', 'name', 'deadline_time', 'average_entry_score',
            'deadline_time_epoch', 'highest_score', 'chip_plays',
            'most_selected', 'most_transferred_in', 'transfers_made',
            'most_captained', 'most_vice_captained', 'bboost', 'freehit', 'wildcard', '3xc'
        ]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None
        df = df[expected_cols]
        return df
    except Exception as e:
        logging.error(f"Error transforming gameweeks: {e}")
        return pd.DataFrame()
    
def transform_player_positions(element_types_data: List[Dict]) -> pd.DataFrame:
    try:
        df = pd.DataFrame(element_types_data)
        expected_cols = ['id', 'singular_name', 'singular_name_short']
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None
        df = df[expected_cols]
        return df
    except Exception as e:
        logging.error(f"Error transforming teams: {e}")
        return pd.DataFrame()

def transform_players(players_data: list[Dict], teams_map: dict) -> pd.DataFrame:
    df = pd.DataFrame(players_data)
    df = df[['id', 'web_name', 'team', 'element_type', 'now_cost', 'total_points',
             'minutes', 'goals_scored', 'assists', 'clean_sheets', 'goals_conceded',
             'yellow_cards', 'red_cards', 'selected_by_percent', 'form', 'ict_index']]
    df['now_cost'] = df['now_cost'] / 10
    df['team'] = df['team'].map(teams_map)
    return df

# def transform_fixtures(fixtures_data: list[Dict], teams_map: dict) -> pd.DataFrame:
def transform_fixtures(fixtures_data: list[Dict]) -> pd.DataFrame:
    # df = pd.DataFrame(fixtures_data)
    # df = df[['id', 'event_id', 'team_h', 'team_a', 'team_h_difficulty', 'team_a_difficulty', 'kickoff_time']]
    # # df['team_h'] = df['team_h'].map(teams_map)
    # # df['team_a'] = df['team_a'].map(teams_map)
    # # df.rename(columns={'id': 'fixture_id', 'event': 'gameweek_id'}, inplace=True)
    # return df

    try:
        df = pd.DataFrame(fixtures_data)
        expected_cols = ['id', 'event', 'team_h', 'team_a', 'team_h_difficulty', 'team_a_difficulty', 'kickoff_time']
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None
        df = df[expected_cols]
        return df
    except Exception as e:
        logging.error(f"Error transforming teams: {e}")
        return pd.DataFrame()

def transform_player_history(player_id: int, web_name: str, history_data: dict) -> pd.DataFrame:
    """Transform current season history of a player into a DataFrame."""
    df = pd.DataFrame(history_data.get('history', []))
    if df.empty:
        return pd.DataFrame()
    df['player_id'] = player_id
    df['web_name'] = web_name
    return df
