from .extract import fetch_bootstrap_static, fetch_fixtures, fetch_player_history
from .transform import transform_bootstrap_static, transform_teams, transform_gameweeks, transform_players, transform_fixtures, transform_player_history
from .load import upsert_dataframe
from .utils import prepare_load_df
from db.postgres_client import get_engine
from tqdm import tqdm
import pandas as pd

def run_etl():
    engine = get_engine()

    # Extract
    bootstrap_data = fetch_bootstrap_static()
    fixtures_data = fetch_fixtures()

    # Transform
    # teams_df = transform_teams(bootstrap_data['teams'])
    # teams_map = dict(zip(teams_df['id'], teams_df['name']))
    # gameweeks_df = transform_gameweeks(bootstrap_data['events'])
    # players_df = transform_players(bootstrap_data['elements'], teams_map)
    bootstrap_row_df = transform_bootstrap_static(bootstrap_data)
    fixtures_df = transform_fixtures(fixtures_data)

    bootstrap_row_df = prepare_load_df(
        bootstrap_row_df,
        json_cols=['data'],           # store entire payload as JSONB
    )

    # Load
    # upsert_dataframe(teams_df, 'raw.raw_teams', engine, unique_key='id')
    # upsert_dataframe(players_df, 'raw.raw_players', engine, unique_key='id')
    # upsert_dataframe(gameweeks_df, 'raw.raw_gameweeks', engine, unique_key='id')
    upsert_dataframe(bootstrap_row_df, 'bootstrap_static', engine, unique_key=None)
    upsert_dataframe(fixtures_df, 'fixtures', engine, unique_key='id')

    # Player history
    # histories = []
    # for pid, web_name in tqdm(zip(players_df['id'], players_df['web_name']),
    #                           total=len(players_df),
    #                           desc="Fetching player histories"):
    #     hist_json = fetch_player_history(pid)
    #     hist_df = transform_player_history(pid, web_name, hist_json)
    #     if not hist_df.empty:
    #         histories.append(hist_df)

    # if histories:
    #     all_hist_df = pd.concat(histories, ignore_index=True)
    #     upsert_dataframe(all_hist_df, 'raw.raw_current_season_stats', engine, unique_key='id')
