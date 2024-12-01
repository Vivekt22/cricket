from pathlib import Path
import yaml

import polars as pl
from prefect import task, flow

from cricket.catalog import Catalog

def extract_delivery_details(file: Path):
    try:
        innings_schema = {
            'match_id': pl.Utf8,
            'innings': pl.Utf8,
            'batting_team': pl.Utf8,
            'bowling_team': pl.Utf8,
            'declared': pl.Int8,
            'delivery': pl.Float64,
            'batter': pl.Utf8,
            'bowler': pl.Utf8,
            'non_striker': pl.Utf8,
            'batter_runs': pl.Int64,
            'extra_runs': pl.Int64,
            'total_runs': pl.Int64,
            'wicket_type': pl.Utf8,
            'player_out': pl.Utf8
        }

        match_id = file.stem

        with open(file) as f:
            match_data: dict = yaml.safe_load(f)
        match_info = match_data.get('info', {})

        all_innings = []
        for innings in match_data.get('innings', []):
            for inning, inning_details in innings.items():
                batting_team = inning_details.get('team')
                bowling_team = [team for team in match_info.get('teams', []) if team != batting_team]
                bowling_team = bowling_team[0]

                for delivery_details in inning_details.get('deliveries', []):
                    for delivery, delivery_info in delivery_details.items():
                        innings_row = [
                            match_id,
                            inning,
                            batting_team,
                            bowling_team,
                            1 if delivery_info.get('declared', '').lower() == 'yes' else 0,
                            float(delivery),
                            delivery_info.get('batsman'),
                            delivery_info.get('bowler'),
                            delivery_info.get('non_striker'),
                            delivery_info['runs'].get('batsman'),
                            delivery_info['runs'].get('extras'),
                            delivery_info['runs'].get('total'),
                            delivery_info.get('wicket', {}).get('kind'),
                            delivery_info.get('wicket', {}).get('player_out')
                        ]
                        all_innings.append(innings_row)

        if all_innings:
            innings_df = pl.DataFrame(all_innings, schema=innings_schema, orient="row")
            return innings_df
        else:
            return None

    except Exception:
        return None

def extract_all_deliveries(raw_data_files):
    all_innings = extract_delivery_details(raw_data_files)
    non_empty_dfs = [df for df in all_innings if df is not None]
    if non_empty_dfs:
        return pl.concat(non_empty_dfs)
    else:
        return pl.DataFrame()

def main_extract_deliveries_raw_data(new_match_ids: pl.DataFrame):
    raw_data_directory = Catalog.folder.raw
    raw_data_files = [
        raw_data_directory.joinpath(f"{file}.yaml")
        for file in
        new_match_ids.select(pl.col("match_id")).to_series().to_list()
    ]
    df_new_deliveries = extract_all_deliveries(raw_data_files)
    if Catalog.staged.deliveries.is_file():
        df_staged_deliveries = pl.read_parquet(Catalog.staged.deliveries)
        df_deliveries = pl.concat([df_new_deliveries, df_staged_deliveries])
    else:
        df_deliveries = df_new_deliveries

    df_deliveries.select("match_id").unique().write_parquet(Catalog.interims.processed_match_ids)
    
    df_deliveries.write_parquet(Catalog.staged.deliveries)


@task(log_prints=True)
def extract_deliveries(new_match_ids: pl.DataFrame):
    main_extract_deliveries_raw_data(new_match_ids)


df_deliveries = pl.read_parquet(Catalog.staged.deliveries)
df_deliveries.select("match_id").unique().write_parquet(Catalog.interims.processed_match_ids)