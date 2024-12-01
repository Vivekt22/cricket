from pathlib import Path
import yaml

import polars as pl
from prefect import task, flow

from cricket.catalog import Catalog


def extract_registry_details(file: Path):
    try:
        registry_schema = {
            'match_id': pl.Utf8,
            'person_name': pl.Utf8,
            'person_id': pl.Utf8
        }

        match_id = file.stem

        with open(file) as f:
            match_data = yaml.safe_load(f)
        match_info = match_data['info']

        registry_dict = match_info.get('registry', {}).get('people', {})
        registry_rows = [(match_id, name, person_id) for name, person_id in registry_dict.items()]

        registry_df = pl.DataFrame(registry_rows, schema=registry_schema, orient="row")

        return registry_df

    except Exception:
        return None

def extract_all_registry(raw_data_files):
    all_registry = extract_registry_details(raw_data_files)
    non_empty_dfs = [df for df in all_registry if df is not None]
    if non_empty_dfs:
        return pl.concat(non_empty_dfs)
    else:
        return pl.DataFrame()

def main_extract_registry_raw_data(new_match_ids: pl.DataFrame) -> pl.LazyFrame:
    raw_data_directory = Catalog.folder.raw
    raw_data_files = [
        raw_data_directory.joinpath(f"{file}.yaml")
        for file in
        new_match_ids.select(pl.col("match_id")).to_series().to_list()
    ]
    df_new_registry = extract_all_registry(raw_data_files)

    if Catalog.staged.registry.is_file():
        df_staged_registry = pl.read_parquet(Catalog.staged.registry)
        df_registry = pl.concat([df_new_registry, df_staged_registry])
    else:
        df_registry = df_new_registry

    df_registry.write_parquet(Catalog.staged.registry)


@task(log_prints=True)
def extract_registry(new_match_ids: pl.DataFrame):
    main_extract_registry_raw_data(new_match_ids)