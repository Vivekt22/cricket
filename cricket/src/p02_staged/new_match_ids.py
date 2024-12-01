from pathlib import Path
from datetime import timedelta
from multiprocessing import Pool, cpu_count

import polars as pl
from prefect import task, flow

from cricket.catalog import Catalog


@task(cache_key_fn=lambda *_: "generate_value_key", cache_expiration=timedelta(minutes=60))
def get_new_match_ids() -> pl.DataFrame:
    df_raw_match_ids = pl.read_parquet(Catalog.interims.raw_match_ids)
    if Catalog.interims.processed_match_ids.exists():
        df_processed_match_ids = pl.read_parquet(Catalog.interims.processed_match_ids)
        df_new_match_ids = df_raw_match_ids.filter(
            ~pl.col("match_id").is_in(df_processed_match_ids.select("match_id"))
        )
    else:
        df_new_match_ids = df_raw_match_ids
    
    df_new_match_ids.write_parquet(Catalog.interims.new_match_ids)

    return df_new_match_ids
