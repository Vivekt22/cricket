import os
import zipfile
from pathlib import Path

import requests
from tqdm import tqdm
import polars as pl

from prefect import task, flow

from cricket.catalog import Catalog
from cricket.params import Params

@task(log_prints=True)
def clean_raw_folder(catalog: Catalog) -> None:
    for file in catalog.folder.raw.glob("*.yaml"):
        file.unlink()
    for file in catalog.folder.raw.glob("*.txt"):
        file.unlink()

@task(log_prints=True)
def download_yaml_files(catalog: Catalog, params: Params) -> None:
    url = params.cricsheet_url
    target_directory = catalog.folder.raw

    zip_file_path = "all.zip"
    new_files_count = 0

    # Download the zip file
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(zip_file_path, 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=8192), unit="KB"):
                f.write(chunk)

    # Extract the zip file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for member in tqdm(zip_ref.infolist(), desc="Extracting"):
            member_path = os.path.join(target_directory, member.filename)
            if os.path.isfile(member_path) and os.path.getsize(member_path) == member.file_size:
                continue
            zip_ref.extract(member, target_directory)
            new_files_count += 1

    os.remove(zip_file_path)

    df_raw_staged_match_ids = (
        pl.DataFrame(
            [file.stem for file in Path(target_directory).glob("*.yaml")],
            schema={"match_id": pl.String}
        )
    )

    df_raw_staged_match_ids.write_parquet(catalog.interims.raw_match_ids)


@flow(log_prints=True)
def raw_flow(catalog: Catalog, params: Params) -> None:
    clean_raw_folder(catalog)
    download_yaml_files(catalog, params)