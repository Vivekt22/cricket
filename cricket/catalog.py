from pathlib import Path
    
class Folder:
    project_root = Path(__file__).parent.parent
    data = project_root.joinpath("data")

    raw = data.joinpath("01_raw")
    staged = data.joinpath("02_staged")
    processed = data.joinpath("03_processed")
    model = data.joinpath("04_model")
    interims = data.joinpath("99_interims")

class Interims:
    raw_match_ids = Folder.interims.joinpath("raw_match_ids.parquet")
    processed_match_ids = Folder.interims.joinpath("processed_match_ids.parquet")
    new_match_ids = Folder.interims.joinpath("new_match_ids.parquet")

class Staged:
    deliveries = Folder.staged.joinpath("deliveries.parquet")
    match_info = Folder.staged.joinpath("match_info.parquet")
    registry = Folder.staged.joinpath("registry.parquet")

class Processed:
    df_cricket = Folder.processed.joinpath("cricket.parquet")

class Catalog:
    folder = Folder
    interims = Interims
    staged = Staged
    processed = Processed
