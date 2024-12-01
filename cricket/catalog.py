from pathlib import Path

class Catalog:
    def __init__(self, base_path: str) -> None:
        # Initialize the folder structure
        self.folder = self.Folder(base_path)

        # Initialize other classes with the folder structure
        self.interims = self.Interims(self.folder)
        self.staged = self.Staged(self.folder)
        self.processed = self.Processed(self.folder)

    class Folder:
        def __init__(self, base_path: str) -> None:
            self.project_root = Path(base_path)
            self.data = self.project_root.joinpath("data")
            self.raw = self.data.joinpath("01_raw")
            self.staged = self.data.joinpath("02_staged")
            self.processed = self.data.joinpath("03_processed")
            self.model = self.data.joinpath("04_model")
            self.interims = self.data.joinpath("99_interims")

    class Interims:
        def __init__(self, folder: "Catalog.Folder") -> None:
            self.raw_match_ids = folder.interims.joinpath("raw_match_ids.parquet")
            self.processed_match_ids = folder.interims.joinpath("processed_match_ids.parquet")
            self.new_match_ids = folder.interims.joinpath("new_match_ids.parquet")

    class Staged:
        def __init__(self, folder: "Catalog.Folder") -> None:
            self.deliveries = folder.staged.joinpath("deliveries.parquet")
            self.match_info = folder.staged.joinpath("match_info.parquet")
            self.registry = folder.staged.joinpath("registry.parquet")

    class Processed:
        def __init__(self, folder: "Catalog.Folder") -> None:
            self.df_cricket = folder.processed.joinpath("cricket.parquet")

