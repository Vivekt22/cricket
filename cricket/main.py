from prefect import flow

from cricket.src.p01_raw.raw import raw_flow
from cricket.src.p02_staged.staged import staged_flow

from cricket.catalog import Catalog
from cricket.params import Params


@flow(log_prints=True)
def main_run():
    base_path = "C:/Users/vivek/OneDrive/Documents/04 - My Projects/30 - Cricket/cricket"
    catalog = Catalog(base_path)
    params = Params()

    raw_flow(catalog, params)
    staged_flow(catalog)


if __name__ == "__main__":
    local_base_path = "C:/Users/vivek/OneDrive/Documents/04 - My Projects/30 - Cricket/cricket"
    main_run(base_path=local_base_path)
    