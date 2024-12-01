from prefect import flow

from cricket.src.p02_staged.new_match_ids import get_new_match_ids, artifact_new_match_ids
from cricket.src.p02_staged.extract_deliveries import extract_deliveries
from cricket.src.p02_staged.extract_match_info import extract_match_info
from cricket.src.p02_staged.extract_registry import extract_registry
from cricket.catalog import Catalog

@flow(log_prints=True)
def staged_flow(catalog: Catalog):
    df_new_match_ids = get_new_match_ids(catalog)
    artifact_new_match_ids(df_new_match_ids)
    extract_deliveries(df_new_match_ids, catalog)
    extract_match_info(df_new_match_ids, catalog)
    extract_registry(df_new_match_ids, catalog)