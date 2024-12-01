from prefect import flow

from cricket.src.p02_staged.new_match_ids import get_new_match_ids, create_table_artifact
from cricket.src.p02_staged.extract_deliveries import extract_deliveries
from cricket.src.p02_staged.extract_match_info import extract_match_info
from cricket.src.p02_staged.extract_registry import extract_registry

@flow(log_prints=True)
def staged_flow():
    df_new_match_ids = get_new_match_ids()
    create_table_artifact(df_new_match_ids)
    extract_deliveries(df_new_match_ids)
    extract_match_info(df_new_match_ids)
    extract_registry(df_new_match_ids)