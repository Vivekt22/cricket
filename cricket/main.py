from prefect import flow

from cricket.src.p01_raw.raw import raw_flow
from cricket.src.p02_staged.staged import staged_flow

@flow
def main_run():
    raw_flow()
    staged_flow()


if __name__ == "__main__":
    main_run()
    