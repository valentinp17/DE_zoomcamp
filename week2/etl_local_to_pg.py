from pathlib import Path
from typing import List

import pandas as pd
import sqlalchemy
from prefect import flow, task


@task(retries=3)
def extract_from_local(color: str, year: int, month: int) -> Path:
    """Download trip data from local storage"""
    local_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    return Path(local_path)


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"Read {path}")
    print(f"df size: {len(df)}")
    return df


@task(log_prints=True)
def write_to_postgres(df: pd.DataFrame) -> None:
    """Write DataFrame to Postgres"""
    engine = sqlalchemy.create_engine("postgresql://root:root@localhost/ny_taxi")
    chunksize = 100_000
    iters = len(df) // chunksize
    for i in range(0, iters, 1):
        left = i * chunksize
        right = left + chunksize
        df[left:right].to_sql('rides',
                            engine,
                            if_exists='append',
                            index=False)
        print(f"chunk from {left} to {right} loaded")


@flow(log_prints=True)
def etl_local_to_pg(year: int, month: int, color: str) -> int:
    """Main ETL flow to load data into Big Query"""

    path = extract_from_local(color, year, month)
    df = transform(path)
    write_to_postgres(df)
    return len(df)

@flow(log_prints=True)
def etl_parent_flow(
        months: List[int] = [2, 3], year: int = 2019, color: str = "yellow"
):
    total = 0
    for month in months:
        total += etl_local_to_pg(year, month, color)

    print(f"Total number of rows: {total}")


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_parent_flow(months, year, color)