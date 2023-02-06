from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task()
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load('data-engineering-gcs')
    gcs_block.get_directory(local_path=f"./data")
    return Path.joinpath(Path(f"data"), gcs_path)


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    print("Paaaaath " + str(path))
    df = pd.read_parquet(path)
    print(df.dtypes)
    #print(f"pre: missing passanger count: {df['passenger_count'].isna().sum()}")
    #df["passenger_count"].fillna(0, inplace=True)
    #print(f"post: missing passanger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write Dataframe to BigQuery"""

    gcs_creds_block = GcpCredentials.load('zoom-gcp-creds')
    df.to_gbq(
        destination_table='ezic_de_zoomcamp.rides',
        project_id='dte-de-course-375215',
        credentials=gcs_creds_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str):
    """Main ETL flow to load data into Big Query"""  
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    return len(df)


@flow(log_prints=True)
def parent_etl_gcs_to_bq(months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"):
    rows = 0
    for month in months:
        rows += etl_gcs_to_bq(year, month, color)
    print("Processed rows:", str(rows))


if __name__ == '__main__':
    parent_etl_gcs_to_bq()
