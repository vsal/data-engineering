#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


# assign data typtes to the fields
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', type=int, default=5432, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database')
@click.option('--year', type=int, default=2021, help='Year')
@click.option('--month', type=int, default=1, help='Month')
@click.option('--target-table', default='yellow_taxi_data', help='Target table')
@click.option('--use_yellow', is_flag=True, help='Use yellow taxi data')
@click.option('--chunksize', type=int, default=100000, help='Chunk size')
@click.command()
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, use_yellow, chunksize):
    if (use_yellow):
        prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
        url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

        df_iter = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunksize)
    else:
        prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata'
        url = f"{prefix}_{year}-{month:02d}.parquet"

        df_iter = pd.read_parquet(
            url,
            engine='pyarrow',
            chunksize=chunksize)

    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            # create table
            df_chunk.head(0).to_sql(name=target_table,
                                    con=engine, if_exists='replace')

            # ingest zone lookup data
            url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
            df_zone_lookup = pd.read_csv(url)
            df_zone_lookup.to_sql(name='taxi_zone_lookup',
                                  con=engine, if_exists='replace')
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')
        print(len(df_chunk))


if __name__ == '__main__':
    run()
