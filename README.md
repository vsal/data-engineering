# data-engineering

Data engineering exercise

To fetch data from 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow' and load it into the local postgres DB run as:

docker run -it --rm \
 --network=data-engineering_default \
 taxi_ingest:v001 \
 --pg-user=root \
 --pg-pass=root \
 --pg-host=pgdatabase \
 --pg-port=5432 \
 --pg-db=ny_taxi \
 --target-table=yellow_taxi_trips_2021_1 \
 --chunksize=100000
