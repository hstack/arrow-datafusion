create external table nyc_trip_data
stored as parquet
location '../../benchmark/yellow_tripdata_2024-01.parquet';

-- create external table nyc_trip_data
-- stored as parquet
-- location 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet';

select * from nyc_trip_data order by fare_amount desc limit 100;
