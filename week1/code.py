# Load data to postgres
import pandas as pd
import sqlalchemy

engine = sqlalchemy.create_engine("postgresql://root:root@localhost/ny_taxi")
conn = engine.connect()

df_iter = pd.read_csv("green_tripdata_2019-01.csv", iterator=True, chunksize=100000)

df = next(df_iter)
df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

df.head(n=0).to_sql(name="green_taxi_data", con=engine, if_exists='replace')
df.to_sql(name="green_taxi_data", con=engine, if_exists='append')
while True:
    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.to_sql(name="green_taxi_data", con=engine, if_exists='append')

    print("inserted one chunk")

# taxi_zone_lookup loading

df = pd.read_csv("taxi+_zone_lookup.csv")
df.head(n=0).to_sql(name="taxi_zone_lookup", con=engine, if_exists='replace')
df.to_sql(name="taxi_zone_lookup", con=engine, if_exists='append')

# SQL code for questions 3-6
"""
-- Q3
SELECT COUNT(*)
FROM green_taxi_data
WHERE DATE(lpep_pickup_datetime) = '2019-01-15' AND
DATE(lpep_dropoff_datetime) = '2019-01-15';
-- Ans: 20530

-- Q4
SELECT DATE(lpep_pickup_datetime), trip_distance
FROM green_taxi_data
WHERE trip_distance = (SELECT MAX(trip_distance)
from green_taxi_data);
-- Ans: 2019-01-15, 117.99

-- Q5
SELECT passenger_count, count(*)
FROM green_taxi_data
WHERE DATE(lpep_pickup_datetime) = '2019-01-01'
AND (passenger_count = 2 OR passenger_count = 3)
GROUP BY passenger_count;
-- Ans: 2,1282; 3,254

-- Q6
SELECt "Zone"
FROM green_taxi_data
JOIN taxi_zone_lookup on "DOLocationID" = "LocationID"
WHERE tip_amount = (SELECT MAX(tip_amount)
FROM green_taxi_data
WHERE "PULocationID" = (SELECT "LocationID"
FROM taxi_zone_lookup
WHERE "Zone" = 'Astoria'));
-- Ans: Long Island City/Queens Plaza
"""