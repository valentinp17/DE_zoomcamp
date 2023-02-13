## Week 3 Homework
Download data by start script: python fhv/fetch_data.py

Create external and not external table in Greeenplum cluster:
```sql
CREATE [EXTERNAL] TABLE fhv_taxi[_ext] (
    affiliated_base_number text,
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    PULocationID integer,
    DOLocationID integer,
    SR_Flag boolean
)
    LOCATION ( 'file://code/fhv/*.csv.gz' )
    FORMAT 'CSV' ( DELIMITER ',' );

```
## Question 1:
What is the count for fhv vehicle records for year 2019?
```sql
SELECT COUNT(*) FROM fhv_taxi;
```

Ans: 43,244,696

## Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br>
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```sql
EXPLAIN ANALYZE SELECT COUNT(DISTINCT affiliated_base_number) 
                FROM fhv_taxi[_ext];
```
Output:
For internal
Statement statistics: 
Memory used: 325000K bytes

Output:
for external
Statement statistics:
Memory used: 0K bytes

Ans: 0 MB for the External Table and 317.94MB for the BQ Table

## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

```sql
SELECT COUNT(1) 
FROM fhv_taxi 
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
```

## Question 4:
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

Answer: Partition by pickup_datetime Cluster on affiliated_base_number

## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).</br>
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

Internal table:
```sql
EXPLAIN ANALYZE SELECT COUNT(DISTINCT affiliated_base_number)
FROM fhv_taxi
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31';
```

Output:
Statement statistics:
Memory used: 654000K bytes

External the partitioned and clustered table.:
```sql
EXPLAIN ANALYZE SELECT COUNT(DISTINCT affiliated_base_number)
FROM fhv_taxi_ext_pc
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31';
```

Output:
Statement statistics:
Memory used: 33000K bytes

Ans: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

## Question 6:
Where is the data stored in the External Table you created?

True ans: .csv.gx files inside GP cluster 

Ans: GCP Bucket


## Question 7:
It is best practice in Big Query to always cluster your data:

Ans: False
