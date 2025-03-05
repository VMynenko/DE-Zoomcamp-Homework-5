# Module 5: Batch Processing

## Question 1: Install Spark and PySpark
- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.
```python
spark.version
```
- `3.5.0`

## Question 2: Yellow October 2024  
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches?  
```python
files = [f for f in os.listdir(output_path) if f.endswith(".parquet")]
total_size = sum(os.path.getsize(os.path.join(output_path, f)) for f in files)
average_size = total_size / len(files) if files else 0
print(f"Average size of the file: {average_size / (1024 * 1024):.2f} MB")
```
`Average size of the file: 22.39 MB`  

Closest option:
- `25MB`

## Question 3: Count records  
How many taxi trips were there on the 15th of October?  
```python
count_records = df.filter(to_date(df.tpep_pickup_datetime) == "2024-10-15").count()
print(f"Number of records with pickup date 2024-10-15: {count_records}")
```
`Number of records with pickup date 2024-10-15: 128893`  

Closest option:
- `125,567`

## Question 4: Longest trip  
What is the length of the longest trip in the dataset in hours?  
```python
df_trip_duration_hours = df.withColumn("trip_duration_hours", 
                   (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600)
max_trip_duration = df_trip_duration_hours.select(max("trip_duration_hours")).collect()[0][0]
print(f"The longest trip lasted: {max_trip_duration:.2f} hours")
```
`The longest trip lasted: 162.62 hours`  

Answer:
- `162`

## Question 5: User Interface  
Spark’s User Interface which shows the application's dashboard runs on which local port?    

Answer:
- `4040`

## Question 6: Least frequent pickup location zone    
Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?    
```python
zone_lookup_df = spark.read.option("header", "true").csv(zone_lookup_path)
pickup_counts = df.groupBy("PULocationID").agg(count("*").alias("trip_count"))
pickup_counts_with_zones = pickup_counts.join(zone_lookup_df, pickup_counts.PULocationID == zone_lookup_df.LocationID, "left")
least_frequent_zone = pickup_counts_with_zones.orderBy(asc("trip_count")).select("Zone", "trip_count").limit(1)
least_frequent_zone.show()
```
`+--------------------+----------+`  
`|                Zone|trip_count|`  
`+--------------------+----------+`  
`|Governor's Island...|         1|`  
`+--------------------+----------+`  

Answer:
- `Governor's Island/Ellis Island/Liberty Island`
