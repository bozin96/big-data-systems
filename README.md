# Big Data Systems projects

Big data projects that consist of Java Spark Submit, Spark Streaming and Spark ML application. All projects are running in a docker containers. The data that is being processed with the Apache Spark platform and is uploaded to a HDFS that has one namenode and one datanode. Hadoop, Spark and Cassandra DB are also running inside a docker containers. There is one master node and two worker nodes.

## Dataset
Dataset contains data on New York City Taxi Cab trips. This dataset represents a subset of the total data and contains data collected for the 3 months of 2014. Dataset can be found at the following [link](https://www.kaggle.com/kentonnlp/2014-new-york-city-taxi-trips?select=nyc_taxi_data_2014.csv).

The data is stored in CSV file with the next columns:
1. **vendor_id**:	A code indicating the TPEP provider that provided the record. Values are: 1= Creative Mobile Technologies, LLC and 2= VeriFone Inc.
2. **pickup_datetime**:	The date and time when the meter was engaged.
3. **dropoff_datetime**:	The date and time when the meter was disengaged.
4. **passenger_count**:	The number of passengers in the vehicle. This is a driver-entered value.
5. **trip_distance**:	The elapsed trip distance in miles reported by the taximeter.
6. **pickup_longitude**:    The longitude where the meter was engaged.
7. **pickup_latitude**:	The latitude where the meter was engaged.
8. **rate_code**:	The final rate code in effect at the end of the trip. Values are: 1= Standard rate, 2= JFK, 3= Newark, 4= Nassau or Westchester, 5= Negotiated fare and 6= Group ride.
9. **store_and_fwd_flag**:	This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka "store and forward," because the vehicle did not have a connection to the server. Values are: Y= store and forward trip and N= not a store and forward trip.
10. **dropoff_longitude**	The longitude where the meter was disengaged.
11. **dropoff_latitude**:	The latitude where the meter was disengaged.
12. **payment_type**:	A numeric code signifying how the passenger paid for the trip. Values are: 1= Credit card, 2= Cash, 3= No charge, 4= Dispute, 5= Unknown, 6= Voided trip.
13. **fare_amount**:	The time-and-distance fare calculated by the meter.
14. **surcharge**: Miscellaneous extras and surcharges.
15. **mta_tax**:	$0.50 MTA tax that is automatically triggered based on the metered rate in use.
16. **tip_amount**:	– This field is automatically populated for credit card tips. Cash tips are not included.
17. **tolls_amount**:	The total amount of all tolls paid in trip.
18. **total_amount**:	The total amount charged to passengers. Does not include cash tips.

## Preparing the projects

- Clone or download project.

- Create a directory in the root directory of the project called 'big-data'. Download the dataset from [link](https://www.kaggle.com/kentonnlp/2014-new-york-city-taxi-trips?select=nyc_taxi_data_2014.csv). Move the downloaded dataset file to newly created directory 'big-data' and rename the file name to 'data.csv'.

- Run docker commands to start Hadoop and Spark required containers and put data on the HDFS.
```
docker-compose up -d
docker exec -it namenode hdfs dfs -mkdir /big-data
docker exec -it namenode hdfs dfs -put /big-data/data.csv /big-data/data.csv
```

## Running the Projects 

Each project is started by running the appropriate docker-compose file. Project start commands are:

- Spark submit project
```
docker-compose -f docker-compose-1.yaml up --build
```

- Spark streaming project
```
docker-compose -f docker-compose-2.yaml up --build
```

- Spark ml project (batch and streaming project separately)
```
docker-compose -f docker-compose-3-batch.yaml up --build
docker-compose -f docker-compose-3-streaming.yaml up --build
```

Each project can be stopped by executing the appropriate docker command.
```
docker-compose -f `docker compose file name` down
```