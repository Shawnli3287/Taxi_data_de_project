CREATE OR REPLACE TABLE `taxi-data-de-project-389214.taxi_data_de.Analytics_table` AS (
SELECT 
f.Id,
f.VendorID,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime,
p.passenger_count,
t.trip_distance,
r.rate_code_name,
pick.PULocationID,
pick.PU_Borough,
pick.PU_Zone,
pick.PU_service_zone,
drop.DOLocationID,
drop.DO_Borough,
drop.DO_Zone,
drop.DO_service_zone,
pay.payment_type_name,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount
FROM `taxi-data-de-project-389214.taxi_data_de.fact_table` f
JOIN `taxi-data-de-project-389214.taxi_data_de.Datetime_table` d  ON f.datetime_id=d.datetime_id
JOIN `taxi-data-de-project-389214.taxi_data_de.passenger_table` p  ON p.passenger_count_id=f.passenger_count_id  
JOIN `taxi-data-de-project-389214.taxi_data_de.trip_info` t  ON t.trip_distance_id=f.trip_distance_id  
JOIN `taxi-data-de-project-389214.taxi_data_de.rate_code` r ON r.rate_code_id=f.rate_code_id  
JOIN `taxi-data-de-project-389214.taxi_data_de.pickup_location` pick ON pick.pickup_location_id=f.pickup_location_id
JOIN `taxi-data-de-project-389214.taxi_data_de.dropoff_location` drop ON drop.dropoff_location_id=f.dropoff_location_id
JOIN `taxi-data-de-project-389214.taxi_data_de.payment_type_dim` pay ON pay.payment_type_id=f.payment_type_id)
;