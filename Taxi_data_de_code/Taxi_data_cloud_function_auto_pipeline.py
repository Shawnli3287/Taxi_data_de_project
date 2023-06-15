import functions_framework
from google.cloud import bigquery
import pandas as pd
from google.cloud import storage
import fastparquet
import pyarrow

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    storage_client = storage.Client()
    storage_bucket = storage_client.bucket(bucket)
    blob = storage_bucket.blob(name)
    blob.make_public()

    url = 'https://storage.googleapis.com/{bucketname}/{filename}'.format(bucketname = bucket, filename = name)

    print(f"Here is the URL:",url)

    # Try to read parquet file through url, fail will return an error
    try:
        df = pd.read_parquet(url)
        if not df.empty:
            print("Parquet file read successfully and contains content.")
        else:
            print("Parquet file read successfully but is empty.")
    except Exception as e:
        print("Failed to read Parquet file:", e)

    # Location look up table
    look_up_url = 'https://storage.googleapis.com/taxi-data-de/taxi%2B_zone_lookup.csv'
    df_look_up = pd.read_csv(look_up_url)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df = df.drop_duplicates().reset_index(drop=True)
    df['Id'] = df.index

    merged_df = df.merge(df_look_up.add_prefix('PU_'), left_on='PULocationID', right_on='PU_LocationID', how='left')
    merged_df.drop('PU_LocationID', axis = 1, inplace = True)
    merged_df = merged_df.merge(df_look_up.add_prefix('DO_'), left_on='DOLocationID', right_on='DO_LocationID', how='left')
    merged_df.drop('DO_LocationID', axis = 1, inplace = True)

    df = merged_df

    Datetime_table = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    Datetime_table['tpep_pickup_datetime'] = Datetime_table['tpep_pickup_datetime']
    Datetime_table['pick_hour'] = Datetime_table['tpep_pickup_datetime'].dt.hour
    Datetime_table['pick_day'] = Datetime_table['tpep_pickup_datetime'].dt.day
    Datetime_table['pick_month'] = Datetime_table['tpep_pickup_datetime'].dt.month
    Datetime_table['pick_year'] = Datetime_table['tpep_pickup_datetime'].dt.year
    Datetime_table['pick_weekday'] = Datetime_table['tpep_pickup_datetime'].dt.weekday

    Datetime_table['drop_hour'] = Datetime_table['tpep_dropoff_datetime'].dt.hour
    Datetime_table['drop_day'] = Datetime_table['tpep_dropoff_datetime'].dt.day
    Datetime_table['drop_month'] = Datetime_table['tpep_dropoff_datetime'].dt.month
    Datetime_table['drop_year'] = Datetime_table['tpep_dropoff_datetime'].dt.year
    Datetime_table['drop_weekday'] = Datetime_table['tpep_dropoff_datetime'].dt.weekday

    Datetime_table['datetime_id'] = Datetime_table.index

    Datetime_table = Datetime_table[['datetime_id',
                             'tpep_pickup_datetime',
                             'pick_hour',
                             'pick_day',
                             'pick_month',
                             'pick_year',
                             'pick_weekday',
                             'tpep_dropoff_datetime',
                             'drop_hour', 'drop_day',
                             'drop_month', 'drop_year',
                             'drop_weekday']]
    
    passenger_table = df[['passenger_count']].reset_index(drop=True)
    passenger_table['passenger_count_id'] = passenger_table.index
    passenger_table = passenger_table[['passenger_count_id','passenger_count']]


    trip_info = df[['trip_distance']].reset_index(drop=True)
    trip_info['trip_distance_id'] = trip_info.index
    trip_info = trip_info[['trip_distance_id','trip_distance']]

    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    rate_code = df[['RatecodeID']].reset_index(drop=True)
    rate_code['rate_code_id'] = rate_code.index
    rate_code['rate_code_name'] = rate_code['RatecodeID'].map(rate_code_type)
    rate_code = rate_code[['rate_code_id','RatecodeID','rate_code_name']]

    pickup_location = df[['PULocationID','PU_Borough','PU_Zone','PU_service_zone']].reset_index(drop=True)
    pickup_location['pickup_location_id'] = pickup_location.index
    pickup_location = pickup_location[['pickup_location_id','PULocationID','PU_Borough','PU_Zone','PU_service_zone']]

    dropoff_location = df[['DOLocationID','DO_Borough','DO_Zone','DO_service_zone']].reset_index(drop=True)
    dropoff_location['dropoff_location_id'] = dropoff_location.index
    dropoff_location = dropoff_location[['dropoff_location_id','DOLocationID','DO_Borough','DO_Zone','DO_service_zone']]

    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    payment_type_dim = df[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]

    fact_table = df.merge(passenger_table, left_on='Id', right_on='passenger_count_id') \
                 .merge(trip_info, left_on='Id', right_on='trip_distance_id') \
                 .merge(rate_code, left_on='Id', right_on='rate_code_id') \
                 .merge(pickup_location, left_on='Id', right_on='pickup_location_id') \
                 .merge(dropoff_location, left_on='Id', right_on='dropoff_location_id')\
                 .merge(Datetime_table, left_on='Id', right_on='datetime_id') \
                 .merge(payment_type_dim, left_on='Id', right_on='payment_type_id') \
                 [['Id','VendorID', 'datetime_id', 'passenger_count_id',
                   'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag',
                   'pickup_location_id','dropoff_location_id',
                   'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                   'improvement_surcharge', 'total_amount']]
    
    bigquery_client = bigquery.Client()

    table_id = 'taxi-data-de-project-389214.taxi_data_de.{}'.format('Datetime_table')

    # Set job config to write only (replace), only use for load the first table
    #job_config_replace = bigquery.job.LoadJobConfig()
    #job_config_replace.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    
    table_list = ['fact_table',
                  'passenger_table',
                  'trip_info',
                  'rate_code',
                  'pickup_location',
                  'dropoff_location',
                  'Datetime_table',
                  'payment_type_dim']

    dataframes = [fact_table,
                  passenger_table,
                  trip_info,
                  rate_code,
                  pickup_location,
                  dropoff_location,
                  Datetime_table,
                  payment_type_dim]
    
    # Load the dataframe into BigQuery
    for table_name, dataframe in zip(table_list, dataframes):
        table_id = 'taxi-data-de-project-389214.taxi_data_de.{}'.format(table_name)
        job = bigquery_client.load_table_from_dataframe(Datetime_table, table_id, job_config=job_config_replace)
        job.result()
        if job == []:
            print(f"Table {table_name} loaded successfully.")