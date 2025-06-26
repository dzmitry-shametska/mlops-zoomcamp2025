import pandas as pd 

from datetime import datetime

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
]

columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df_input = pd.DataFrame(data, columns=columns)


import os 
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')

options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
    
}

year = 2023
month = 1

input_file = "s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
input_file = input_file.format(year=year, month=month)


df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)


output_file = "s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"
output_file = output_file.format(year=year, month=month)

os.system("python batch.py 2023 1")

df = pd.read_parquet(output_file, storage_options=options)

print(df)

