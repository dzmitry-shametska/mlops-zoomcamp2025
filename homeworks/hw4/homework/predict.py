import pickle
import pandas as pd

with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)

categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


import sys 
import numpy as np
def run():
    # taxi_type = sys.argv[1] # 'green'
    year = int(sys.argv[1]) # 2021
    month = int(sys.argv[2]) # 3

    # run_id = sys.argv[4] # 'e1efc53e9bd149078b0c12aeaa6365df'

    # ride_duration_prediction(      
    #     run_id=run_id,
    #     run_date=datetime(year=year, month=month, day=1)
    # )

    

    # year = 2023
    # month = 3
    df = read_data(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet')

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

    df_result = df[['ride_id']]
    df_result['duration_predicted'] = y_pred

    mean = np.mean(y_pred)
    print(mean)

    output_file = 'df_result.parquet'

    df_result.to_parquet(
        output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )

if __name__ == '__main__':
    run()

