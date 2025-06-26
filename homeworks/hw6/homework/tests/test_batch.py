from batch import prepare_data

from datetime import datetime
import pandas as pd

categorical = ['PULocationID', 'DOLocationID']

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

def test_1():
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
    ]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)


    data_exp = [
        (-1, -1, dt(1, 1), dt(1, 10), 9.0),
        (1, 1, dt(1, 2), dt(1, 10), 8.0),  
    ]

    columns_exp = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration']
    df_exp = pd.DataFrame(data_exp, columns=columns_exp)
    df_exp[categorical] = df_exp[categorical].fillna(-1).astype('int').astype('str')


    res = prepare_data(df, categorical)
    
    assert df_exp.equals(res)

