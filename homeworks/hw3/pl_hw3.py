#!/usr/bin/env python
# coding: utf-8

import pickle
from pathlib import Path

import pandas as pd
# import xgboost as xgb
from sklearn.linear_model import LinearRegression

from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import root_mean_squared_error


import mlflow

from prefect import flow, task
import logging

# mlflow.set_tracking_uri("http://localhost:5000")
# mlflow.set_experiment("nyc-taxi-experiment")

# models_folder = Path('models')
# models_folder.mkdir(exist_ok=True)


# @task
def read_dataframe(year, month):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
    df = pd.read_parquet(url)

    print(f'URL: {url}')
    print(f'original DF size: {df.shape}')

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)

    print(f'transformed DF size: {df.shape}')
    
    return df


def create_X(df, dv=None):
    categorical = ['PULocationID', 'DOLocationID']
    numerical = ['trip_distance']
    # dicts = df[categorical + numerical].to_dict(orient='records')
    dicts = df[categorical].to_dict(orient='records')

    if dv is None:
        dv = DictVectorizer(sparse=True)
        X = dv.fit_transform(dicts)
    else:
        X = dv.transform(dicts)

    return X, dv


def train_model(X_train, y_train, X_val, y_val, dv):
    with mlflow.start_run() as run:

        # train = xgb.DMatrix(X_train, label=y_train)
        # valid = xgb.DMatrix(X_val, label=y_val)

        # best_params = {
        #     'learning_rate': 0.09585355369315604,
        #     'max_depth': 30,
        #     'min_child_weight': 1.060597050922164,
        #     'objective': 'reg:linear',
        #     'reg_alpha': 0.018060244040060163,
        #     'reg_lambda': 0.011658731377413597,
        #     'seed': 42
        # }

        # mlflow.log_params(best_params)

        # booster = xgb.train(
        #     params=best_params,
        #     dtrain=train,
        #     num_boost_round=30,
        #     evals=[(valid, 'validation')],
        #     early_stopping_rounds=50
        # )

        lr = LinearRegression()
        lr.fit(X_train, y_train)

        y_pred = lr.predict(X_train)


        # y_pred = booster.predict(valid)
        # rmse = root_mean_squared_error(y_val, y_pred)
        # mlflow.log_metric("rmse", rmse)
        
        print(f' Intercept:  {lr.intercept_} ')

        # with open("models/preprocessor.b", "wb") as f_out:
        #     pickle.dump(dv, f_out)

        # mlflow.log_artifact("models/preprocessor.b", artifact_path="preprocessor")

        # mlflow.xgboost.log_model(booster, artifact_path="models_mlflow")
        
        mlflow.sklearn.log_model(lr, "linear_regression_model")

        return run.info.run_id

@task
def run(year, month):
    df_train = read_dataframe(year=year, month=month)

    next_year = year if month < 12 else year + 1
    next_month = month + 1 if month < 12 else 1
    df_val = read_dataframe(year=next_year, month=next_month)

    X_train, dv = create_X(df_train)
    X_val, _ = create_X(df_val, dv)

    target = 'duration'
    y_train = df_train[target].values
    y_val = df_val[target].values

    run_id = train_model(X_train, y_train, X_val, y_val, dv)
    print(f"MLflow run_id: {run_id}")
    return run_id


# if __name__ == "__main__":
#     import argparse

#     parser = argparse.ArgumentParser(description='Train a model to predict taxi trip duration.')
#     parser.add_argument('--year', type=int, required=True, help='Year of the data to train on')
#     parser.add_argument('--month', type=int, required=True, help='Month of the data to train on')
#     args = parser.parse_args()

#     run_id = run(year=args.year, month=args.month)

#     with open("run_id.txt", "w") as f:
#         f.write(run_id)

# ########################

@flow
def main_flow(
    year: int = 2023,
    month: int = 3,
) -> None:
    """The main training pipeline"""


    # MLflow settings
    mlflow.set_tracking_uri("sqlite:///mlflow.db")
    mlflow.set_experiment("nyc-taxi-experiment")

    # # Load
    # read_dataframe(year,month)
    run(year,month)
    # df_train = read_data(train_path)
    # df_val = read_data(val_path)

    # # Transform
    # X_train, X_val, y_train, y_val, dv = add_features(df_train, df_val)

    # # Train
    # train_best_model(X_train, X_val, y_train, y_val, dv)


if __name__ == "__main__":
    main_flow()
