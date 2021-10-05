CREATE OR REPLACE TABLE iris
WITH (
    location = 'iris.csv',
    format = 'csv',
    persist = True

);

select * from iris limit 10;

CREATE OR REPLACE TABLE second_iris
AS SELECT * FROM iris;

# test models

CREATE OR REPLACE TABLE enriched_iris AS (
    SELECT
        sepal_length, sepal_width, petal_length, petal_width,
        CASE
            WHEN species = 'setosa' THEN 0 ELSE CASE
            WHEN species = 'versicolor' THEN 1
            ELSE 2
        END END AS "species"
    FROM iris
)
CREATE OR REPLACE MODEL my_model WITH (
    model_class = 'xgboost.dask.DaskXGBClassifier',
    target_column = 'species',
    num_class = 3
) AS (
    SELECT * FROM enriched_iris
)

# experiments

CREATE EXPERIMENT my_exp WITH (
        model_class = 'sklearn.ensemble.GradientBoostingClassifier',
        experiment_class = 'dask_ml.model_selection.GridSearchCV',
        tune_parameters = (n_estimators = ARRAY [16, 32, 2],learning_rate = ARRAY [0.1,0.01,0.001],
                           max_depth = ARRAY [3,4,5,10]),
        target_column = 'species'
    ) AS (
            SELECT * FROM enriched_iris
        )

# functions
CREATE OR REPLACE TABLE enriched_iris AS (
    SELECT
        sepal_length, sepal_width, petal_length, petal_width,
        CASE
            WHEN species = 'setosa' THEN 0 ELSE CASE
            WHEN species = 'versicolor' THEN 1
            ELSE 2
        END END AS "species",
        IRIS_VOLUME(sepal_length, sepal_width) AS volume
    FROM iris
)

df = dd.read_csv(
    "s3://nyc-tlc/trip data/yellow_tripdata_2019-*.csv",
    parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    dtype={
        "payment_type": "UInt8",
        "VendorID": "UInt8",
        "passenger_count": "UInt8",
        "RatecodeID": "UInt8",
        "store_and_fwd_flag": "string",
        "PULocationID": "UInt16",
        "DOLocationID": "UInt16",
    },
    storage_options={"anon": True},
    blocksize="16 MiB",
).persist()


c.create_table("trip_data",df)

CREATE OR REPLACE TABLE trip_data
WITH (
    location = 's3://nyc-tlc/trip data/yellow_tripdata_2019-*.csv',
    format = 'csv',
    persist = True,
    parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    dtype={
        "payment_type": "UInt8",
        "VendorID": "UInt8",
        "passenger_count": "UInt8",
        "RatecodeID": "UInt8",
        "store_and_fwd_flag": "category",
        "PULocationID": "UInt16",
        "DOLocationID": "UInt16",
    },
    storage_options={"anon": True},
    blocksize="16 MiB",
);
