# This is an example feature definition file

from datetime import timedelta

import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    RequestSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int32, Int64, String


from feast.value_type import ValueType

# Entity
driver_entity = Entity(name = "driver", join_keys=["driver_id"], value_type=ValueType.INT64)

# FileSource
raw_driver_data = FileSource(
    path="./data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# FeatureView
driver_conv_and_accel_rate_fv = FeatureView(
    name="driver_conv_and_accel_rate_fv",
    source=raw_driver_data,
    schema=[
        Field(
            name="driver_id",
            dtype=Int64,
        ),
        Field(
            name="conv_rate",
            dtype=Float32,
        ),
        Field(
            name="acc_rate",
            dtype=Float32,
        ),
        Field(
            name="avg_daily_trips",
            dtype=Int32,
        ),
    ],
    entities=[driver_entity],
    ttl=timedelta(days=1),
    online=True,
)

# RequestSource
trip_multiplier_on_request_source = RequestSource(
    name="trip_multiplier_on_request_source",
    schema=[
        Field(
            name="acc_rate_multiplier",
            dtype=Float64,
        ),
        Field(
            name="conv_rate_multiplier",
            dtype=Float64,
        ),
    ],
)

# on demand FeatureView
@on_demand_feature_view(
   sources=[
       driver_conv_and_accel_rate_fv,
       trip_multiplier_on_request_source
   ],
    schema=[
        Field(
            name="acc_rate_multiplied",
            dtype=Float64,
        ),
        Field(
            name="conv_rate_multiplied",
            dtype=Float64,
        ),
    ],
)
def transformed_conv_rate_and_acc_rate_ofv(input_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df['acc_rate_multiplied'] = (input_df['acc_rate'] * input_df['acc_rate_multiplier'])
    df['conv_rate_multiplied'] = (input_df['conv_rate'] * input_df['conv_rate_multiplier'])
    return df


# FeatureService
driver_features_service_offline = FeatureService(
    name="driver_activity_features_service_offline",
    features=[driver_conv_and_accel_rate_fv]
)

driver_features_service_online = FeatureService(
    name="driver_activity_features_service_online",
    features=[driver_conv_and_accel_rate_fv, transformed_conv_rate_and_acc_rate_ofv[["acc_rate_multiplied"]]]
)








# # Define an entity for the driver. You can think of an entity as a primary key used to
# # fetch features.
# driver = Entity(name="driver", join_keys=["driver_id"])

# # Read data from parquet files. Parquet is convenient for local development mode. For
# # production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# # for more info.
# driver_stats_source = FileSource(
#     name="driver_hourly_stats_source",
#     path="/Users/crispin.logan/Documents/courses/feast-getting-started/my_project/feature_repo/data/driver_stats.parquet",
#     timestamp_field="event_timestamp",
#     created_timestamp_column="created",
# )

# # Our parquet files contain sample data that includes a driver_id column, timestamps and
# # three feature column. Here we define a Feature View that will allow us to serve this
# # data to our model online.
# driver_stats_fv = FeatureView(
#     # The unique name of this feature view. Two feature views in a single
#     # project cannot have the same name
#     name="driver_hourly_stats",
#     entities=[driver],
#     ttl=timedelta(days=1),
#     # The list of features defined below act as a schema to both define features
#     # for both materialization of features into a store, and are used as references
#     # during retrieval for building a training dataset or serving features
#     schema=[
#         Field(name="conv_rate", dtype=Float32),
#         Field(name="acc_rate", dtype=Float32),
#         Field(name="avg_daily_trips", dtype=Int64, description="Average daily trips"),
#     ],
#     online=True,
#     source=driver_stats_source,
#     # Tags are user defined key/value pairs that are attached to each
#     # feature view
#     tags={"team": "driver_performance"},
# )

# # Define a request data source which encodes features / information only
# # available at request time (e.g. part of the user initiated HTTP request)
# input_request = RequestSource(
#     name="vals_to_add",
#     schema=[
#         Field(name="val_to_add", dtype=Int64),
#         Field(name="val_to_add_2", dtype=Int64),
#     ],
# )


# # Define an on demand feature view which can generate new features based on
# # existing feature views and RequestSource features
# @on_demand_feature_view(
#     sources=[driver_stats_fv, input_request],
#     schema=[
#         Field(name="conv_rate_plus_val1", dtype=Float64),
#         Field(name="conv_rate_plus_val2", dtype=Float64),
#     ],
# )
# def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
#     df = pd.DataFrame()
#     df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
#     df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]
#     return df


# # This groups features into a model version
# driver_activity_v1 = FeatureService(
#     name="driver_activity_v1",
#     features=[
#         driver_stats_fv[["conv_rate"]],  # Sub-selects a feature from a feature view
#         transformed_conv_rate,  # Selects all features from the feature view
#     ],
# )
# driver_activity_v2 = FeatureService(
#     name="driver_activity_v2", features=[driver_stats_fv, transformed_conv_rate]
# )
