import subprocess
from datetime import datetime

import pandas as pd

from feast import FeatureStore


def main():
    # instantiate feature store instance
    store = FeatureStore(repo_path=".")

    # run feast apply in the terminal
    subprocess.run(["feast", "apply"])

    # define entity df for retrieval of historical features
    entity_df = pd.DataFrame(
            {
                "driver_id":[1001,1002,1003,1004,1005,1006], # TODO: try 1006 too - will just get nans
            "event_timestamp": [
                datetime(2023, 5, 22, 10, 59, 42),
                datetime(2023, 5, 22, 8, 12, 10),
                datetime(2023, 5, 22, 16, 40, 26),
                datetime(2023, 5, 22, 15, 1, 12),
                datetime.now(),
                datetime.now(),
            ]
            }
        )

    # retrieve historical features as though for training - with and without FeatureService
    store.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_conv_and_accel_rate_fv:conv_rate",
            "driver_conv_and_accel_rate_fv:acc_rate",
            "driver_conv_and_accel_rate_fv:avg_daily_trips",
        ]
    ).to_df()

    feature_service_offline = store.get_feature_service("driver_activity_features_service_offline")
    features = store.get_historical_features(features=feature_service_offline, entity_df=entity_df).to_df()

    # retrieve historical features as though for batch scoring - with and without FeatureService
    # this is just same as above but you'd pass current time...

    # materialize (make online store most up to date by getting the most up to date feature from offline store)
    subprocess.run(["feast","materialize-incremental", "2023-05-26T17:00:00"])

    # retrieve online features
    feature_service_online = store.get_feature_service("driver_activity_features_service_online")
    features = store.get_online_features(
        features=feature_service_online,
        entity_rows=[
            {
                'driver_id': '1001',
                'acc_rate_multiplier': 2,
                'conv_rate_multiplier': 3,
            },
            {
                'driver_id': '1002',
                'acc_rate_multiplier': 4,
                'conv_rate_multiplier': 5,
            },
        ]
    )

    # run feast teardown in the terminal
    subprocess.run(["feast", "teardown"])

    # ALSO:
    # - don't forget to do feast ui in the terminal to check out the UI
    # - you can run some curl requests to the endpoint too - I think! Run feast serve and then:
# curl -X POST \
#   "http://localhost:6566/get-online-features" \
#   -d '{
#     "features": [
#       "driver_conv_and_accel_rate_fv:conv_rate",
#       "driver_conv_and_accel_rate_fv:acc_rate",
#       "driver_conv_and_accel_rate_fv:avg_daily_trips"
#     ],
#     "entities": {
#       "driver_id": [1001, 1002, 1003]
#     }
#   }'


if __name__ == "__main__":
    main()
















# def run_demo():
#     store = FeatureStore(repo_path=".")
#     print("\n--- Run feast apply ---")
#     subprocess.run(["feast", "apply"])

#     print("\n--- Historical features for training ---")
#     fetch_historical_features_entity_df(store, for_batch_scoring=False)

#     print("\n--- Historical features for batch scoring ---")
#     fetch_historical_features_entity_df(store, for_batch_scoring=True)

#     print("\n--- Load features into online store ---")
#     store.materialize_incremental(end_date=datetime.now())

#     print("\n--- Online features ---")
#     fetch_online_features(store)

#     print("\n--- Online features retrieved (instead) through a feature service---")
#     fetch_online_features(store, source="feature_service")

#     print("\n--- Run feast teardown ---")
#     subprocess.run(["feast", "teardown"])


# def fetch_historical_features_entity_df(store: FeatureStore, for_batch_scoring: bool):
#     # Note: see https://docs.feast.dev/getting-started/concepts/feature-retrieval for more details on how to retrieve
#     # for all entities in the offline store instead
#     entity_df = pd.DataFrame.from_dict(
#         {
#             # entity's join key -> entity values
#             "driver_id": [1001, 1002, 1003],
#             # "event_timestamp" (reserved key) -> timestamps
#             "event_timestamp": [
#                 datetime(2021, 4, 12, 10, 59, 42),
#                 datetime(2021, 4, 12, 8, 12, 10),
#                 datetime(2021, 4, 12, 16, 40, 26),
#             ],
#             # (optional) label name -> label values. Feast does not process these
#             "label_driver_reported_satisfaction": [1, 5, 3],
#             # values we're using for an on-demand transformation
#             "val_to_add": [1, 2, 3],
#             "val_to_add_2": [10, 20, 30],
#         }
#     )
#     # For batch scoring, we want the latest timestamps
#     if for_batch_scoring:
#         entity_df["event_timestamp"] = pd.to_datetime("now", utc=True)

#     training_df = store.get_historical_features(
#         entity_df=entity_df,
#         features=[
#             "driver_hourly_stats:conv_rate",
#             "driver_hourly_stats:acc_rate",
#             "driver_hourly_stats:avg_daily_trips",
#             "transformed_conv_rate:conv_rate_plus_val1",
#             "transformed_conv_rate:conv_rate_plus_val2",
#         ],
#     ).to_df()
#     print(training_df.head())


# def fetch_online_features(store, source: str = ""):
#     entity_rows = [
#         # {join_key: entity_value}
#         {
#             "driver_id": 1001,
#             "val_to_add": 1000,
#             "val_to_add_2": 2000,
#         },
#         {
#             "driver_id": 1002,
#             "val_to_add": 1001,
#             "val_to_add_2": 2002,
#         },
#     ]
#     if source == "feature_service":
#         features_to_fetch = store.get_feature_service("driver_activity_v1")
#     elif source == "push":
#         features_to_fetch = store.get_feature_service("driver_activity_v3")
#     else:
#         features_to_fetch = [
#             "driver_hourly_stats:acc_rate",
#             "transformed_conv_rate:conv_rate_plus_val1",
#             "transformed_conv_rate:conv_rate_plus_val2",
#         ]
#     returned_features = store.get_online_features(
#         features=features_to_fetch,
#         entity_rows=entity_rows,
#     ).to_dict()
#     for key, value in sorted(returned_features.items()):
#         print(key, " : ", value)


# if __name__ == "__main__":
#     run_demo()
