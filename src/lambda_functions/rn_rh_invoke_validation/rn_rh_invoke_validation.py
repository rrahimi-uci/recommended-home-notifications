from test_rn_rh import *
import os
import time

env = os.environ['ENV']
activity = os.environ['ACTIVITY_ARN']


def lambda_handler(event, context):
    start_time = time.time()
    config_dict = setup(env)

    for func in [test_duplicate_listing_records(env, config_dict),
                 test_empty_rn_rh_users_records(env, config_dict),
                 test_empty_rn_rh_listings_records(env, config_dict),
                 test_empty_rn_rh_recommendations_records(env, config_dict),
                 test_empty_rn_rh_candidates_ranked_records(env, config_dict),
                 test_candidates_ranked_listing_seq(env, config_dict),
                 test_candidates_ranked_listing_coverage(env, config_dict),
                 test_recommendations_listing_seq(env, config_dict),
                 test_listings_count_current_previous_day(env, config_dict),
                 test_users_count_current_previous_day(env, config_dict)]:

        try:
            func(env, config_dict)
        except Exception:
            continue
