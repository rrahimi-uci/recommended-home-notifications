from test_rn_nl import *
import os
import time

env = os.environ['ENV']
activity = os.environ['ACTIVITY_ARN']


def lambda_handler(event, context):
    start_time = time.time()
    config_dict = setup(env)

    for func in [test_duplicate_listing_records(env, config_dict),
                 test_listing_date_listing_records(env, config_dict),
                 test_empty_rn_nl_listings_records(env, config_dict),
                 test_empty_rn_nl_users_records(env, config_dict),
                 test_empty_rn_nl_candidates_records(env, config_dict),
                 test_empty_rn_nl_candidates_ranked_records(env, config_dict),
                 test_user_ranking(env, config_dict),
                 test_candidates_ranked_listing_coverage(env, config_dict),
                 test_listings_count_current_previous_day(env, config_dict),
                 test_users_count_current_previous_day(env, config_dict)]:

        try:
            func(env, config_dict)
        except Exception:
            continue
