from athena_helper import  *
from query_util import *
from s3_helper import *
from validation_queries import *
from logger import *
import json
from collections import defaultdict, OrderedDict

log = get_logger('')
pidInfo = 'PID:{}:'.format(os.getpid())


def setup(env):
    log.info('{} Currently executing the setup '.format(pidInfo))
    config_dict = {}
    athena_instance = AWSAthenaHelper(env)
    rn_rh_config_instance = RN_RH(env)
    s3_instance = AWSS3Helper(env, 'parquet')

    all_keys = s3_instance.get_list_of_files('rdc-recommended-notifications-{}'.format(env),
                                             prefix='notifications/recommended_homes/rh_users/target_date={}'
                                             .format(rn_rh_config_instance.get_target_date()))
    log.info('{} Fetching data from date: {}'.format(pidInfo, rn_rh_config_instance.get_target_date()))
    if not all_keys:
        for days_delay in range(1, 14):
            new_date = rn_rh_config_instance.get_subtract_date(days_to_subtract=1)
            rn_rh_config_instance.set_target_date(new_date)
            all_keys = s3_instance.get_list_of_files('rdc-recommended-notifications-{}'.format(env),
                                                     prefix='notifications/recommended_homes/rh_users/target_date={}'
                                                     .format(rn_rh_config_instance.get_target_date()))
            log.info('{} No data in env: {} from date: {}, Fetching data from date: {}'
                     .format(pidInfo,env, new_date,rn_rh_config_instance.get_target_date()))
            if all_keys:
                break

    if not all_keys:
        return None

    log.debug("debug athena database name: {}".format(rn_rh_config_instance.get_db_athena()))

    current_date = rn_rh_config_instance.get_target_date()
    old_date =  rn_rh_config_instance.get_subtract_date(1)

    for days_delay in range(1,10):
        new_date = old_date
        rn_rh_config_instance.set_target_date(new_date)
        all_keys = s3_instance.get_list_of_files('rdc-recommended-notifications-{}'.format(env),
                                                 prefix='notifications/recommended_homes/rh_users/target_date={}'
                                                 .format(rn_rh_config_instance.get_target_date()))
        log.info('{} No data in env: {} from date: {}, Fetching data from date: {}'
                 .format(pidInfo, env, new_date,rn_rh_config_instance.get_target_date()))
        if all_keys:
            break
        old_date = rn_rh_config_instance.get_subtract_date(days_to_subtract=1)

    if not all_keys:
        return None

    listings_list_query_response_map = {}
    recommendations_list_query_response_map = {}
    candidates_list_query_response_map = {}

    users_user_query_response_map = {}
    recommendations_user_query_response_map = {}
    candidates_user_query_response_map = {}

    for date_now in [current_date, old_date]:
        rn_rh_config_instance.set_target_date(date_now)

        tmp_list_map_listings = QueryUtil.get_rn_rh_listings(athena_instance, rn_rh_config_instance
                                                        , count_rn_listings_fs_status=True)
        tmp_list_map_recommendations = QueryUtil.get_rn_rh_recommendations(athena_instance, rn_rh_config_instance
                                                             , count_rn_recommendations_fs_status=True)
        tmp_list_map_candidates = QueryUtil.get_rn_rh_candidates_ranked(athena_instance, rn_rh_config_instance
                                                                           , count_rn_candidates_ranked_fs_status=True)
        tmp_user_map_users = QueryUtil.get_rn_rh_users(athena_instance, rn_rh_config_instance
                                                     , count_user_id_rn_users=True)
        tmp_user_map_recommendations = QueryUtil.get_rn_rh_recommendations(athena_instance, rn_rh_config_instance
                                                       , count_user_id_rn_recommendations=True)
        tmp_user_map_candidates = QueryUtil.get_rn_rh_candidates_ranked(athena_instance, rn_rh_config_instance
                                                                           , count_user_id_rn_candidates_ranked=True)

        listings_list_query_response_map = {**listings_list_query_response_map,
                                            **tmp_list_map_listings}
        recommendations_list_query_response_map = {**recommendations_list_query_response_map,
                                                   **tmp_list_map_recommendations}
        candidates_list_query_response_map = {**candidates_list_query_response_map,
                                                   **tmp_list_map_candidates}
        users_user_query_response_map = {**users_user_query_response_map, **tmp_user_map_users}
        recommendations_user_query_response_map = {**recommendations_user_query_response_map,
                                                   **tmp_user_map_recommendations}
        candidates_user_query_response_map = {**candidates_user_query_response_map,
                                                   **tmp_user_map_candidates}

    rn_rh_config_instance.set_target_date(current_date)
    log.info('{} Currently executing setup with the target_date: {} '
             .format(pidInfo, rn_rh_config_instance.get_target_date()))

    curr_list_rh_listings_rows = athena_instance.parse_query \
        (listings_list_query_response_map[AthenaQueries.count_rn_listings_fs_status
         .replace('$output_table_name', rn_rh_config_instance.get_rn_listings_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])

    curr_list_rh_recommendations_rows = athena_instance.parse_query \
        (recommendations_list_query_response_map[AthenaQueries.count_rn_recommendations_fs_status
         .replace('$output_table_name', rn_rh_config_instance.get_rn_recommendations_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])

    curr_list_rh_candidates_rows = athena_instance.parse_query \
        (candidates_list_query_response_map[AthenaQueries.count_rn_candidates_ranked_fs_status
         .replace('$output_table_name', rn_rh_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])

    curr_user_rh_users_rows = athena_instance.parse_query \
        (users_user_query_response_map[AthenaQueries.count_user_id_rn_users
         .replace('$output_table_name', rn_rh_config_instance.get_rn_users_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])

    curr_user_rh_recommendations_rows = athena_instance.parse_query \
        (recommendations_user_query_response_map[AthenaQueries.count_user_id_rn_recommendations
         .replace('$output_table_name', rn_rh_config_instance.get_rn_recommendations_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])

    curr_user_rh_candidates_rows = athena_instance.parse_query \
        (candidates_user_query_response_map[AthenaQueries.count_user_id_rn_candidates_ranked
         .replace('$output_table_name', rn_rh_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])

    log.info('{} Done with executing setup with the target_date: {} '
             .format(pidInfo, rn_rh_config_instance.get_target_date()))

    rn_rh_config_instance.set_target_date(old_date)

    log.info('{} Currently executing setup with the previous date as target_date: {} '
             .format(pidInfo, rn_rh_config_instance.get_target_date()))

    prev_list_rh_listings_rows = athena_instance.parse_query \
        (listings_list_query_response_map[AthenaQueries.count_rn_listings_fs_status
         .replace('$output_table_name', rn_rh_config_instance.get_rn_listings_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])

    prev_list_rh_recommendations_rows = athena_instance.parse_query \
        (recommendations_list_query_response_map[AthenaQueries.count_rn_recommendations_fs_status
         .replace('$output_table_name', rn_rh_config_instance.get_rn_recommendations_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])

    prev_list_rh_candidates_rows = athena_instance.parse_query \
        (candidates_list_query_response_map[AthenaQueries.count_rn_candidates_ranked_fs_status
         .replace('$output_table_name', rn_rh_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])

    prev_user_rh_users_rows = athena_instance.parse_query \
        (users_user_query_response_map[AthenaQueries.count_user_id_rn_users
         .replace('$output_table_name', rn_rh_config_instance.get_rn_users_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])

    prev_user_rh_recommendations_rows = athena_instance.parse_query \
        (recommendations_user_query_response_map[AthenaQueries.count_user_id_rn_recommendations
         .replace('$output_table_name', rn_rh_config_instance.get_rn_recommendations_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])

    prev_user_rh_candidates_rows = athena_instance.parse_query \
        (candidates_user_query_response_map[AthenaQueries.count_user_id_rn_candidates_ranked
         .replace('$output_table_name', rn_rh_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])

    rn_rh_config_instance.set_target_date(current_date)

    config_dict['athena_instance'] = athena_instance
    config_dict['rn_rh_instance'] = rn_rh_config_instance
    config_dict['s3_instance'] = s3_instance

    config_dict['curr_list_listings_count'] = curr_list_rh_listings_rows
    config_dict['curr_list_recommendations_count'] = curr_list_rh_recommendations_rows
    config_dict['curr_list_candidates_count'] = curr_list_rh_candidates_rows
    config_dict['curr_user_users_count'] = curr_user_rh_users_rows
    config_dict['curr_user_recommendations_count'] = curr_user_rh_recommendations_rows
    config_dict['curr_user_candidates_count'] = curr_user_rh_candidates_rows

    config_dict['prev_list_listings_count'] = prev_list_rh_listings_rows
    config_dict['prev_list_recommendations_count'] = prev_list_rh_recommendations_rows
    config_dict['prev_list_candidates_count'] = prev_list_rh_candidates_rows
    config_dict['prev_user_users_count'] = prev_user_rh_users_rows
    config_dict['prev_user_recommendations_count'] = prev_user_rh_recommendations_rows
    config_dict['prev_user_candidates_count'] = prev_user_rh_candidates_rows

    return config_dict

def test_listings_count_current_previous_day(env, setup):
    '''
    Validate the number of listing_ids in
    the target_date wrt prev target_date for
    rn_rh_listings, rn_rh_recommendations, rn_rh_candidates_ranked
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the count of listing_ids wrt to previous target_date for rn_rh_listings, '
             'rn_rh_recommendations, rn_rh_candidates_ranked'
             .format(pidInfo, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no data present in current date and current date - 4")
    rn_rh_config_instance = setup['rn_rh_instance']
    s3_instance = setup['s3_instance']

    curr_list_listings_count = setup['curr_list_listings_count']
    curr_list_recommendations_count = setup['curr_list_recommendations_count']
    curr_list_candidates_count = setup['curr_list_candidates_count']

    prev_list_listings_count = setup['prev_list_listings_count']
    prev_list_recommendations_count = setup['prev_list_recommendations_count']
    prev_list_candidates_count = setup['prev_list_candidates_count']

    error_msg = []
    metrics_bucket = rn_rh_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_rh_config_instance.get_validation_metrics_key('listings_coverage_wrt_previousday')
    error_bucket = rn_rh_config_instance.get_validation_error_bucket()
    err_key = rn_rh_config_instance.get_validation_error_key('listings_coverage_wrt_previousday')

    curr_list_listings = int(curr_list_listings_count[0][0])
    curr_list_recommendations = int(curr_list_recommendations_count[0][0])
    curr_list_candidates = int(curr_list_candidates_count[0][0])

    prev_list_listings = int(prev_list_listings_count[0][0])
    prev_list_recommendations = int(prev_list_recommendations_count[0][0])
    prev_list_candidates = int(prev_list_candidates_count[0][0])


    perc_list_listings = abs(prev_list_listings - curr_list_listings) / \
                          float(prev_list_listings) * 100
    if perc_list_listings > 20:
        error_msg.append("Percentage diff of curr_list_listings: {} wrt prev_list_listings: {} "
                         " is {} which is greater than 10%"
                         .format(curr_list_listings, prev_list_listings,perc_list_listings ))

    perc_list_recommendations = abs( prev_list_recommendations - curr_list_recommendations) / \
                           float(prev_list_recommendations) * 100

    if perc_list_recommendations > 20:
        error_msg.append("Percentage diff of curr_list_recommendations: {} wrt prev_list_recommendations: {} "
                         " is {} which is greater than 10%"
                         .format(curr_list_recommendations, prev_list_recommendations,perc_list_recommendations ))

    perc_list_candidates = abs(prev_list_candidates - curr_list_candidates) / \
                                float(prev_list_recommendations) * 100

    if perc_list_candidates > 20:
        error_msg.append("Percentage diff of curr_list_candidates: {} wrt prev_list_candidates: {} "
                         " is {} which is greater than 10%"
                         .format(curr_list_candidates, prev_list_candidates, perc_list_candidates))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    validation_pass = 'Success: Validated the count of listing_ids for rn_listings, rn_rh_recommendations, ' \
                      'rn_rh_candidates_ranked' \
                      'wrt previous date which is within 10%'\
        .format(env)
    log.info('{} Success: Validated the count of listing_ids for rn_listings, rn_rh_recommendations'
             ',rn_rh_candidates_ranked ' \
                      'wrt previous date which is within 10%'
             .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_users_count_current_previous_day(env, setup):
    '''
    Validate the number of user_ids in
    the target_date wrt prev target_date for
    rn_rh_users, rn_rh_recommendations, rn_rh_candidates_ranked
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the count of user_ids wrt to previous target_date for rn_rh_users, '
             'rn_rh_recommendations, rn_rh_candidates_ranked'
             .format(pidInfo, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no data present in current date and current date - 4")
    rn_rh_config_instance = setup['rn_rh_instance']
    s3_instance = setup['s3_instance']

    curr_user_users_count = setup['curr_user_users_count']
    curr_user_recommendations_count = setup['curr_user_recommendations_count']
    curr_user_candidates_count = setup['curr_user_candidates_count']

    prev_user_users_count = setup['prev_user_users_count']
    prev_user_recommendations_count = setup['prev_user_recommendations_count']
    prev_user_candidates_count = setup['prev_user_candidates_count']

    error_msg = []
    metrics_bucket = rn_rh_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_rh_config_instance.get_validation_metrics_key('users_coverage_wrt_previousday')
    error_bucket = rn_rh_config_instance.get_validation_error_bucket()
    err_key = rn_rh_config_instance.get_validation_error_key('users_coverage_wrt_previousday')

    curr_user_users = int(curr_user_users_count[0][0])
    curr_user_recommendations = int(curr_user_recommendations_count[0][0])
    curr_user_candidates = int(curr_user_candidates_count[0][0])

    prev_user_users = int(prev_user_users_count[0][0])
    prev_user_recommendations = int(prev_user_recommendations_count[0][0])
    prev_user_candidates = int(prev_user_candidates_count[0][0])

    perc_user_users = abs(prev_user_users - curr_user_users) / \
                         float(prev_user_users) * 100
    if perc_user_users > 20:
        error_msg.append("Percentage diff of curr_user_users: {} wrt prev_user_users: {} "
                         " is {} which is greater than 10%"
                         .format(curr_user_users, prev_user_users, perc_user_users))

    perc_user_recommendations = abs (prev_user_recommendations - curr_user_recommendations) / \
                                float(prev_user_recommendations) * 100

    if perc_user_recommendations > 20:
        error_msg.append("Percentage diff of curr_user_recommendations: {} wrt prev_user_recommendations: {} "
                         " is {} which is greater than 10%"
                         .format(curr_user_recommendations, prev_user_recommendations, perc_user_recommendations))

    perc_user_candidates = abs(curr_user_candidates - prev_user_candidates) / \
                           float(prev_user_candidates) * 100

    if perc_user_candidates > 20:
        error_msg.append("Percentage diff of curr_user_candidates: {} wrt prev_user_candidates: {} "
                         " is {} which is greater than 10%"
                         .format(curr_user_candidates, prev_user_candidates, perc_user_candidates))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    validation_pass = 'Success: Validated the count of user_ids for rn_rh_users, rn_rh_recommendations, ' \
                      'rn_rh_candidates_ranked' \
                      'wrt previous date which is within 10%' \
        .format(env)
    log.info('{} Success: Validated the count of user_ids for rn_rh_users, rn_rh_recommendations'
             ',rn_rh_candidates_ranked ' \
             'wrt previous date which is within 10%'
             .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_duplicate_listing_records(env, setup):
    '''
    Validate the rn_rh_listings in
    rdc-recommended-notifications-env/notifications/recommended_homes/rh_listings
    duplicate listing_id
    duplicate listing_id and property_id combination is unique
    Validate the rn_rh_candidates_ranked in
    rdc-recommended-notifications-env/notifications/recommended_homes/rh_candidates_ranked
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the duplicate records in '
             'rdc-recommended-notifications-env/notifications/recommended_homes/listings/rh_listings and rh_candidates_ranked'
             .format(pidInfo, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    s3_instance = setup['s3_instance']
    rn_rh_config_instance = setup['rn_rh_instance']
    error_msg = []
    metrics_bucket = rn_rh_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_rh_config_instance.get_validation_metrics_key('duplicate_listing_id')
    error_bucket = rn_rh_config_instance.get_validation_error_bucket()
    err_key = rn_rh_config_instance.get_validation_error_key('duplicate_listing_id')

    unique_listing_id_query_map = QueryUtil.get_rn_rh_listings(athena_instance, rn_rh_config_instance
                                                                     , count_unique_listing_id=True)
    unique_listing_property_id_query_map = QueryUtil.get_rn_rh_listings(athena_instance, rn_rh_config_instance
                                                            , count_unique_listing_property_id=True)
    unique_listing_user_id_query_map = QueryUtil.get_rn_rh_candidates_ranked(athena_instance, rn_rh_config_instance
                                                                        , count_unique_listing_user_id=True)

    unique_listing_id_resp = unique_listing_id_query_map[AthenaQueries.count_unique_listing_id
        .replace('$output_table_name', rn_rh_config_instance.get_rn_listings_output())
        .replace("$target_date", rn_rh_config_instance.get_target_date())]
    unique_listing_property_id_resp = unique_listing_property_id_query_map[AthenaQueries.count_unique_listing_property_id
        .replace('$output_table_name', rn_rh_config_instance.get_rn_listings_output())
        .replace("$target_date", rn_rh_config_instance.get_target_date())]
    unique_listing_user_id_resp = unique_listing_user_id_query_map[AthenaQueries.count_unique_listing_user_id
            .replace('$output_table_name', rn_rh_config_instance.get_rn_candidates_ranked_output())
            .replace("$target_date", rn_rh_config_instance.get_target_date())]

    unique_listing_id_result = athena_instance.parse_query(unique_listing_id_resp)
    unique_listing_property_id_result = athena_instance.parse_query(unique_listing_property_id_resp)
    unique_listing_user_id_result = athena_instance.parse_query(unique_listing_user_id_resp)

    if len(unique_listing_id_result) != 0:
        error_msg.append("Duplicate listing_id is present in "
                         "rdc-recommended-notifications-{}/notifications/recommended_homes/rh_listings"
                         .format(env))
    if len(unique_listing_property_id_result) != 0:
        error_msg.append("Duplicate listing_id and property_id is present in "
                         "rdc-recommended-notifications-{}/notifications/recommended_homes/rh_listings"
                         .format(env))
    if len(unique_listing_user_id_result) != 0:
        error_msg.append("Duplicate listing_id and user_id is present in "
                         "rdc-recommended-notifications-{}/notifications/recommended_homes/rh_candidates_ranked"
                         .format(env))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the rdc-recommended-notifications-{}/notifications/recommended_homes/rh_listings' \
                      'and rh_candidates_ranked ' \
                      ' for duplicate listing_id and listing_id with property_id and user_id and listing_id combination'\
        .format(env)
    log.info('{} Success: Validated the rdc-recommended-notifications-{}/notifications/recommended_homes/rh_listings ' \
                      ' for duplicate listing_id and listing_id with property_id and user_id and listing_id combination'
             .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_empty_rn_rh_users_records(env, setup):
    '''
    Validate the empty records:
    user_id, score, overall_mean, price_ceiling, zipcode, overall_std_dev
    in rdc-recommended-notifications-env/notifications/recommended_homes/rn_rh_users
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the empty records in '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rn_rh_users'
             .format(pidInfo, env, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_rh_config_instance = setup['rn_rh_instance']
    s3_instance = setup['s3_instance']
    error_msg = []
    metrics_bucket = rn_rh_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_rh_config_instance.get_validation_metrics_key('empty_rn_rh_users_records')
    error_bucket = rn_rh_config_instance.get_validation_error_bucket()
    err_key = rn_rh_config_instance.get_validation_error_key('empty_rn_rh_users_records')

    curr_user_users_count = setup['curr_user_users_count']
    count_rn_rh_users = int(curr_user_users_count[0][0])


    empty_rn_rh_users_userid_query_map = QueryUtil.get_rn_rh_users(athena_instance, rn_rh_config_instance
                                                                  , count_empty_rn_rh_user_userid=True)
    empty_rn_rh_users_userid_resp = athena_instance.parse_query \
        (empty_rn_rh_users_userid_query_map[AthenaQueries.count_empty_rn_rh_user_userid
         .replace('$output_table_name', rn_rh_config_instance.get_rn_users_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_empty_rn_rh_users_userid = int(empty_rn_rh_users_userid_resp[0][0])

    empty_rn_rh_users_score_query_map = QueryUtil.get_rn_rh_users(athena_instance, rn_rh_config_instance
                                                         , count_empty_rn_rh_user_score=True)
    empty_rn_rh_users_score_resp = athena_instance.parse_query \
        (empty_rn_rh_users_score_query_map[AthenaQueries.count_empty_rn_rh_user_score
         .replace('$output_table_name', rn_rh_config_instance.get_rn_users_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_empty_rn_rh_users_score = int(empty_rn_rh_users_score_resp[0][0])

    empty_rn_rh_users_overallmean_query_map = QueryUtil.get_rn_rh_users(athena_instance, rn_rh_config_instance
                                                                  , count_empty_rn_rh_user_overallmean=True)
    empty_rn_rh_users_overallmean_resp = athena_instance.parse_query \
        (empty_rn_rh_users_overallmean_query_map[AthenaQueries.count_empty_rn_rh_user_overallmean
         .replace('$output_table_name', rn_rh_config_instance.get_rn_users_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_empty_rn_rh_users_overallmean = int(empty_rn_rh_users_overallmean_resp[0][0])

    empty_rn_rh_users_priceceiling_query_map = QueryUtil.get_rn_rh_users(athena_instance, rn_rh_config_instance
                                                                        , count_empty_rn_rh_user_priceceiling=True)
    empty_rn_rh_users_priceceiling_resp = athena_instance.parse_query \
        (empty_rn_rh_users_priceceiling_query_map[AthenaQueries.count_empty_rn_rh_user_priceceiling
         .replace('$output_table_name', rn_rh_config_instance.get_rn_users_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_empty_rn_rh_users_priceceiling = int(empty_rn_rh_users_priceceiling_resp[0][0])

    empty_rn_rh_users_zipcode_query_map = QueryUtil.get_rn_rh_users(athena_instance, rn_rh_config_instance
                                                                         , count_empty_rn_rh_user_zipcode=True)
    empty_rn_rh_users_zipcode_resp = athena_instance.parse_query \
        (empty_rn_rh_users_zipcode_query_map[AthenaQueries.count_empty_rn_rh_user_zipcode
         .replace('$output_table_name', rn_rh_config_instance.get_rn_users_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_empty_rn_rh_users_zipcode = int(empty_rn_rh_users_zipcode_resp[0][0])

    empty_rn_rh_users_overallstddev_query_map = QueryUtil.get_rn_rh_users(athena_instance, rn_rh_config_instance
                                                                    , count_empty_rn_rh_user_overallstddev=True)
    empty_rn_rh_users_overallstddev_resp = athena_instance.parse_query \
        (empty_rn_rh_users_overallstddev_query_map[AthenaQueries.count_empty_rn_rh_user_overallstddev
         .replace('$output_table_name', rn_rh_config_instance.get_rn_users_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_empty_rn_rh_users_overallstddev = int(empty_rn_rh_users_overallstddev_resp[0][0])

    value_rn_rh_users_overallstddev_query_map = QueryUtil.get_rn_rh_users(athena_instance, rn_rh_config_instance
                                                                          , count_value_rn_rh_user_overallstddev=True)
    value_rn_rh_users_overallstddev_resp = athena_instance.parse_query \
        (value_rn_rh_users_overallstddev_query_map[AthenaQueries.count_value_rn_rh_user_overallstddev
         .replace('$output_table_name', rn_rh_config_instance.get_rn_users_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_value_rn_rh_users_overallstddev = int(value_rn_rh_users_overallstddev_resp[0][0])

    if count_empty_rn_rh_users_userid > 0:
        error_msg.append('{} Empty user_ids {} present in '
                         'rdc-recommended-notifications-env/notifications/recommended_homes/rn_rh_users'
                         .format(count_empty_rn_rh_users_userid, env))

    perc_users_empty_score = 100 - (count_rn_rh_users - count_empty_rn_rh_users_score) / \
                               float(count_rn_rh_users) * 100
    if perc_users_empty_score > 15:
        error_msg.append("Percentage diff of user_ids in rn_rh_users: {} with empty score :{}  is : {} "
                         " which is greater than 10%".format(count_rn_rh_users, count_empty_rn_rh_users_score,
                                                             perc_users_empty_score))

    perc_users_empty_overallmean = 100 - (count_rn_rh_users - count_empty_rn_rh_users_overallmean) / \
                             float(count_rn_rh_users) * 100
    if perc_users_empty_overallmean > 10:
        error_msg.append("Percentage diff of user_ids in rn_rh_users: {} with empty overallmean: {}  is : {} "
                         " which is greater than 10%".format(count_rn_rh_users, count_empty_rn_rh_users_overallmean,
                                                             perc_users_empty_overallmean))

    perc_users_empty_priceceiling = 100 - (count_rn_rh_users - count_empty_rn_rh_users_priceceiling) / \
                                   float(count_rn_rh_users) * 100
    if perc_users_empty_priceceiling > 10:
        error_msg.append("Percentage diff of user_ids in rn_rh_users: {} with empty priceceiling: {} is : {} "
                         " which is greater than 10%".format(count_rn_rh_users, count_empty_rn_rh_users_priceceiling,
                                                             perc_users_empty_overallmean))

    perc_users_empty_zipcode = 100 - (count_rn_rh_users - count_empty_rn_rh_users_zipcode) / \
                                    float(count_rn_rh_users) * 100
    if perc_users_empty_zipcode > 15:
        error_msg.append("Percentage diff of user_ids in rn_rh_users: {} with empty zipcode: {} is : {} "
                         " which is greater than 10%".format(count_rn_rh_users, count_empty_rn_rh_users_zipcode,
                                                             perc_users_empty_zipcode))

    perc_users_empty_overallstddev = 100 - (count_rn_rh_users - count_empty_rn_rh_users_overallstddev) / \
                               float(count_rn_rh_users) * 100
    if perc_users_empty_overallstddev > 5:
        error_msg.append("Percentage diff of user_ids in rn_rh_users: {} with empty overallstddev: {} is : {} "
                         " which is greater than 10%".format(count_rn_rh_users, count_empty_rn_rh_users_overallstddev,
                                                             perc_users_empty_overallstddev))

    if count_value_rn_rh_users_overallstddev is not 0:
        error_msg.append("Number of user_ids in rn_rh_users with overallstddev less than 67 is: {}"
                         .format(count_value_rn_rh_users_overallstddev))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the empty records in ' \
                      'rdc-recommended-notifications-env/notifications/recommended_homes/rh_users, '.format(env)
    log.info('{} Success: Validated the empty records in '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rh_users '.format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_empty_rn_rh_listings_records(env, setup):
    '''
    Validate the empty records:
    listing_id, property_id, listing_status, listing_type, listing_city,
    listing_state, listing_postal_code
    in rdc-recommended-notifications-env/notifications/recommended_homes/rn_rh_listings
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the empty records in '
            'rdc-recommended-notifications-env/notifications/recommended_homes/rn_rh_listings'
            .format(pidInfo, env, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_rh_config_instance = setup['rn_rh_instance']
    s3_instance = setup['s3_instance']
    error_msg = []
    metrics_bucket = rn_rh_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_rh_config_instance.get_validation_metrics_key('empty_rn_rh_listings_records')
    error_bucket = rn_rh_config_instance.get_validation_error_bucket()
    err_key = rn_rh_config_instance.get_validation_error_key('empty_rn_rh_listings_records')

    curr_list_listings_count = setup['curr_list_listings_count']
    count_rn_rh_listings = int(curr_list_listings_count[0][0])

    empty_rn_rh_listings_listingid_query_map = QueryUtil.get_rn_rh_listings(athena_instance, rn_rh_config_instance
                                                               , count_empty_rn_rh_listing_listingid=True)
    empty_rn_rh_listings_listingid_resp = athena_instance.parse_query \
        (empty_rn_rh_listings_listingid_query_map[AthenaQueries.count_empty_rn_rh_listing_listingid
         .replace('$output_table_name', rn_rh_config_instance.get_rn_listings_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_empty_rn_rh_listings_listingid = int(empty_rn_rh_listings_listingid_resp[0][0])

    empty_rn_rh_listings_fields_query_map = QueryUtil.get_rn_rh_listings(athena_instance, rn_rh_config_instance
                                                                            , count_empty_rn_rh_listings_field=True)
    empty_rn_rh_listings_fields_resp = athena_instance.parse_query \
        (empty_rn_rh_listings_fields_query_map[AthenaQueries.count_empty_rn_rh_listings_field
         .replace('$output_table_name', rn_rh_config_instance.get_rn_listings_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_empty_rn_rh_listings_fields = int(empty_rn_rh_listings_fields_resp[0][0])

    if count_empty_rn_rh_listings_listingid > 0:
        error_msg.append('{} Empty listing_ids or property_ids or listing_status {} present in '
                         'rdc-recommended-notifications-env/notifications/recommended_homes/rn_rh_listings'
                         .format(count_empty_rn_rh_listings_listingid, env))

    perc_listings_empty_fields = 100 - (count_rn_rh_listings - count_empty_rn_rh_listings_fields) / \
                               float(count_rn_rh_listings) * 100
    if perc_listings_empty_fields > 10:
        error_msg.append("Percentage diff of listing_ids in rn_rh_listings: {} with empty fields: {} is : {} "
                         " which is greater than 10%".format(count_rn_rh_listings, count_empty_rn_rh_listings_fields,
                                                             perc_listings_empty_fields))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the empty records in ' \
                      'rdc-recommended-notifications-env/notifications/recommended_homes/rh_listings, '.format(env)
    log.info('{} Success: Validated the empty records in '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rh_listings '.format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_empty_rn_rh_recommendations_records(env, setup):
    '''
    Validate the empty records:
    user_id, row_id, listing_id, state
    in rdc-recommended-notifications-env/notifications/recommended_homes/rn_rh_recommendations
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the empty records in '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rn_rh_recommendations'
             .format(pidInfo, env, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_rh_config_instance = setup['rn_rh_instance']
    s3_instance = setup['s3_instance']
    error_msg = []
    metrics_bucket = rn_rh_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_rh_config_instance.get_validation_metrics_key('empty_rn_rh_recommendations_records')
    error_bucket = rn_rh_config_instance.get_validation_error_bucket()
    err_key = rn_rh_config_instance.get_validation_error_key('empty_rn_rh_recommendations_records')

    empty_rn_recommendations_query_map = QueryUtil.get_rn_rh_recommendations(athena_instance, rn_rh_config_instance
                                                                             , count_empty_rn_recommendations=True)
    empty_rn_recommendations_resp = athena_instance.parse_query \
        (empty_rn_recommendations_query_map[AthenaQueries.count_empty_rn_recommendations
         .replace('$output_table_name', rn_rh_config_instance.get_rn_recommendations_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_empty_rn_recommendations = len(empty_rn_recommendations_resp)

    if count_empty_rn_recommendations > 0:
        error_msg.append('{} Empty fields {} present in '
                         'rdc-recommended-notifications-env/notifications/recommended_homes/rn_rh_recommendations'
                         .format(count_empty_rn_recommendations, env))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the empty fields in ' \
                      'rdc-recommended-notifications-env/notifications/recommended_homes/rh_recommendations ' \
                      .format(env)
    log.info('{} Success: Validated the empty fields in '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rh_recommendations ' \
                      .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_empty_rn_rh_candidates_ranked_records(env, setup):
    '''
    Validate the empty records:
    listing_type, listing_id, listing_city, listing_postal_code, listing_status, seq_id
    in rdc-recommended-notifications-env/notifications/recommended_homes/rn_rh_candidates_ranked
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the empty records in '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rn_rh_candidates_ranked'
             .format(pidInfo, env, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_rh_config_instance = setup['rn_rh_instance']
    s3_instance = setup['s3_instance']
    error_msg = []
    metrics_bucket = rn_rh_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_rh_config_instance.get_validation_metrics_key('empty_rn_rh_candidates_ranked_records')
    error_bucket = rn_rh_config_instance.get_validation_error_bucket()
    err_key = rn_rh_config_instance.get_validation_error_key('empty_rn_rh_candidates_ranked_records')

    empty_rn_candidates_ranked_query_map = QueryUtil.get_rn_rh_candidates_ranked(athena_instance, rn_rh_config_instance
                                                                            ,count_empty_rn_candidates_ranked=True)
    empty_rn_candidates_ranked_resp = athena_instance.parse_query \
        (empty_rn_candidates_ranked_query_map[AthenaQueries.count_empty_rn_candidates_ranked
         .replace('$output_table_name', rn_rh_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_empty_rn_candidates_ranked = len(empty_rn_candidates_ranked_resp)
    if count_empty_rn_candidates_ranked > 0:
        error_msg.append('{} Empty fields {} present in '
                         'rdc-recommended-notifications-env/notifications/recommended_homes/rh_candidates_ranked'
                         .format(count_empty_rn_candidates_ranked, env))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the empty fields in ' \
                      'rdc-recommended-notifications-env/notifications/recommended_homes/rh_candidates_ranked, ' \
                      .format(env)
    log.info('{} Success: Validated the empty user_id in '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rh_candidates_ranked' \
             .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_candidates_ranked_listing_coverage(env, setup):
    '''
    Validate the empty records of in
    rdc-recommended-notifications-env/notifications/recommended_homes/rh_candidates_ranked
    with seq_id as 1
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the count of empty records in '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rh_candidates_ranked with seq_id as 1 '
             ''.format(pidInfo, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_rh_config_instance = setup['rn_rh_instance']
    s3_instance = setup['s3_instance']
    error_msg = []
    metrics_bucket = rn_rh_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_rh_config_instance.get_validation_metrics_key('empty_rn_rh_candidates_ranked_seq_1')
    error_bucket = rn_rh_config_instance.get_validation_error_bucket()
    err_key = rn_rh_config_instance.get_validation_error_key('empty_rn_rh_candidates_ranked_seq_1')

    curr_list_candidates_count = setup['curr_list_candidates_count']
    count_list_rn_candidates_ranked = int(curr_list_candidates_count[0][0])

    curr_user_candidates_count = setup['curr_user_candidates_count']
    count_user_rn_candidates_ranked = int(curr_user_candidates_count[0][0])

    count__seq1_user_rn_candidates_ranked_query_map = QueryUtil.get_rn_rh_candidates_ranked(athena_instance,
                                                                                      rn_rh_config_instance
                                                                             ,count_user_rn_candidates_ranked_seq_1=True)
    count_seq1_user_rn_candidates_ranked_resp = athena_instance.parse_query \
        (count__seq1_user_rn_candidates_ranked_query_map[AthenaQueries.count_user_rn_candidates_ranked_seq_1
         .replace('$output_table_name', rn_rh_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_seq1_user_rn_candidates_ranked = int(count_seq1_user_rn_candidates_ranked_resp[0][0])

    empty_fields_rn_candidates_ranked_query_map = QueryUtil.get_rn_rh_candidates_ranked(athena_instance,
                                                                                        rn_rh_config_instance,
                                                                        count_empty_fields_rn_candidates_ranked=True)
    empty_fields_rn_candidates_ranked_resp = athena_instance.parse_query \
        (empty_fields_rn_candidates_ranked_query_map[AthenaQueries.count_empty_fields_rn_candidates_ranked
         .replace('$output_table_name', rn_rh_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_empty_fields_rn_candidates_ranked = int(empty_fields_rn_candidates_ranked_resp[0][0])

    empty_fields_seq1_rn_candidates_ranked_query_map = QueryUtil.get_rn_rh_candidates_ranked(athena_instance,
                                                                rn_rh_config_instance,
                                                                count_empty_user_rn_candidates_ranked_seq_1=True)
    empty_fields_seq1_rn_candidates_ranked_resp = athena_instance.parse_query \
        (empty_fields_seq1_rn_candidates_ranked_query_map[AthenaQueries.count_empty_user_rn_candidates_ranked_seq_1
         .replace('$output_table_name', rn_rh_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    count_empty_fields_seq1_rn_candidates_ranked = int(empty_fields_seq1_rn_candidates_ranked_resp[0][0])
    if count_empty_fields_seq1_rn_candidates_ranked > 0:
        error_msg.append('{} Empty fields {} with seq_id as 1 present in '
                         'rdc-recommended-notifications-env/notifications/recommended_homes/rh_candidates_ranked'
                         .format(count_empty_fields_seq1_rn_candidates_ranked, env))

    perc_listings_empty_candidates_ranked = 100 - (count_list_rn_candidates_ranked - count_empty_fields_rn_candidates_ranked) / \
                                            float(count_list_rn_candidates_ranked) * 100

    if perc_listings_empty_candidates_ranked > 10:
        error_msg.append(
            "Percentage diff of listing_ids with empty fields in rh_candidates_ranked: {} wrt total listings"
            ": {} is {} which is greater than 10%"
            .format(count_empty_fields_rn_candidates_ranked, count_list_rn_candidates_ranked,
                    perc_listings_empty_candidates_ranked))

    if count_user_rn_candidates_ranked != count_seq1_user_rn_candidates_ranked:
        error_msg.append(
            "The count of user_id with seq_id as 1 in rh_candidates_ranked: {} wrt total distinct user_id"
            ": {} are not equal"
                .format(count_seq1_user_rn_candidates_ranked, count_user_rn_candidates_ranked))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the count of empty records in ' \
                      'rdc-recommended-notifications-env/notifications/recommended_homes/rh_candidates_ranked with seq_id as 1 ' \
                      .format(env)
    log.info('{} Success: Validated the count of empty records in '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rh_candidates_ranked with seq_id as 1' \
                      .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_recommendations_listing_seq(env, setup):
    '''
    Validate the sequence of listing_id for user_id in
    rdc-recommended-notifications-env/notifications/recommended_homes/rh_recommendations
    wrt recommended_homes in rdc_prod
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the sequence of listing_id for user_id in  '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rh_recommendations'
             .format(pidInfo, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_rh_config_instance = setup['rn_rh_instance']
    s3_instance = setup['s3_instance']
    metrics_bucket = rn_rh_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_rh_config_instance.get_validation_metrics_key('recommendation_sequence')
    error_bucket = rn_rh_config_instance.get_validation_error_bucket()
    err_key = rn_rh_config_instance.get_validation_error_key('recommendation_sequence')

    s3_helper_prod = AWSS3Helper('prod', 'json')
    prod_prefix = rn_rh_config_instance.get_recommended_homes_key_path()
    prod_bucket = rn_rh_config_instance.RECOMMENDED_HOMES_PROD_BUCKET
    log.info("the prod bucket is: {} and path is: {}".format(prod_bucket, prod_prefix))

    just_one_file = s3_helper_prod.get_list_of_files(prod_bucket, prod_prefix)[1]
    count = 0
    recommended_homes_map = defaultdict(list)
    error_msg = []
    for each_content in s3_helper_prod.get_key_content(prod_bucket, just_one_file):
        data_dict = json.loads(each_content)
        user_id = data_dict['user_id']
        rn_rh_config_instance.set_user_id(user_id)
        reco_listing_order_query_map = QueryUtil.get_rn_rh_recommendations(athena_instance, rn_rh_config_instance
                                                                , order_listing_id_seq_rn_recommendations=True)
        reco_listing_order_user_resp = athena_instance.parse_query \
            (reco_listing_order_query_map[AthenaQueries.order_listing_id_seq_rn_recommendations
             .replace('$output_table_name', rn_rh_config_instance.get_rn_recommendations_output())
             .replace('$target_date', rn_rh_config_instance.get_target_date())
             .replace('$user_id', user_id)])
        reco_seq_listing_id_list = [x[1] for x in reco_listing_order_user_resp]
        log.info("user_id: {} seq_listing_id_list : {}".format(user_id, reco_seq_listing_id_list))

        for each_recommended_objects in data_dict['recommendations']:
            listing_id = each_recommended_objects['listing_id']
            recommended_homes_map[user_id].append(listing_id)

        for actual, expected in zip(reco_seq_listing_id_list, recommended_homes_map[user_id]):
            if actual != expected:
                error_msg.append(" For the user_id: {} The actual id is {}, the expected id is {}"
                                 .format(user_id, actual, expected))

        count += 1
        if count > 3:
            break
    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the sequence of listing_id for  user_ids  in ' \
                      'rdc-recommended-notifications-env/notifications/recommended_homes/rh_recommendations ' \
                      .format(env)
    log.info('{} Success: Validated the sequence of listing_id for  user_ids  in '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rh_recommendations ' \
                      .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_candidates_ranked_listing_seq(env, setup):
    '''
    Validate the sequence of listing_id for user_id in
    rdc-recommended-notifications-env/notifications/recommended_homes/rh_candidates_ranked
    wrt rdc-recommended-notifications-env/notifications/recommended_homes/rh_recommendations
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the sequence of listing_id for user_id in  '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rh_candidates_ranked and rh_recommendations'
             .format(pidInfo, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_rh_config_instance = setup['rn_rh_instance']
    s3_instance = setup['s3_instance']
    error_msg = []
    metrics_bucket = rn_rh_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_rh_config_instance.get_validation_metrics_key('candidates_ranked_sequence')
    error_bucket = rn_rh_config_instance.get_validation_error_bucket()
    err_key = rn_rh_config_instance.get_validation_error_key('candidates_ranked_sequence')

    candidates_users_query_map = QueryUtil.get_rn_rh_candidates_ranked(athena_instance, rn_rh_config_instance
                                                         , user_rn_candidates_ranked=True)
    candidates_users_resp = athena_instance.parse_query \
        (candidates_users_query_map[AthenaQueries.user_rn_candidates_ranked
         .replace('$output_table_name', rn_rh_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_rh_config_instance.get_target_date())])
    candidates_user_id_resp_list = [x[0] for x in candidates_users_resp]

    log.debug("candidates_user_id_resp_list: {}".format(candidates_user_id_resp_list))

    for each_user_id in candidates_user_id_resp_list:
        rn_rh_config_instance.set_user_id(each_user_id)
        candidates_listing_order_query_map = QueryUtil.get_rn_rh_candidates_ranked(athena_instance, rn_rh_config_instance
                                                               , listing_id_seq_rn_candidates_ranked=True)
        candidates_listing_order_resp = athena_instance.parse_query \
            (candidates_listing_order_query_map[AthenaQueries.listing_id_seq_rn_candidates_ranked
             .replace('$output_table_name', rn_rh_config_instance.get_rn_candidates_ranked_output())
             .replace('$target_date', rn_rh_config_instance.get_target_date())
             .replace('$user_id', each_user_id)])
        candidates_seq_listing_id_list = [int(x[1]) for x in candidates_listing_order_resp]

        log.info("user_id: {} candidates_seq_listing_id_list: {}".format(each_user_id,candidates_seq_listing_id_list))


        recommendations_listing_order_query_map = QueryUtil.get_rn_rh_recommendations(athena_instance,
                                                                                   rn_rh_config_instance,
                                                                                   listing_id_seq_rn_recommendations=True)
        recommendations_listing_order_resp = athena_instance.parse_query \
            (recommendations_listing_order_query_map[AthenaQueries.listing_id_seq_rn_recommendations
             .replace('$output_table_name', rn_rh_config_instance.get_rn_recommendations_output())
             .replace('$target_date', rn_rh_config_instance.get_target_date())
             .replace('$user_id', each_user_id)])
        recommendations_seq_listing_id_list = [int(x[1]) for x in recommendations_listing_order_resp]

        log.info("user_id: {} recommendations_seq_listing_id_list: {}"
              .format(each_user_id, recommendations_seq_listing_id_list))

        recommendations_seq_listing_id_map = OrderedDict()
        for v in recommendations_seq_listing_id_list:
            recommendations_seq_listing_id_map[v] = 1

        candidates_seq_listing_id_map = OrderedDict()
        for v in candidates_seq_listing_id_list:
            if v in recommendations_seq_listing_id_map:
                candidates_seq_listing_id_map[v] = 1

        for k,v in zip(recommendations_seq_listing_id_map.keys(), candidates_seq_listing_id_map.keys()):
            if recommendations_seq_listing_id_map[k] != candidates_seq_listing_id_map[v]:
                error_msg.append("both are not in order, rn_rh_recommendations : {} and rh_rh_candidates_ranked : {}"
                                 .format(recommendations_seq_listing_id_list, candidates_seq_listing_id_list))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the sequence of listing_id for  user_ids  in ' \
                      'rdc-recommended-notifications-env/notifications/recommended_homes/rh_recommendations and ' \
                      'rh_candidates_ranked ' \
        .format(env)
    log.info('{} Success: Validated the sequence of listing_id for  user_ids  in '
             'rdc-recommended-notifications-env/notifications/recommended_homes/rh_recommendations and '
             'rh_candidates_ranked ' \
             .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))
