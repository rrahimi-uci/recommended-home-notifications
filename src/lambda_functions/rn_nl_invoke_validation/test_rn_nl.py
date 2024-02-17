from athena_helper import  *
from query_util import *
from s3_helper import *
from validation_queries import *
from datetime import datetime
from logger import *

log = get_logger('')
pidInfo = 'PID:{}:'.format(os.getpid())


def setup(env):
    log.info('{} Currently executing the setup '.format(pidInfo))
    config_dict = {}
    athena_instance = AWSAthenaHelper(env)
    rn_nl_config_instance = RN_NL(env)
    s3_instance = AWSS3Helper(env)

    all_keys = s3_instance.get_list_of_files('rdc-recommended-notifications-{}'.format(env),
                                             prefix='notifications/new_listings/nl_users/target_date={}'
                                             .format(rn_nl_config_instance.get_target_date()))
    log.info('{} Fetching data from date: {}'.format(pidInfo, rn_nl_config_instance.get_target_date()))
    old_date = rn_nl_config_instance.get_target_date()
    if not all_keys:
        for days_delay in range(1, 60): #changed to 6 ideally it should be 4
            new_date = rn_nl_config_instance.get_subtract_date(days_to_subtract=1)
            rn_nl_config_instance.set_target_date(new_date)
            all_keys = s3_instance.get_list_of_files('rdc-recommended-notifications-{}'.format(env),
                                                     prefix='notifications/new_listings/nl_users/target_date={}'
                                                     .format(rn_nl_config_instance.get_target_date()))
            log.info('{} No data from date: {}, Fetching data from date: {}'
                     .format(pidInfo, old_date,
                             rn_nl_config_instance.get_target_date()))
            if all_keys:
                break

    if not all_keys:
        return None

    log.debug("debug athena database name: {}".format(rn_nl_config_instance.get_db_athena()))

    current_date = rn_nl_config_instance.get_target_date()
    old_date =  rn_nl_config_instance.get_subtract_date(1)

    for days_delay in range(1, 10):
        new_date = old_date
        rn_nl_config_instance.set_target_date(new_date)
        all_keys = s3_instance.get_list_of_files('rdc-recommended-notifications-{}'.format(env),
                                                 prefix='notifications/new_listings/nl_users/target_date={}'
                                                 .format(rn_nl_config_instance.get_target_date()))
        log.info('{} No data in env: {} from date: {}, Fetching data from date: {}'
                 .format(pidInfo, env, new_date, rn_nl_config_instance.get_target_date()))
        if all_keys:
            break
        old_date = rn_nl_config_instance.get_subtract_date(days_to_subtract=1)

    if not all_keys:
        return None

    listings_list_query_response_map = {}
    candidates_list_query_response_map = {}
    users_user_query_response_map = {}
    candidates_user_query_response_map = {}

    for date_now in [current_date, old_date]:
        rn_nl_config_instance.set_target_date(date_now)
        tmp_list_map_listings = QueryUtil.get_rn_nl_listings(athena_instance, rn_nl_config_instance
                                                        , count_rn_listings_fs_status=True)
        tmp_user_map_users = QueryUtil.get_rn_nl_users(athena_instance, rn_nl_config_instance
                                                       , count_user_id_rn_users=True)
        tmp_list_map_candidates = QueryUtil.get_rn_nl_candidates(athena_instance, rn_nl_config_instance
                                                                        , count_rn_candidates_fs_status=True)
        tmp_user_map_candidates = QueryUtil.get_rn_nl_candidates(athena_instance, rn_nl_config_instance
                                                                 , count_user_id_rn_candidates=True)
        listings_list_query_response_map = {**listings_list_query_response_map,
                                            **tmp_list_map_listings}
        users_user_query_response_map = {**users_user_query_response_map, **tmp_user_map_users}
        candidates_list_query_response_map = {**candidates_list_query_response_map,
                                              **tmp_list_map_candidates}
        candidates_user_query_response_map = {**candidates_user_query_response_map,
                                              **tmp_user_map_candidates}

    rn_nl_config_instance.set_target_date(current_date)
    log.info('{} Currently executing setup with the target_date: {} '
             .format(pidInfo, rn_nl_config_instance.get_target_date()))

    curr_list_nl_listings_rows = athena_instance.parse_query \
        (listings_list_query_response_map[AthenaQueries.count_rn_listings_fs_status
         .replace('$output_table_name', rn_nl_config_instance.get_rn_listings_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])

    curr_user_nl_users_rows = athena_instance.parse_query \
        (users_user_query_response_map[AthenaQueries.count_user_id_rn_users
         .replace('$output_table_name', rn_nl_config_instance.get_rn_users_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])

    curr_list_nl_candidates_rows = athena_instance.parse_query \
        (candidates_list_query_response_map[AthenaQueries.count_rn_candidates_fs_status
         .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])

    curr_user_nl_candidates_rows = athena_instance.parse_query \
        (candidates_user_query_response_map[AthenaQueries.count_user_id_rn_candidates
         .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])

    log.info('{} Done with executing setup with the target_date: {} '
             .format(pidInfo, rn_nl_config_instance.get_target_date()))

    rn_nl_config_instance.set_target_date(old_date)

    log.info('{} Currently executing setup with the previous date as target_date: {} '
             .format(pidInfo, rn_nl_config_instance.get_target_date()))

    prev_list_nl_listings_rows = athena_instance.parse_query \
        (listings_list_query_response_map[AthenaQueries.count_rn_listings_fs_status
         .replace('$output_table_name', rn_nl_config_instance.get_rn_listings_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])

    prev_user_nl_users_rows = athena_instance.parse_query \
        (users_user_query_response_map[AthenaQueries.count_user_id_rn_users
         .replace('$output_table_name', rn_nl_config_instance.get_rn_users_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])

    prev_list_nl_candidates_rows = athena_instance.parse_query \
        (candidates_list_query_response_map[AthenaQueries.count_rn_candidates_fs_status
         .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])

    prev_user_nl_candidates_rows = athena_instance.parse_query \
        (candidates_user_query_response_map[AthenaQueries.count_user_id_rn_candidates
         .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])


    rn_nl_config_instance.set_target_date(current_date)

    log.info('{} Done with executing setup with the target_date: {} '
             .format(pidInfo, rn_nl_config_instance.get_target_date()))

    config_dict['athena_instance'] = athena_instance
    config_dict['rn_nl_instance'] = rn_nl_config_instance
    config_dict['s3_instance'] = s3_instance

    config_dict['curr_list_listings_count'] = curr_list_nl_listings_rows
    config_dict['curr_list_candidates_count'] = curr_list_nl_candidates_rows
    config_dict['curr_user_users_count'] = curr_user_nl_users_rows
    config_dict['curr_user_candidates_count'] = curr_user_nl_candidates_rows

    config_dict['prev_list_listings_count'] = prev_list_nl_listings_rows
    config_dict['prev_list_candidates_count'] = prev_list_nl_candidates_rows
    config_dict['prev_user_users_count'] = prev_user_nl_users_rows
    config_dict['prev_user_candidates_count'] = prev_user_nl_candidates_rows

    return config_dict

def test_listings_count_current_previous_day(env, setup):
    '''
    Validate the number of listing_ids in
    the target_date wrt target_date -1 for
    rn_nl_listings and rn_nl_candidates
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the count of listing_ids wrt to previous target_date for rn_nl_listings, '
             'rn_nl_candidates'
             .format(pidInfo, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no data present in current date and current date - 4")
    rn_nl_config_instance = setup['rn_nl_instance']
    s3_instance = setup['s3_instance']

    curr_list_listings_count = setup['curr_list_listings_count']
    curr_list_candidates_count = setup['curr_list_candidates_count']

    prev_list_listings_count = setup['prev_list_listings_count']
    prev_list_candidates_count = setup['prev_list_candidates_count']

    error_msg = []
    metrics_bucket = rn_nl_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_nl_config_instance.get_validation_metrics_key('listings_coverage_wrt_previousday')
    error_bucket = rn_nl_config_instance.get_validation_error_bucket()
    err_key = rn_nl_config_instance.get_validation_error_key('listings_coverage_wrt_previousday')

    curr_list_listings = int(curr_list_listings_count[0][0])
    curr_list_candidates = int(curr_list_candidates_count[0][0])

    prev_list_listings = int(prev_list_listings_count[0][0])
    prev_list_candidates = int(prev_list_candidates_count[0][0])


    perc_list_listings = abs(prev_list_listings - curr_list_listings) / \
                          float(prev_list_listings) * 100
    if perc_list_listings > 20:
        error_msg.append("Percentage diff of curr_list_listings: {} wrt prev_list_listings: {} "
                         " is {} which is greater than 10%"
                         .format(curr_list_listings, prev_list_listings,perc_list_listings ))

    perc_list_candidates = abs(prev_list_candidates - curr_list_candidates) / \
                                float(prev_list_candidates) * 100

    if perc_list_candidates > 20:
        error_msg.append("Percentage diff of curr_list_candidates: {} wrt prev_list_candidates: {} "
                         " is {} which is greater than 10%"
                         .format(curr_list_candidates, prev_list_candidates, perc_list_candidates))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    validation_pass = 'Success: Validated the count of listing_ids for rn_nl_listings, ' \
                      'rn_nl_candidates wrt previous date which is within 10%'.format(env)
    log.info('{} Success: Validated the count of listing_ids for rn_nl_listings, rn_nl_candidates'
                      'wrt previous date which is within 10%'.format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_users_count_current_previous_day(env, setup):
    '''
    Validate the number of user_ids in
    the target_date wrt target_date -1 for
    rn_nl_users, rn_nl_candidates
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the count of user_ids wrt to previous target_date for rn_nl_users, '
             'rn_nl_candidates'
             .format(pidInfo, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no data present in current date and current date - 4")
    rn_nl_config_instance = setup['rn_nl_instance']
    s3_instance = setup['s3_instance']

    curr_user_users_count = setup['curr_user_users_count']
    curr_user_candidates_count = setup['curr_user_candidates_count']

    prev_user_users_count = setup['prev_user_users_count']
    prev_user_candidates_count = setup['prev_user_candidates_count']

    error_msg = []
    metrics_bucket = rn_nl_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_nl_config_instance.get_validation_metrics_key('users_coverage_wrt_previousday')
    error_bucket = rn_nl_config_instance.get_validation_error_bucket()
    err_key = rn_nl_config_instance.get_validation_error_key('users_coverage_wrt_previousday')

    curr_user_users = int(curr_user_users_count[0][0])
    curr_user_candidates = int(curr_user_candidates_count[0][0])

    prev_user_users = int(prev_user_users_count[0][0])
    prev_user_candidates = int(prev_user_candidates_count[0][0])

    perc_user_users = abs(prev_user_users - curr_user_users) / \
                         float(prev_user_users) * 100
    if perc_user_users > 20:
        error_msg.append("Percentage diff of curr_user_users: {} wrt prev_user_users: {} "
                         " is {} which is greater than 10%"
                         .format(curr_user_users, prev_user_users, perc_user_users))

    perc_user_candidates = abs(curr_user_candidates - prev_user_candidates) / \
                           float(prev_user_candidates) * 100

    if perc_user_candidates > 20:
        error_msg.append("Percentage diff of curr_user_candidates: {} wrt prev_user_candidates: {} "
                         " is {} which is greater than 10%"
                         .format(curr_user_candidates, prev_user_candidates, perc_user_candidates))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    validation_pass = 'Success: Validated the count of user_ids for rn_nl_users, ' \
                      'rn_nl_candidates wrt previous date which is within 10%'.format(env)
    log.info('{} Success: Validated the count of user_ids for rn_nl_users, rn_nl_candidates_ranked'
             'wrt previous date which is within 10%' .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_duplicate_listing_records(env, setup):
    '''
    Validate the rn_nl_listings in
    rdc-recommended-notifications-env/notifications/new_listings/
    duplicate listing_id
    duplicate listing_id and property_id combination is unique
    Validate the rn_nl_candidates and rn_nl_candidates_ranked in
    rdc-recommended-notifications-env/notifications/new_listings/
    duplicate user_id and listing_id combination is unique
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the duplicate records in '
             'rdc-recommended-notifications-env/notifications/new_listings/listings/nl_listings'
             .format(pidInfo, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    s3_instance = setup['s3_instance']
    rn_nl_config_instance = setup['rn_nl_instance']
    error_msg = []
    metrics_bucket = rn_nl_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_nl_config_instance.get_validation_metrics_key('duplicate_listing_id')
    error_bucket = rn_nl_config_instance.get_validation_error_bucket()
    err_key = rn_nl_config_instance.get_validation_error_key('duplicate_listing_id')

    unique_listing_id_query_map = QueryUtil.get_rn_nl_listings(athena_instance, rn_nl_config_instance
                                                                     , count_unique_listing_id=True)
    unique_listing_property_id_query_map = QueryUtil.get_rn_nl_listings(athena_instance, rn_nl_config_instance
                                                            , count_unique_listing_property_id=True)
    unique_user_listing_id_cand_query_map = QueryUtil.get_rn_nl_candidates(athena_instance, rn_nl_config_instance,
                                                                      count_unique_user_listing_id_cand=True )
    unique_user_listing_id_cand_ranked_query_map = QueryUtil.get_rn_nl_candidates_ranked(athena_instance, rn_nl_config_instance,
                                                                           count_unique_user_listing_id_cand_ranked=True)


    unique_listing_id_resp = unique_listing_id_query_map[AthenaQueries.count_unique_listing_id
        .replace('$output_table_name', rn_nl_config_instance.get_rn_listings_output())
        .replace("$target_date", rn_nl_config_instance.get_target_date())]
    unique_listing_property_id_resp = unique_listing_property_id_query_map[AthenaQueries.count_unique_listing_property_id
        .replace('$output_table_name', rn_nl_config_instance.get_rn_listings_output())
        .replace("$target_date", rn_nl_config_instance.get_target_date())]
    unique_user_listing_id_cand_resp = unique_user_listing_id_cand_query_map[AthenaQueries.count_unique_user_listing_id_cand
            .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_output())
            .replace("$target_date", rn_nl_config_instance.get_target_date())]
    unique_user_listing_id_cand_ranked_resp = unique_user_listing_id_cand_ranked_query_map[AthenaQueries.count_unique_user_listing_id_cand_ranked
        .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_ranked_output())
        .replace("$target_date", rn_nl_config_instance.get_target_date())]

    unique_listing_id_result = athena_instance.parse_query(unique_listing_id_resp)
    unique_listing_property_id_result = athena_instance.parse_query(unique_listing_property_id_resp)
    unique_user_listing_id_cand_result = athena_instance.parse_query(unique_user_listing_id_cand_resp)
    unique_user_listing_id_cand_ranked_result = athena_instance.parse_query(unique_user_listing_id_cand_ranked_resp)

    if len(unique_listing_id_result) != 0:
        error_msg.append("Duplicate listing_id is present in "
                         "rdc-recommended-notifications-{}/notifications/new_listings/nl_listings"
                         .format(env))
    if len(unique_listing_property_id_result) != 0:
        error_msg.append("Duplicate listing_id and property_id is present in "
                         "rdc-recommended-notifications-{}/notifications/new_listings/nl_listings"
                         .format(env))
    if len(unique_user_listing_id_cand_result) != 0:
        error_msg.append("Duplicate listing_id and user_id is present in "
                         "rdc-recommended-notifications-{}/notifications/new_listings/nl_candidates"
                         .format(env))
    if len(unique_user_listing_id_cand_ranked_result) != 0:
        error_msg.append("Duplicate listing_id and user_id is present in "
                         "rdc-recommended-notifications-{}/notifications/new_listings/nl_candidates_ranked"
                         .format(env))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the rdc-recommended-notifications-{}/notifications/new_listings/nl_listings' \
                      'nl_candidates and nl_candidates_ranked ' \
                      ' for duplicate listing_id and listing_id with property_id and user_id with listing_id'.format(env)
    log.info('{} Success: Validated the rdc-recommended-notifications-{}/notifications/new_listings/nl_listings'
             'nl_candidates and nl_candidates_ranked ' \
                      ' for duplicate listing_id and listing_id with property_id and user_id and listing_id'.format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_listing_date_listing_records(env, setup):
    '''
    Validate the rn_nl_listings in
    rdc-recommended-notifications-env/notifications/new_listings/listings
    listing_start_datetime_mst and run_date_yyyymmdd is less than 5
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the listing_date records in '
             'rdc-recommended-notifications-env/notifications/new_listings/nl_listings'
             .format(pidInfo, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    s3_instance = setup['s3_instance']
    rn_nl_config_instance = setup['rn_nl_instance']
    error_msg = []
    metrics_bucket = rn_nl_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_nl_config_instance.get_validation_metrics_key('listing_start_datetime_listing_id')
    error_bucket = rn_nl_config_instance.get_validation_error_bucket()
    err_key = rn_nl_config_instance.get_validation_error_key('listing_start_datetime_listing_id')

    date_listing_id_query_map = QueryUtil.get_rn_nl_listings(athena_instance, rn_nl_config_instance
                                                            , list_listing_id_start_run_date=True)
    date_listing_id_resp = date_listing_id_query_map[AthenaQueries.list_listing_id_start_run_date
        .replace('$output_table_name', rn_nl_config_instance.get_rn_listings_output())
        .replace("$target_date", rn_nl_config_instance.get_target_date())]
    date_listing_id_result = athena_instance.parse_query(date_listing_id_resp)
    print("debug date_listing_id_result")
    for values in date_listing_id_result:
        listing_id, start_date = values;
        start_date_object = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date_object = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        if end_date_object.day - start_date_object.day > 5:
            error_msg.append('listing_id: {} is having the difference between run_date: {} and '
                             'start_date: {} greater than 5'.format(listing_id, end_date_object,start_date_object))
    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the listing_date records in ' \
                      'rdc-recommended-notifications-env/notifications/new_listings/nl_listings, '.format(env)
    log.info('{} Success: Validated the listing_date records in '
             'rdc-recommended-notifications-env/notifications/new_listings/nl_listings '.format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_empty_rn_nl_listings_records(env, setup):
    '''
    Validate the empty records:
    listing_id, property_id, listing_status, listing_type, listing_city,
    listing_state, listing_postal_code
    in rdc-recommended-notifications-env/notifications/new_listings/rn_nl_listings
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the empty records in '
            'rdc-recommended-notifications-env/notifications/new_listings/nl_listings'
            .format(pidInfo, env, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_nl_config_instance = setup['rn_nl_instance']
    s3_instance = setup['s3_instance']
    error_msg = []
    metrics_bucket = rn_nl_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_nl_config_instance.get_validation_metrics_key('empty_rn_nl_listings_records')
    error_bucket = rn_nl_config_instance.get_validation_error_bucket()
    err_key = rn_nl_config_instance.get_validation_error_key('empty_rn_nl_listings_records')

    curr_list_listings_count = setup['curr_list_listings_count']
    count_rn_nl_listings = int(curr_list_listings_count[0][0])


    empty_rn_nl_listings_listingid_query_map = QueryUtil.get_rn_nl_listings(athena_instance, rn_nl_config_instance
                                                               , count_empty_rn_nl_listing_listingid=True)
    empty_rn_nl_listings_listingid_resp = athena_instance.parse_query \
        (empty_rn_nl_listings_listingid_query_map[AthenaQueries.count_empty_rn_nl_listing_listingid
         .replace('$output_table_name', rn_nl_config_instance.get_rn_listings_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_empty_rn_nl_listings_listingid = int(empty_rn_nl_listings_listingid_resp[0][0])

    empty_rn_nl_listings_fields_query_map = QueryUtil.get_rn_nl_listings(athena_instance, rn_nl_config_instance
                                                                            , count_empty_rn_nl_listings_field=True)
    empty_rn_nl_listings_fields_resp = athena_instance.parse_query \
        (empty_rn_nl_listings_fields_query_map[AthenaQueries.count_empty_rn_nl_listings_field
         .replace('$output_table_name', rn_nl_config_instance.get_rn_listings_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_empty_rn_nl_listings_fields = int(empty_rn_nl_listings_fields_resp[0][0])

    if count_empty_rn_nl_listings_listingid > 0:
        error_msg.append('{} Empty listing_ids or property_ids or listing_status {} present in '
                         'rdc-recommended-notifications-env/notifications/new_listings/nl_listings'
                         .format(count_empty_rn_nl_listings_listingid, env))

    perc_listings_empty_fields = 100 - (count_rn_nl_listings - count_empty_rn_nl_listings_fields) / \
                               float(count_rn_nl_listings) * 100
    if perc_listings_empty_fields > 10:
        error_msg.append("Percentage diff of listing_ids in rn_nl_listings: {} with empty fields: {} is : {} "
                         " which is greater than 10%".format(count_rn_nl_listings, count_empty_rn_nl_listings_fields,
                                                             perc_listings_empty_fields))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the empty records in ' \
                      'rdc-recommended-notifications-env/notifications/new_listings/nl_listings, '.format(env)
    log.info('{} Success: Validated the empty records in '
             'rdc-recommended-notifications-env/notifications/new_listings/nl_listings '.format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_empty_rn_nl_users_records(env, setup):
    '''
    Validate the empty records:
    user_id, score, overall_mean, price_ceiling, zipcode, overall_std_dev
    in rdc-recommended-notifications-env/notifications/new_listings/rn_nl_users
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the empty records in '
             'rdc-recommended-notifications-env/notifications/new_listings/rn_nl_users'
             .format(pidInfo, env, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_nl_config_instance = setup['rn_nl_instance']
    s3_instance = setup['s3_instance']
    error_msg = []
    metrics_bucket = rn_nl_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_nl_config_instance.get_validation_metrics_key('empty_rn_nl_users_records')
    error_bucket = rn_nl_config_instance.get_validation_error_bucket()
    err_key = rn_nl_config_instance.get_validation_error_key('empty_rn_nl_users_records')

    curr_user_users_count = setup['curr_user_users_count']
    count_rn_nl_users = int(curr_user_users_count[0][0])

    empty_rn_nl_users_userid_query_map = QueryUtil.get_rn_nl_users(athena_instance, rn_nl_config_instance
                                                                  , count_empty_rn_nl_user_userid=True)
    empty_rn_nl_users_userid_resp = athena_instance.parse_query \
        (empty_rn_nl_users_userid_query_map[AthenaQueries.count_empty_rn_nl_user_userid
         .replace('$output_table_name', rn_nl_config_instance.get_rn_users_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_empty_rn_nl_users_userid = int(empty_rn_nl_users_userid_resp[0][0])

    empty_rn_nl_users_score_query_map = QueryUtil.get_rn_nl_users(athena_instance, rn_nl_config_instance
                                                         , count_empty_rn_nl_user_score=True)
    empty_rn_nl_users_score_resp = athena_instance.parse_query \
        (empty_rn_nl_users_score_query_map[AthenaQueries.count_empty_rn_nl_user_score
         .replace('$output_table_name', rn_nl_config_instance.get_rn_users_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_empty_rn_nl_users_score = int(empty_rn_nl_users_score_resp[0][0])

    empty_rn_nl_users_overallmean_query_map = QueryUtil.get_rn_nl_users(athena_instance, rn_nl_config_instance
                                                                  , count_empty_rn_nl_user_overallmean=True)
    empty_rn_nl_users_overallmean_resp = athena_instance.parse_query \
        (empty_rn_nl_users_overallmean_query_map[AthenaQueries.count_empty_rn_nl_user_overallmean
         .replace('$output_table_name', rn_nl_config_instance.get_rn_users_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_empty_rn_nl_users_overallmean = int(empty_rn_nl_users_overallmean_resp[0][0])

    empty_rn_nl_users_priceceiling_query_map = QueryUtil.get_rn_nl_users(athena_instance, rn_nl_config_instance
                                                                        , count_empty_rn_nl_user_priceceiling=True)
    empty_rn_nl_users_priceceiling_resp = athena_instance.parse_query \
        (empty_rn_nl_users_priceceiling_query_map[AthenaQueries.count_empty_rn_nl_user_priceceiling
         .replace('$output_table_name', rn_nl_config_instance.get_rn_users_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_empty_rn_nl_users_priceceiling = int(empty_rn_nl_users_priceceiling_resp[0][0])

    empty_rn_nl_users_zipcode_query_map = QueryUtil.get_rn_nl_users(athena_instance, rn_nl_config_instance
                                                                         , count_empty_rn_nl_user_zipcode=True)
    empty_rn_nl_users_zipcode_resp = athena_instance.parse_query \
        (empty_rn_nl_users_zipcode_query_map[AthenaQueries.count_empty_rn_nl_user_zipcode
         .replace('$output_table_name', rn_nl_config_instance.get_rn_users_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_empty_rn_nl_users_zipcode = int(empty_rn_nl_users_zipcode_resp[0][0])

    empty_rn_nl_users_overallstddev_query_map = QueryUtil.get_rn_nl_users(athena_instance, rn_nl_config_instance
                                                                    , count_empty_rn_nl_user_overallstddev=True)
    empty_rn_nl_users_overallstddev_resp = athena_instance.parse_query \
        (empty_rn_nl_users_overallstddev_query_map[AthenaQueries.count_empty_rn_nl_user_overallstddev
         .replace('$output_table_name', rn_nl_config_instance.get_rn_users_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_empty_rn_nl_users_overallstddev = int(empty_rn_nl_users_overallstddev_resp[0][0])

    value_rn_nl_users_overallstddev_query_map = QueryUtil.get_rn_nl_users(athena_instance, rn_nl_config_instance
                                                                          , count_value_rn_nl_user_overallstddev=True)
    value_rn_nl_users_overallstddev_resp = athena_instance.parse_query \
        (value_rn_nl_users_overallstddev_query_map[AthenaQueries.count_value_rn_nl_user_overallstddev
         .replace('$output_table_name', rn_nl_config_instance.get_rn_users_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_value_rn_nl_users_overallstddev = int(value_rn_nl_users_overallstddev_resp[0][0])

    if count_empty_rn_nl_users_userid > 0:
        error_msg.append('{} Empty user_ids {} present in '
                         'rdc-recommended-notifications-env/notifications/new-listings/rn_nl_users'
                         .format(count_empty_rn_nl_users_userid, env))

    perc_users_empty_score = 100 - (count_rn_nl_users - count_empty_rn_nl_users_score) / \
                               float(count_rn_nl_users) * 100
    if perc_users_empty_score > 15:
        error_msg.append("Percentage diff of user_ids in rn_nl_users: {} with empty score :{}  is : {} "
                         " which is greater than 10%".format(count_rn_nl_users, count_empty_rn_nl_users_score,
                                                             perc_users_empty_score))

    perc_users_empty_overallmean = 100 - (count_rn_nl_users - count_empty_rn_nl_users_overallmean) / \
                             float(count_rn_nl_users) * 100
    if perc_users_empty_overallmean > 10:
        error_msg.append("Percentage diff of user_ids in rn_nl_users: {} with empty overallmean: {}  is : {} "
                         " which is greater than 10%".format(count_rn_nl_users, count_empty_rn_nl_users_overallmean,
                                                             perc_users_empty_overallmean))

    perc_users_empty_priceceiling = 100 - (count_rn_nl_users - count_empty_rn_nl_users_priceceiling) / \
                                   float(count_rn_nl_users) * 100
    if perc_users_empty_priceceiling > 10:
        error_msg.append("Percentage diff of user_ids in rn_nl_users: {} with empty priceceiling: {} is : {} "
                         " which is greater than 10%".format(count_rn_nl_users, count_empty_rn_nl_users_priceceiling,
                                                             perc_users_empty_overallmean))

    perc_users_empty_zipcode = 100 - (count_rn_nl_users - count_empty_rn_nl_users_zipcode) / \
                                    float(count_rn_nl_users) * 100
    if perc_users_empty_zipcode > 15:
        error_msg.append("Percentage diff of user_ids in rn_nl_users: {} with empty zipcode: {} is : {} "
                         " which is greater than 10%".format(count_rn_nl_users, count_empty_rn_nl_users_zipcode,
                                                             perc_users_empty_zipcode))

    perc_users_empty_overallstddev = 100 - (count_rn_nl_users - count_empty_rn_nl_users_overallstddev) / \
                               float(count_rn_nl_users) * 100
    if perc_users_empty_overallstddev > 5:
        error_msg.append("Percentage diff of user_ids in rn_nl_users: {} with empty overallstddev: {} is : {} "
                         " which is greater than 10%".format(count_rn_nl_users, count_empty_rn_nl_users_overallstddev,
                                                             perc_users_empty_overallstddev))

    if count_value_rn_nl_users_overallstddev is not 0:
        error_msg.append("Number of user_ids in rn_nl_users with overallstddev less than 67 is: {}"
                         .format(count_value_rn_nl_users_overallstddev))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the empty records in ' \
                      'rdc-recommended-notifications-env/notifications/new-listings/nl_users, '.format(env)
    log.info('{} Success: Validated the empty records in '
             'rdc-recommended-notifications-env/notifications/listings/nl_users '.format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_empty_rn_nl_candidates_records(env, setup):
    '''
    Validate the empty records:
    listing_type, listing_id, listing_city, listing_postal_code, listing_status, property_id
    in rdc-recommended-notifications-env/notifications/new-listings/rn_nl_candidates
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the empty records in '
             'rdc-recommended-notifications-env/notifications/new-listings/nl_candidates'
             .format(pidInfo, env, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_nl_config_instance = setup['rn_nl_instance']
    s3_instance = setup['s3_instance']
    error_msg = []
    metrics_bucket = rn_nl_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_nl_config_instance.get_validation_metrics_key('empty_rn_nl_candidates_records')
    error_bucket = rn_nl_config_instance.get_validation_error_bucket()
    err_key = rn_nl_config_instance.get_validation_error_key('empty_rn_nl_candidates_records')

    empty_rn_candidates_query_map = QueryUtil.get_rn_nl_candidates(athena_instance, rn_nl_config_instance
                                                                            ,count_empty_rn_candidates=True)
    empty_rn_candidates_resp = athena_instance.parse_query \
        (empty_rn_candidates_query_map[AthenaQueries.count_empty_rn_candidates
         .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_empty_rn_candidates = len(empty_rn_candidates_resp)
    if count_empty_rn_candidates > 0:
        error_msg.append('{} Empty fields {} present in '
                         'rdc-recommended-notifications-env/notifications/new-listings/nl_candidates'
                         .format(count_empty_rn_candidates, env))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the empty fields in ' \
                      'rdc-recommended-notifications-env/notifications/new-listings/nl_candidates, ' \
                      .format(env)
    log.info('{} Success: Validated the empty user_id in '
             'rdc-recommended-notifications-env/notifications/new-listings/nl_candidates' \
             .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_empty_rn_nl_candidates_ranked_records(env, setup):
    '''
    Validate the empty records:
    listing_type, listing_id, listing_city, listing_postal_code, listing_status, property_id,
    seq_id, match_score
    in rdc-recommended-notifications-env/notifications/new-listings/rn_nl_candidates_ranked
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the empty records in '
             'rdc-recommended-notifications-env/notifications/new-listings/nl_candidates_ranked'
             .format(pidInfo, env, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_nl_config_instance = setup['rn_nl_instance']
    s3_instance = setup['s3_instance']
    error_msg = []
    metrics_bucket = rn_nl_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_nl_config_instance.get_validation_metrics_key('empty_rn_nl_candidates_ranked_records')
    error_bucket = rn_nl_config_instance.get_validation_error_bucket()
    err_key = rn_nl_config_instance.get_validation_error_key('empty_rn_nl_candidates_ranked_records')

    empty_rn_candidates_ranked_query_map = QueryUtil.get_rn_nl_candidates_ranked(athena_instance, rn_nl_config_instance
                                                                            ,count_empty_rn_candidates_ranked=True)
    empty_rn_candidates_ranked_resp = athena_instance.parse_query \
        (empty_rn_candidates_ranked_query_map[AthenaQueries.count_empty_rn_candidates_ranked
         .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_empty_rn_candidates_ranked = len(empty_rn_candidates_ranked_resp)
    if count_empty_rn_candidates_ranked > 0:
        error_msg.append('{} Empty fields {} present in '
                         'rdc-recommended-notifications-env/notifications/new-listings/nl_candidates_ranked'
                         .format(count_empty_rn_candidates_ranked, env))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the empty fields in ' \
                      'rdc-recommended-notifications-env/notifications/new-listings/nl_candidates_ranked, ' \
                      .format(env)
    log.info('{} Success: Validated the empty fields in '
             'rdc-recommended-notifications-env/notifications/new-listings/nl_candidates_ranked' \
             .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_user_ranking(env, setup):
    '''
    Validate the rank of user_id wrt matchscore and seq_id in
    rdc-recommended-notifications-env/notifications/new_listings/rn_candidates_ranked
    wrt seq_id and match_score
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the rank of user_id wrt match-score and seq_id in  '
             'rdc-recommended-notifications-env/notifications/new_listings/rn_candidates_ranked'
             .format(pidInfo, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_nl_config_instance = setup['rn_nl_instance']
    s3_instance = setup['s3_instance']
    error_msg = []
    metrics_bucket = rn_nl_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_nl_config_instance.get_validation_metrics_key('user_id_ranking_seq_id')
    error_bucket = rn_nl_config_instance.get_validation_error_bucket()
    err_key = rn_nl_config_instance.get_validation_error_key('user_id_ranking_seq_id')

    user_id_query_map = QueryUtil.get_rn_nl_candidates_ranked(athena_instance, rn_nl_config_instance
                                                            , distinct_users_candidates_ranked=True)
    user_id_resp = athena_instance.parse_query \
        (user_id_query_map[AthenaQueries.distinct_users_candidates_ranked
         .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    user_id_resp_list = [x[0] for x in user_id_resp]

    for each_user_id in user_id_resp_list:
        rn_nl_config_instance.set_user_id(each_user_id)
        ms_order_query_map = QueryUtil.get_rn_nl_candidates_ranked(athena_instance, rn_nl_config_instance
                                                               , order_match_score_rn_cand_ranked=True)
        ms_order_user_resp = athena_instance.parse_query \
            (ms_order_query_map[AthenaQueries.order_match_score_rn_cand_ranked
             .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_ranked_output())
             .replace('$target_date', rn_nl_config_instance.get_target_date())
             .replace('$user_id', each_user_id)])
        sequence_id_list = [int(float(x[3])) for x in ms_order_user_resp]
        if sorted(sequence_id_list, reverse=True) != sequence_id_list:
            error_msg.append(" For user_id {} data is not sorted {}".format(each_user_id, sequence_id_list))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the order of user_id wrt match-score and seq_id in ' \
                      'rdc-recommended-notifications-env/notifications/new_listings/candidates_ranked '.format(env)
    log.info('{} Success: Validated the the order of user_id wrt match-score and seq_id in  '
             'rdc-recommended-notifications-env/notifications/new_listings/candidates_ranked, ' \
                      .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))

def test_candidates_ranked_listing_coverage(env, setup):
    '''
    Validate the empty records of in
    rdc-recommended-notifications-env/notifications/new_listings/nl_candidates_ranked
    with seq_id as 1
    :param environment: prod/dev/qa
    :return: None
    '''
    log.info('{} Validating the count of empty records in '
             'rdc-recommended-notifications-env/notifications/new_listings/nl_candidates_ranked with seq_id as 1 '
             ''.format(pidInfo, env))
    check_emptiness = setup == None
    assert check_emptiness is False, log.error(" There is no  data present in current date and current date - 4")
    athena_instance = setup['athena_instance']
    rn_nl_config_instance = setup['rn_nl_instance']
    s3_instance = setup['s3_instance']
    error_msg = []
    metrics_bucket = rn_nl_config_instance.get_validation_metrics_bucket()
    metrics_key = rn_nl_config_instance.get_validation_metrics_key('empty_rn_nl_candidates_ranked_seq_1')
    error_bucket = rn_nl_config_instance.get_validation_error_bucket()
    err_key = rn_nl_config_instance.get_validation_error_key('empty_rn_nl_candidates_ranked_seq_1')

    count_list_rn_candidates_ranked_query_map = QueryUtil.get_rn_nl_candidates_ranked(athena_instance, rn_nl_config_instance
                                                                            ,count_rn_candidates_ranked_fs_status=True)
    count_list_rn_candidates_ranked_resp = athena_instance.parse_query \
        (count_list_rn_candidates_ranked_query_map[AthenaQueries.count_rn_candidates_ranked_fs_status
         .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_list_rn_candidates_ranked = int(count_list_rn_candidates_ranked_resp[0][0])

    count_user_rn_candidates_ranked_query_map = QueryUtil.get_rn_nl_candidates_ranked(athena_instance, rn_nl_config_instance
                                                                                ,count_user_id_rn_candidates_ranked=True)
    count_user_rn_candidates_ranked_resp = athena_instance.parse_query \
        (count_user_rn_candidates_ranked_query_map[AthenaQueries.count_user_id_rn_candidates_ranked
         .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_user_rn_candidates_ranked = int(count_user_rn_candidates_ranked_resp[0][0])

    count_seq1_user_rn_candidates_ranked_query_map = QueryUtil.get_rn_nl_candidates_ranked(athena_instance,
                                                                                      rn_nl_config_instance
                                                                             ,count_user_rn_candidates_ranked_seq_1=True)
    count_seq1_user_rn_candidates_ranked_resp = athena_instance.parse_query \
        (count_seq1_user_rn_candidates_ranked_query_map[AthenaQueries.count_user_rn_candidates_ranked_seq_1
         .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_seq1_user_rn_candidates_ranked = int(count_seq1_user_rn_candidates_ranked_resp[0][0])

    empty_fields_rn_candidates_ranked_query_map = QueryUtil.get_rn_nl_candidates_ranked(athena_instance,
                                                                                        rn_nl_config_instance,
                                                                        count_empty_fields_rn_candidates_ranked=True)
    empty_fields_rn_candidates_ranked_resp = athena_instance.parse_query \
        (empty_fields_rn_candidates_ranked_query_map[AthenaQueries.count_empty_fields_rn_candidates_ranked
         .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_empty_fields_rn_candidates_ranked = int(empty_fields_rn_candidates_ranked_resp[0][0])

    empty_fields_seq1_rn_candidates_ranked_query_map = QueryUtil.get_rn_nl_candidates_ranked(athena_instance,
                                                                rn_nl_config_instance,
                                                                count_empty_user_rn_candidates_ranked_seq_1=True)
    empty_fields_seq1_rn_candidates_ranked_resp = athena_instance.parse_query \
        (empty_fields_seq1_rn_candidates_ranked_query_map[AthenaQueries.count_empty_user_rn_candidates_ranked_seq_1
         .replace('$output_table_name', rn_nl_config_instance.get_rn_candidates_ranked_output())
         .replace('$target_date', rn_nl_config_instance.get_target_date())])
    count_empty_fields_seq1_rn_candidates_ranked = int(empty_fields_seq1_rn_candidates_ranked_resp[0][0])
    if count_empty_fields_seq1_rn_candidates_ranked > 0:
        error_msg.append('{} Empty fields {} with seq_id as 1 present in '
                         'rdc-recommended-notifications-env/notifications/new_listings/nl_candidates_ranked'
                         .format(count_empty_fields_seq1_rn_candidates_ranked, env))

    perc_listings_empty_candidates_ranked = count_empty_fields_rn_candidates_ranked / \
                                            float(count_list_rn_candidates_ranked) * 100
    if perc_listings_empty_candidates_ranked > 10:
        error_msg.append(
            "Percentage diff of listing_ids with empty fields in nl_candidates_ranked: {} wrt total listings"
            ": {} is {} which is greater than 10%"
            .format(count_empty_fields_rn_candidates_ranked, count_list_rn_candidates_ranked,
                    perc_listings_empty_candidates_ranked))

    if count_user_rn_candidates_ranked != count_seq1_user_rn_candidates_ranked:
        error_msg.append(
            "The count of user_id with seq_id as 1 in nl_candidates_ranked: {} wrt total distinct user_id"
            ": {} are not equal"
                .format(count_seq1_user_rn_candidates_ranked, count_user_rn_candidates_ranked))

    if error_msg:
        s3_instance.write_to_s3(error_bucket, err_key, str(error_msg))
    assert len(error_msg) == 0, log.error(",".join(error_msg))
    validation_pass = 'Success: Validated the count of empty records in ' \
                      'rdc-recommended-notifications-env/notifications/new_listings/nl_candidates_ranked with seq_id as 1 '\
        .format(env)
    log.info('{} Success: Validated the count of empty records in  '
             'rdc-recommended-notifications-env/notifications/new_listings/nl_candidates_ranked with seq_id as 1 ' \
                      .format(pidInfo, env))
    s3_instance.write_to_s3(metrics_bucket, metrics_key, str(validation_pass))
