class DeploymentConstants:
    APPLICATION_NAME = "rdc-recommended-notifications"
    AWS_REGION = "us-west-2"
    S3_SOURCE_BUCKET = ""
    ROLE_ARN_PROD = "arn:aws:iam::057425096214:role/move-dataeng-assumerole-dataproducts"
    ROLE_ARN_DEV = "arn:aws:iam::289154003759:role/move-dataeng-assumerole-dataproducts"
    S3_DATAENG_BUCKET = "s3://move-dataeng-dapexternal-prod/dataproducts/notifications/member_email_map"
    S3_DATAENG_BUCKET_LOCAL = "testdata/member_email_map"
    S3_DATAENG_BUCKET_NAME = "move-dataeng-dapexternal-prod"
    ATHENA_TABLES_PREFIX = "dataproducts/notifications/member_email_map/tables/"
    S3_BUCKET_PREFIX = "rdc-recommended-notifications-"
    S3_LOCAL_PATH = "testdata/"
    S3_RECOMMENDED_HOMES_BUCKET_NAME = "rdc-recommended-homes-ir-prod"


class JobConstants:
    RDC_BIZ_DATA_PATH = "s3a://move-dataeng-omniture-prod/homerealtor/biz_data_product_event_v2/rdc_biz_data"
    LISTING_HISTORY_PATH = "s3://move-dataeng-lstp-prod/business-data/homes/listing/listing_history"
    DAILY_LISTINGS_PATH = "s3://move-dataeng-lstp-prod/business-data/homes/listing/listing_history/year_yyyy=9999"
    OPEN_HOUSES_PATH = "s3://move-dataeng-lstp-prod/business-data/homes/open_houses"
    ANALYTICAL_PROFILE_PATH = "s3://move-dataeng-bd-prod/cnsp/consumer_analytical_profile_srp_member_id_details_t001"

    PUSH_NOTIFICATION_OPTED_PATH = "s3://pigeon-braze-segment-users-bucket-prod/user-opt-in"
    USERS_WITH_DOMZIP_PATH = "s3://move-dataeng-bd-encrypt-prod/cnsm/cnsm_bd_braze_new_listings_ldp_dominant_zip/braze_new_listings_ldp_dominant_zip"
    USERS_WITH_NOTIF_PATH = "s3://move-dataeng-cnsm-prod/braze/users_messages_pushnotification_send/processed-data-xact"
    PUSH_NOTIFICATION_OPTED_LOCAL_PATH = ""
    USERS_WITH_DOMZIP_LOCAL_PATH = ""
    USERS_WITH_NOTIF_LOCAL_PATH = ""
    APP_ELIGIBLE_USERS_OUTPUT_PATH = "s3://rdc-recommended-notifications-$env/notifications/app_eligible_users"
    APP_BUCKETED_USERS_OUTPUT_PATH = "s3://rdc-recommended-notifications-$env/notifications/app_bucketed_users"
    APP_ELIGIBLE_USERS_OUTPUT_LOCAL_PATH = ""
    APP_BUCKETED_USERS_OUTPUT_LOCAL_PATH = ""

    EMAIL_NOTIFICATIONS_PATH = "s3://move-dataeng-cnsm-prod/rspy/processed-data-xact/sent"
    EMAIL_NOTIFICATIONS_LOCAL_PATH = ""
    EMAIL_ELIGIBLE_USERS_OUTPUT_LOCAL_PATH = ""
    EMAIL_ELIGIBLE_USERS_OUTPUT_PATH = "s3://rdc-recommended-notifications-$env/notifications/email_eligible_users"

    REAL_PROFILE_PATH = "s3://rdc-real-profile-prod/output"
    REAL_PROFILE_OUTPUT_PATH = "s3://rdc-recommended-notifications-$env/real_profile"
    NEW_LISTINGS_OUTPUT_PATH = "s3://rdc-recommended-notifications-$env/notifications/new_listings/nl_"
    RECOMMENDED_FILTERS_OUTPUT_PATH = "s3://rdc-recommended-notifications-$env/notifications/recommended_homes/rh_"
    USER_FEATURES_PATH = "s3://rdc-match-score-prod/features/user_features_snapshot"
    LISTING_FEATURES_PATH = "s3://rdc-match-score-prod/features/listing_features_snapshot"

    SAMPLE_EVENT_DATE = "20200601"
    # SAMPLE_EVENT_DATE = "20200922"
    REAL_PROFILE_LOCAL_PATH = "testdata/real_profile"
    RDC_BIZ_DATA_LOCAL_PATH = "testdata/rdc_biz_data/"
    DAILY_LISTINGS_LOCAL_PATH = "testdata/listing/listing_history/year_yyyy=9999"
    LISTING_HISTORY_LOCAL_PATH = "testdata/listing_history"
    ANALYTICAL_PROFILE_LOCAL_PATH = "testdata/consumer_analytical_profile_srp_member_id_details_t001"
    PUSH_NOTIFICATION_OPTED_LOCAL_PATH = "testdata/user-opt-in"
    OPEN_HOUSES_LOCAL_PATH = "testdata/open_houses"
    REAL_PROFILE_OUTPUT_LOCAL_PATH = "testdata/output/real_profile"
    NEW_LISTINGS_OUTPUT_LOCAL_PATH = "testdata/output/notifications/new_listings/nl_"
    RECOMMENDED_FILTERS_OUTPUT_LOCAL_PATH = "data/output/notifications/recommended-homes/rh_"
    USER_FEATURES_LOCAL_PATH = "testdata/match_score/features/user_features_snapshot"
    LISTING_FEATURES_LOCAL_PATH = "testdata/match_score/features/listing_features_snapshot"

    # Recommended Homes Filtering Constants
    RECOMMENDED_HOMES_PATH = "s3://rdc-recommended-homes-ir-prod/output/generated-recommendations"
    RECOMMENDED_LISTINGS_PATH = "s3://move-dataeng-dapexternal-prod/dataproducts/recommendation_homes_ir_usa/recommended_homes/visid"
    RECOMMENDED_HOMES_LOCAL_PATH = "testdata/recommended_homes/"
    RECOMMENDED_LISTINGS_LOCAL_PATH = "testdata/recommended_listings/"


class DataConstants:
    LISTING_TYPES = ['mfd/mobile home', 'townhomes', 'condo/townhome/row home/co-op', 'condo/townhome', 'coop',
                     'condos', 'duplex/triplex', 'single family home', 'condo/townhome/rowhome/coop',
                     'multi-family home']
    USER_GROUPS_NL = 50
    USER_GROUPS_RH = 12
    LAG_5 = 5
    LAG_30 = 30
    LAG_90 = 90
    LAG_180 = 180
    NUMBER_OF_GEOS_NL = 3
    NUMBER_OF_GEOS_RH = 4
    STANDARD_DEVIATION_NL = 1.5
    STANDARD_DEVIATION_RH = 3
    FOR_SALE_STATUS = 'for sale'
    #SQS_QUEUE = 'rn-test-queue'
    SQS_QUEUE = 'pigeon-notifications-queue-$env'

    STATE_LIST = ['NM', 'VI', 'GU', 'PR', 'VT', 'DC', 'RI', 'DE', 'WY', 'NH', 'ND', 'AK', 'SD', 'HI', 'ME',
                          'WV', 'MT', 'NE', 'MS', 'KS', 'ID', 'KY', 'AR', 'VA', 'AZ', 'NC', 'MN', 'MO', 'PA', 'SC',
                          'MI', 'MA', 'NJ', 'CO', 'OH', 'TN', 'NY', 'GA', 'IL', 'CA', 'FL', 'AL', 'CT', 'IA', 'IN',
                          'LA', 'MD', 'NV', 'OK', 'OR', 'UT', 'WA', 'WI', 'TX']

    STATE_LIST_LIMITED = ['NM', 'VI', 'GU', 'PR', 'VT', 'DC', 'RI', 'DE', 'WY', 'NH', 'ND', 'AK', 'SD', 'HI', 'ME',
                          'WV', 'MT', 'NE', 'MS', 'KS', 'ID', 'KY', 'AR', 'VA', 'AZ', 'NC', 'MN', 'MO', 'PA', 'SC',
                          'MI', 'MA', 'NJ', 'CO', 'OH', 'TN', 'NY', 'GA', 'IL', 'CA', 'FL']

    STATE_LIST_AAA = ['NM']
    STATE_LOCAL = 'DC'


class ModelConstants:
    MODEL_FILE = 'model.txt'
    LOOKUP_CONSUMER_CITY = "s3://rdc-recommended-notifications-$env/src/match_score/look_up_data/out_consumer_city_df.csv"
    LOOKUP_LDP_CITY = "s3://rdc-recommended-notifications-$env/src/match_score/look_up_data/out_ldp_city_df.csv"
    LOOKUP_CONSUMER_ZIP = "s3://rdc-recommended-notifications-$env/src/match_score/look_up_data/out_consumer_zip_df.csv"
    LOOKUP_LDP_ZIP = "s3://rdc-recommended-notifications-$env/src/match_score/look_up_data/out_ldp_zip_df.csv"

    MODEL_FILE_LOCAL = 'src/match_score/model.txt'
    LOOKUP_CONSUMER_CITY_LOCAL = "src/match_score/look_up_data/out_consumer_city_df.csv"
    LOOKUP_LDP_CITY_LOCAL = "src/match_score/look_up_data/out_ldp_city_df.csv"
    LOOKUP_CONSUMER_ZIP_LOCAL = "src/match_score/look_up_data/out_consumer_zip_df.csv"
    LOOKUP_LDP_ZIP_LOCAL = "src/match_score/look_up_data/out_ldp_zip_df.csv"


class ABTesting:
    OPTIMIZELY_DEV = 'Sgu2BYsPkinyLX6anXs8xZ'
    OPTIMIZELY_PROD = 'J6RY8mrsgH6JmiguM8ZnRy'
    OPTIMIZELY_BETA = '2hBBXFiETRruBPS3yRS4CT'
    EXPERIMENT_NAME_AAA = 'NP_RECOMMENDED_HOMES_DEMO'
    EXPERIMENT_NAME_ABC = 'RECUP_LDP_NOT_202103'
    EXPERIMENT_NAME_AB = 'RECUP_LDP_NOTV2_202103'
    APP_EXPERIMENT_NAME_ABC = 'APP_DOM_RH_NL'
    VARIANT1 = "V1"
    VARIANT2 = "V2"
