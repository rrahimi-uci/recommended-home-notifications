from datetime import date, timedelta, datetime


class AwsConfig(object):
    AWS_REGION = "us-west-2"
    AWS_ROLE_MAP = {
        "dev" : "arn:aws:iam::289154003759:role/move-dataeng-assumerole-dataproducts",
        "prod": "arn:aws:iam::057425096214:role/move-dataeng-assumerole-dataproducts",
        "qa"  : "arn:aws:iam::289154003759:role/move-dataeng-assumerole-dataproducts"
        }

    DATABASE_ATHENA = "recommended_notifications"

    def __init__(self, env):
        self.env = env
        self.date_expected = date.today() - timedelta(1)

    def current_date(self):
        return self.date_expected.strftime("%Y%m%d")

    def get_role_arn(self, env):
        return self.AWS_ROLE_MAP[env]

    def get_Region(self):
        return self.AWS_REGION

    def get_dynamo_db(self):
        raise NotImplementedError

    def get_db_athena(self):
        return self.DATABASE_ATHENA

    def set_target_date(self, new_date):
        if isinstance(new_date, str):
            new_date = datetime.strptime(new_date, '%Y%m%d')
        self.date_expected = new_date

    def get_subtract_date(self, days_to_subtract=0):
        return self.date_expected - timedelta(days=days_to_subtract)


class RN_RH(AwsConfig):
    RN_VALIDATION_RESULT = "s3://rdc-recommended-notifications-{}/validation/rn_rh/querylogs/target_date={}"
    RN_VALIDATION_METRICS_BUCKET = "rdc-recommended-notifications-{}"
    RN_VALIDATION_METRICS_KEY = "validation/rn_rh/metrics/target_date={}/metrics-{}.txt"
    RN_VALIDATION_ERROR_BUCKET = "rdc-recommended-notifications-{}"
    RN_VALIDATION_ERROR_KEY = "validation/rn_rh/error/target_date={}/err-{}.txt"
    RN_CANDIDATES_RANKED_OUTPUT_TABLE = "rn_rh_candidates_ranked"
    RN_LISTINGS_OUTPUT_TABLE = "rn_rh_listings"
    RN_USERS_OUTPUT_TABLE = "rn_rh_users"
    RN_RECOMMENDATIONS_OUTPUT_TABLE = "rn_rh_recommendations"
    RECOMMENDED_HOMES_PROD_BUCKET = "rdc-recommended-homes-ir-prod"
    RECOMMENDED_HOMES_PROD_KEY_PATTERN = "output/generated-recommendations/dt={}/dwell_time/CA"
    user_id = None

    def __init__(self, env):
        super(RN_RH, self).__init__(env)

    def get_validation_result(self):
        return self.RN_VALIDATION_RESULT.format(self.env, self.current_date())

    def get_validation_error_bucket(self):
        return self.RN_VALIDATION_ERROR_BUCKET.format(self.env, self.current_date())

    def get_validation_error_key(self, test_name):
        return self.RN_VALIDATION_ERROR_KEY.format( self.current_date(), test_name)

    def get_validation_metrics_bucket(self):
        return self.RN_VALIDATION_ERROR_BUCKET.format(self.env, self.current_date())

    def get_validation_metrics_key(self, test_name):
        return self.RN_VALIDATION_METRICS_KEY.format( self.current_date(), test_name)

    def get_rn_recommendations_output(self):
        return self.RN_RECOMMENDATIONS_OUTPUT_TABLE.format(self.current_date())

    def get_rn_candidates_ranked_output(self):
        return self.RN_CANDIDATES_RANKED_OUTPUT_TABLE.format(self.current_date())

    def get_rn_listings_output(self):
        return self.RN_LISTINGS_OUTPUT_TABLE.format(self.current_date())

    def get_rn_users_output(self):
        return self.RN_USERS_OUTPUT_TABLE.format(self.current_date())

    def get_target_date(self):
        return self.current_date()

    def get_recommended_homes_key_path(self):
        return self.RECOMMENDED_HOMES_PROD_KEY_PATTERN.format(self.current_date())

    def set_user_id(self, user_id):
        self.user_id = user_id

    def get_user_id(self):
        return self.user_id
