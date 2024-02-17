import pyspark.sql.functions as F
import pytest
import src.utils.logger as logger
from src.real_profile import candidate_generation
from src.match_score import feature_engineering
from src.utils import constants
from tests.spark import get_spark

log, _ = logger.getlogger()


class TestCandidateGeneration(object):

    def get_defaults(self):
        args = dict()
        args['env'] = 'local'
        args['spark'] = get_spark()
        args['target_date_input'] = '20200601'
        args['target_date_output'] = '20200601'
        args['target_status'] = 'for sale'
        args['standard_deviation_factor'] = constants.DataConstants.STANDARD_DEVIATION_NL
        args['number_of_geos'] = constants.DataConstants.NUMBER_OF_GEOS_NL
        args['lag'] = constants.DataConstants.LAG_5
        args['user_groups'] = None
        args['output_base_path'] = None
        args['output_base_local_path'] = constants.JobConstants.NEW_LISTINGS_OUTPUT_LOCAL_PATH
        args['real_profile_df'] = None
        args['candidates_df'] = None
        args['ldp_metrics_df'] = None
        args['listings_df'] = None
        args['rdc_biz_df'] = None
        args['member_email_df'] = None
        args['listing_price_median_df'] = None
        args['user_features_df'] = None
        args['listing_features_df'] = None
        args['candidates_df'] = None
        return args

    def test_get_real_profiles(self):
        args = self.get_defaults()
        cg = candidate_generation.CandidateGeneration(args['env'], args['spark'], args['target_date_input'],
                                                      args['target_date_output'], args['target_status'],
                                                      args['number_of_geos'], args['standard_deviation_factor'],
                                                      args['lag'], args['user_groups'],
                                                      args['output_base_path'], args['output_base_local_path'],
                                                      args['real_profile_df'],
                                                      args['candidates_df'], args['ldp_metrics_df'],
                                                      args['listings_df'],
                                                      args['rdc_biz_df'], args['member_email_df'],
                                                      args['listing_price_median_df'])

        actual_biz_data = cg.get_rdc_biz_data()
        assert (actual_biz_data.count() == 325)

        actual_listings = cg.generate_listings()
        assert (actual_listings.count() == 15)

        merged_listings = cg.generate_ldp_metrics()
        assert (merged_listings.count() == 15)
        cg.write_generated_listings()

        actual_listing_price_median = cg.get_median_listing_price()
        assert (actual_listing_price_median.count() == 14)

        actual_real_profiles = cg.get_real_profiles()
        assert (actual_real_profiles.count() == 675)
        assert (actual_real_profiles.select('user_id').distinct().count() == 270)

        actual_candidates = cg.generate_candidates()
        assert (actual_candidates.count() == 2)

        tagged_candidates = cg.tag_viewed_saved_listings()
        assert (tagged_candidates.count() == 2)

        candidates_df_schema = list(cg.candidates_df.schema)
        null_cols = []

        # iterate over schema list to filter for NullType columns
        for st in candidates_df_schema:
            if str(st.dataType) == 'NullType':
                null_cols.append(st)

        # change the null type columns to string
        for n_col in null_cols:
            col_name = str(n_col.name)
            cg.candidates_df = cg.candidates_df.withColumn(col_name, cg.candidates_df[col_name].cast('string'))

        cg.write_generated_candidates()

        fe = feature_engineering.FeatureEngineering(args['env'], args['spark'], args['target_date_input'],
                                                    args['target_date_output'], args['output_base_path'],
                                                    args['output_base_local_path'], args['user_features_df'],
                                                    args['listing_features_df'], args['candidates_df'])
        fe.lookup_files()
        assert (fe.ldp_zip_dict.count() == 237885)
        assert (fe.ldp_city_dict.count() == 156887)
        assert (fe.con_zip_dict.count() == 240856)
        assert (fe.con_city_dict.count() == 154738)

        fe.load_udfs()
        actual_user_features, actual_listing_features = fe.get_match_score_features()
        print(actual_listing_features.count())
        assert (actual_user_features.count() == 24)
        assert (actual_listing_features.count() == 60)

        fe.get_candidate_features()
        assert (fe.candidates_df.count() == 2)
        fe.write_candidate_features()


if __name__ == '__main__':
    tcg = TestCandidateGeneration()
    tcg.test_get_real_profiles()
