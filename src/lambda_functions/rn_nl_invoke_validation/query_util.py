from validation_queries import *


class QueryUtil:

    @staticmethod
    def get_rn_nl_listings(rn_listings_instance, config_instance, **kwargs):
        query_list = [
        ]
        if kwargs.get('count_unique_listing_id', None):
            new_query_list = [
                AthenaQueries.count_unique_listing_id
                    .replace('$output_table_name', config_instance.get_rn_listings_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_unique_listing_property_id', None):
            new_query_list = [
                AthenaQueries.count_unique_listing_property_id
                    .replace('$output_table_name', config_instance.get_rn_listings_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_rn_listings_fs_status', None):
            new_query_list = [
                AthenaQueries.count_rn_listings_fs_status
                    .replace('$output_table_name', config_instance.get_rn_listings_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_nl_listing_listingid', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_nl_listing_listingid
                    .replace('$output_table_name', config_instance.get_rn_listings_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_nl_listings_field', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_nl_listings_field
                    .replace('$output_table_name', config_instance.get_rn_listings_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('list_listing_id_start_run_date', None):
            new_query_list = [
                AthenaQueries.list_listing_id_start_run_date
                    .replace('$output_table_name', config_instance.get_rn_listings_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        return rn_listings_instance.run(
            query_list,
            config_instance.get_validation_result(),
            config_instance.get_db_athena()
        )

    @staticmethod
    def get_rn_nl_users(rn_users_instance, config_instance, **kwargs):
        query_list = [
        ]
        if kwargs.get('count_user_id_rn_users', None):
            new_query_list = [
                AthenaQueries.count_user_id_rn_users
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_nl_user_userid', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_nl_user_userid
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_nl_user_score', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_nl_user_score
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_nl_user_overallmean', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_nl_user_overallmean
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_nl_user_priceceiling', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_nl_user_priceceiling
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_nl_user_zipcode', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_nl_user_zipcode
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_nl_user_overallstddev', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_nl_user_overallstddev
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_value_rn_nl_user_overallstddev', None):
            new_query_list = [
                AthenaQueries.count_value_rn_nl_user_overallstddev
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        return rn_users_instance.run(
            query_list,
            config_instance.get_validation_result(),
            config_instance.get_db_athena()
        )

    @staticmethod
    def get_rn_nl_candidates(rn_candidates_instance, config_instance, **kwargs):
        query_list = [
        ]
        if kwargs.get('count_user_id_rn_candidates', None):
            new_query_list = [
                AthenaQueries.count_user_id_rn_candidates
                    .replace('$output_table_name', config_instance.get_rn_candidates_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_rn_candidates_fs_status', None):
            new_query_list = [
                AthenaQueries.count_rn_candidates_fs_status
                    .replace('$output_table_name', config_instance.get_rn_candidates_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_candidates', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_candidates
                    .replace('$output_table_name', config_instance.get_rn_candidates_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_unique_user_listing_id_cand', None):
            new_query_list = [
                AthenaQueries.count_unique_user_listing_id_cand
                    .replace('$output_table_name', config_instance.get_rn_candidates_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        return rn_candidates_instance.run(
            query_list,
            config_instance.get_validation_result(),
            config_instance.get_db_athena()
        )

    @staticmethod
    def get_rn_nl_candidates_ranked(rn_candidates_ranked_instance, config_instance, **kwargs):
        query_list = [
        ]
        if kwargs.get('count_user_id_rn_candidates_ranked', None):
            new_query_list = [
                AthenaQueries.count_user_id_rn_candidates_ranked
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_rn_candidates_ranked_fs_status', None):
            new_query_list = [
                AthenaQueries.count_rn_candidates_fs_status
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_candidates_ranked', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_candidates_ranked
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('order_match_score_rn_cand_ranked', None):
            new_query_list = [
                AthenaQueries.order_match_score_rn_cand_ranked
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
                    .replace('$user_id', config_instance.get_user_id())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('distinct_users_candidates_ranked', None):
            new_query_list = [
                AthenaQueries.distinct_users_candidates_ranked
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_user_rn_candidates_ranked_seq_1', None):
            new_query_list = [
                AthenaQueries.count_user_rn_candidates_ranked_seq_1
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_fields_rn_candidates_ranked', None):
            new_query_list = [
                AthenaQueries.count_empty_fields_rn_candidates_ranked
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_user_rn_candidates_ranked_seq_1', None):
            new_query_list = [
                AthenaQueries.count_empty_user_rn_candidates_ranked_seq_1
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_unique_user_listing_id_cand_ranked', None):
            new_query_list = [
                AthenaQueries.count_unique_user_listing_id_cand_ranked
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        return rn_candidates_ranked_instance.run(
            query_list,
            config_instance.get_validation_result(),
            config_instance.get_db_athena()
        )
