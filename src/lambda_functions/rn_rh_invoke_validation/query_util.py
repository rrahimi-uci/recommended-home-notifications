from validation_queries import *


class QueryUtil:

    @staticmethod
    def get_rn_rh_listings(rn_listings_instance, config_instance, **kwargs):
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
        if kwargs.get('count_empty_rn_rh_listing_listingid', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_rh_listing_listingid
                    .replace('$output_table_name', config_instance.get_rn_listings_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_rh_listings_field', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_rh_listings_field
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
    def get_rn_rh_users(rn_users_instance, config_instance, **kwargs):
        query_list = [
        ]
        if kwargs.get('count_user_id_rn_users', None):
            new_query_list = [
                AthenaQueries.count_user_id_rn_users
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_rh_user_score', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_rh_user_score
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_rh_user_userid', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_rh_user_userid
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_rh_user_overallmean', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_rh_user_overallmean
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_rh_user_priceceiling', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_rh_user_priceceiling
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_rh_user_zipcode', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_rh_user_zipcode
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_rh_user_overallstddev', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_rh_user_overallstddev
                    .replace('$output_table_name', config_instance.get_rn_users_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_value_rn_rh_user_overallstddev', None):
            new_query_list = [
                AthenaQueries.count_value_rn_rh_user_overallstddev
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
    def get_rn_rh_recommendations(rn_recommendations_instance, config_instance, **kwargs):
        query_list = [
        ]
        if kwargs.get('count_user_id_rn_recommendations', None):
            new_query_list = [
                AthenaQueries.count_user_id_rn_recommendations
                    .replace('$output_table_name', config_instance.get_rn_recommendations_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_rn_recommendations_fs_status', None):
            new_query_list = [
                AthenaQueries.count_rn_recommendations_fs_status
                    .replace('$output_table_name', config_instance.get_rn_recommendations_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_rn_recommendations', None):
            new_query_list = [
                AthenaQueries.count_empty_rn_recommendations
                    .replace('$output_table_name', config_instance.get_rn_recommendations_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('order_listing_id_seq_rn_recommendations', None):
            new_query_list = [
                AthenaQueries.order_listing_id_seq_rn_recommendations
                    .replace('$output_table_name', config_instance.get_rn_recommendations_output())
                    .replace('$target_date', config_instance.get_target_date())
                    .replace('$user_id', config_instance.get_user_id())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('listing_id_seq_rn_recommendations', None):
            new_query_list = [
                AthenaQueries.listing_id_seq_rn_recommendations
                    .replace('$output_table_name', config_instance.get_rn_recommendations_output())
                    .replace('$target_date', config_instance.get_target_date())
                    .replace('$user_id', config_instance.get_user_id())
            ]
            query_list = query_list + new_query_list
        return rn_recommendations_instance.run(
            query_list,
            config_instance.get_validation_result(),
            config_instance.get_db_athena()
        )

    @staticmethod
    def get_rn_rh_candidates_ranked(rn_candidates_ranked_instance, config_instance, **kwargs):
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
                AthenaQueries.count_rn_candidates_ranked_fs_status
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
        if kwargs.get('user_rn_candidates_ranked', None):
            new_query_list = [
                AthenaQueries.user_rn_candidates_ranked
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('listing_id_seq_rn_candidates_ranked', None):
            new_query_list = [
                AthenaQueries.listing_id_seq_rn_candidates_ranked
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
                    .replace('$user_id', config_instance.get_user_id())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_empty_fields_rn_candidates_ranked', None):
            new_query_list = [
                AthenaQueries.count_empty_fields_rn_candidates_ranked
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
        if kwargs.get('count_empty_user_rn_candidates_ranked_seq_1', None):
            new_query_list = [
                AthenaQueries.count_empty_user_rn_candidates_ranked_seq_1
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        if kwargs.get('count_unique_listing_user_id', None):
            new_query_list = [
                AthenaQueries.count_unique_listing_user_id
                    .replace('$output_table_name', config_instance.get_rn_candidates_ranked_output())
                    .replace('$target_date', config_instance.get_target_date())
            ]
            query_list = query_list + new_query_list
        return rn_candidates_ranked_instance.run(
            query_list,
            config_instance.get_validation_result(),
            config_instance.get_db_athena()
        )
