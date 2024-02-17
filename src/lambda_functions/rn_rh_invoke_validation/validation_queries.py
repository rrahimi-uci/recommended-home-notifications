class AthenaQueries:

    # query to get listing_id count is unique in recommended_notifications.rn_rh_listings
    count_unique_listing_id = """
        SELECT listing_id as listing_id, 
        count(listing_id) as listing_id_count, 
        target_date as target_date
        FROM recommended_notifications.$output_table_name
        where target_date = '$target_date'
        group by listing_id, target_date
        having count(listing_id) > 1
    """

    # query to get unique listing_id and property_id combination recommended_notifications.rn_rh_listings
    count_unique_listing_property_id = """
        SELECT listing_id as listing_id, 
        count(listing_id) as listing_id_count,
        count(property_id) as property_id_count,
        target_date as target_date
        FROM recommended_notifications.$output_table_name 
        where target_date = '$target_date'
        group by listing_id, target_date, property_id
        having count(listing_id) > 1 and count(property_id) > 1
    """

    # query to get count of listing_ids with status as for sale in recommended_notifications.rn_rh_listings
    count_rn_listings_fs_status = """
        SELECT count(distinct listing_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where listing_status = 'for sale'
        and target_date = '$target_date';
    """

    #query to get count of listing_id with empty listing_id and property_id in rn_rh_listings
    count_empty_rn_rh_listing_listingid = """
        SELECT count(distinct listing_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
        and (listing_id = '' or listing_id is null
        or property_id = '' or property_id is null
        or listing_status = '' or listing_status is null)

    """

    # query to get count of listing_id with null fields and empty check in recommended_notifications.rn_rh_listings
    count_empty_rn_rh_listings_field = """
        SELECT count(distinct listing_id)
        FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
        and (listing_type = '' or listing_type is null
        or listing_city = '' or listing_city is null
        or listing_state = '' or listing_state is null
        or listing_postal_code = '' or listing_postal_code is null)
    """

    # query to get count user_id in recommended_notifications.rn_rh_users
    count_user_id_rn_users = """
        SELECT count(distinct user_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
    """

    # query to get count of user_id with empty user_id in recommended_notifications.rn_rh_users
    count_empty_rn_rh_user_userid = """
        SELECT count(distinct user_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where user_id is null or user_id = ''
        and target_date = '$target_date';
    """

    #query to get count of user_id with empty score in recommended_notifications.rn_rh_users
    count_empty_rn_rh_user_score = """
        SELECT count(distinct user_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where score is null or score = 0
        and target_date = '$target_date';
    """

    #query to get count of user_id with empty overall_mean in recommended_notifications.rn_rh_users
    count_empty_rn_rh_user_overallmean = """
        SELECT count(distinct user_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where overall_mean is null or overall_mean = 0
        and target_date = '$target_date';
    """

    #query to get count of user_id with empty price_ceiling in recommended_notifications.rn_rh_users
    count_empty_rn_rh_user_priceceiling = """
        SELECT count(distinct user_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where price_ceiling is null or price_ceiling = 0
        and target_date = '$target_date';   
    """

    # query to get count of user_id with empty zip_code in recommended_notifications.rn_rh_users
    count_empty_rn_rh_user_zipcode = """
        SELECT count(distinct user_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where zip_code is null or zip_code = ''
        and target_date = '$target_date';   
    """

    # query to get count of user_id with empty overall_std_dev in recommended_notifications.rn_rh_users
    count_empty_rn_rh_user_overallstddev = """
        SELECT count(distinct user_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where overall_std_dev is null or overall_std_dev = 0
        and target_date = '$target_date';   
    """

    # query to get count of user_id with overall_std_dev less than 67 in recommended_notifications.rn_rh_users
    count_value_rn_rh_user_overallstddev = """
        SELECT count(distinct user_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where overall_std_dev < 67
        and target_date = '$target_date';
    """

    #query to get count with empty fields in recommended_notifications.rn_candidates
    count_empty_rn_candidates = """
        SELECT * FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
        and (user_id = '' or user_id = null
        or listing_id = '' or listing_id = null
        or property_id = '' or property_id = null
        or listing_city = '' or listing_city = null
        or listing_state = '' or listing_state = null
        or listing_status = '' or listing_status = null
        or listing_postal_code = '' or listing_postal_code = null)   
    """

    #query to get count user_id in recommended_notifications.rn_candidates
    count_user_id_rn_candidates = """
        SELECT count(distinct user_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
    """

    # query to get count with empty fields in recommended_notifications.rn_rh_candidates_ranked
    count_empty_rn_candidates_ranked = """
        SELECT * FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
        and (listing_type = '' or listing_type is null
        or listing_id = '' or listing_id is null
        or listing_city = '' or listing_city is null
        or listing_state = '' or listing_state is null
        or listing_postal_code = '' or listing_postal_code is null
        or listing_status = '' or listing_status is null
        or seq_id = 0 or seq_id is null)
    """

    # query to get unique listing_id and property_id combination recommended_notifications.rn_rh_candidates_ranked
    count_unique_listing_user_id = """
        SELECT listing_id as listing_id,
        user_id as user_id,
        count(listing_id) as listing_id_count,
        count(user_id) as user_id_count,
        target_date as target_date
        FROM recommended_notifications.$output_table_name 
        where target_date = '$target_date'
        group by listing_id, target_date, user_id
        having count(listing_id) > 1 and count(user_id) > 1
    """


    # query to get count with empty specific fields in recommended_notifications.rn_rh_candidates_ranked
    count_empty_fields_rn_candidates_ranked = """
        SELECT count(distinct listing_id) FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
        and (listing_number_of_bath_rooms = 0 or listing_number_of_bath_rooms = null
        or listing_number_of_bed_rooms = 0 or listing_number_of_bed_rooms = null
        or listing_current_price = 0 or listing_current_price = null
        or ldp_url = '' or ldp_url = null  
        or listing_lot_square_feet = 0 or listing_lot_square_feet = null
        or listing_square_feet = 0 or listing_square_feet = null
        or listing_photo_count = 0 or listing_photo_count = null
        or listing_photo_url = '' or listing_photo_url = null)
    """

    # query to get count with empty fields in recommended_notifications.rn_rh_recommendations
    count_empty_rn_recommendations = """
        SELECT * FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
        and (user_id = '' or user_id is null
        or row_id is null
        or listing_id = '' or listing_id is null
        or state = '' or state is null)
    """

    #query to get count of user_id with seq_id as 1 in recommended_notifications.rn_rh_candidates_ranked
    count_user_rn_candidates_ranked_seq_1 = """
        SELECT count(distinct user_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
        and seq_id = 1
    """

    #query to get count of user_id with seq_id as 1 with empty fields in recommended_notifications.rn_rh_candidates_ranked
    count_empty_user_rn_candidates_ranked_seq_1 = """
        SELECT count(distinct user_id)
        FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
        and seq_id = 1
        and (listing_type = '' or listing_type = null)
        and (listing_id = '' or listing_id = null)
        and (listing_city = '' or listing_city = null)
        and (listing_state = '' or listing_state = null)
        and (listing_postal_code = '' or listing_postal_code = null)
        and (listing_status = '' or listing_status = null)
        and (listing_number_of_bath_rooms = 0 or listing_number_of_bath_rooms = null)
        and (listing_number_of_bed_rooms = 0 or listing_number_of_bed_rooms = null)
        and (listing_current_price = 0 or listing_current_price = null)
        and (ldp_url = '' or ldp_url = null  )
        and (listing_lot_square_feet = 0 or listing_lot_square_feet = null)
        and (listing_square_feet = 0 or listing_square_feet = null)
        and (listing_photo_count = 0 or listing_photo_count = null)
        and (listing_photo_url = '' or listing_photo_url = null)
    """

    # query to get count listing_id with for sale status in recommended_notifications.rn_rh_recommendations
    count_rn_recommendations_fs_status = """
        SELECT count(distinct listing_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date';
    """

    # query to get count user_id in recommended_notifications.rn_rh_recommendations
    count_user_id_rn_recommendations = """
        SELECT count(distinct user_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
    """

    # query to get count listing_id with for sale status in recommended_notifications.rn_rh_candidates_ranked
    count_rn_candidates_ranked_fs_status = """
        SELECT count(distinct listing_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
        and listing_status = 'for sale';
    """

    # query to get count user_id in recommended_notifications.rn_rh_candidates_ranked
    count_user_id_rn_candidates_ranked = """
        SELECT count(distinct user_id) 
        FROM "recommended_notifications"."$output_table_name" 
        where target_date = '$target_date'
    """

    # query to get all the listing_id for user_id in CA with order in recommended_notifications.rn_rh_recommendations
    order_listing_id_seq_rn_recommendations = """
        SELECT user_id, listing_id
        FROM "recommended_notifications"."$output_table_name" 
        where user_id = '$user_id'
        and state = 'CA'
        and target_date = '$target_date'
        order by row_id;   
    """

    # query to get list of 10 user_ids in recommended_notifications.rn_rh_candidates_ranked
    user_rn_candidates_ranked = """
        SELECT distinct user_id 
        FROM "recommended_notifications"."$output_table_name"
        where target_date = '$target_date'
        limit 3;
    """

    # query to get all the listing_id for user_id with order in recommended_notifications.rn_rh_candidates_ranked
    listing_id_seq_rn_candidates_ranked = """
        SELECT user_id, listing_id, seq_id 
        FROM "recommended_notifications"."$output_table_name" 
        where user_id = '$user_id'
        and target_date = '$target_date'
        order by seq_id;
    """

    # query to get all the listing_id for user_id with order in recommended_notifications.rn_rh_recommendations
    listing_id_seq_rn_recommendations = """
        SELECT user_id, listing_id, row_id 
        FROM "recommended_notifications"."$output_table_name" 
        where user_id = '$user_id'
        and target_date = '$target_date'
        order by row_id;
    """
