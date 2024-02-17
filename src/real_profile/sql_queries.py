interactions = """
SELECT DISTINCT *
FROM (
SELECT coalesce(nullif(member_id, ''), rdc_visitor_id) as interactions_user_id,
               listing_id as interactions_listing_id
        FROM biz_data
        WHERE page_type like '%ldp%'
            AND ((member_id <> '') or (rdc_visitor_id is not null) and (trim(rdc_visitor_id) <> '') and (lower(rdc_visitor_id) != 'null'))
            AND (property_status='for_sale')
            AND listing_id is NOT null
            AND listing_id != 'null'
            AND listing_id != ''
            AND listing_id != 'unknown')
"""

saved_items = """
SELECT DISTINCT *
FROM (
SELECT coalesce(nullif(member_id, ''), rdc_visitor_id) as saved_items_user_id,
                listing_id as saved_items_listing_id,
                saved_items
        FROM biz_data
        WHERE ((member_id <> '') or (rdc_visitor_id is not null) and (trim(rdc_visitor_id) <> '') and (lower(rdc_visitor_id) != 'null'))
            AND (property_status='for_sale')
            AND listing_id is NOT null
            AND listing_id != 'null'
            AND listing_id != ''
            AND listing_id != 'unknown'
            AND lower(saved_items) = 'y')
"""

ldp_metrics = """
SELECT listing_id,
sum(CASE WHEN  page_type_group = 'ldp' THEN 1 ELSE 0 END) ldp_pageview_count,
sum(CASE WHEN (lower(social_shares) = 'y') THEN 1 ELSE 0 END) shares_count,
sum(CASE WHEN (lower(saved_items) = 'y') THEN 1 ELSE 0 END) saves_count
FROM bizTable
WHERE listing_id is not null
AND trim(listing_id) != ''
AND lower(listing_id) != 'null'
GROUP BY  listing_id
"""

listing_history_details = """
SELECT *
FROM listingHistory
WHERE
    to_timestamp(cast('$target_date_timestamp' as string), 'yyyyMMdd HH:mm:ss') < effective_to_datetime_mst
    AND to_timestamp(cast('$target_date_timestamp' as string), 'yyyyMMdd HH:mm:ss') >= effective_from_datetime_mst
    AND listing_status = 'for sale'
"""

listing_columns = [
    "property_id",
    "listing_id",
    "is_contingent",
    "is_da_pending",
    "listing_address",
    "listing_status",
    "listing_type",
    "listing_photo_url",
    "listing_photo_count",
    "listing_square_feet",
    "listing_lot_square_feet",
    "rdc_property_url",
    "listing_city",
    "listing_state",
    "listing_postal_code",
    "listing_current_price",
    "listing_start_datetime_mst",
    "listing_end_datetime_mst",
    "effective_from_datetime_mst",
    "effective_to_datetime_mst",
    "listing_number_of_bath_rooms",
    "listing_number_of_bed_rooms"
]

v1_bucketing_candidates = """
SELECT *
FROM usersView
WHERE (experiment_name == '$experiment_aaa')
OR (experiment_name == '$experiment_abc' AND variation = '$variation_group')
"""
