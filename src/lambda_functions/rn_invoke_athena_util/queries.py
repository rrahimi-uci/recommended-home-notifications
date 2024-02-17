query1 = """
CREATE TABLE dataproducts.notifications_member_email_map_$target_date
WITH (
    format = 'PARQUET',
    external_location = $member_email_map_output_folder) 
AS
SELECT member_id, member_profile__email_address
FROM(
SELECT member_id, member_profile__email_address,
ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY updated_at_mst_ts DESC) AS rn
FROM "cnsp_rdc_pdt"."user_saved_data" 
WHERE member_profile__email_address NOT LIKE '%amaua.realtor%')
WHERE rn=1
"""

drop_query1 = """
DROP TABLE IF EXISTS dataproducts.notifications_member_email_map_$drop_table_date
"""
