from pyspark.sql.types import *


class Schema:
    real_profile_schema = StructType([
        StructField('user_id', StringType(), True),
        StructField('zip_stats', ArrayType(StructType([
            StructField('category_name', StringType()),
            StructField('overall_prop', StringType()),
            StructField('overall_description', StringType()),
            StructField('overall_success', StringType()),
            StructField('overall_size', StringType()),
            StructField('score', StringType()),
            StructField('change_in_current_pref', StringType()),
            StructField('change_in_lead_share_save_pref', StringType()),
            StructField('is_top_n_geo', StringType()),
            StructField('if_manual_override', StringType())
        ])), True),
        StructField('price_stats', StructType([
            StructField('overall_median', StringType()),
            StructField('overall_min_conf_interval', StringType()),
            StructField('change_in_lead_share_save_pref', StringType()),
            StructField('change_in_current_pref', StringType()),
            StructField('overall_mean', StringType()),
            StructField('overall_third_quartile', StringType()),
            StructField('overall_std_dev', StringType()),
            StructField('overall_max_conf_interval', StringType()),
            StructField('overall_IQR', StringType()),
            StructField('overall_first_quartile', StringType()),
            StructField('overall_std_err', StringType()),
            StructField('overall_n_obs', StringType()),
            StructField('if_manual_override', StringType()),
            StructField('overall_bins', StringType()),
            StructField('overall_description', StringType()),
            StructField('overall_bin_cnt', StringType()),
            StructField('manual_override_datetime', StringType()),
        ]), True),
        StructField('target_date', StringType(), True)
    ])

    sqs_recommended_homes_schema = {
        "meta": {
            "version": "1.0",
            "clientId": "rdc-rau",
            "clientName": "recommend-and-update",
            "notificationType": "recommended_homes",
            "experiment": {
                "key": None,
                "variation": None
            }
        },
        "payload": {
            "content": {},
            "recipients": [{
                "memberId": None,
                "visitorId": None
                }]
        }
    }

    sqs_recommended_homes_app_schema = {
        "meta": {
            "version": "1.1",
            "clientId": "rdc-rau",
            "clientName": "recommend-and-update",
            "notificationType": "recommended_homes_app_push-v1",
            "experiment": {
                "key": None,
                "variation": None
            }
        },
        "payload": {
            "content": {},
            "recipients": [{
                "memberId": None,
                "visitorId": None
                }]
        }
    }

    sqs_recommended_homes_email_schema = {
        "meta": {
            "version": "1.1",
            "clientId": "rdc-rau",
            "clientName": "recommend-and-update",
            "notificationType": "recommended_homes_email-v1",
            "experiment": {
                "key": None,
                "variation": None
            }
        },
        "payload": {
            "content": {},
            "recipients": [{
                "memberId": None,
                "visitorId": None
                }]
        }
    }

    sqs_new_listings_schema = {
        "meta": {
            "version": "2.0",
            "clientId": "rdc-rau",
            "clientName": "recommend-and-update",
            "notificationType": "recommended_homes",
            "experiment": {
                "key": None,
                "variation": None
            }
        },
        "payload": {
            "content": {},
            "recipients": [{
                "memberId": None,
                "visitorId": None
                }]
        }
    }

    sqs_new_listings_app_schema = {
        "meta": {
            "version": "2.1",
            "clientId": "rdc-rau",
            "clientName": "recommend-and-update",
            "notificationType": "recommended_homes_app_push-v2",
            "experiment": {
                "key": None,
                "variation": None
            }
        },
        "payload": {
            "content": {},
            "recipients": [{
                "memberId": None,
                "visitorId": None
                }]
        }
    }

    sqs_new_listings_email_schema = {
        "meta": {
            "version": "2.1",
            "clientId": "rdc-rau",
            "clientName": "recommend-and-update",
            "notificationType": "recommended_homes_email-v2",
            "experiment": {
                "key": None,
                "variation": None
            }
        },
        "payload": {
            "content": {},
            "recipients": [{
                "memberId": None,
                "visitorId": None
                }]
        }
    }

    sqs_dom_zip_app_schema = {
       "meta": {
            "version": "3.0",
            "clientId": "rdc-rau",
            "clientName": "recommend-and-update",
            "notificationType": "recommended_homes_app_push-c",
            "experiment": {
                "key": None,
                "variation": None
            }
        },
        "payload": {
            "content": {},
            "recipients": [{
                "memberId": None,
                "visitorId": None
                }]
        } 
    }

    recommended_homes_schema = StructType([
        StructField('user_id', StringType()),
        StructField('recommendations', ArrayType(
            StructType([
                StructField('mpr_id', StringType()),
                StructField('listing_id', StringType())
            ])
        ))
    ])

    recommended_listings_schema = StructType([
        StructField('listing_id', StringType()),
        StructField('zip_code', StringType())
    ])

    metrics_schema = StructType([
        StructField('mean_precision', DoubleType(), True),
        StructField('mean_recall', DoubleType(), True),
        StructField('user_coverage', IntegerType(), True),
        StructField('mean_ndcg', DoubleType(), True),
        StructField('rank', IntegerType(), True)
    ])
    
    push_opted_schema = StructType([
        StructField('userId', StringType()),
        StructField('optIns', StructType([
            StructField('app', BooleanType()),
            StructField('web', BooleanType())
        ])),

    ])

    users_domzip_schema = StructType([
        StructField('member_id', StringType()),
        StructField('ldp_dominant_zip', StringType()),
        StructField('listing_state', StringType())
    ])

    users_with_notif_schema = StructType([
        StructField('external_user_id', StringType()),
        StructField('campaign_id', StringType()),
        StructField('canvas_id', StringType()),
        StructField('year', StringType()),
        StructField('month', StringType()),
        StructField('day', StringType())
    ])

    users_sent_email_schema = StructType([
        StructField('customer_id', StringType()),
        StructField('campaign_id', StringType()),
        StructField('email', StringType()),
        StructField('year', StringType()),
        StructField('month', StringType()),
        StructField('day', StringType())
    ])