# recommended-notifications

The mobile and web push notifications along with the email campaigns are a great source for increasing user engagement. The scope of this project is to improve and personalize the existing notifications. It also includes creating and sending notifications with new and relevant content to the users.
Use Cases that are personalized are as below:

1. Recommended Homes Alerts (Mobile and Web Push Notifications)
    1. LightFM Model (Variant 1)
    1. New Listings Recommendations using Match Score and Real Profile (Variant 2)
1. New Listings Recommendations using Match Score and Real Profile (Marketing Emails)

## Cloud-formation Templates
1. **cloudformation-template-nl.yml**
    * This template is used to create the Step Functions, lambdas, glue jobs, Activity, SNS Topics and Event Bridge Rules for Recommended Homes, New Listings Recommendations Pipeline
    * The step functions definition used to generate the workflow for the new listings pipeline can be referred at `step-functions-new-listings.json`
1. **cloudformation-template-rh.yml**
    * This template is used to create the Step Functions, lambdas, glue jobs, Activity, SNS Topics and Event Bridge Rules for Recommended Homes, LightFM Variant Pipeline
    * The step functions definition used to generate the workflow for the recommended homes pipeline can be referred at `step-functions-recommended-homes.json`
1. **cloudformation-template-iam.yml**
    * This template is used to create IAM roles used across the pipelines
1. **cloudformation-template-glue.yml**
    * This template is used to create a Glue Database and Crawler for the users, listings, candidates, features and ranked candidates inventory


## Source directory folder structure
The components for candidate generation and candidate ranking are modularized to be reused for multiple campaigns

1. **src.lambda_functions**
    1. Lambda functions to invoke glue crawler, glue jobs, failure cleanups, validations 
1. **src.match_score**
    1. This module is used to generate feature vectors for user listing interactions using the user and listing features generated from the ETL job of Match Score project
    1. Given an input path for the candidates, this module returns the feature vectors in s3 for a target date
1. **src.new_listings**
    1. This module has the driver method to generate new listings candidates using real profile candidate generation module
    1. The generated candidates are ranked using SageMaker batch transforms invoked by lambda
1. **src.real_profile**
    1. This module is used to generate potential candidates for all the active users in the last 30 days using the output of the Real Profile project
    1. The candidate generation can be configured with different parameters jotted below:
        1. Days on market of the active listings
        1. Status of the listings (for sale/open houses etc.)
        1. Number of targeted zip codes, price variance for the users from Real Profile
1. **src.recommended_homes**
    1. This module has the driver method to post process the LightFM Model recommendations
        1. The recommendations are filtered off if they do not fall in the zip code and price range of the users from Real Profile
1. **src.sqs**
    1. This module is used to push the generated recommendations to the SQS queue
1. **src.utils**
    1. This module has the util methods used across all the modules

## Naming Convention
All the lambdas and the output folders must be prefixed with the campaign name. Any new addition to the notification campaign must follow this convention.
* Prefix `rn` &rarr; recommended notifications (Lambdas which are generic to the recommended notifications pipeline and used across multiple campaigns must be prefixed with `rn`)
    * **Example**: `rn_invoke_crawler`
* Prefix `nl` &rarr; new listings (Lambdas which are specific to the new listings recommendations pipeline must be prefixed with `nl`)
    * **Example**: `rn_nl_invoke_new_listings_candidate_generation_job`
* Prefix `rh` &rarr; recommended homes (Lambdas which are specific to the recommended homes LightFM Model pipeline must be prefixed with `rh`)
    * **Example**: `rn_rh_invoke_recommended_homes_sqs_job`
