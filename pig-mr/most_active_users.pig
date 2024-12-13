REGISTER /Users/nehadevarapalli/INFO7250/final-project/Yelp-Analysis/elephantBirdLib/elephant-bird-core-4.17.jar;
REGISTER /Users/nehadevarapalli/INFO7250/final-project/Yelp-Analysis/elephantBirdLib/elephant-bird-hadoop-compat-4.17.jar;
REGISTER /Users/nehadevarapalli/INFO7250/final-project/Yelp-Analysis/elephantBirdLib/elephant-bird-pig-4.17.jar;
REGISTER /Users/nehadevarapalli/INFO7250/final-project/Yelp-Analysis/elephantBirdLib/json-simple-1.1.1.jar;

-- Load data
raw_data = load '/yelp_data/yelp_academic_dataset_review.json' using com.twitter.elephantbird.pig.load.JsonLoader();
extracted_data = foreach raw_data generate (chararray)$0#'review_id' as review_id,
(chararray)$0#'user_id' as user_id, (chararray)$0#'business_id' as business_id,
(float)$0#'stars' as stars, (int)$0#'useful' as useful, (int)$0#'funny' as funny, (int)$0#'cool' as cool, (chararray)$0#'text' as text,
(chararray)$0#'date' as date;

-- Group reviews by user_id
grouped_by_user = GROUP extracted_data BY user_id;

-- Count the number of reviews per user
review_count_per_user = FOREACH grouped_by_user GENERATE
    group AS user_id,
    COUNT(extracted_data) AS review_count;

-- Order users by review count in descending order
ordered_users = ORDER review_count_per_user BY review_count DESC;

-- Display the top 10 most active users
top_10_users = LIMIT ordered_users 10;
DUMP top_10_users;

STORE top_10_users INTO '/output/top_10_users' USING PigStorage(',');