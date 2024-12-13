REGISTER /Users/nehadevarapalli/INFO7250/final-project/Yelp-Analysis/elephantBirdLib/elephant-bird-core-4.17.jar;
REGISTER /Users/nehadevarapalli/INFO7250/final-project/Yelp-Analysis/elephantBirdLib/elephant-bird-hadoop-compat-4.17.jar;
REGISTER /Users/nehadevarapalli/INFO7250/final-project/Yelp-Analysis/elephantBirdLib/elephant-bird-pig-4.17.jar;
REGISTER /Users/nehadevarapalli/INFO7250/final-project/Yelp-Analysis/elephantBirdLib/json-simple-1.1.1.jar;

-- Load data
raw_data = load '/yelp_data/yelp_academic_dataset_business.json' using com.twitter.elephantbird.pig.load.JsonLoader();
extracted_data = foreach raw_data generate (chararray)$0#'business_id' as business_id,
(chararray)$0#'name' as name, (chararray)$0#'address' as address, (chararray)$0#'city' as city,
(chararray)$0#'state' as state, (chararray)$0#'postal_code' as postal_code, (float)$0#'latitude' as latitude,
(float)$0#'longitude' as longitude, (float)$0#'stars' as stars, (int)$0#'review_count' as review_count,
(chararray)$0#'is_open' as is_open, (chararray)$0#'attributes' as attributes,
(chararray)$0#'categories' as categories, (chararray)$0#'hours' as hours;

-- Group businesses by state
grouped_by_state = GROUP extracted_data BY state;

-- Calculate the average review count for each state
avg_reviews_by_state = FOREACH grouped_by_state GENERATE
    group AS state,
    AVG(extracted_data.review_count) AS avg_review_count;

-- Order states by average review count in descending order
ordered_states = ORDER avg_reviews_by_state BY avg_review_count DESC;

-- Display the top 10 states with the highest average review counts
top_10_states = LIMIT ordered_states 10;
DUMP top_10_states;

STORE top_10_states INTO '/output/top_10_states' USING PigStorage(',');