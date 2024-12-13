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

-- Split the categories field into individual categories
categories_split = FOREACH extracted_data GENERATE
    business_id,
    FLATTEN(STRSPLIT(categories, ',')) AS category;

-- Group by category
grouped_by_category = GROUP categories_split BY category;

-- Count the number of businesses in each category
business_count_by_category = FOREACH grouped_by_category GENERATE
    group AS category,
    COUNT(categories_split) AS business_count;

-- Order categories by business count in descending order
ordered_categories = ORDER business_count_by_category BY business_count DESC;

-- Display the top 10 categories with the most businesses
top_10_categories = LIMIT ordered_categories 10;
DUMP top_10_categories;