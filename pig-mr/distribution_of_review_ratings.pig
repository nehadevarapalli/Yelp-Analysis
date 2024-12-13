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

-- Group reviews by star rating
grouped_by_stars = GROUP extracted_data BY stars;

-- Count the number of reviews for each star rating
review_count_by_stars = FOREACH grouped_by_stars GENERATE
    group AS star_rating,
    COUNT(extracted_data) AS review_count;

-- Order star ratings in ascending order
ordered_star_ratings = ORDER review_count_by_stars BY star_rating ASC;

-- Display the distribution of review ratings
DUMP ordered_star_ratings;

STORE ordered_star_ratings INTO '/output/ordered_star_ratings' USING PigStorage(',');