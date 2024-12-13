-- Find the top 10 most reviewed businesses in each state
use my_database;
drop table if exists top10_reviewed_businesses_per_state;
CREATE TABLE top10_reviewed_businesses_per_state
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
AS
SELECT b.state, b.business_id, b.name, b.review_count
FROM (
    SELECT state, business_id, name, review_count,
           ROW_NUMBER() OVER (PARTITION BY state ORDER BY review_count DESC) AS rank
    FROM business
) b
WHERE b.rank <= 10;

select * from top10_reviewed_businesses_per_state;
