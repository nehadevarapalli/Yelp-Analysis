use my_database;
drop table if exists number_of_businesses_each_city_and_state;
create table number_of_businesses_each_city_and_state
row format delimited
fields terminated by ','
lines terminated by '\n'
as
select state, city, count(*) as count
from business
group by state, city;
select * from number_of_businesses_each_city_and_state;
