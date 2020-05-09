/*
b. Find name, continent, and population of countries whose name contains “united” and
population is at least 1 million. (note the “like” operator in SQL is NOT case sensitive).
*/

select Name, Continent, Population from country
where Name like "%united%"
and Population >= 1000000;