/*
c. Find names of countries which do not have any languages recorded (in the country
language table).
ii. Using outer join
*/

SELECT Name
FROM country
LEFT OUTER JOIN countrylanguage
ON country.code = countrylanguage.CountryCode
where countrylanguage.CountryCode IS NULL;