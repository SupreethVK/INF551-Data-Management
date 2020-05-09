/*
c. Find names of countries which do not have any languages recorded (in the country
language table).
i. Using subquery
*/

select Name from country
where Code not in (select CountryCode from countrylanguage);