/* 
a. Find the 10 most populated cities in the United States (country name). Output names of
such cities and their populations in the descending order of populations.
*/

select Name, Population from city 
where CountryCode = (Select Code from country where Name="United States") 
order by Population desc
limit 10;

