/*
f. Find the top 10 languages by populations of countries where they are official languages,
but only the countries whose populations are over 1 million are counted. For example,
English may be spoked in 5 countries as official language and 4 of them each has a
population of 2 million and one 500K. Then the total population count for English would
8 million.
*/

select a.Language, sum(b.Population) as Tot_Pop
from countrylanguage a, country b 
where a.CountryCode = b.Code
and a.isOfficial = 'T'
and b.Population >= 1000000
group by a.language
order by sum(b.Population) desc
limit 10;
