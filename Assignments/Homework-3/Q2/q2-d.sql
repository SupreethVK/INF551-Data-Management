/*
d. Find names of countries which have at least 10 unofficial languages. Output the
countries by the descending order of the number of such languages.
*/

select a.Name, count(b.Language) as num_languages
from country a, countrylanguage b
where a.Code = b.CountryCode
group by b.CountryCode
having count(b.Language)>=10
order by count(b.Language) desc;
