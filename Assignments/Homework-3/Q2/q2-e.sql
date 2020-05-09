/*
e. Create a view “LangCnt” that lists country code and the number of languages (both
official and unofficial). Use the view to find the names of countries which has the largest
number of both official and unofficial languages.
*/

CREATE OR replace VIEW LangCnt AS
SELECT CountryCode, count(Language) as num_languages
FROM countrylanguage
group by CountryCode;

select Name 
from country 
where Code in
(select CountryCode from LangCnt
where num_languages = (select max(num_languages) from LangCnt)
);

