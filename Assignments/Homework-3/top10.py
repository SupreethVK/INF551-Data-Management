"""
Write a Python script “top10.py” (that does not use SQL query, so no SQL connected should be used) 
that takes the country.csv and countrylanguage.csv (these are in the same format as produced in question 1) 
as the input, and answers the query in question q2.f (i.e., finding top 10 languages). It should output 
the same result as the SQL query answer of the question.

Find the top 10 languages by populations of countries where they are official languages, but only the 
countries whose populations are over 1 million are counted. For example, English may be spoked in 5 
countries as official language and 4 of them each has a population of 2 million and one 500K. 
Then the total population count for English would 8 million
"""

import csv

def get_country_data():
    country_header = []
    countries = []
    with open('country.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar = "'")
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                country_header=list(row)
                country_header[0] = country_header[0][2:]
                line_count += 1
            else:
                countries.append(list(row))
                line_count += 1
    return country_header, countries

def get_countrylanguage_data():
    clang_header = []
    clangs = []
    with open('countrylanguage.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar = "'")
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                clang_header=list(row)
                clang_header[0] = clang_header[0][2:]
                line_count += 1
            else:
                clangs.append(list(row))
                line_count += 1
    #print (clang_header)
    return clang_header, clangs

def get_relevant_lang(l):
    r = []
    for i in l:
        if i[2] == 'T':
            r.append([i[0], i[1]])
    return r

def get_rel_count(c):
    rc = []
    for i in c:
        if int(i[6]) >= 1000000:
            rc.append([i[0], i[1], i[6]])
    return rc
    
def get_top10(rel_l, rel_c):
    data = []
    langs = []
    for rl in rel_l:
        for rc in rel_c:
            if rl[0] == rc[0]:
                data.append([rc[1], int(rc[2]), rl[1]]) # [Name, Population, Language]
    
    data.sort(key = lambda x: x[2])
    dt = []
    l = data[0][2]
    pop = data[0][1]
    for i in range(1, len(data)):
        if data[i][2] == l:
            pop += data[i][1]
        else:
            dt.append([l, pop])
            l = data[i][2]
            pop = data[i][1]
            
    dt.sort(reverse = True, key = lambda x: x[1])
    for i in range(0, 10):
        #print(dt[i])
        langs.append(dt[i][0])
    return langs
    
    
ch, c = get_country_data()
lh, l = get_countrylanguage_data()
rel_lang = get_relevant_lang(l)
rel_countries = get_rel_count(c)
top10 = get_top10(rel_lang, rel_countries)
print(top10)

