"""
2. [40 points] Write a Python script “search.py” that takes a list of keywords 
delimited by white spaces and returns primary key values of tuples which contain 
at least one keyword in the values of its attributes. For example, searching for 
“America” will return:
country: “ABW”, “AIA”, ...
city: ... (if the keyword also appears in some city tuples) countrylanguage: ...
Note that search is NOT case-sensitive.
Also note that the tuples that contain more search keywords should be listed first. 
For example, tuples that contain “South America” should be returned after tuples 
containing “North America”, if the search query is “north america”. Note also that 
the keywords may appear in multiple attributes of a tuple.
The search program should utilize the index built above.
"""

import requests 
import json 
import sys
import pandas as pd



def retrieve_tuples(word, data):

	temp = data[word]
	#print(temp[0])
	countryR = []
	cityR = []
	languageR = []
	for i in range(len(temp)):
		if temp[i]['Table'] == 'country':
			countryR.append(temp[i]['Code'])
		if temp[i]['Table'] == 'city':
			cityR.append(temp[i]['ID'])
		if temp[i]['Table'] == 'countrylanguage':
			languageR.append(temp[i]['CountryCode'])

	print("Country: ", countryR)
	print("City: ", cityR)
	print("languageR: ", languageR)


if __name__ == "__main__": 

	url = "https://inf551-89468.firebaseio.com/index.json"

	indexData = requests.get(url)
	indexData = json.loads(indexData.text)
	#print(indexData)
	results = []

	for i in range(1, len(sys.argv)):
		keyword = sys.argv[i].lower()

		retrieve_tuples(keyword, indexData)




