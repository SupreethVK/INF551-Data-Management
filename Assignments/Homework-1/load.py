import requests
import csv
import json
import re 
import unicodedata
import pandas as pd 
import sys
import numpy as np

def convertToJson(data):
	data = data.to_json(orient='records')
	return data

def createIndex(data, filteredColumns, indexData, csvFilePath):	
	for ind, row in data.iterrows():
		for i in filteredColumns:
			for word in row[i].split():
				word = word.lower()
				if word not in indexData:
					indexData[word] = []
				indexData[word].append({"Table":str(csvFilePath.split(".")[0]), "Column":str(data.columns[i]), "PKey":str(row[0])})

def uploadToFirebase(url, data, csvFilePath):

	response = requests.put(url+csvFilePath.split(".")[0]+'.json', data)
	if response.status_code == 200:
		print("Successfully Uploaded to Firebase")
	else:
		print("Upload Failure")


def cleanItUp(data):
	data.columns = data.columns.map(lambda x: re.sub(r'\W+', '', x))
	filteredColumns = []
	i = 0
	for _ in data.columns:
		if data[_].dtype == np.object:
			data[_] = data[_].astype(str)
			filteredColumns.append(i)
			data[_] = data[_].map(lambda x: re.sub(r'[-[\]/.(\)]', ' ', x))
			data[_] = data[_].map(lambda x: re.sub(r'(& )', '', x))
			data[_] = data[_].map(lambda x: unicodedata.normalize('NFKD', x).encode('ascii', errors='ignore').decode('utf-8'))
		i += 1
	return data, filteredColumns		

if __name__ == "__main__": 
	
	indexData = {}
	for i in range(1, len(sys.argv)):
		#Initializing the Global Variables
		csvFilePath = sys.argv[i]
		url = "https://inf551-89468.firebaseio.com/"
		try:
			data = pd.read_csv(csvFilePath, encoding='utf-8', quotechar="'", skipinitialspace = True)			
		except:
			data = pd.read_csv(csvFilePath, encoding='latin-1', quotechar="'", skipinitialspace = True)				
		data, filteredColumns = cleanItUp(data)
		json_data = convertToJson(data)
		uploadToFirebase(url, json_data, csvFilePath)
		createIndex(data, filteredColumns, indexData, csvFilePath)

	indexData = json.dumps(indexData)
	response = requests.put(url+'index.json', indexData)
	if response.status_code == 200:
		print("Successfully Uploaded INDEX to Firebase")
	else:
		print("INDEX Upload Failure")
		#print(response.text)


