from operator import add

import sys
from pyspark import SparkContext

def splitFunc(row):
	cols = row.split(',')
	return cols[0].replace("'",""), cols[1].replace("'", "")


def splitFunc2(row):
	cols = row.split(',')
	if cols[2].replace("'", "").strip() == 'F':
		return cols[0].replace("'", ""), 1
	#return cols[1], "ahhh"

if __name__ == "__main__":

	input_file1 = "country.csv"
	input_file2 = "countrylanguage.csv"
	output_file = "output.txt"


	sc = SparkContext.getOrCreate()

	lines1 = sc.textFile(input_file1, use_unicode=False)
	lines2 = sc.textFile(input_file2, use_unicode=False)
	
	
	
	op1 = lines1.map(splitFunc)
	op2 = lines2.map(splitFunc2) \
		 .filter(lambda row: row is not None) \
		 .reduceByKey(add) \
		 .filter(lambda row: row[1] >= 10) 
	op3 = op1.join(op2) \
		 .sortBy(lambda row: row[1][1], ascending = False)

	result = op3.map(lambda row: str(row[1][0]) + "\t" + str(row[1][1]) + "\n").collect()
	


	output_file = open(output_file, "w+")
	output_file.writelines(result)
	output_file.close()