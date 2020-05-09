from operator import add

import sys
from pyspark import SparkContext

def splitFunc(row):
	cols = row.split(',')
	return cols[0].replace("'", ""), cols[1].replace("'", "")
	#return cols[1], "ahhh"

if __name__ == "__main__":

	input_file1 = "country.csv"
	input_file2 = "countrylanguage.csv"
	output_file = "output.txt"

	#continent = sys.argv[1]

	sc = SparkContext.getOrCreate()

	lines1 = sc.textFile(input_file1, use_unicode=False)
	lines2 = sc.textFile(input_file2, use_unicode=False)
	
	
	
	op1 = lines1.map(splitFunc)
	op2 = lines2.map(splitFunc)
	op3 = op1.subtractByKey(op2)
	result = op3.values().map(lambda row: str(row) + "\n").collect()
	


	output_file = open(output_file, "w+")
	output_file.writelines(result)
	output_file.close()