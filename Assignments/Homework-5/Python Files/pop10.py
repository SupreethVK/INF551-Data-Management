from operator import add

import sys
from pyspark import SparkContext

def splitFunc(row, continent):
	cols = row.split(',')

	if cols[2].replace("'", "").strip() == continent:
		return cols[1].replace("'", ""), int(cols[6].replace("'", ""))
	#return cols[1], "ahhh"

if __name__ == "__main__":

	input_file = "country.csv"
	output_file = "output.txt"

	continent = sys.argv[1]

	sc = SparkContext.getOrCreate()

	lines = sc.textFile(input_file, use_unicode=False)

	result = lines.map(lambda row: splitFunc(row, continent)) \
		 	 .filter(lambda row: row is not None) \
		 	 .sortBy(lambda row: row[1], ascending = False) \
		 	 .map(lambda row: str(row[0]) + "\t" + str(row[1]) + "\n").take(10)


	output_file = open(output_file, "w+")
	output_file.writelines(result)
	output_file.close()