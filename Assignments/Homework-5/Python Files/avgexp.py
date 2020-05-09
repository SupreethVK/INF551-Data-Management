from operator import add

import sys
from pyspark import SparkContext


def splitFunc(row):
	cols = row.split(',')

	if float(cols[8].replace("'", "")) > 10000:
		return cols[2], float(cols[7].replace("'", ""))


if __name__ == "__main__":

	input_file = "country.csv"
	output_file = "output.txt"

	#continent = sys.argv[1]

	sc = SparkContext.getOrCreate()

	lines = sc.textFile(input_file)

	'''
	res = lines.map(splitFunc) \
		 .filter(lambda row: row is not None ) \
		 .map(lambda (x,y): (x,[y])) \
		 .reduceByKey(lambda p, q: p+q)\
		 .mapValues(lambda value: value if len(value) >= 5 else 1) \
		 .reduceByKey(lambda value: sum(value)/len(value)) \
		 .map(lambda row: str(row[0]) + "\t" + str(row[1]) + "\n").collect()
	'''

	result = lines.map(splitFunc) \
		 .filter(lambda row: row is not None) \
		 .mapValues(lambda v: (v, 1)) \
		 .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
		 .filter(lambda row: row[1][1] >= 5) \
		 .mapValues(lambda v: v[0]/v[1]) \
    	 .map(lambda row: str(row[0]) + "\t" + str(row[1]) + "\n").collect()


	output_file = open(output_file, "w+")
	output_file.writelines(result)
	output_file.close()