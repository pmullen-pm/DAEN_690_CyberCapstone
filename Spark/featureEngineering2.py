#!/usr/bin/env python
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, collect_set, concat_ws, udf
from pyspark.sql import Row
from collections import Counter

if __name__== "__main__":
	def genRow(origRow):
		customer = origRow.__getattr__("_c2")
		score = float(origRow.__getattr__("_c9"))
		firstSeen = float(origRow.__getattr__("_c7"))
		lastSeen = float(origRow.__getattr__("_c8"))
		count = float(origRow.__getattr__("_c6"))
		maxScore = score
		minScore = score
		currAvg = score
		trueCount = count
		customerCounter = Counter()
		customerCounter[customer] += 1
		return (maxScore, minScore, currAvg, (firstSeen,score),
				(lastSeen,score), trueCount,customerCounter)


	# this method will take two entries and combine them into our new running avg.
	# they have the form of the genRow return statement.
	def combineIpRows(row0, row1):
		# Current accumulation values
		max0 = row0[0]
		min0 = row0[1]
		avg0 = row0[2]
		firstSeen0 = row0[3][0]
		firstSeenScore0 = row0[3][1]
		lastSeen0 = row0[4][0]
		lastSeenScore0 = row0[4][1]
		trueCount0 = row0[5]
		custCounter0 = row0[6]
		max1 = row1[1]
		min1 = row1[1]
		avg1 = row1[2]
		firstSeen1 = row1[3][1]
		firstSeenScore1 = row1[3][0]
		lastSeen1 = row1[4][1]
		lastSeenScore1 = row1[4][0]
		trueCount1 = row1[5]
		custCounter1 = row1[6]
		max,min,avg,firstSeen,firstSeenScore = None, None, None, None, None
		lastSeen,lastSeenScore, trueCount,custCounter = None, None, None,None
		max = max0 if max0 > max1 else max1
		min = min0 if min0 < min1 else min1
		avg = (avg0 + avg1) / 2.0
		firstSeen = firstSeen0 if firstSeen0 < firstSeen1 else firstSeen1
		firstSeenScore = firstSeenScore0 if firstSeen0 < firstSeen1 else firstSeenScore1
		lastSeen = lastSeen0 if lastSeen0 > lastSeen1 else lastSeen1
		lastSeenScore = lastSeenScore0 if lastSeen0 > lastSeen1 else lastSeenScore1
		trueCount = trueCount0 if trueCount0 > trueCount1 else trueCount1
		custCounter = custCounter0 + custCounter1
		return(max,min,avg,(firstSeen,firstSeenScore),
			   (lastSeen,lastSeenScore),trueCount,custCounter)

	def convertToFeatures(inputTuple, customerList):
		ip = inputTuple[0]
		inVals = inputTuple[1]
		max = inVals[0]
		min = inVals[1]
		avg = inVals[2]
		firstSeen = inVals[3][0]
		firstSeenScore = inVals[3][1]
		lastSeen = inVals[4][0]
		lastSeenScore = inVals[4][1]
		trueCount = inVals[5]
		custCounter = inVals[6]
		trendUp = 1.0 if firstSeenScore < lastSeenScore else 0.0
		trendDown = 1.0 if firstSeenScore > lastSeenScore else 0.0
		avgAbove5 = 1.0 if avg > 5.0 else 0.0
		if max == min:
			trendUp = 0.0
			trendDown = 0.0
		mostCommonCustomer = custCounter.most_common(1)[0][0]
		index = 0
		totalCustomers = len(custCounter.values())
		customerCountList = []
		for customer in customerList:
			thisCount = custCounter[customer]
			customerCountList.append(thisCount)
			index += 1
		return(ip,avgAbove5,trendUp,trendDown,trueCount,
			   mostCommonCustomer,totalCustomers) + tuple(customerCountList)


	spark = SparkSession.builder.appName("downsample").getOrCreate()
	lines = spark.read.csv("s3://daen-cyber/filteredSource/no1s9s5s/targeted.csv")
	#Drop the fields not interested in
	#Don't want customer specific info: just source info.
	#This is going to convert the dataset into IP records not customer records.
	#We only want _c2, _c3, _c4, _c5, _c6, _c7, _c8, _c9
	lines = lines.select(lines['_c2'], lines['_c5'], lines['_c6'], lines['_c7'],lines['_c8'],lines['_c9'])
	lines.cache()
	#Group by the customer label.
	customerGrp = lines.groupBy("_c2")
	#Count number of unique customers in this dataset.
	#GenerateDataFrame
	#customerRow = abc.agg({"*": "count"})

	#To do this as a sorted list format from high to low:
	customerGrpList = []
	customerGrpList = sorted(customerGrp.agg({"*": "count"}).collect())
	#Result:
	#There are 86 customers in our dataset (with excluding all 9s, 5s, 1s)
	customerList = []
	for customer in customerGrpList:
		#customerList is a list of Row objects
		#Row javadoc here: https://spark.apache.org/docs/1.1.1/api/python/pyspark.sql.Row-class.html
		print customer
		customerId = customer.__getattr__("_c2")
		#if(customerId == 'ccf24ccc529ae3eb9382d5224482fd719e82361c'):
		customerList.append(customerId)
		#Now we have a dictionary of all of the customerIds in the dataset

	#this operation is expensive, but we need to setup all the rows in the dataset
	#to fit into our reduce by key function.
	# There are 1794626 IPs in here. This is how many rows we will have in the end.
	linesKeyRdd = lines.rdd.map(lambda x: (x.__getattr__("_c5"),genRow(x)))
	reducedIpRdd = linesKeyRdd.reduceByKey(combineIpRows)
	finalFeatures = reducedIpRdd.map(lambda x: convertToFeatures(x,customerList))

	completeDf = spark.createDataFrame(finalFeatures, ['ip','avgScore > 5','trendUp','trendDown','trueCount','mostCommonCustomerHit','totalCustomersHit'] + customerList )
	#Saving the csv off as well just because.
	completeDf.write.csv("s3://daen-cyber/filteredSource/no1s9s5s/testFeature/featureEngineering2.csv",header=True)

