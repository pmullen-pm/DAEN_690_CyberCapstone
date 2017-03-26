#!/usr/bin/env python
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from collections import Counter
from sets import Set

if __name__== "__main__":
	sc = SparkContext(appName="daen")
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
	customerList = sorted(customerGrp.agg({"*": "count"}).collect())
	#Result:
	#There are 86 customers in our dataset (with excluding all 9s, 5s, 1s)
	customerList = []
	for customer in customerList:
		#customerList is a list of Row objects
		#Row javadoc here: https://spark.apache.org/docs/1.1.1/api/python/pyspark.sql.Row-class.html
		customerId = customer.__getattr__("_c2")
		#if(customerId == 'ccf24ccc529ae3eb9382d5224482fd719e82361c'):
		customerList.append(customerId)
		#Now we have a dictionary of all of the customerIds in the dataset

	ipGrp = lines.groupBy("_c5")
	ipList = sorted(ipGrp.agg({"*": "count"}).collect())
	ipValList = []
	for ip in ipList:
		ipVal = ip.__getattr__("_c5")
		#append each ipVal as a list in the list. (we're beginning to build an RDD)
		ipValList.append(ipVal)
		#There are 1794626 IPs in here. This is how many rows we will have in the end.

	#sc is the default SparkContext variable.
	#Make this into an rdd so we can map over it and generate our new rdd nicely.
	newIpRdd = sc.parallelize(ipValList)

	#This function will be used to take in an IP and all of its relevant data
	#and turn it into the desired row out in tuple form
	#this is (ip, maxScore, lowScore, avgScore, trendUp (1 for yes, 0 for no for score change), trendDown (1 for yes, 0 for no score change) trueCount, customerTotal
	#followed by every customerId and the number of times it appeared for that customer
	#this may need to be a binary but its easy to convert this later to a 1 or 0 if need be.
	#General knowledge: avg score excluding 0s,9s,5s is 3.791
	def generateIpRow(ipAsString, dfList, customerList):
		customerCounter = Counter()
		totalScore = 0.0
		totalAppearances = float(len(dfList))
		highestScore = 0.0
		lowestScore = 10.0
		earliestSeen = float('inf')
		latestSeen = 0.0
		oldestScore = 0.0
		newestScore = 0.0
		highestCount = 0.0
		for row in dfList:
			customer = row.__getattr__("_c2")
			score = float(row.__getattr__("_c9"))
			firstSeen = float(row.__getattr__("_c7"))
			lastSeen = float(row.__getattr__("_c8"))
			count = float(row.__getattr__("_c6"))
			totalScore += score
			if score < lowestScore:
				lowestScore = score
			if score > highestScore:
				highestScore = score
			customerCounter[customer] += 1
			#if we see something with a smaller time (older) first seen
			#adjust oldest score
			if earliestSeen > firstSeen:
				earliestSeen = firstSeen
				oldestScore = score
			#if we see something with a larger (more recent) last seen
			#update newest score
			if latestSeen < lastSeen:
				latestSeen = lastSeen
				newestScore = score
			if highestCount < count:
				highestCount = count

		maxScore = highestScore
		minScore = lowestScore
		avgScore = totalScore / totalAppearances
		#highest value for the count field
		trueCount = highestCount
		#numberOfRows we have in dataset of them
		dataSetCount = totalAppearances

		trendUp = 0.0
		trendDown = 0.0
		if newestScore > oldestScore:
			trendUp = 1.0
		if newestScore < oldestScore:
			trendDown = 1.0

		index = 0
		totalCustomers = 0
		customerCountList = []
		for customer in customerList:
			thisCount = customerCounter[customer]
			customerCountList.append(thisCount)
			index += 1
			if thisCount > 0:
				totalCustomers += 1

		mostCommonCustomer = customerCounter.most_common(1)[0][0]
		returnTuple = (ipAsString, maxScore, minScore, avgScore, trendUp, trendDown, trueCount, dataSetCount,mostCommonCustomer, totalCustomers) + tuple(customerCountList)
		return returnTuple


	#This is certainly not the smartest way to do this, this should fail if the resultant RDD of
	#where the ip occurs is too large.
	#We're mapping an IP and all of the relevant data rows. If we had an ip that had some millions
	#of appearances in the dataset we'd have to be smart and do it in smaller chunks.
	#THIS WORKS BUT TAKES LITERALLY FOREVER. DONT EVER DO THIS. RAN FOR 12 HOURS WITHOUT COMPELTING.
	ipDfDict = {}
	for ip in ipValList:
		ipDfDict[ip] = lines.filter(lines._c5 == ip).collect()
	lines.unpersist()

	#this is (ip, maxScore, lowScore, avgScore, trendUp (1 for yes, 0 for no for score change), trendDown (1 for yes, 0 for no score change) trueCount, customerTotal
	#followed by every customerId and the number of times it appeared for that customer
	#Rdd of tuples
	newIpRdd.map(lambda x: generateIpRow(x,ipDfDict[x], customerList))

	completeRdd = spark.createDataFrame(newIpRdd, ['ip','maxScore','minScore','avgScore','trendUp','trendDown','maxCount','dataSetCount','mostCommonCustomerHit','totalCustomersHit'] + customerList )

	#Saving the csv off as well just because.
	completeRdd.write.csv("s3://daen-cyber/filteredSource/no1s9s6s/testFeature/ipRowData.csv")

