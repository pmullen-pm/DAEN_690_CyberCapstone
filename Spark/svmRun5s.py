#!/usr/bin/env python
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="LogReg")
    spark = SparkSession.builder.appName("downsample").getOrCreate()

    lines = spark.read.csv("s3://daen-cyber/filteredSource/only5s",header=True)
    lines = lines.select([c for c in lines.columns if c not in {'ip','maxScore','minScore','avgScore','trendUp','trendDown','trueCount','dataSetCount','mostCommonCustomerHit'}])

    #for 5s test we dont care about label, just use 1.0
    def labeledPointConverter(row):
        try:
            return LabeledPoint(1.0, row[1:])
        except ValueError:
            return LabeledPoint(50.0,[1.0])

    parsedData = lines.rdd.map(lambda x: labeledPointConverter(x))
    parsedData = parsedData.filter(lambda x: x.label != 50.0)
    parsedData.cache()
    model = SVMModel.load(sc, "s3://daen-cyber/models/no5sSvmModel0")
    preds = parsedData.map(lambda p: model.predict(p.features))
    parsedData.unpersist()
    preds.cache()

    below5 = preds.filter(lambda p: p == 0.0).count()
    above5 = preds.filter(lambda p: p == 1.0).count()

    listToOutput = []
    listToOutput = listToOutput + [("Above 5", str(above5))]
    listToOutput = listToOutput + [("Below5", str(below5))]

    listToOutputRDD = sc.parallelize(listToOutput, 1)\
         .saveAsTextFile("s3://daen-cyber/models/only5sSvmResults0")

    #model.save(sc, "s3://daen-cyber/modelsb/no5sSvmModel0")
    #sameModel = SVMModel.load(sc, "target/tmp/pythonSVMWithSGDModel")