#!/usr/bin/env python
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="LogReg")
    spark = SparkSession.builder.appName("downsample").getOrCreate()

    lines = spark.read.csv("s3://daen-cyber/filteredSource/no5s",header=True)
    lines = lines.select([c for c in lines.columns if c not in {'ip','maxScore','minScore','avgScore','trendUp','trendDown','trueCount','dataSetCount','mostCommonCustomerHit'}])

    def labeledPointConverter(row):
        try:
            if float(row[0]) > 5.0:
                return LabeledPoint(1.0, row[1:])
            else:
                return LabeledPoint(0.0,row[1:])
        except ValueError:
            return LabeledPoint(50.0,[1.0])

    parsedData = lines.rdd.map(lambda x: labeledPointConverter(x))
    parsedData = parsedData.filter(lambda x: x.label != 50.0)
    train,test = parsedData.randomSplit([0.65,0.35])
    train.cache()
    model = SVMWithSGD.train(train, iterations =250)
    train.unpersist()
    test.cache()
    labelsAndPreds = test.map(lambda p: (p.label, model.predict(p.features)))
    labelsAndPreds.cache()
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(test.count())
    test.unpersist()
    trainErr =("Training Error = " + str(trainErr))
    truePos = labelsAndPreds.filter(lambda (v, p): v == 1.0 and p == 1.0).count()
    trueNeg = labelsAndPreds.filter(lambda (v, p): v == 0.0 and p == 0.0).count()
    falsePos = labelsAndPreds.filter(lambda (v, p): v == 1.0 and p == 0.0).count()
    falseNeg = labelsAndPreds.filter(lambda (v, p): v == 0.0 and p == 1.0).count()

    listToOutput = []
    listToOutput = listToOutput + [("TrainingError", trainErr)]
    listToOutput = listToOutput + [("True Positive", str(truePos))]
    listToOutput = listToOutput + [("True Negative", str(trueNeg))]
    listToOutput = listToOutput + [("False Positive", str(falsePos))]
    listToOutput = listToOutput + [("False Negative", str(falseNeg))]

    listToOutputRDD = sc.parallelize(listToOutput, 1)\
        .saveAsTextFile("s3://daen-cyber/modelsb/no5sSvmResults0")

    model.save(sc, "s3://daen-cyber/modelsb/no5sSvmModel0")
    #sameModel = SVMModel.load(sc, "target/tmp/pythonSVMWithSGDModel")