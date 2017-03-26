#!/usr/bin/env python
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__== "__main__":
    spark = SparkSession.builder.appName("downsample").getOrCreate()
    lines = spark.read.csv("s3://daen-cyber/uncompressed/fullDataset03_01_2017.csv")
    
    #Going to just set the seed here as 42 such that all larger samples are supersets of the smaller samples.
    seed = 42

	#sample(withReplacement, percentage,seed)
    #also going to do a 0.5% sample
    sample = lines.sample(False, 0.005,seed)
    #Save it as a parquet file rather than slow and awful csv.
    sample.write.parquet("s3://daen-cyber/uncompressed/dataset05.parquet")
    #Saving the csv off as well just because.
    sample.write.csv("s3://daen-cyber/uncompressed/dataset05.csv")


    #Do a 1% sample. This is composed of 1788559 rows.
    sample = lines.sample(False, 0.01,seed)
    #Save it as a parquet file rather than slow and awful csv.
    sample.write.parquet("s3://daen-cyber/uncompressed/dataset1.parquet")
    #Saving the csv off as well just because.
    sample.write.csv("s3://daen-cyber/uncompressed/dataset1.csv")

    #2%
    sample = lines.sample(False,0.02,seed)
    sample.write.parquet("s3://daen-cyber/uncompressed/dataset2.parquet")

    #3%
    sample = lines.sample(False,0.03,seed)
    sample.write.parquet("s3://daen-cyber/uncompressed/dataset3.parquet")

    #5%
    sample = lines.sample(False,0.05,seed)
    sample.write.parquet("s3://daen-cyber/uncompressed/dataset5.parquet")

    #10%
    sample = lines.sample(False,0.10,seed)
    sample.write.parquet("s3://daen-cyber/uncompressed/dataset10.parquet")

    #25%
    sample = lines.sample(False,0.25,seed)
    sample.write.parquet("s3://daen-cyber/uncompressed/dataset25.parquet")

    #50%
    sample = lines.sample(False,0.50,seed)
    sample.write.parquet("s3://daen-cyber/uncompressed/dataset50.parquet")
