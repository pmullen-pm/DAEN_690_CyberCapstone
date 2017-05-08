from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, LongType, MapType, ArrayType
import pyspark.sql.functions as func
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier, LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from operator import add
from graphframes import *
import re
import uuid
import iptools

# Establish a session on the cluster
spark = SparkSession.builder.master('URI FOR SPARK').config('spark.executor.memory', '16g').appName('SparkDark3').getOrCreate()
# IPv4 regular expression
ipv4 = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
# Datastore URI root.
datastore = 'URI FOR HDFS'

def dt(x):
    if x.last_seen and x.first_seen:
        dt = long(x.last_seen) - long(x.first_seen)
    else:
        dt = long(0)
    y = list(x)
    y.append(dt)
    return tuple(y)

# Simple label assignment of Dark^3 scores.
def rescore(x):
    if x.dark3_score > 5:
        s = 'High'
    elif x.dark3_score < 5:
        s = 'Low'
    else:
        s = 'Neutral'
    return (s, x.count_seen, x.country)

# Re-scoring of the records to account for shift in scores for all observations.
def set_rescore(x):
    high = [6, 7, 8, 9]
    low = [0, 1, 2, 3, 4]
    th = [c for c in x.scores if c in high]
    tl = [c for c in x.scores if c in low]
    if 5 in x.scores:
        if len(th) > 0:
            return (x.data, 'Trends_High')
        elif len(tl) > 0:
            return (x.data, 'Trends_Low')
        else:
            return (x.data, 'Neutral')
    else:
        if len(th) > 0 and len(tl) == 0:
            return (x.data, 'Started_High')
        elif len(th) > 0 and len(tl) > 0:
            return (x.data, 'Started_Low_Went_High')
        elif len(th) == 0 and len(tl) > 0:
            return (x.data, 'Stayed_Low')
        else:
            return (x.data, 'Unknown_State')
        
# Function that checks if a string is a IPv4 address.
def isIP(x):
    if ipv4.match(x):
        return True
    else:
        return False
    
# Uses a Pandas DataFrame to perform search for IP geolocation.
def geo_lookup(row, data):
    if row.host:
        if isIP(row.host):
            ip = iptools.ipv4.ip2long(row.host)
            d = data[(data['start'] <= ip) & (data['stop'] >= ip)]
            if len(d) > 0:
                d = d.iloc[0]
                return (row.host, d['continent_name'], d['country_name'])
            else:
                return (row.host, 'Unknown', 'Unknown')
        else:
            return (row.host, 'Unknown', 'Unknown')
    else:
        return row

# Method for trimming long host records, e.g. abc.123.rfg.example.com becomes rfg.example.com
def trimHosts(x):
    data = x.data.strip()
    if isIP(data):
        pass
    else:
        max_depth = 2
        ctr = 0
        result = str()
        for i in reversed(data.split('.')):
            if ctr != 0:
                result = i + '.' + result
            else:
                result = i
            ctr += 1
            if ctr == max_depth:
                break
        data = result
    return (str(uuid.uuid4()), 'HOST', data)

def weightGen(x):
    if x.count_seen:
        try:
            i = float(x.count_seen)
        except:
            i = 0.0
        return (1, i)
    else:
        return (1, 0.0)

def edgeGen(x, total):
    if x.count_seen:
        w = float(x.count_seen) / float(total)
    else:
        print x.count_seen
        w = 0.0
    return [(x.anonymous_id, x.data, w), (x.data, x.anonymous_id, w)]

# Returns the default Dark^3 schema as StructType for DataFrames.
def get_dark3_schema():
    schema = StructType()
    schema.add('record_id', StringType(), True).add('report_date', StringType(), True).add('anonymous_id', StringType(), True)
    schema.add('sector', StringType(), True).add('size', StringType(), True).add('data', StringType(), True)
    schema.add('count_seen', DoubleType(), True).add('first_seen', StringType(), True).add('last_seen', StringType(), True)
    schema.add('dark3_score', DoubleType(), True).add('estimate', StringType(), True)
    return schema

def get_dark3_enriched_schema():
    schema = StructType()
    schema.add('record_id', StringType(), True).add('report_date', StringType(), True).add('anonymous_id', StringType(), True)
    schema.add('sector', StringType(), True).add('size', StringType(), True)
    schema.add('count_seen', DoubleType(), True).add('first_seen', StringType(), True).add('last_seen', StringType(), True)
    schema.add('dark3_score', DoubleType(), True).add('estimate', StringType(), True).add('data', StringType(), True)
    schema.add('continent', StringType(), True).add('country', StringType(), True).add('dark3_rescore', StringType(), True)
    schema.add('elapsed', LongType(), True)
    return schema

def get_featureengineering_schema():
    schema = StructType()
    schema.add('ip', StringType(), True).add('maxScore', DoubleType(), True).add('minScore', DoubleType(), True)
    schema.add('avgScore', DoubleType(), True).add('lastScore', DoubleType(), True).add('trendUp', DoubleType(), True)
    schema.add('trendDown', DoubleType(), True).add('trueCount', DoubleType(), True).add('dataSetCount', DoubleType(), True)
    schema.add('mostCommonCustomerHit', StringType(), True).add('totalCustomersHit', DoubleType(), True)
    schema.add('0549ca77de276efd3b57753b1489a17748b85da3', DoubleType(), True).add('07d5d0d8f59cd398a6beeb58a06d4c522a57f112', DoubleType(), True)
    return schema


# Returns the graph node schema as StructType for DataFrames.
def get_graph_node_schema():
    schema = StructType()
    schema.add('id', StringType(), True)
    schema.add('label', StringType(), True)
    schema.add('data', StringType(), True)
    return schema

def get_graph_edge_schema():
    schema = StructType()
    schema.add('src', StringType(), True)
    schema.add('dst', StringType(), True)
    schema.add('weight', DoubleType(), True)
    return schema

# Returns the schema for the geolocation dataset as StuctType for DataFrames.
def get_ipdata_schema():
    schema = StructType()
    schema.add('start', LongType(), True)
    schema.add('stop', LongType(), True)
    schema.add('continent_code', StringType(), True)
    schema.add('continent_name', StringType(), True)
    schema.add('country_iso_code', StringType(), True)
    schema.add('country_name', StringType(), True)
    return schema

# Returns the schema for the host geolocation enriched dataset as StructType for DataFrames.
def get_enrich_schema():
    schema = StructType()
    schema.add('host', StringType(), True)
    schema.add('continent', StringType(), True)
    schema.add('country', StringType(), True)
    return schema

def get_rescored_schema():
    schema = StructType()
    schema.add('dark3_rescore', StringType(), True).add('count_seen', DoubleType(), True).add('country', StringType(), True)
    return schema

def get_rescored_schema2():
    schema = StructType()
    schema.add('data', StringType(), True).add('dark3_rescore', StringType(), True)
    return schema

def get_dt_schema():
    schema = StructType()
    schema.add('record_id', StringType(), True).add('dt', LongType(), True)
    return schema

# Returns the default Dark^3 headers as a list for CSV files.
def get_dark3_headers():
    return ['record_id', 'report_date', 'anonymous_id', 'sector', 'size', 'data', 'count_seen', 'first_seen', 'last_seen', 'dark3_score', 'estimate']

# Filters out data which is not in the middle or extremes.
def get_targeted(df):
    try:
        targeted = spark.read.parquet('%s/targeted.parquet' % datastore)
    except:
        targeted = df.filter(df.dark3_score > 0).filter(df.dark3_score < 9).filter(df.dark3_score != 5)
        targeted.write.parquet(path='%s/targeted.parquet' % datastore, mode='ignore')    
    return targeted

# Creates a graph topology from the Dark^3 dataset to be used in GraphFrames.
def create_graph(df):
    try:
        v = spark.read.parquet('%s/graph/v.parquet' % datastore)
    except:
        totalEvents = df.rdd.map(weightGen).reduceByKey(add).collect()
        totalEvents = float(totalEvents[0][1])

        # Create nodes from hosts
        try:
            host_nodes = spark.read.parquet('%s/graph/host_nodes.parquet' % datastore)
        except:
            hosts = df.groupBy(['data']).agg({'count_seen':'sum'}).withColumnRenamed('SUM(count_seen)', 'count_seen')
            rdd = hosts.rdd.map(lambda x: trimHosts(x))
            host_nodes = spark.createDataFrame(data=rdd, schema=get_graph_node_schema())
            host_nodes.write.parquet(path='%s/graph/host_nodes.parquet' % datastore, mode='ignore')
            
        # Create nodes from customer sensors
        try:
            sensors = spark.read.parquet('%s/graph/sensor_nodes.parquet' % datastore)
        except:
            sensors = df.groupBy(['anonymous_id', 'sector', 'size']).agg({'count_seen':'sum'}).withColumnRenamed('SUM(count_seen)', 'count_seen')
            rdd = sensors.rdd.map(lambda x: (str(uuid.uuid4()), 'CUSTOMER', x.anonymous_id))
            sensor_nodes = spark.createDataFrame(data=rdd, schema=get_graph_node_schema())
            sensor_nodes.write.parquet(path='%s/graph/sensor_nodes.parquet' % datastore, mode='ignore')
            
        # Create edges from connections
        try:
            e = spark.read.parquet('%s/graph/e.parquet' % datastore)
        except:
            connections = df.groupBy(['anonymous_id', 'data']).agg({'count_seen':'sum'}).withColumnRenamed('SUM(count_seen)', 'count_seen')
            rdd = connections.rdd.flatMap(lambda x : edgeGen(x, totalEvents))
            e = spark.createDataFrame(data=rdd, schema=get_graph_edge_schema())
            e.write.parquet(path='%s/graph/e.parquet' % datastore, mode='ignore')
        
        v = sensor_nodes.union(host_nodes)
        v.write.parquet(path='%s/graph/v.parquet' % datastore, mode='ignore')     
    return GraphFrame(v, e)
    
# Run series of built-in graph analytic algorithms.
def run_graph_analytics(g):
    g.pageRank(resetProbability=0.15, maxIter=20).vertices.select('id', 'pagerank').write.parquet(path='%s/graph/pageRank.parquet' % datastore, mode='ignore')
    g.stronglyConnectedComponents(20).write.parquet(path='%s/graph/stronglyConnected.parquet' % datastore, mode='ignore')
    # g.svdPlusPlus()
    g.triangleCount().write.parquet(path='%s/graph/triangles.parquet' % datastore, mode='ignore')
    g.labelPropagation(100).write.parquet(path='%s/graph/labelPropagation.parquet' % datastore, mode='ignore')     

def geolocation_enrich(df):
    try:
        enriched = spark.read.parquet('%s/geo_ip.parquet' % datastore)
    except:
        hosts = df.groupBy(['data']).agg({'count_seen':'sum'}).withColumnRenamed('SUM(count_seen)', 'count_seen').withColumnRenamed('data', 'host')
        ip_data = spark.read.csv(path='%s/ipData.csv' % datastore, schema=get_ipdata_schema(), header=True).toPandas()
        tagged = hosts.rdd.map(lambda x: geo_lookup(x, ip_data))
        enriched = spark.createDataFrame(data=tagged, schema=get_enrich_schema())
        enriched.write.parquet(path='%s/geo_ip.parquet' % datastore, mode='ignore')
    return enriched

def combine_geolocation(df):    
    try:
        combined = spark.read.parquet('%s/combined.parquet' % datastore)
    except:
        enriched = geolocation_enrich(df)
        combined = df.join(enriched, enriched.data == df.data, 'inner').drop(df.data)
        combined.write.parquet(path='%s/combined.parquet' % datastore, mode='ignore')
    return combined

# Create stat tables for visualizations and simpler analytics.
def create_stat_tables(df):
    try:
        customers = spark.read.csv(path='%s/stats/customers.csv' % datastore, header=True)
    except:
        customers = df.groupBy(['anonymous_id', 'sector']).agg({'anonymous_id':'count'}).withColumnRenamed('count(anonymous_id)', 'count')
        customers.write.csv(path='%s/stats/customers.csv' % datastore, header=True)
    try:
        time = spark.read.csv(path='%s/stats/time.csv' % datastore, header=True)
    except:
        time = df.groupBy(['first_seen']).agg({'first_seen':'count'}).withColumnRenamed('count(first_seen)', 'count')
        time.write.csv(path='%s/stats/time.csv' % datastore, header=True)
    try:
        scores = spark.read.csv(path='%s/stats/scores.csv' % datastore, header=True)
    except:
        scores = df.groupBy(['dark3_score']).agg({'dark3_score':'count'}).withColumnRenamed('count(dark3_score)', 'count')
        scores.write.csv(path='%s/stats/scores.csv' % datastore, header=True)
    try:
        sectors = spark.read.csv(path='%s/stats/sectors.csv' % datastore, header=True)
    except:
        sectors = df.groupBy(['sector']).agg({'sector':'count'}).withColumnRenamed('count(sector)', 'count')
        sectors.write.csv(path='%s/stats/sectors.csv' % datastore, header=True)
    try:
        rescored = spark.read.csv(path='%s/stats/location_rescored.csv' % datastore, header=True)
    except:
        # Not checking for existence of the 'combined.parquet' file so can still throw error.
        df = spark.read.parquet('%s/combined.parquet' % datastore)
        rdd = df.rdd.map(lambda x: rescore(x))
        summarized = spark.createDataFrame(data=rdd, schema=get_rescored_schema())
        rescored = summarized.groupBy(['country', 'dark3_rescore']).agg({'country':'count'}).withColumnRenamed('count(country)', 'count')
        rescored.write.csv(path='%s/stat/location_scores_reduced.csv' % datastore, header=True)

def label_rescore(df):
    try:
        rescored = spark.read.parquet('%s/score_direction.parquet' % datastore)
    except:
        summarized = df.groupBy(['data']).agg({'dark3_score':'collect_set'}).withColumnRenamed('collect_set(dark3_score)', 'scores')
        rdd = summarized.rdd.map(lambda x : set_rescore(x))
        rescored = spark.createDataFrame(data=rdd, schema=get_rescored_schema2()).groupBy(['data', 'dark3_rescore']).agg({'dark3_rescore':'count'}).withColumnRenamed('count(dark3_rescore)', 'count')
        rescored.write.parquet(path='%s/score_direction.parquet' % datastore)
    return rescored

def feature_enrich(df):
    try:
        enriched = spark.read.parquet('%s/enriched1.parquet' % datastore)
    # enriched2 = spark.createDataFrame(data=enriched.rdd.map(lambda x : dt(x)), schema=get_dark3_enriched_schema())
        # enriched2.write.parquet(path='%s/feature_enriched.parquet' % datastore, mode='ignore')
    except:
        geo = geolocation_enrich(df)
    rescored = label_rescore(df)
    combined = df.join(geo, geo.data == df.data, 'inner').drop(df.data)
    enriched = combined.join(rescored, rescored.data == combined.data, 'inner').drop(rescored.data).drop('count')
    # enriched.write.parquet('%s/enriched1.parquet' % datastore)
    enriched2 = spark.createDataFrame(data=enriched.rdd.map(lambda x : dt(x)))
    enriched2.write.parquet(path='%s/feature_enriched.parquet' % datastore, mode='ignore')

def ml_indexing():
    df = spark.read.parquet('%s/feature_enriched.parquet' % datastore).groupBy(['data', 'continent', 'country', 'count_seen', 'dark3_rescore', 'elapsed', 'sector', 'size', 'estimate']).agg({'data':'count'}).drop('count(data)')
    sectorIndexer = StringIndexer(inputCol='sector', outputCol='sectorIndex')
    sizeIndexer = StringIndexer(inputCol='size', outputCol='sizeIndex')
    sectorIndexed = sectorIndexer.fit(df).transform(df)
    sizeIndexed = sizeIndexer.fit(sectorIndexed).transform(sectorIndexed)
    estimateIndexer = StringIndexer(inputCol='estimate', outputCol='estimateIndex')
    estimateIndexed = estimateIndexer.fit(sizeIndexed).transform(sizeIndexed)
    # countryIndexer = StringIndexer(inputCol='country', outputCol='countryIndex')
    # countryIndexed = countryIndexer.fit(estimateIndexed).transform(estimateIndexed)
    rescoreIndexer = StringIndexer(inputCol='dark3_rescore', outputCol='rescoreIndex')
    rescoreIndexed = rescoreIndexer.fit(estimateIndexed).transform(estimateIndexed)
    continentIndexer = StringIndexer(inputCol='continent', outputCol='continentIndex')
    continentIndexed = continentIndexer.fit(rescoreIndexed).transform(rescoreIndexed) 
    return continentIndexed.select('sizeIndex', 'sectorIndex', 'estimateIndex', 'rescoreIndex', 'continentIndex', 'elapsed', 'count_seen')

def ml_df(x):
    return (x.rescoreIndex, Vectors.dense([x.sizeIndex, x.sectorIndex, x.estimateIndex, x.continentIndex, x.elapsed, x.count_seen]))

def ml_df2(x):
    y = list()
    for key in x.asDict():
        if key == 'ip':
            pass 
        elif key == 'lastScore':
            pass
        elif key == 'mostCommonCustomerHit':
            pass
        elif key == 'avgScore':
            pass
        elif key == 'minScore':
            pass
        elif key == 'maxScore':
            pass
        else:
            y.append(x[key])
    if x.lastScore > 5.0:
        label = 1.0
    else:
        label = 0.0
    features = Vectors.dense(y)
    return (label, features)
    
def ml_tree(targeted=False):
    try:
        df = spark.read.parquet('%s/ml/ml_full.parquet' % datastore)
    except:
        df = spark.createDataFrame(data=ml_indexing().rdd.map(lambda x : ml_df(x)), schema=['label', 'features']).dropna()
        df.write.parquet(path='%s/ml/ml_full.parquet' % datastore)
    if targeted:
        df = df.filter(df.label > 0).filter(df.label < 9).filter(df.label != 5)
    dt = DecisionTreeClassifier(maxDepth=2)
    grid = ParamGridBuilder().addGrid(3, [0, 5]).build()
    evaluator = MulticlassClassificationEvaluator()
    cv = CrossValidator(estimator=dt, estimatorParamMaps=grid, evaluator=evaluator, numFolds=10)
    model = cv.fit(df)
    print evaluator.evaluate(model.transform(df))
    print model.bestModel.featureImportances()

def ml_tree2():
    try:
        df = spark.read.parquet('%s/ml/ml_feature_engineered.parquet' % datastore)
    except:
        df = spark.createDataFrame(data=spark.read.parquet('%s/featureEngineering.parquet' % datastore).rdd.map(lambda x : ml_df2(x)), schema=['label', 'features']).dropna()
        df.write.parquet(path='%s/ml/ml_feature_engineered.parquet' % datastore)
    dt = DecisionTreeClassifier(maxDepth=2)
    grid = ParamGridBuilder().addGrid(3, [0, 5]).build()
    evaluator = MulticlassClassificationEvaluator()
    cv = CrossValidator(estimator=dt, estimatorParamMaps=grid, evaluator=evaluator, numFolds=10)
    model = cv.fit(df)
    print evaluator.evaluate(model.transform(df))
    print model.bestModel.featureImportances()
        
def ml_rf(seed=90):
    try:
        df = spark.read.parquet('%s/ml/ml_full.parquet' % datastore)
    except:
        df = spark.createDataFrame(data=ml_indexing().rdd.map(lambda x : ml_df(x)), schema=['label', 'features']).dropna()
        df.write.parquet(path='%s/ml/ml_full.parquet' % datastore)
    rf = RandomForestClassifier(numTrees=3, maxDepth=2, seed=seed)
    grid = ParamGridBuilder().addGrid(3, [0, 5]).build()
    evaluator = MulticlassClassificationEvaluator()
    cv = CrossValidator(estimator=rf, estimatorParamMaps=grid, evaluator=evaluator, numFolds=10)
    model = cv.fit(df)
    print evaluator.evaluate(model.transform(df))
    print model.bestModel.featureImportances()
    
def ml_rf2(seed=90):
    try:
        df = spark.read.parquet('%s/ml/ml_feature_engineered.parquet' % datastore)
    except:
        df = spark.createDataFrame(data=spark.read.parquet('%s/featureEngineering.parquet' % datastore).rdd.map(lambda x : ml_df2(x)), schema=['label', 'features']).dropna()
        df.write.parquet(path='%s/ml/ml_feature_engineered.parquet' % datastore)
    rf = RandomForestClassifier(numTrees=3, maxDepth=2, seed=seed)
    grid = ParamGridBuilder().addGrid(3, [0, 5]).build()
    evaluator = BinaryClassificationEvaluator()
    cv = CrossValidator(estimator=rf, estimatorParamMaps=grid, evaluator=evaluator, numFolds=10)
    model = cv.fit(df)
    print evaluator.evaluate(model.transform(df))
    print model.bestModel.featureImportances()    

def ml_lr():
    try:
        df = spark.read.parquet('%s/ml/ml_full.parquet' % datastore)
    except:
        df = spark.createDataFrame(data=ml_indexing().rdd.map(lambda x : ml_df(x)), schema=['label', 'features']).dropna()
        df.write.parquet(path='%s/ml/ml_full.parquet' % datastore)
    mlr = LogisticRegression(maxIter=10)
    grid = ParamGridBuilder().addGrid(3, [0, 5]).build()
    evaluator = MulticlassClassificationEvaluator()
    cv = CrossValidator(estimator=mlr, estimatorParamMaps=grid, evaluator=evaluator, numFolds=10)
    model = cv.fit(df)
    print evaluator.evaluate(model.transform(df))
    
def ml_lr2():
    try:
        df = spark.read.parquet('%s/ml/ml_feature_engineered.parquet' % datastore)
    except:
        df = spark.createDataFrame(data=spark.read.parquet('%s/featureEngineering.parquet' % datastore).rdd.map(lambda x : ml_df2(x)), schema=['label', 'features']).dropna()
        df.write.parquet(path='%s/ml/ml_feature_engineered.parquet' % datastore)
    mlr = LogisticRegression(maxIter=10)
    grid = ParamGridBuilder().addGrid(3, [0, 5]).build()
    evaluator = BinaryClassificationEvaluator()
    cv = CrossValidator(estimator=mlr, estimatorParamMaps=grid, evaluator=evaluator, numFolds=10)
    model = cv.fit(df)
    print evaluator.evaluate(model.transform(df))

def ml_gbt(train_split=0.70, seed=90):
    try:
        df = spark.read.parquet('%s/ml/ml_feature_engineered.parquet' % datastore)
    except:
        df = spark.createDataFrame(data=spark.read.parquet('%s/featureEngineering.parquet' % datastore).rdd.map(lambda x : ml_df2(x)), schema=['label', 'features']).dropna()
        df.write.parquet(path='%s/ml/ml_feature_engineered.parquet' % datastore)
    gbt = GBTClassifier(maxIter=5, maxDepth=2, seed=seed)
    grid = ParamGridBuilder().addGrid(3, [0, 5]).build()
    evaluator = BinaryClassificationEvaluator()
    cv = CrossValidator(estimator=gbt, estimatorParamMaps=grid, evaluator=evaluator, numFolds=10)
    model = cv.fit(df)
    print evaluator.evaluate(model.transform(df))
    print model.bestModel.featureImportances()
    
if __name__ == '__main__':
    # Read-in the Dark^3 dataset as a csv file and include schema
    # df = spark.read.csv(path='%s/big_export_03_01_2017.csv' % datastore, schema=get_dark3_schema())
    # Print out a sample of the DataFrame to ensure it loaded
    # df.show()
    
    # Get the data which is not scored as 0, 5, or 9
    # targeted = get_targeted(df)
    # targeted.show()
    
    # Create the graph topology
    # g = create_graph(df)
    # run_graph_analytics(g)
    
    # combined = combine_geolocation(df)    
    # combined.show()
    # feature_enrich(df)

    ml_tree()
    ml_tree2()
    ml_rf()
    ml_rf2()
    ml_gbt()
    ml_lr()
    ml_lr2()
