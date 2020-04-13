import re

from numpy.ma import sqrt
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.shell import spark
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeans
from pyspark.streaming.kafka import KafkaUtils

#spark = SparkSession.builder.master("spark://192.168.56.1:7077").appName("Network Log Analyser").getOrCreate()

#set up spark config
conf = SparkConf().setAppName("Network Log Analyser").setMaster("spark://192.168.56.1:7077")

sc = SparkContext.getOrCreate(conf = conf)

ssc = StreamingContext(sc, 60)
ssc.checkpoint('ckpt')

APACHE_ACCESS_LOG_PATTERN = "(?P<source>.+?) \[\d+:\d+:\d+:\d+\] \".+?\" (?P<response_code>\d+) .+?"

#df = spark.readStream\
 #   .format("kafka").\
  #  option("kafka.bootstrap.servers", "localhost:9092")\
   # .option("subscribe", "test")\
    #.load()
#df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

print('from kafka')
#print(df)

def parse_log_line(logline, pattern):
    line = re.match(pattern, logline)
    logline = line.groupdict()
    print('logline now ')
    print(logline)

    nxx = 0
    twoxx = 0

    # If response code is 2xx, then we set the count for twoxx as 1
    if str(logline['response_code']).startswith("2"):
        twoxx = 1
    # If response code is 5xx, then we set the count for nxx is 1
        nxx = 1

    # return data in the form (1.1.1.1, [1, 0])
    return (logline['source'], [twoxx, nxx])

def extract_features(val1, val2):
    #add up counts of 2xx and 5xx for each of the source IP
    #val1 is for 1strecord and val2 for second record of same source ip. likewise until all the records are added
    twoxx = val1[0] + val2[0]
    nxx = val1[1] + val2[1]

    return [twoxx, nxx]

def load(stream, model):
    #rawTestingData = stream.map(lambda s: parse_log_line(s, APACHE_ACCESS_LOG_PATTERN))
    print('load called')
    testing_data = stream.reduceByKeyAndWindow(extract_features, lambda a, b: [a[0] - b[0], a[1] - b[1]], 60, 60).\
                    map(lambda s: predict_cluster(s, model))

    print("before printng")
    print(testing_data)
    return testing_data



# Cluster Prediction and distance calculation
def predict_cluster(row, model):
    # Predict the cluster for the current data row
    cluster = model.predict(row[1])

    # Find the center for the current cluster
    center = model.centers[cluster]

    # Calculate the disance between the Current Row Data and Cluster Center
    distance = sqrt(sum([x ** 2 for x in (row[1] - center)]))

    return (row[0], distance, {"cluster": model.predict(row[1]), "twoxx": row[1][0], "nxx": row[1][1]})


logfilepath = 'D:\\Big data\\PycharmSource\\ML network detection\\test_access_log.txt'

trainingData = sc.textFile(logfilepath)

#file:///D:/Big data/PycharmSource/ML network detection/test_access_log
#process raw data logs using map and parse each logline

rawTrainingData = trainingData.map(lambda s: parse_log_line(s, APACHE_ACCESS_LOG_PATTERN)).cache()

print('total lines ')
print(rawTrainingData.count())

#process the rawTrainingdata RDD toeach hostkey using mapreducebykey. mapreduceBykey returns KV. Reduce returns a single value
rawTrainingData = rawTrainingData.reduceByKey(extract_features)

#K-means accepts data in the form of [a, b] its called feature vector. use vector assembler or map function.
training_dataset =rawTrainingData.map(lambda data: data[1])

print("before training dataset")
print(training_dataset.count())
print(training_dataset)
print("hoii")

#set cluster count equals to 2
cluster_count = 2

#train the k-means algo to get the model
trained_model = KMeans.train(training_dataset, cluster_count)

#print the cluster centroids from trained model
for center in range(cluster_count):
    print('centre me')
    print(trained_model.centers[center])

#streamingData = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group", {"test" : 1})
#lines = streamingData.map(lambda x:x[1])
print("hiiiii")
#print(lines)

#counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
#counts.pprint()

# Process incoming JSON Logs from Kafka Stream
#streamingData = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group", {"test" : 1}).cache()
#localStream = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group", {"test": 1})#.cache()
#print(localStream)
#rawTestingData = streamingData.map(lambda s: parse_log_line(s, APACHE_ACCESS_LOG_PATTERN))

# Reduce 60 seconds of incoming data in a 60 seconds sliding window and calculate distance from cluster center it belongs to
#testing_data = rawTestingData.reduceByKeyAndWindow(extract_features, lambda a, b: [a[0] - b[0], a[1] - b[1]], 60, 60).map(lambda s: predict_cluster(s, trained_model))

stream_data = ssc.textFileStream("file:///D:/logsfolder").map(lambda s: parse_log_line(s, APACHE_ACCESS_LOG_PATTERN)).reduceByKeyAndWindow(extract_features, lambda a, b: [a[0] - b[0], a[1] - b[1]], 60, 60).map(lambda s: predict_cluster(s, trained_model))
    #map( lambda x: x.split(" "))
#map(lambda s: parse_log_line(s, APACHE_ACCESS_LOG_PATTERN))

print("please please")
stream_data.pprint()
print(stream_data.count())

dataRDD = load(stream_data, trained_model)
print(dataRDD)
print("yukkkkk")


#print(dataRDD)
ssc.start()
ssc.awaitTermination()

sc.stop()



















