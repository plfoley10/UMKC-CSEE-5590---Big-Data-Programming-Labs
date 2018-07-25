import os

os.environ["SPARK_HOME"] = "C:\\Users\\plfoley\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\Users\\plfoley\\winutils"

import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def queryjson(tweet):
    strtweet = tweet.encode('utf-8')
    tweetdict = json.loads(strtweet)
    text = tweetdict["text"]
    strtext = (text.encode('utf-8')).split(' ')
    return strtext

sc = SparkContext("local[2]", "Twitter Demo")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)
IP = "localhost"
Port = 9998
lines = ssc.socketTextStream(IP, Port)

words = lines.flatMap(lambda line: queryjson(line))

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.pprint()
ssc.start()
ssc.awaitTermination()