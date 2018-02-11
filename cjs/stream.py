from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import redis

sc = SparkContext()
ssc = StreamingContext(sc, 10)

zkQuorum = 'localhost:2181' # port for consumer
topic = 'cjs' # topic name

kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

# split line by " and take the second part, the unnormal line will be filled with "NA"
rdd_s1 = kvs.map(lambda x: x[1]).map(lambda x: (x.split("\"")[0], x.split("\"")[1]) if len(x.split("\""))==7 else "NA").filter(lambda x: type(x)!=str)
# further split by " " and take out the ip, visit datetime and page
rdd_s2 = rdd_s1.map(lambda x: (x[0].split(" ")[0],x[0].split(" ")[3][1:], x[1].split(" ")[1]))
# filt out the page with "H@" and make datetime and page as key
rdd_s3 = rdd_s2.filter(lambda x: "/subject_H@" in x[2]).map(lambda x: ((x[0], x[1], x[2]), 1))
# reduce by key to remove multiple records and then get rid of datetime
rdd_host = rdd_s3.reduceByKey(lambda a,b: a).map(lambda x: ((x[0][0], x[0][2]), x[1]))
rdd_path = rdd_s3.reduceByKey(lambda a,b: a).map(lambda x: (x[0][2], x[1]))

# further reduce by key to add up visit time
d_host = rdd_host.reduceByKey(lambda a,b: a+b) # ip访问特定page的次数 (ip, page, count)
d_path = rdd_path.reduceByKey(lambda a,b: a+b) # page被访问次数 (page, count)

# d_host.saveAsTextFiles("file:///Users/baobao/Desktop/path_count", "txt"

def process(rdd):
    # one record get one pool
    redisIp='localhost'
    redisPort=6379
    redisDB=0
    pool = redis.ConnectionPool(host=redisIp, port=redisPort, db=redisDB)
    r = redis.Redis(connection_pool=pool)
    # r = redis.Redis(host='localhost',port=6379)
    r.incrby(rdd[0], rdd[1])

d_path.foreachRDD(lambda rdd: rdd.foreach(process))
# d_path.pprint()

ssc.start()
ssc.awaitTermination()
