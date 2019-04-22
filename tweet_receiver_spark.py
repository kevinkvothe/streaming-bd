
# Este script se encargará de recibir los tweets enviados mediante el socket
# anterior a un objeto socketTextStream de spark.

import os

import findspark
findspark.init('/home/kubote/spark/spark-2.2.0-bin-hadoop2.7/')

import pyspark

try:

    sc.stop()

except:

    print("")

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.functions import desc

sc = SparkContext("local[4]")
# quiet_logs(sc)

ssc = StreamingContext(sc, 20) # Definimos una actualización de 20 segundos
sqlContext = SQLContext(sc)

# Create a DStream that will connect to hostname:port, like localhost:9999
socket_stream = ssc.socketTextStream("127.0.0.1", 9992)
lines = socket_stream.window(20)

from collections import namedtuple # Each element will be assigned a field
fields = ("tag", "count")
Tweet = namedtuple('Tweet', fields)

# Use Parenthesis for multiple lines or use \.
(lines.flatMap( lambda text: text.split( " " ) ) #Splits to a list
  .filter( lambda word: word.lower().startswith("#") ) # Checks for hashtag calls
  .map( lambda word: ( word.lower(), 1 ) ) # Lower cases the word, sets up a tuple
  .reduceByKey( lambda a, b: a + b ) # Reduces by key
  .map( lambda rec: Tweet( rec[0], rec[1] ) ) # Stores in a Tweet Object
  .foreachRDD(lambda rdd: rdd.toDF().sort( desc("count") ) # Sorts in descendoing order by count
  .limit(10).registerTempTable("tweets") ) ) # For every ten tweets is will be egistered as a table.


ssc.start()


import time
from IPython import display # Enables us to show stuff in the notebook
import matplotlib.pyplot as plt #Visualization library
import seaborn as sns # Visualization library
# Only works for Jupyter Notebooks!
# The following code enables us to view the bar plot within a cell in the jupyter notebook
# %matplotlib inline

count = 0
while count < 10:

    time.sleep(20)
    top_10_tweets = sqlContext.sql( 'Select tag, count from tweets' )
    top_10_df = top_10_tweets.toPandas() # Dataframe library
    # display.clear_output(wait=True) #Clears the output, if a plot exists.
    plt.figure( figsize = ( 10, 8 ) )
    sns.barplot( x="count", y="tag", data=top_10_df)
    plt.show()
    count = count + 1

ssc.stop()

os.system('kill $(lsof -ti tcp:9992)')
