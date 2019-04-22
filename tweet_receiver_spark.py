
# Este script se encargará de recibir los tweets enviados mediante el socket
# anterior a un objeto socketTextStream de spark.

# Librerías generales
import os
import time
import matplotlib.pyplot as plt
import seaborn as sns # Visualization library

# Pyspark
import findspark
findspark.init('/home/kubote/spark/spark-2.2.0-bin-hadoop2.7/')

# Librerías derivadas de pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.functions import desc

try:

    sc.stop()

except:

    print("")

# No queremos excesivos logs
def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )


# Creamos una instancia de spark
sc = SparkContext("local[4]")

# Aplicamos el silencio de logs
quiet_logs(sc)

# Creamos un Streaming object con actualización de 1 segundos. Esto significa que a la hora de capturar
# el flujo, cada 1 segundo StreamingContext me va a crear un nuevo batch, es decir, un nuevo "paquete"
# de tweets.
ssc = StreamingContext(sc, 1)

# Creamos un SQLContext para poder ejecutar comandos sql en spark.
sqlContext = SQLContext(sc)

# Creamos un Stream directo que conecta con el socket usado en tweet_stream_tweepy.py.
# Hay que asegurarse de que el host y el puerto sean iguales.
socket_stream = ssc.socketTextStream("127.0.0.1", 9992)

# Ahora seleccionamos la ventana a partir de la cual trabajar. Dado que hemos creado
# un StreamingContext de 1 segundo, podemos definir el número de batches como el número
# de segundos de la ventana. Usaremos ventanas de 20 segundos cada 20 segundos, de forma
# que todos los datos que procesemos cada 20 segundos serán independientes (no tendremos
# batches superpuestos).
lines = socket_stream.window(20, 20)

from collections import namedtuple # Each element will be assigned a field
fields = ("tag", "count")
Tweet = namedtuple('Tweet', fields)

# Use Parenthesis for multiple lines or use \.
(lines.flatMap(lambda text: text.split(" ")) #Splits to a list
.filter(lambda word: word.lower().startswith("#")) # Checks for hashtag calls
.map(lambda word: (word.lower(), 1)) # Lower cases the word, sets up a tuple
.reduceByKey(lambda a, b: a + b) # Reduces by key
.map(lambda rec: Tweet(rec[0], rec[1])) # Stores in a Tweet Object
.foreachRDD(lambda rdd: rdd.toDF().sort(desc("count")) # Sorts in descendoing order by count
.limit(10).registerTempTable("tweets"))) # For every ten tweets is will be egistered as a table.

ssc.start()

count = 0

while count < 3:

    # Demos utilizar un periodo de tiempo alrededor de 20 segundos (tamaño y periodo de
    # actualización de la ventana) para asegurarnos de no estar usando los mismos datos.
    time.sleep(20)
    top_10_tweets = sqlContext.sql( 'Select tag, count from tweets' )
    top_10_df = top_10_tweets.toPandas()

    x = top_10_df['tag']
    y = top_10_df['count']

    fig, ax = plt.subplots()
    ax.bar([idx for idx in range(len(x))], y, color = 'blue')
    ax.set_xticks([idx+0.5 for idx in range(len(x))])
    ax.set_xticklabels(x, rotation=35, ha='right', size=10)
    plt.show()

    count = count + 1

ssc.stop()

os.system('kill $(lsof -ti tcp:9992)')
os.system('fuser 9992/tcp')
