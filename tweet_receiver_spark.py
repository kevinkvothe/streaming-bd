
# Este script se encargará de recibir los tweets enviados mediante el socket
# anterior a un objeto socketTextStream de spark.

# Librerías generales
import os
import time
import matplotlib.pyplot as plt
import seaborn as sns
import collections
import json
import pprint

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
#lines = socket_stream.window(20, 20)


lines = socket_stream.map(lambda x: json.loads(x))
# dstream = lines.map(lambda x: json.loads(x))
#lines2 = lines.map(lambda x: x['text'])
lines.pprint()
lines.map(lambda x: x['text']).pprint()

ssc.start()
#lines.foreachRDD(lambda rdd: rdd.toDF()).limit(10).registerTempTable("tabla_tweets")

#lines2.pprint()
# Creamos una tupla con nombre, para poder asignar valores a campos de una tupla y llamarlos.
#valores = ("hashtag", "count")
#valores = ("texto", "geo", "hashtag")
#Tweet = collections.namedtuple('Tweet', valores)

#(lines.flatMap(lambda line: (line['text'].split(" "))).filter(lambda word: word.lower().startswith("#"))
#.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).map(lambda rec: Tweet(rec[0], rec[1]))
#.foreachRDD(lambda rdd: rdd.toDF().sort(desc("count")).limit(10).registerTempTable("tabla_tweets")))

# Ahora vamos a transformar nuestro objeto DStream en un dataframe para manipularlo.
# Spliteamos por espacios.
#(lines

# Dividimos las lineas por espacios, formando palabras.
#.flatMap(lambda line: line.split(" "))

# Filtramos por hashtag en minúsculas.
#.filter(lambda word: word.lower().startswith("#"))

# Mapeamos en forma de tupla con un 1 para contar.
#.map(lambda word: (word, 1))

# Reducimos por palabra del hashtag.
#.reduceByKey(lambda a, b: a + b)

# Transformamos en la tupla nombrada "Tweet".
#.map(lambda rec: Tweet(rec[0], rec[1]))

# Para cada batch, pasamos el conjunto de tuplas nombradas a dataframes ordenando
# por orden descendiente de su repetición.
#.foreachRDD(lambda rdd: rdd.toDF().sort(desc("count"))

# Limitamos la salida a 15 y creamos una tabla temporal para usar comandos SQL en ella.
#.limit(10).registerTempTable("tabla_tweets")))

# Iniciamos la conexión y por tanto la gestión de tweets por parte de Spark.
ssc.start()

#top_tweets = sqlContext.sql('Select hashtag, count from tabla_tweets')

ssc.stop()

os.system('kill $(lsof -ti tcp:9992)')
os.system('fuser 9992/tcp')
