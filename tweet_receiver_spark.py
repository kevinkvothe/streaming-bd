
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
# socket_stream = socket_stream.window(20, 20)

# Definimos la clase Tweet para seleccionar lo que deseamos del Stream
class Tweet(dict):

    def __init__(self, tweet_in):

        self['followers'] = tweet_in['user']['followers_count']
        self['text'] = tweet_in['text']
        self['hashtags'] = [x['text'] for x in tweet_in['entities']['hashtags']] if tweet_in['entities']['hashtags'] else "None"

# valores = ("text", "hashtag", "followers")
# Tweet_tuple = collections.namedtuple('Tweet_tuple', valores)

lines = socket_stream.map(lambda x: json.loads(x))
tweets = lines.map(lambda x: Tweet(x))
# tweets.map(lambda x: x['hashtags']).pprint()

#tweets_clean = tweets.map(lambda x: Tweet_tuple(x['text'], x['hashtags'], x['followers']))
#tweets_clean.pprint()

# ================ Ahora buscamos obtener estadísticas: =================== #

## En primer lugar, una estadística de hashtags que utilice la gente que escriba determinadas palabras clave.
# Definimos una tupla nombrada para delimitar los hashtags y su repetición.
valores_hash = ("hashtag", "count")
hash_tuple = collections.namedtuple('hash_tuple', valores_hash)

# Filtramos los tweets que contienen hashtags.
tweets_hashtags = tweets.filter(lambda x: ("None" in x['hashtag'])).map(lambda x: x['hashtag'])
tweets_hashtags.pprint()

# Seleccionamos los hashtags
(tweets_hashtags.map(lambda x: x['hashtag'])

# Mapeamos en forma de tupla con un 1 para contar.
.map(lambda word: (word, 1))

# Reducimos por palabra del hashtag.
.reduceByKey(lambda a, b: a + b)

# Transformamos en la tupla nombrada "hash_tuple".
.map(lambda x: hash_tuple(x[0], x[1]))

# Para cada batch, pasamos el conjunto de tuplas nombradas a dataframes ordenando
# por orden descendiente de su repetición.
.foreachRDD(lambda rdd: rdd.toDF().sort(desc("count"))

# Limitamos la salida a 15 y creamos una tabla temporal para usar comandos SQL en ella.
.limit(10).registerTempTable("tabla_hashtags")))
#
ssc.start()

## En primer lugar, una estadística de hashtags que utilice la gente que escriba determinadas palabras
## clave.
#valores_hash = ("hashtag", "count")
#hash_tuple = collections.namedtuple('hash_tuple', valores_hash)

# Ahora vamos a transformar nuestro objeto DStream en un dataframe para manipularlo. Spliteamos por espacios.
#(tweets

# Dividimos las lineas por espacios, formando palabras.
#.flatMap(lambda line: line['text'].split(" "))

# Filtramos por hashtag en minúsculas.
#.filter(lambda word: word.lower().startswith("#"))

# Mapeamos en forma de tupla con un 1 para contar.
#.map(lambda word: (word, 1))

# Reducimos por palabra del hashtag.
#.reduceByKey(lambda a, b: a + b)

# Transformamos en la tupla nombrada "Tweet".
#.map(lambda rec: hash_tuple(rec[0], rec[1]))

# Para cada batch, pasamos el conjunto de tuplas nombradas a dataframes ordenando
# por orden descendiente de su repetición.
#.foreachRDD(lambda rdd: rdd.toDF().sort(desc("count"))

# Limitamos la salida a 15 y creamos una tabla temporal para usar comandos SQL en ella.
#.limit(10).registerTempTable("tabla_hashtags")))
#

ssc.start()


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
time.sleep(20)

count = 0

while count < 3:

    top_hashtags = sqlContext.sql('Select hashtag, count from tabla_hashtags')
    df = top_hashtags.toPandas()

    # Para visualizar, descomponemos en x e y.
    x = df['hashtag']
    y = df['count']

    # Creamos el barplot.
    fig, ax = plt.subplots()
    ax.bar([idx for idx in range(len(x))], y, color = 'blue')
    ax.set_xticks([idx+0.5 for idx in range(len(x))])
    ax.set_xticklabels(x, rotation=35, ha='right', size=10)
    plt.show()

    count = count + 1

time.sleep(5)

ssc.stop()

os.system('kill $(lsof -ti tcp:9992)')
os.system('fuser 9992/tcp')
