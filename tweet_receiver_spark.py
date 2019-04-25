
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
import unidecode
import numpy as np
import pandas as pd

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
socket_stream = socket_stream.window(15, 15)

# Definimos la clase Tweet para seleccionar lo que deseamos del Stream
class Tweet(dict):

    def __init__(self, tweet_in):

        self['followers'] = tweet_in['user']['followers_count']
        self['text'] = tweet_in['text']
        self['hashtags'] = [x['text'] for x in tweet_in['entities']['hashtags']] if tweet_in['entities']['hashtags'] else None
        self['has_hashtag'] = True if tweet_in['entities']['hashtags'] else False

#valores = ("text", "hashtag", "followers")
#Tweet_tuple = collections.namedtuple('Tweet_tuple', valores)

lines = socket_stream.map(lambda x: json.loads(x))
tweets = lines.map(lambda x: Tweet(x))
#tweets.map(lambda x: x['has_hashtag']).pprint()

#tweets_clean = tweets.map(lambda x: Tweet_tuple(x['text'], x['hashtags'], x['followers']))
#tweets_clean.pprint()

# ================ Ahora buscamos obtener estadísticas: =================== #

# ====================== Primer análisis: Hashtags =========================#
# En primer lugar, una estadística de hashtags que utilice la gente que tweittee sobre cualquiera
# de los 5 principales candidatos a presidente del gobierno.

# Definimos una tupla nombrada para delimitar los hashtags y su repetición.
valores_hash = ("hashtag", "count")
hash_tuple = collections.namedtuple('hash_tuple', valores_hash)

# Ahora vamos a transformar nuestro objeto DStream en un dataframe para manipularlo. Spliteamos por espacios.
lines = tweets.map(lambda x: x['text'])

(lines
# Dividimos las lineas por espacios, formando palabras.
.flatMap(lambda line: line.split(" "))

# Filtramos por hashtag en minúsculas.
.filter(lambda word: word.lower().startswith("#"))

# Mapeamos en forma de tupla con un 1 para contar.
.map(lambda word: (word, 1))

# Reducimos por palabra del hashtag.
.reduceByKey(lambda a, b: a + b)

# Transformamos en la tupla nombrada "Tweet".
.map(lambda rec: hash_tuple(rec[0], rec[1]))

# Para cada batch, pasamos el conjunto de tuplas nombradas a dataframes ordenando
# por orden descendiente de su repetición.
.foreachRDD(lambda rdd: rdd.toDF().sort(desc("count"))

# Limitamos la salida a 15 y creamos una tabla temporal para usar comandos SQL en ella.
.registerTempTable("tabla_hashtags")))


# =================== Segundo análisis: popularidad ===================== #
# Vamos a medir la popularidad de los 5 principales candidatos a presidente de España en las elecciones
# del 28 de Abril. Lo haremos contando el número de veces que sus nombres son tweitteados en relación
# al resto.

# Definimos una tupla nombrada para delimitar los hashtags y su repetición.
valores_pop = ("candidato", "count")
pop_tuple = collections.namedtuple('pop_tuple', valores_pop)

# Definimos los candidatos por los que detectar tweets.
#candidatos_lower = ['trump', 'clinton', 'obama', 'abascal', 'iglesias', 'sanchez', 'rajoy', 'rivera']
candidatos_lower = ['abascal', 'iglesias', 'sanchez', 'casado', 'rivera']

# Cremos una función para crear una tabla con las veces que se menciona a cada candidato.
def calcular_tabla(candidato):

    nombre_tabla = "tabla_pop_" + candidato

    # Ahora vamos a transformar nuestro objeto DStream en un dataframe para manipularlo. Spliteamos por espacios.
    lines = tweets.map(lambda x: x['text'])
    (lines
    # Dividimos las lineas por espacios, formando palabras.
    .flatMap(lambda line: line.replace("#", "").replace("'", "").replace(",", "").replace(".", "").split(" "))

    # Filtramos por palabras clave en minúsculas y sin acentos.
    .map(lambda word: (candidato in unidecode.unidecode(word).lower(), 1))

    # Reducimos por True y False, siendo true y false los valores de palabra detectada y no.
    .reduceByKey(lambda a, b: a + b)

    # Transformamos en la tupla nombrada "Tweet".
    .map(lambda tupla: pop_tuple(tupla[0], tupla[1]))

    # Para cada batch, pasamos el conjunto de tuplas nombradas a dataframes ordenando
    # por orden descendiente de su repetición.
    .foreachRDD(lambda rdd: rdd.toDF().sort(desc("count"))

    # Limitamos la salida a 2 y creamos una tabla temporal para usar comandos SQL en ella.
    .limit(2).registerTempTable(nombre_tabla)))

# Llamadas para crear cada una de las tablas necesarias con cada batch.
for cand in candidatos_lower:
    calcular_tabla(cand)

# Iniciamos el stream.
ssc.start()

# Introducimos un delay para que se calcule el primer batch y las tablas derivadas.
time.sleep(22)

df_dict = {"candidatos": candidatos_lower, "suma_1": np.zeros(len(candidatos_lower)), "suma_2": np.zeros(len(candidatos_lower)), "suma_3": np.zeros(len(candidatos_lower)), "suma_4": np.zeros(len(candidatos_lower)), "suma_5": np.zeros(len(candidatos_lower))}

# Definimos dos dataframes vacíos con los candidatos y sus menciones y otro con los hashtags y su conteo.
df_can = pd.DataFrame(df_dict)
df_hashtags_fin = pd.DataFrame({'hashtag': [], 'count': []})

# Rellenamos el dataframe anterior de forma acumulativa en 5 ciclos.
for a in range(5):

    for i in candidatos_lower:

        nombre_tabla = "tabla_pop_" + i

        # Añadimos los valores de True al dataframe creado anteriormente.
        top_pop = sqlContext.sql('Select candidato, count from ' + nombre_tabla)
        tabla = top_pop.toPandas()

        # Nos aseguramos de que exista un valor para añadir, en caso contrario añadimos 0.
        if tabla.shape[0] > 1:
            val = tabla.iloc[np.where(tabla['candidato'] == True)[0], 1]
            val = val[1]
        else:
            val = 0

        # Sumamos a los 0s los valores obtenidos.
        df_can.iloc[np.where(df_can['candidatos'] == i)[0], a + 1] += val

        # Necesitamos sumar las nuevas menciones para mostrar el aumento.
        if a > 0:

            df_can.iloc[np.where(df_can['candidatos'] == i)[0], a + 1] += df_can.iloc[np.where(df_can['candidatos'] == i)[0], a]

        # Hacemos algo similar con los hashtags, hacemos un merge conservando todos los hashtags diferentes
        # y sumando los ya utilizados.
        top_hashtags = sqlContext.sql('Select hashtag, count from tabla_hashtags')
        df_hashtags = top_hashtags.toPandas()

        df_hashtags_fin = df_hashtags_fin.set_index('hashtag').add(df_hashtags.set_index('hashtag'), fill_value=0).reset_index()

    # Mostramos la comparativa de menciones en cada iteración.
    x = df_can['candidatos']
    y = df_can.iloc[:, 1:(a + 2)]

    colores = ['red', 'blue', 'green', 'black', 'pink']
    for o in range(a + 1):

        plt.bar([idx + 0.8*o/(a + 1) for idx in range(len(x))], y.iloc[:, o], color = colores[o], width = 0.8/(a + 1), tick_label = x)
    plt.show()

    # Introducimos un delay igual al tiempo entre ventanas para que se calcule el nuevo batch y tablas derivadas.
    time.sleep(15)

# Plots finales:

# de menciones:
x = df_can['candidatos']
y = df_can.iloc[:, 1:(a + 2)]

colores = ['red', 'blue', 'green', 'black', 'pink']
for o in range(a + 1):

    plt.bar([idx + 0.8*o/(a + 1) for idx in range(len(x))], y.iloc[:, o], color = colores[o], width = 0.8/(a + 1), tick_label = x)
plt.title("Número de menciones acumuladas cada 15 segundos por candidato.")
plt.savefig('menciones.png')
plt.show()

# de hashtags.
x = df_hashtags_fin['hashtag']
y = df_hashtags_fin.iloc[:, 1]

plt.figure(figsize = (15, 10))
plt.barh([idx for idx in range(len(x))], y, tick_label = x)
plt.title("Hashtags más utilizados en tweets mencionando a alguno de los 5 candidatos a presidente del gobierno.")
plt.savefig('hashtags.png')
plt.show()

print(df_can)

print("\n")

print(df_hashtags_fin)

# Detenemos el streaming.
ssc.stop()

# Limpiamos el puerto utilizado para poder usarlo inmediatamente después.
os.system('kill $(lsof -ti tcp:9992)')
os.system('fuser 9992/tcp')

print(" ")
