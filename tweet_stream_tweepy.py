
# Vamos a crear un flujo de Tweets (stream) con la librería tweepy, y lo enviaremos
# mediante un socket (un canal interno). Al cual nos conectaremos desde Spark para
# analizar los tweets. Esto script ejecuta la primera parte, crea al Stream y lo transmite.

# Librerías de tweepy
import tweepy
from tweepy.streaming import StreamListener

## Librerías generales
import json
import socket
import unidecode

# Claves de acceso a la API de Twitter. Yo lo he hecho de forma que estén en un archivo "secret.py" aparte.
import secret
from secret import consumer_key, consumer_secret, access_token, access_token_secret

# Creamos una clase para usarla posteriormente. Se basa en el uso de un objeto
# StreamListener de tweepy.
class TweetsListener(StreamListener):

    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """

    # Definimos la creación del objeto TweetsListener a partir de un socket con
    # el cual nos conectaremos.
    def __init__(self, csocket):

        self.client_socket = csocket

    # Creamos una función on_data que intenta transmitir cada tweet (únicamente el texto)
    # A través del socket.

    def on_data(self, data):

        try:

            msg = json.loads(data)
            
            analize = unidecode.unidecode(msg['text']).replace("#", "")
            print(any(x in analize.split(" ") for x in candidatos))
            #print(msg['text'])

            self.client_socket.send(data.encode())

            return True

        except BaseException as e:

            print("Error on_data: %s" % str(e))

        return True

    # Creamos una función para un caso de error.
    def on_error(self, status):

        print(status)
        return True

# Creamos una función de autenticación.
def get_auth():

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth

# De vuelta en el script, fuera de la casle, creamos una función para inicial el Stream
# usando la API de Twitter y la clase creada.
def sendData(c_socket):

    auth = get_auth()

    # Iniciamos la escucha de Tweets usando TweetsListener, y definiendo el socket a utilizar
    # dentro de la propia clase.
    twitter_stream = tweepy.Stream(auth, TweetsListener(c_socket))

    # Filtramos los tweets por una palabra clave.
    twitter_stream.filter(track=candidatos)

# Volvemos al script, esta parte se ejecuta independientemente de la clase. Es lo que inicia
# el socket y espera conexión para continuar con la transmisión.
if __name__ == "__main__":

    # Creamos un objeto socket.
    s = socket.socket()

    # Asociamos el socket a un host (dirección, string) y un puerto (entero).
    s.bind(("127.0.0.1", 9992))

    print("Listening on port: %s" % str(9992))

    # Espera conexión en el host y puerto determinados
    s.listen()

    # Una vez que se produce la conexión, se recogen el socket y la dirección del cliente.
    c, addr = s.accept()

    print("Received request from: " + str(addr))
    
    # Definimos el filtro:
    candidatos = ['trump', 'Trump', 'clinton', 'Clinton', 'obama', 'Obama', 'abascal', 'Abascal', 'iglesias', 'Iglesias', 'sanchez', 'Sanchez', 'rajoy', 'Rajoy']

    # Dado que hay conexión, iniciamos la transferencia por el socket.
    sendData(c)
