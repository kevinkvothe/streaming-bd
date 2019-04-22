
# Vamos a crear un flujo de Tweets (stream) con la librería tweepy, y lo enviaremos
# mediante un socket (un canal interno). Al cual nos conectaremos desde Spark para
# analizar los tweets.

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import json
import socket


consumer_key = 'tRc5oUBPuDgzOQiki68CgBWjx'
consumer_secret = 'NLM70L5uLhOQY22LXzoKtYHi0vEMaPDFpYko4Oy23Gc00mJN0n'
access_token = '901472410666500096-OKR9c3pqHmupX5pkE08MpwsTOq1MGxX'
access_secret = 'xXa0uHQ5FDcfZZ6wTGwtJMBOC3f5JBueg0qjcCpgZmIn6'


class TweetsListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):

        try:

            msg = json.loads(data)
            print(msg['text'])

            return True

        except BaseException as e:

            print("Error on_data: %s" % str(e))

        return True

    def on_error(self, status):

        print(status)

    def sendData(c_socket): # Send the data to client socket, setting up connection

        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)

        twitter_stream = Stream(auth, TweetsListener(c_socket)) # Passes the tweets into the client socket
        twitter_stream.filter(track=['Trump'])

    if __name__ == "__main__":

        s = socket.socket()         # Create a socket object
        host = "127.0.0.1"          # Get local machine name
        port = 6666                 # Reserve a port for your connection service.
        s.bind((host, port))        # Bind to the port, create tuple

        print("Listening on port: %s" % str(port))

        s.listen(5)                 # Now wait for client connection.
        c, addr = s.accept()        # Establish connection with client.

        print("Received request from: " + str(addr))

        sendData(c)
