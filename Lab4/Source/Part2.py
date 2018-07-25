import os

os.environ["SPARK_HOME"] = "C:\\Users\\plfoley\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\Users\\plfoley\\winutils"

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import requests_oauthlib


# Replace the values below with yours
ACCESS_TOKEN = 'TWITTER_ACCESS_TOKEN'
ACCESS_SECRET = 'TWITTER_ACCESS_SECRET'
CONSUMER_KEY = 'TWITTER_CONSUMER_KEY'
CONSUMER_SECRET = 'TWITTER_CONSUMER_SECRET'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            print(data.split('\n'))
            self.client_socket.send(data)
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['dog'])


if __name__ == "__main__":
    s = socket.socket()  # Create a socket object
    host = "localhost"  # Get local machine name
    port = 9998  # Reserve a port for your service.
    s.bind((host, port))  # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)  # Now wait for client connection.
    c, addr = s.accept()  # Establish connection with client.
    print(c)

    print("Received request from: " + str(addr))

    sendData(c)