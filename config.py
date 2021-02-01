import nump as np

stream_name = 'twitter-stream' #Stream name on Amazon Kinesis

# Twitter API credentials:
ACCESS_KEY = np.genfromtxt('/home/sebastian/twitter-pws',dtype=object)[0].decode()
SECRET_KEY = np.genfromtxt('/home/sebastian/twitter-pws',dtype=object)[1].decode()
ACCESS_TOKEN = np.genfromtxt('/home/sebastian/twitter-pws',dtype=object)[3].decode()
SECRET_TOKEN = np.genfromtxt('/home/sebastian/twitter-pws',dtype=object)[4].decode()
