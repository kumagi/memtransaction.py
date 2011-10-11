from sys import path
from os.path import dirname
path.append(dirname(__file__) + '/../../')
path.append(dirname(__file__) + '/../../../')
import client

def client_test():
  cl = client.HibariClient('localhost', 7580)
  cl.add('tab1','k','fuga')
  for i in range(100):
    tmpcl = client.HibariClient('localhost', 7580)
    tmpcl.add('tab1','k','fuga')
  cl.get('tab1','k')
  print cl.latency()
