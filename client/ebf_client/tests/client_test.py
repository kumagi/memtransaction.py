from sys import path
from os.path import dirname
path.append(dirname(__file__) + '/../../')
path.append(dirname(__file__) + '/../../../')
import client

def client_test():
  cl = client.HibariClient('localhost', 7580)
  cl.add('tmp','k','fuga')
  for i in range(100):
    tmpcl = client.HibariClient('localhost', 7580)
    for i in range(10):
      tmpcl.set('tmp','k','fuga')
  cl.get('tmp','k')
  cl.add('tmp','k','hoge')
  print 'time',cl.latency()
  print 'called',cl.call_num()

from threading import Thread
def set_work(client, num):
  for i in range(num):
    client.set('tmp', 'k', i)
def get_work(client, num):
  for i in range(num):
    client.get('tmp', 'k')

def concurrent_client_test():
  cl = client.HibariClient('localhost', 7580)
  cl.add('tmp','k','fuga')
  workers = []
  for i in range(100):
    cl = client.HibariClient('localhost', 7580)
    worker = Thread(target = lambda: set_work(cl, 10))
    worker.start()
    workers.append(worker)
  for i in range(len(workers)):
    workers[i].join()
  cl.add('tmp','k','fff')
  print 'time',cl.latency()
  print 'called',cl.call_num()
  assert False
