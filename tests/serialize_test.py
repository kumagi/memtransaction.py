from sys import path
from os.path import dirname
path.append(dirname(__file__) + '/..')
from client import serializer
from hibari_transaction import *
import sys

def serialize_test():
  table_key = serializer.serialize(('aa', 'fuga'))
  table, key = separate_table_and_key(table_key)
  assert table == 'aa' and key == 'fuga'

def add_random_test():
  mt = MemTr(client.HibariClient('localhost', 7580))
  result = mt.add_random('hoge')
  assert result != ''
  print sys.version

def init(s, g):
  s('counter',0)

def incr(s, g):
  d = g('counter')
  print "counter:",d
  s('counter', d+1)
  print "incr done\n"

def init_test():
  client.HibariClient.init_latency()
  mc = client.HibariClient('localhost', 7580)
  result = rr_transaction(mc, init)
  assert result['counter'] == 0
  print 'latency', client.HibariClient.latency(), ' called', client.HibariClient.call_num()

def double_init_test():
  client.HibariClient.init_latency()
  mc = client.HibariClient('localhost', 7580)
  result = rr_transaction(mc, init)
  result = rr_transaction(mc, init)
  assert result['counter'] == 0
  print 'latency', mc.latency()

def reuse_client_test():
  client.HibariClient.init_latency()
  mc1 = client.HibariClient('localhost', 7580)
  mc2 = client.HibariClient('localhost', 7580)
  result = rr_transaction(mc1, init)
  assert result['counter'] == 0
  result = rr_transaction(mc2, incr)
  assert result['counter'] == 1
  result = rr_transaction(mc1, incr)
  assert result['counter'] == 2
  print 'latency', mc1.latency()

def test_count():
  client.HibariClient.init_latency()
  mc = client.HibariClient('localhost', 7580)
  result = rr_transaction(mc, init)
  for i in range(10):
    result = rr_transaction(mc, incr)
  print result
  assert result['counter'] == 10
  print mc.latency()

def use_many_client_test():
  client.HibariClient.init_log()
  mc1 = client.HibariClient('localhost', 7580)
  result = rr_transaction(mc1, init)
  assert result['counter'] == 0
  for i in range(100):
    cl = client.HibariClient('localhost', 7580)
    result = rr_transaction(cl, incr)
    mc1.set('tmp','k','hoge')
    assert result['counter'] == i + 1
  #result = rr_transaction(mc1, incr)
  mc1.set('tmp','k','hoge')
  #assert result['counter'] == 101

from threading import Thread
def incr_worker(target_host, target_port, num):
  mc = client.HibariClient(target_host, target_port)
  for i in range(num):
    rr_transaction(mc, incr)
    #print "*"*i+"-"*(num-i)

def est_concurrent():
  cl = client.HibariClient('localhost', 7580)
  rr_transaction(cl, init)
  workers = []
  for i in range(2):
    worker = Thread(target = lambda: incr_worker('localhost', 7580, 50))
    worker.start()
    workers.append(worker)
  for i in range(len(workers)):
    workers[i].join()
  #result = rr_transaction(cl, incr)
  cl.set('tmp','k','hoge')
  assert result['counter'] == 101





