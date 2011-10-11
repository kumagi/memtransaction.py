from pyubf import Atom, Integer
import serializer
import pyebf
from time import time

class LatencyAccum(object):
  def __init__(self):
    self.init()
  def accum(self,fn):
    self.num += 1
    start = time()
    result = fn()
    self.latency += time() - start
    return result
  def get(self):
    return self.latency
  def nums(self):
    return self.num
  def init(self):
    self.latency = 0.0
    self.num = 0
latency = LatencyAccum()

class HibariClient(object):
  def __init__(self, host = 'localhost', port = 7580):
    self.ebf = pyebf.EBF(host,port)
    self.ebf.login('gdss', 'gdss_meta_server')
    self.timestamp_cache = {}
    latency = LatencyAccum()
  def add(self, table, key, value):
    value = serializer.serialize(value)
    req = (Atom('do'), Atom(table), [(Atom('add'), key, 0, value, 0, [])], [], 1000)
    result = latency.accum(lambda:self.ebf.rpc('gdss', req))
    return result[0][0] == 'ok'
  def get(self, table, key):
    req = (Atom('do'), Atom(table), [(Atom('get'), key, [])], [], 1000)
    result = latency.accum(lambda:self.ebf.rpc('gdss',req))
    if result[0][0] == 'ok':
      return serializer.deserialize(result[0][2])
    else:
      return None
  def gets(self, table, key):
    req = (Atom('do'), Atom(table), [(Atom('get'), key, [Atom('get_attribs')])], [], 1000)
    result = latency.accum(lambda:self.ebf.rpc('gdss',req))
    self.timestamp_cache[(table,key)] = result[0][1]
    #print "cached:",self.timestamp_cache[(table,key)]
    if result[0][0] == 'ok':
      return serializer.deserialize(result[0][2])
    else:
      return None
  def _dump(self, table, key):
    req = (Atom('do'), Atom(table), [(Atom('get'), key, [Atom('get_attribs')])], [], 100,0)
    result = latency.accum(lambda:self.ebf.rpc('gdss',req))
    print table,":",key,":",result
  def set(self, table, key, value):
    value = serializer.serialize(value)
    req = (Atom('set'), table, key, value)
    req = (Atom('do'), Atom(table), [(Atom('set'), key, 0, value, 0, [])], [], 1000)
    #req = (Atom('do'), Atom(table), [(Atom('set'), key, 0, value, 0, [])], [], 1000)
    result = latency.accum(lambda:self.ebf.rpc('gdss',req))
    return result[0][0] == 'ok'
  def cas(self, table, key, value):
    value = serializer.serialize(value)
    if (table,key) not in self.timestamp_cache:
      raise Exception("you must 'gets' key before cas")
    req = (Atom('do'), Atom(table), [(Atom('set'), key, 0, value, 0,
                                      [(Atom('testset'), self.timestamp_cache[(table,key)])])], [], 1000)
    #req = (Atom('do'), Atom(table), [(Atom('set'), key, 0, value, 0, [])], [], 1000)
    result = latency.accum(lambda:self.ebf.rpc('gdss',req))
    return result[0][0] == 'ok'
  def replace(self, table, key, value):
    value = serializer.serialize(value)
    req = (Atom('do'), Atom(table), [(Atom('replace'), key, 0, value, 0, [])], [], 1000)
    result = latency.accum(lambda:self.ebf.rpc('gdss',req))
    return result[0][0] == 'ok'
  def delete(self, table, key):
    req = (Atom('do'), Atom(table), [(Atom('delete'), key, [])], [], 1000)
    result = latency.accum(lambda:self.ebf.rpc('gdss',req))
    if result[0] == 'ok':
      return True
    else:
      return False
  def get_many(self, table, key, num):
    req = (Atom('get'), table, key, [], 1000)
    #req = (Atom('do'), Atom(table), [(Atom('get_many'), key, num, [])], [], 1000)
    result = latency.accum(lambda:self.ebf.rpc('gdss',req))
    print result
  @classmethod
  def init_latency(cls):
    latency.init()
  @classmethod
  def latency(cls):
    return latency.get()
  @classmethod
  def init_num(cls):
    latency.init()
  @classmethod
  def call_num(cls):
    return latency.nums()

if __name__ == '__main__':
  client = HibariClient('localhost', 7580)
  print 'add k->fuga',client.add('tab1','k','fuga')
  print 'add k->fug',client.add('tab1','k','fug')
  print 'set k->hoge',client.set('tab1','k','hoge')
  print 'gets k->',client.gets('tab1','k')
  print 'cas k->fu',client.cas('tab1', 'k', 'fu')
  print 'cas k->hana',client.cas('tab1', 'k', 'hana')
  print 'gets k->',client.gets('tab1','k')
  print 'cas k->hahaha',client.cas('tab1', 'k', 'hahaha')
  print 'replace k->dd',client.replace('tab1', 'k', 'dd')
  print 'replace none->hoge',client.replace('tab1', 'none', 'hoge')
  print 'delete k', client.delete('tab1', 'k')
  print 'delete k', client.delete('tab1', 'k')
  print 'delete none', client.delete('tab1', 'none')
  print 'latency', HibariClient.latency()

  print 'get_many', client.get_many('tab1', "", 10)

