import thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
import serializer

from sys import path
from os.path import dirname
path.append(dirname(__file__) + '/gen-py')
print (dirname(__file__) + '/gen-py')
from hibari import Hibari
from hibari import ttypes

class HibariClient(object):
  def __init__(self, host, port):
    self.socket = TSocket.TSocket(host, port)
    self.socket.setTimeout(None)
    self.transport = TTransport.TBufferedTransport(self.socket)
    self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
    self.client = Hibari.Client(self.protocol)
    self.socket.open()
    self.timestamp_cache = {}
  def __del__(self):
    self.socket.close()

  def add(self, table, key, value):
    value = serializer.serialize(value)
    try:
      self.client.Add(ttypes.Add(table, key, value))
      return True
    except Exception, e:
      return False
  def get(self, table, key):
    try:
      value = self.client.Get(ttypes.Get(table,key)).value
    except Exception, e:
      return None
    return serializer.deserialize(value)

  def gets(self, table, key):
    try:
      result = self.client.Get(ttypes.Get(table,key))
    except Exception, e:
      return None
    self.timestamp_cache[(table,key)] = result.timestamp
    return serializer.deserialize(result.value)

  def set(self, table, key, value):
    value = serializer.serialize(value)
    self.client.Set(ttypes.Set(table, key, value))
  def replace(self, table, key, value):
    value = serializer.serialize(value)
    try:
      self.client.Replace(ttypes.Replace(table, key, value))
    except Exception, e:
      print 'exception:',e.why
      return False
    return True
  def delete(self, table, key):
    try:
      self.client.Delete(ttypes.Delete(table,key,False))
    except Exception, e:
      return False
    return True
  def cas(self, table, key, value):
    if (table,key) not in self.timestamp_cache:
      raise Exception("you must 'gets' key before cas")
    try:
      self.client.Replace(ttypes.Replace(table, key, value))
    except:
      pass
if __name__ == '__main__':
  client = HibariClient('localhost', 7600)
  client.set('tab1','key3',3)
  got = client.replace('tab1','key3',4)
  print 'replace:',got
  got = client.get('tab1','key3')
  print 'get:',got
  got = client.delete('tab1','key3')
  print 'delete:',got
  got = client.delete('tab1','key3')
  print 'delete:',got
  got = client.get('tab1','key3')
  print 'get:',got











