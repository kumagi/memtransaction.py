# -*- coding: utf-8 -*-
import time
import memcache
# import thrift_client
# import traceback,sys

from time import sleep
from random import randint

class AbortException(Exception):
    pass

def read_committed(old, new, status):
    if status == 'committed':
        return new, old
    elif status == 'abort' or status == 'active':
        return old, new
    else:
        raise Exception('invalid status' + status)
def read_repeatable(old, new, status):
    if status == 'committed':
        return new,old
    elif status == 'abort':
        return old,new
    elif status == 'active':
        return None
    else:
        raise Exception('invalid status' + status)
def async_delete(client, target):
    client.delete(target)

class AsyncDeletingMemcacheClient(memcache.Client):
  def _async_delete(self):
    self.del_que = []
    while True:
      time.sleep(0.1)
      print self.del_que, " from", self.del_que
      while 0 < len(self.del_que):
        print '###########deleting:' + target
        target = self.del_que.pop(0)
        if target != None:
          print '###########deleting:' + target
          self.delete(target)
  def start_delete_thread(self):
    try:
      self.del_thread
    except AttributeError:
      from threading import Thread
      from threading import Lock
      self.del_thread = Thread(target = lambda:self._async_delete())
      self.que_lock = Lock()
      self.del_que = []
      self.del_thread.setDaemon(True)
      self.del_thread.start()
  def add_del_que(self,target):
    #print "appending ",target, " to", len(self.del_que)
    self.del_que.append(target)

class MemTr(object):
  """ transaction on memcached """
  @staticmethod
  def random_string(length):
    string = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
    ans = ''
    for i in range(length):
      ans += string[randint(0, len(string) - 1)]
    return ans
  def add_random(self,value):
    length = 1
    while 1:
      key = self.prefix + MemTr.random_string(length)
      result = self.mc.add(key, value)
      if result == True:
        return key
      length += 1
  def __init__(self, client):
    self.prefix = 'MTP:'
    self.mc = client
    self.mc.start_delete_thread()
  def begin(self):
    self.transaction_status = self.add_random('active')
    self.cache = {}
  def commit(self):
    status = self.mc.gets(self.transaction_status)
    if status != 'active': raise AbortException
    return self.mc.cas(self.transaction_status, 'committed')
  class resolver(object):
    def __init__(self, mc):
      self.count = 10
      self.mc = mc
    def __call__(self, other_status):
      time.sleep(0.001 * randint(0, 1 << self.count))
      if self.count <= 10:
        self.count += 1
      else:
        self.count = 0
        #print 'cas: ', other_status, '-> abort'
        self.mc.cas(other_status, 'abort')
  def set(self, key, value):
    resolver = self.resolver(self.mc)
    new_key = None
    while 1:
      old = new = status_name = None
      try:
              #print "set:",self.mc.gets(key)
        old, new, status_name = self.mc.gets(key)
      except TypeError:
        if new_key == None:
          new_key = self.add_random(value)
        if self.mc.add(key,[None, new_key, self.transaction_status]):
          self.cache[key] = value
          break
        continue
      #print "old:%s ,new:%s status:%s" % (old, new, status_name)
      if status_name == self.transaction_status:
        self.cache[key] = value
        if old == new:
          new_key = self.add_random(value)
          if self.mc.cas(key, [old, new_key, self.transaction_status]):
            break
        else:
          if not self.mc.replace(new, value):
            raise AbortException
          break
      else:
        state = self.mc.gets(status_name)
        next_old , to_delete = None, None
        try:
          next_old, to_delete = read_repeatable(old,new,state)
        except TypeError:
          resolver(status_name)
          continue
        # print "set: cas for ", [next_old, new_key, self.transaction_status]
        if new_key == None:
          new_key = self.add_random(value)
        result = self.mc.cas(key,[next_old, new_key, self.transaction_status])
        if result:
          self.cache[key] = value
          if to_delete != None and to_delete != next_old:
            self.mc.add_del_que(to_delete)
            #print "setting cache:",self.cache
          break
  def get_committed(self, key):
    while 1:
      old = new = status_name = None
      try:
        #print "get:",self.mc.gets(key)
        old, new, status_name = self.mc.gets(key)
      except TypeError:
        #print "typeerror"
        return None  # read committed!!

      if status_name == self.transaction_status:
        return self.cache[key]
      #return self.mc.get(new)
      else:
        state = self.mc.get(status_name)
        committed_value, to_delete = read_committed(old,new,state)
        if state != 'active':
          self.mc.add_del_que(to_delete)
        result = self.mc.get(committed_value)
        self.cache[key] = result
        return result

  def get_repeatable(self, key):
    resolver = self.resolver(self.mc)
    while 1:
      old = new = status_name = None
      try:
        #print "get:",self.mc.gets(key)
        old, new, status_name = self.mc.gets(key)
      except TypeError:
        #print "typeerror"
        return None  # read repeatable!!
      if status_name == self.transaction_status:
        #print 'cache hit'
        return self.cache[key]
        #return self.mc.get(new)
      else:
        state = self.mc.get(status_name)
        try:
          committed_value, to_delete = read_repeatable(old,new,state)
        except Typeerror: # other active transaction living!
          resolver(status_name)
          continue
        if self.mc.cas(key, [committed_value,
                             committed_value,
                             self.transaction_status]):
          if state != 'active' and new != old and to_delete != None:
            self.mc.add_del_que(to_delete)
          result = self.mc.get(committed_value)
          self.cache[key] = result
          return result

def rr_transaction(kvs, target_transaction):
  transaction = MemTr(kvs)
  setter = lambda k,v : transaction.set(k,v)
  getter = lambda k :   transaction.get_repeatable(k)
  while(1):
    transaction.begin()
    try:
      target_transaction(setter, getter)
      if transaction.commit() == True:
        return transaction.cache
    except AbortException:
      continue
def rc_transaction(kvs, target_transaction):
  transaction = MemTr(kvs)
  setter = lambda k,v : transaction.set(k,v)
  getter = lambda k :   transaction.get_committed(k)
  repeatable_getter = lambda k : transaction.get_repeatable(k)
  while(1):
    transaction.begin()
    try:
      target_transaction(setter, getter, repeatable_getter)
      if transaction.commit() == True:
        return transaction.cache
    except AbortException:
      continue

if __name__ == '__main__':
  mc = AsyncDeletingMemcacheClient(['127.0.0.1:11211'])
  def init(s, g):
    s('counter',0)
  def incr(setter, getter):
    d = getter('counter')
    #print "counter:",d
    setter('counter', d+1)
  result = rr_transaction(mc, init)
  from time import time
  begin = time()
  for i in range(10000):
    result = rr_transaction(mc, incr)
  print result['counter']
  print str(10000 / (time() - begin)) + " qps"
