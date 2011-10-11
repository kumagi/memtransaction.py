import client
from time import sleep
from random import randint
from client import serializer
from pylru import lrucache

class AbortException:
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
def separate_table_and_key(org):
  if org == None: return None
  table, key = serializer.deserialize(org)
  return (table,key)
def get_by_table_and_key(mc, org):
  try:
    table, key = separate_table_and_key(org)
    return mc.get(table, key)
  except TypeError:
    return None
class MemTr(object):
  """ transaction on Hibari """
  @staticmethod
  def random_string(length):
    string = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
    ans = ''
    for i in range(length):
      ans += string[randint(0, len(string) - 1)]
    return ans
  def add_random(self, value):
    """ random add for indirect_table """
    length = 10
    while True:
      key = MemTr.random_string(length)
      result = self.mc.add(self.indirect_table, key, value)
      print result
      if result == True:
        return serializer.serialize((self.indirect_table, key))
      length += 1
  def __init__(self, client):
    self.main_table = 'main'
    self.indirect_table = 'tmp'
    self.mc = client
  def begin(self):
    self.transaction_status = self.add_random('active')
    self.cache = {}
  def commit(self):
    status_table, status_key = serializer.deserialize(self.transaction_status)
    status = self.mc.gets(status_table, status_key)
    if status != 'active': raise AbortException
    return self.mc.cas(status_table, status_key, 'committed')
  class resolver(object):
    def __init__(self, mc):
      self.count = 10
      self.mc = mc
    def __call__(self, other_status):
      status_table, status_key = serializer.deserialize(other_status)
      while True:
        sleep(0.001 * randint(0, 1 << self.count))
        status = self.mc.gets(status_table, status_key)
        if status != 'active':
          return
        if self.count <= 6:
          self.count += 1
        else:
          self.count = 0
          if self.mc.cas(status_table, status_key, 'abort'):
            print "robbed!"
            return
  def set(self, key, value):
    resolver = self.resolver(self.mc)
    while 1:
      old = new = status_name = None
      try:
        old, new, status_name = self.mc.gets(self.main_table, key)
        print "set: old=", get_by_table_and_key(self.mc,old), " new=", get_by_table_and_key(self.mc, new), " status_name=", get_by_table_and_key(self.mc, status_name)
      except TypeError:
        new_key = self.add_random(value)
        if self.mc.add(self.main_table, key, [None, new_key, self.transaction_status]):
          break
        continue
      if status_name == self.transaction_status:
        self.cache[key] = value
        if old == new:
          new_key = self.add_random(value)
          if self.mc.cas(self.main_table, key, [old, new_key, self.transaction_status]):
            break
          else:
            del_table, del_key = separate_table_and_key(new_key)
            self.mc.delete(del_table, del_key)
        else:
          target_table, target_key = serializer.deserialize(new)
          if not self.mc.replace(target_table, target_key, value):
            raise AbortException
          break
      else:
        status_table, status_key = serializer.deserialize(status_name)
        state = self.mc.gets(status_table, status_key)
        next_old, to_delete = None, None
        try:
          next_old, to_delete = read_repeatable(old, new, state)
        except TypeError:
          resolver(status_name)
          continue
        new_key = self.add_random(value)
        result = self.mc.cas(self.main_table, key, [next_old, new_key, self.transaction_status])
        if result:
          self.cache[key] = value
          if to_delete != None and to_delete != next_old:
            target_table, target_key = serializer.deserialize(to_delete)
            self.mc.delete(target_table, target_key)
            #print "setting cache:",self.cache
          break
  def get_committed(self, key):
    while 1:
      old = new = status_name = None
      try:
        #print "get:",self.mc.gets(key)
        old, new, status_name = self.mc.gets(self.main_table, key)
      except TypeError:
        #print "typeerror"
        return None  # read committed!!
      if status_name == self.transaction_status:
        return self.cache[key]
        #return self.mc.get(new)
      else:
        status_table, status_key = serializer.deserialize(status_name)
        state = self.mc.gets(status_name)
        committed_value, to_delete = read_committed(old,new,state)
        if state != 'active':
          delete_table, delete_key = serializer.deserialize(to_delete)
          self.mc.delete(delete_table, delete_key)
        value_table, value_key = serializer.deserialize(committed_value)
        result = self.mc.get(value_table, value_key)
        self.cache[key] = result
        return result

  def get_repeatable(self, key):
    resolver = self.resolver(self.mc)
    while 1:
      old = new = status_name = None
      try:
        old, new, status_name = self.mc.gets(self.main_table, key)
        print "get: old=", get_by_table_and_key(self.mc,old), " new=", get_by_table_and_key(self.mc, new), " status_name=", get_by_table_and_key(self.mc, status_name)
      except TypeError:
        print "typeerror"
        return None  # read committed!!
      if status_name == self.transaction_status: # I already got
        print 'cache hit'
        return self.cache[key]
      else:
        status_table, status_key = serializer.deserialize(status_name)
        state = self.mc.gets(status_table, status_key)
        try:
          committed_value, to_delete = read_repeatable(old, new, state)
        except TypeError:
          resolver(status_name)
          continue
        if self.mc.cas(self.main_table, key, [committed_value,
                                              committed_value,
                                              self.transaction_status]):
          if state != 'active' and new != old:
            delete_table, delete_key = serializer.deserialize(to_delete)
            self.mc.delete(delete_table, delete_key)
        else:
          continue
        target_table, target_key = serializer.deserialize(committed_value)
        result = self.mc.gets(target_table, target_key)
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
  mc = client.HibariClient('localhost', 7580)
  def init(s, g):
    s('counter',0)
  def incr(setter, getter):
    d = getter('counter')
    print "counter:",d
    setter('counter', d+1)
  result = rr_transaction(mc, init)
  print 'set counter ok'

  print result
  for i in range(10000):
    result = rr_transaction(mc, incr)
  print result['counter']
