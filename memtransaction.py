# -*- coding: utf-8 -*-

import time
import memcache

from time import sleep
from random import randint



class AbortException(Exception):
    pass

def read_commited(old, new, status):
    if status == 'commited':
        return new
    elif status == 'abort' or status == 'active':
        return old
    else:
        raise Exception('invalid status' + status)
def read_repeatable(old, new, status):
    if status == 'commited':
        return new
    elif status == 'abort':
        return old
    elif status == 'active':
        return None
    else:
        raise Exception('invalid status' + status)

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
    def begin(self):
        self.transaction_status = self.add_random('active')
        self.cache = {}
    def commit(self):
        status = self.mc.gets(self.transaction_status)
        if status != 'active': raise AbortException
        return self.mc.cas(self.transaction_status, 'commited')
    class resolver():
        def __init__(self, mc):
            self.count = 10
            self.mc = mc
        def __call__(self, other_status):
            sleep(0.001 * randint(0, 1 << self.count))
            if self.count <= 10:
                self.count += 1
            else:
                self.count = 0
                #print 'cas: ', other_status, '-> abort'
                self.mc.cas(other_status, 'abort')

    def set(self, key, value):
        new_key = self.add_random(value)
        resolver = self.resolver(self.mc)
        while 1:
            old = new = status_name = None
            try:
                #print "set:",self.mc.gets(key)
                old, new, status_name = self.mc.gets(key)
            except TypeError:
                if self.mc.add(key,[None, new_key, self.transaction_status]):
                    break
                continue
            #print "old:%s ,new:%s status:%s" % (old, new, status_name)
            if status_name == self.transaction_status:
                self.cache[key] = value
                if old == new:
                    if self.mc.cas(key, [old, new_key, self.transaction_status]):
                        break
                else:
                    if not self.mc.replace(new, value):
                        #print 'abort!!'
                        raise AbortException
                    else:
                        self.mc.delete(new_key)
                    break
            else:
                state = self.mc.get(status_name)
                next_old = read_repeatable(old,new,state)
                if next_old == None:
                    #print 'set: contention'
                    resolver(status_name)
                    continue
                #print "set: cas for ", [next_old, new_key, self.transaction_status]
                result = self.mc.cas(key,[next_old, new_key, self.transaction_status])
                if result:
                    self.cache[key] = value
                    #print "setting cache:",self.cache
                    break
    def get(self, key):
        while 1:
            old = new = status_name = None
            try:
                #print "get:",self.mc.gets(key)
                old, new, status_name = self.mc.gets(key)
            except TypeError:
                #print "typeerror"
                return None  # read commited!!
            if status_name == self.transaction_status:
                #print 'cache hit'
                return self.cache[key]
                #return self.mc.get(new)
            else:
                state = self.mc.get(status_name)
                if state == 'commited':
                    commited_value, to_delete = new, old
                elif state == 'abort':
                    commited_value, to_delete = old, new
                elif state == 'active':
                    commited_value, to_delete = old, None
                else:
                    raise Exception
                result = self.mc.get(commited_value)
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
                return None  # read commited!!
            if status_name == self.transaction_status:
                #print 'cache hit'
                return self.cache[key]
                #return self.mc.get(new)
            else:
                state = self.mc.get(status_name)
                if state == 'commited':
                    commited_value, to_delete = new, old
                elif state == 'abort':
                    commited_value, to_delete = old, new
                elif state == 'active':
                    resolver(status_name)
                    continue
                else:
                    raise Exception
                if self.mc.cas(key, [commited_value,
                                     commited_value,
                                     self.transaction_status]):
                    if to_delete != None and new != old:
                        self.mc.delete(to_delete)
                else:
                    continue
                result = self.mc.get(commited_value)
                self.cache[key] = result
                return result
def transaction(kvs):
    def inner_transaction(tr):
        def impl():
            transaction = MemTr(kvs)
            setter = lambda k,v : transaction.set(k,v)
            getter = lambda k :   transaction.get_repeatable(k)
            while(1):
                transaction.begin()
                try:
                    tr(setter, getter)
                    if transaction.commit() == True:
                        return transaction.cache
                except AbortException:
                    continue
        return impl()
    return inner_transaction

if __name__ == '__main__':
    mc = memcache.Client(['127.0.0.1:11211'])
    @transaction(mc)
    def _(s, g):
        s('counter',0)
    for i in range(10000):
        @transaction(mc)
        def _(setter, getter):
            d = getter('counter')
            setter('counter', d+1)
    @transaction(mc)
    def _(setter, getter):
        d = getter('counter')
    assert(_['counter'] == 10000)
