from memtransaction import rr_transaction
from memcache import Client

def incr(setter,getter):
    d = getter('counter')
    print "counter:",d
    setter('counter', d+1)

if __name__ == '__main__':
    mc = Client(["127.0.0.1:11211"])
    for i in range(10000):
        rr_transaction(mc,incr)
    print 'finally:', result['counter']
