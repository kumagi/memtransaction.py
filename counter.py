from memtransaction import transaction
from memcache import Client

def incr(mc):
    @transaction(mc)
    def _(setter, getter):
        d = getter('counter')
        print "counter:",d
        setter('counter', d+1)

if __name__ == '__main__':
    mc = Client(["127.0.0.1:11211"])
    for i in range(10000):
        incr(mc)
    @transaction(mc)
    def result(setter,getter):
        getter('counter')
    print 'finally:', result['counter']
