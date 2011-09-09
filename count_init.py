from memtransaction import transaction
from memcache import Client

def init(mc):
    @transaction(mc)
    def _(s, g):
        s('counter',0)

if __name__ == '__main__':
    mc = Client(["127.0.0.1:11211"])
    mc.flush_all()
    @transaction(mc)
    def _(s,g):
        s('counter', 0)
    @transaction(mc)
    def _(setter, getter):
        getter('counter')
    print 'counter:',_['counter']
