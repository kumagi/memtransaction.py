from memtransaction import rr_transaction
from memcache import Client

def init(setter, getter):
    setter('counter',0)

if __name__ == '__main__':
    mc = Client(["127.0.0.1:11211"])
    mc.flush_all()
    
    def _(setter, getter):
        getter('counter')
    print 'counter:',_['counter']
