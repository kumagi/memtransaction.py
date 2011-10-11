import thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from sys import path
path.append('gen-py')
from hibari import Hibari
from hibari import ttypes

socket = TSocket.TSocket('localhost',7600)
socket.setTimeout(None)
transport = TTransport.TBufferedTransport(socket)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hibari.Client(protocol)
socket.open()

# create the input parameter object
response = None
request = ttypes.Add(table = b"tab2", key = "defookey", value = b"hello wol")
response = client.Add(request)
print response

response = client.Get(ttypes.Get(b"tab1", b'fookey'))
print response.value

socket.close()
