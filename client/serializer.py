from pickle import Pickler
from pickle import Unpickler
from StringIO import StringIO

def serialize(obj):
  buff = StringIO()
  Pickler(buff).dump(obj)
  return buff.getvalue()
def deserialize(binary):
  return Unpickler(StringIO(binary)).load()

if __name__ == '__main__':
  dat = serialize([2,4,"3223",{"yama":4}])
  print dat
  des = deserialize(dat)
  print des
