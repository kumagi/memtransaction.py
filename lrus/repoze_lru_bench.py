from repoze.lru import LRUCache

from pylru import lrucache
from time import time
from random import randint
import sys

def benchtime(fn):
    oldtime = time()
    fn()
    return time() - oldtime

N = int(sys.argv[1])
size = int(sys.argv[2])

cache = LRUCache(size)

@benchtime
def random_generate():
  for i in range(N):
    randint(0,N*100)

@benchtime
def put_bench():
  for i in range(N):
    cache.put(i,i)
@benchtime
def put_overwrite_bench():
  for i in range(N):
    cache.put(i, N - size + (i % size))
@benchtime
def put_random_bench():
  for i in range(N):
    cache.put(i, randint(0, N))
@benchtime
def get_bench():
  for i in range(N):
    try:
      result = cache.get(randint(0,N))
    except KeyError:
      pass
@benchtime
def success_get_bench():
  for i in range(N):
    try:
      result = cache.get(randint(N-size,N))
    except KeyError:
      pass
@benchtime
def fail_get_bench():
  for i in range(N):
    try:
      result = cache.get(randint(0,N-size))
    except KeyError:
      pass

print random_generate," sec "
print N, "put      ", put_bench, "sec"
print N, "put_ovw  ", put_overwrite_bench, "sec"
print N, "put_rnd  ", put_random_bench - random_generate, "sec"
print N, "get      ", get_bench - random_generate, "sec"
print N, "suc_get  ", success_get_bench - random_generate, "sec"
print N, "fail_get ", fail_get_bench - random_generate, "sec"
