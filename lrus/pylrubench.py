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

cache = lrucache(size)

@benchtime
def random_generate():
  for i in range(N):
    randint(0,N*100)

@benchtime
def put_bench():
  for i in range(N):
    cache[i] = i
@benchtime
def put_overwrite_bench():
  for i in range(N):
    cache[i] = N - size + (i % size)
@benchtime
def put_random_bench():
  for i in range(N):
    cache[i] = randint(0, N)
@benchtime
def get_bench():
  for i in range(N):
    try:
      result = cache[randint(0,N)]
    except KeyError:
      pass
@benchtime
def success_get_bench():
  for i in range(N):
    try:
      result = cache[randint(N-size,N)]
    except KeyError:
      pass
@benchtime
def fail_get_bench():
  for i in range(N):
    try:
      result = cache[randint(0,N-size)]
    except KeyError:
      pass

print N,"items for", size, "entry put      ", put_bench, "sec"
print N,"items for", size, "entry put_ovw  ", put_overwrite_bench, "sec"
print N,"items for", size, "entry put_rnd  ", put_random_bench - random_generate, "sec"
print N,"items for", size, "entry get      ", get_bench - random_generate, "sec"
print N,"items for", size, "entry suc_get  ", success_get_bench - random_generate, "sec"
print N,"items for", size, "entry fail_get ", fail_get_bench - random_generate, "sec"







