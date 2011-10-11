import lru

cache = lru.LRUCache(1000)

for i in range(1001):
  cache[i] = i*i

try:
  cache[0]
  assert False
except KeyError:
  pass
assert cache[1] == 1
