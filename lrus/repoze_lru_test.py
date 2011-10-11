import repoze.lru

cache = repoze.lru.LRUCache(1000)

for i in range(1001):
  cache.put(i,i*i)

assert cache.get(0) == None
assert cache.get(1) == 1
