import redis

r = redis.Redis(host='111.231.79.146', port=19000)
print r.get('ad:cvr_ftrl')
