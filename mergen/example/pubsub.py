import redis
r = redis.Redis(port=6380)
#r = redis.Redis()

#r.set("foo", "bar")
#print "setted"

pubsub = r.pubsub()

# r.execute_command("SUBSCRIBE", "ccc", "bar")

pubsub.subscribe("foo")
print "sent subsc"
for msg in pubsub.listen():
    print "in listen mode"
    print "received >>>", msg