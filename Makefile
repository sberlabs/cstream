all:
	mkdir -p lib
	cp -r contrib/mongol/mongol lib
	cp contrib/redis-lua/src/redis.lua lib
	cp contrib/Microlight/ml.lua lib

.PHONY: all

