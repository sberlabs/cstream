all:
	mkdir -p lib
	cp -r ../mongol/mongol lib/
	sudo cp -r ../mongol/mongol /usr/local/openresty/luajit/share/lua/5.1/
	cp contrib/redis-lua/src/redis.lua lib/
	cp contrib/Microlight/ml.lua lib/

.PHONY: all

