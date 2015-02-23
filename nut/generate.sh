head -n 1000 /usr/share/dict/words | xargs -n1 redis-cli -p 22122 GET
