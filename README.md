# metis [![GitHub](https://img.shields.io/github/license/IfanTsai/metis?style=flat)](https://github.com/IfanTsai/metis/blob/master/LICENSE)

A simple Redis server clone written in Golang.

### Command

You can use redis-cli to connect to metis. Supported commands are listed below.

- PING
- SET
- GET
- KEYS
- EXPIRE
- TTL
- RANDOMKEY
- ZADD
- ZRANGE
- ZRANGEBYSCORE
- ZREM
- ZREMBYRANK
- ZREMBYSCORE
- ZCARD
- ZCOUNT
- ZSCORE
- LPUSH
- RPUSH
- LPOP
- RPOP
- LLEN
- LINDEX
- LRANGE

### Run

```bash
# cat config.toml
go run main.go
```

![image-20230131230958613](https://img.caiyifan.cn/typora_pico/image-20230131230958613.png) 
