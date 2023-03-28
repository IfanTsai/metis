# metis [![GitHub](https://img.shields.io/github/license/IfanTsai/metis?style=flat)](https://github.com/IfanTsai/metis/blob/master/LICENSE)

A simple Redis server clone written in Golang.

### Command

You can use redis-cli to connect to metis. Supported commands are listed below.

- PING
- SELECT
- AUTH
- SET
- GET
- KEYS
- EXPIRE
- TTL
- RANDOMKEY
- HSET
- HGET
- HDEL
- LPUSH
- RPUSH
- LPOP
- RPOP
- LLEN
- LINDEX
- LRANGE
- SADD
- SREM
- SPOP
- SCARD
- SISMEMBER
- SMEMBERS
- SDIFF
- SINTER
- SUNION
- ZADD
- ZRANGE
- ZRANGEBYSCORE
- ZREM
- ZREMBYRANK
- ZREMBYSCORE
- ZCARD
- ZCOUNT
- ZSCORE

### Run

```bash
# cat config.toml
go run main.go
```

![image-20230131230958613](https://img.caiyifan.cn/typora_pico/image-20230131230958613.png) 
