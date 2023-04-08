# metis [![GitHub](https://img.shields.io/github/license/IfanTsai/metis?style=flat)](https://github.com/IfanTsai/metis/blob/master/LICENSE)

A simple Redis server clone written in Golang.

### Features

You can use redis-cli to connect to metis server. Supported commands are listed in [command table](https://github.com/IfanTsai/metis/blob/master/server/command.go#L21).

Key Features:

- Support datastructures: string, list, hash, set, sorted set
- Multi databases and `SELECT` command
- TTL for keys, support `EXPIRE` and `TTL` commands 
- Auth by password, support `AUTH` command
- AOF persistence and rewrite, support rewrite manually by `BGREWRITEAOF` command

### Run

```bash
# cat config.toml
go run main.go
```

![image-20230131230958613](https://img.caiyifan.cn/typora_pico/image-20230131230958613.png) 
