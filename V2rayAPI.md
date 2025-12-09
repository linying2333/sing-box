# ZH-CN
## 此fork为`sing-box`的`v2ray api`的 数据统计 功能添加了一个数据持久化, 每隔指定的时间将内存的数据保存到bbolt数据库中.

##### 该功能的json开启方式如下:
```json
{
  "experimental": {
    // ...其他参数
    "v2ray_api": {
      "stats": {
        "enabled": true,
        // 持久化流量信息
        "store": {
          "enabled": true, // 启用持久化
          "path": "stats.db", // 持久化文件路径，默认使用`stats.db`, 不可与字段experimental.cache_file.path重复
          "stats_id": "", // 文件中的标识符, 如果不为空, 配置特定的数据将使用由其键控的单独存储(参阅字段experimental.cache_file.cache_id)
          "interval": "30s" // 数据库同步间隔(无更新不同步), 默认使用 30秒
        }
        // ...其他参数
      }
    }
    // ...其他参数
  }
}
```

##### bbolt数据库路径模板如下
```text
主键路径: v2ray_api.stats
入站流量: v2ray_api.stats.inbounds.<json中experimental.v2ray_api.stats.inbounds的tag>.traffic.[up|down]
出站流量: v2ray_api.stats.outbounds.<json中experimental.v2ray_api.stats.outbounds的tag>.traffic.[up|down]
用户流量: v2ray_api.stats.users.<json中experimental.v2ray_api.stats.users的tag>.traffic.[up|down]
```

# EN
## This fork adds data persistence to the `v2ray api` statistics feature of `sing-box`. It periodically saves in-memory data into a bbolt database at a specified interval.

##### The JSON example for this feature is as follows:
```json
{
  "experimental": {
    // ...other parameters
    "v2ray_api": {
      "stats": {
        "enabled": true,
        // Persist traffic information
        "store": {
          "enabled": true, // Enable persistence
          "path": "stats.db", // Path to persistence file, defaults to `stats.db`; must not conflict with experimental.cache_file.path
          "stats_id": "", // Identifier in the file; if not empty, specific configured data will use separate storage keyed by this (see experimental.cache_file.cache_id)
          "interval": "30s" // Database sync interval (no update, no sync); defaults to 30 seconds
        }
        // ...other parameters
      }
    }
    // ...other parameters
  }
}
```

##### bbolt database path template:
```text
Primary key path: v2ray_api.stats
Inbound traffic: v2ray_api.stats.inbounds.<tag from experimental.v2ray_api.stats.inbounds in JSON>.traffic.[up|down]
Outbound traffic: v2ray_api.stats.outbounds.<tag from experimental.v2ray_api.stats.outbounds in JSON>.traffic.[up|down]
User traffic: v2ray_api.stats.users.<tag from experimental.v2ray_api.stats.users in JSON>.traffic.[up|down]
```
