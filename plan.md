## 更推荐的方向：升级成严肃的事务型 KV

我建议把 SkiffDB 从“Redis-like cache”升级成：

> 一个支持持久化 Raft、MVCC、事务和多种读取一致性的分布式 KV。

这是从现有代码自然演进，而且已经足够成为很强的简历项目。

### 第一阶段：把分布式正确性做实

- 三个 voter，non-voter 同步完成后再提升。
- Raft log、stable state、snapshot 全部落盘。
- 节点重启和整个集群重启恢复。
- Linearizable、leader-local、stale 三种读取模式。
- Leader discovery 和自动重定向。
- 安全的成员新增、删除和替换。
- 明确超时后的 unknown outcome。
- 修复命令大小写绕过复制的问题。

做到这里，它就已经不是普通玩具项目了。

### 第二阶段：增加数据库内核含量

- 有序 KV，而不只是 Go map。
- Range scan。
- Batch write。
- Compare-and-swap。
- MVCC 多版本。
- Snapshot read。
- 单 Raft Group 内的原子事务。
- Watch 和 Lease/TTL。
- Snapshot 与日志压缩。

此时可以把定位改为“事务型元数据存储”，比 Redis clone 更有辨识度。

### 第三阶段：用证据证明正确性

这是简历含金量最高的部分：

- Leader 写入后立即宕机。
- 网络分区和脑裂测试。
- follower 长时间落后后恢复。
- snapshot 期间持续写入。
- 随机进程 kill/restart。
- 磁盘损坏和截断日志测试。
- Linearizability checker。
- Chaos test。
- 与 etcd/Redis 的延迟和吞吐对比。

一个能展示“跑了十万次故障注入，没有发现线性一致性违规”的 KV，远比一个只能执行 SELECT 的所谓分布式 SQL 数据库更有说服力。
