# 演练资料格式

诊断日志演练只使用本地合成记录和本地模拟收集端。size_units 是演示配额单位，由输入提供，不等于 Python 对象本身的内存。

## `sources` 来源的诊断保护和回看规则

horizon_ms 给出 ERROR 出现前可回看的时长上限。protected_types 是不可主动采样的记录种类。reserve_units 为本来源最大保留预算，不含全机保护空间。

字段为 `source`、`horizon_ms`、`protected_types`、`reserve_units`。

## `disk` 设施占用变化和写入压力

spool_limit 是日志所能占用的上限，total_free 是报告时刻的实际可用单位，online_floor 是必须替在线请求保留的单位。上限和空闲量独立变化。 total_free 是收到设施快照时的实际空间，此后又发生的本代理写入会减少空闲，回收则反之。

字段为 `at_ms`、`spool_limit`、`total_free`、`online_floor`。

## `records` 来源记录和诊断边界

scope 按来源标识和 trace 组成。part 从同一 scope 的 1 开始递增，记录 id 跨重送稳定。kind 为 detail、error、end 或 routine。end 的 part 给出本 scope 所有片段的最高 part。 part 表示该来源在采集前给出的顺序，允许观察到达顺序不同。已收集到的 end 并不证明较早分片已到。

字段为 `at_ms`、`source`、`trace`、`part`、`record_id`、`kind`、`size_units`。

## `endpoint` 收集端接收和确认状态

at_ms 是该模拟状态对读取者生效时间。max_accept 为单批可持久接收的记录数。reply=false 时连接在持久接收之后断开，调用方要通过查询取得已经接收的标识。

字段为 `at_ms`、`max_accept`、`reply`。
