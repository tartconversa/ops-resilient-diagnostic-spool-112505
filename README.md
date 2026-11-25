# 诊断日志积压时的磁盘保护与交付接续

诊断日志演练只使用本地合成记录和本地模拟收集端。size_units 是演示配额单位，由输入提供，不等于 Python 对象本身的内存。

## 在本地查看资料

运行环境使用 Python 3.11.2 和标准库。启动资料浏览器无需安装额外包。

```sh
python materials.py
python materials.py sources
python -m unittest discover -s tests -v
```

资料浏览器也接受下方的其他资料名。JSONL 使用 UTF-8 编码，每行是一个对象。表内的顺序对应来源记录，事件时间与接收时间的区别以资料说明为准。各入口可以在无外网的环境中运行。

资料字段和业务口径见[演练资料格式](materials/README.md)。

## 本地合成收集端

使用 `python spool_peer.py` 启动，默认监听 127.0.0.1:8099。用 `--state` 选择收集端保留的本地 SQLite 文件。HTTP `POST /batch` 接收带整数 `at_ms` 和数组 `records` 的 JSON，每条记录需要稳定的 `record_id`。成功响应的 `accepted` 列出已经接收的标识。连接可能在接收之后断开。HTTP `GET /received` 可查询该收集端实际保有的标识，合成收集端重复收同一标识只记一次。该状态库属于收集端。
