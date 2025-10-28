# Parser Server 使用指南

## 功能概述
Parser Server 是一个异步文件解析工具，支持从OSS下载文件、线上解析和本地文件解析。

## 使用方法

### 1. 下载 OSS 文件
```bash
C:/Users/26567/anaconda3/envs/big-data/python.exe f:/parser_server/parser_cli.py oss-download-folder --folder-prefix "zfw/1760441100" --local-dir "./spider_log"
```

### 2. 线上解析
```bash
& C:/Users/26567/anaconda3/envs/big-data/python.exe parser_cli.py oss-parse zfw --object-key zfw/1760441250/
```

### 3. 本地解析线下文件
```bash
& C:/Users/26567/anaconda3/envs/big-data/python.exe parser_cli.py parse zfw --log-identifier zfw/1760441130
```

## 参数说明
- --folder-prefix: OSS文件夹前缀路径
- --local-dir: 本地存储目录
- --object-key: OSS对象键名
- --log-identifier: 日志标识符

## 工作流程
1. 异步读取文件内容
2. 根据文件类型调用相应的解析器
3. 将解析结果按ID分组缓冲
4. 合并相同ID的数据并写入输出文件

## 性能特点
- 支持异步处理提高效率
- 多线程处理同步解析器
- 自动合并重复数据
- 线程安全的写入操作