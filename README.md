# Parser Server 从 0 到跑起来（完整安装与启动指南）

> 本文档基于当前代码仓库实际实现整理，目标是让你在一台新机器上，从零配置到成功跑通 `parser_server`。

---

## 1. 项目是什么

这个项目包含两部分：

1. **解析器命令行**（`parser_cli.py` + `base_parser.py` + `parser_script/*.py`）
   - 支持解析本地日志文件
   - 支持直接解析 OSS 对象 / OSS 目录
   - 支持先下载 OSS 目录到本地

2. **Flask API 服务**（`app.py`）
   - 提供接口查询 MySQL 数据
   - 提供接口触发 OSS 下载与解析
   - 解析成功后可回写数据库状态与解析结果文件名

---

## 2. 运行前准备

### 2.1 系统与 Python

- Windows / Linux 都可
- Python 3.10+（建议 3.10 或 3.11）

### 2.2 必需外部依赖

#### A. MySQL（必需）

项目会读写 MySQL（如 `/api/parser/list`、`/api/oss_parse`），必须可连接。

你需要准备：

- `host`
- `port`
- `user`
- `password`
- `database`（默认代码里是 `parser`）

#### B. 阿里云 OSS（必需）

项目通过 `oss2` 访问 OSS，需要：

- `OSS_ACCESS_KEY_ID`
- `OSS_ACCESS_KEY_SECRET`
- `OSS_ENDPOINT`
- `OSS_BUCKET_NAME`
- `SPIDER_BUCKET`

---

## 3. 克隆项目与进入目录

```bash
git clone <你的仓库地址>
cd parser_server
```

---

## 4. 创建虚拟环境并安装依赖

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -U pip
pip install flask flask-cors aiofiles oss2 pymysql
```

### Linux/macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install flask flask-cors aiofiles oss2 pymysql
```

---

## 5. 配置 `config.py`

打开 `config.py`，把以下配置改成你自己的真实值：

- OSS 配置
  - `OSS_ACCESS_KEY_ID`
  - `OSS_ACCESS_KEY_SECRET`
  - `OSS_ENDPOINT`
  - `OSS_BUCKET_NAME`
  - `SPIDER_BUCKET`
- 服务配置
  - `SERVER_HOST`
  - `SERVER_PORT`
- Python/脚本路径（用于 `app.py` 调用 `parser_cli.py`）
  - `PYTHON_PATH`
  - `SCRIPT_PATH`
- 数据库配置
  - `DB_HOST`
  - `DB_PORT`
  - `DB_USER`
  - `DB_PASSWORD`
  - `DB_NAME`

### 关于路径的关键说明

`app.py` 会通过 `subprocess` 执行：

- `PYTHON_PATH`：应指向当前实际可用的 python（建议虚拟环境 python）
- `SCRIPT_PATH`：应指向当前项目里的 `parser_cli.py` 绝对路径

例如（Windows）：

```python
PYTHON_PATH = r"F:/parser_server/.venv/Scripts/python.exe"
SCRIPT_PATH = r"F:/parser_server/parser_cli.py"
```

---

## 6. 准备目录结构

项目运行时会使用这些目录：

- `spider_log/`：日志文件与下载文件记录
- `parsed_results/`：解析结果输出目录（代码会自动创建）
- `parser_script/`：每个爬虫对应一个解析脚本（如 `zfw.py`）

建议先手动确认存在：

```bash
mkdir spider_log
mkdir parsed_results
mkdir parser_script
```

> Windows 下目录已存在可忽略报错。

---

## 7. 准备解析器脚本（非常重要）

`parser_cli.py` 会按爬虫名加载脚本：

- `parser_script/<spider_name>.py`

并且该文件里必须有一个继承 `BaseParser` 的类，否则命令会报：

- “解析器脚本不存在”
- 或 “未找到 BaseParser 的子类”

例如你要跑 `zfw`，就必须有：

- `parser_script/zfw.py`

---

## 8. MySQL 初始化（最小可跑）

先建库：

```sql
CREATE DATABASE IF NOT EXISTS parser DEFAULT CHARACTER SET utf8mb4;
```

接口里 `project` 会被当作表名使用。比如 `project=zfw`，就需要 `zfw` 表。

最小字段建议：

- `id`（主键）
- `path`
- `status`
- `parser_path`

示例：

```sql
USE parser;

CREATE TABLE IF NOT EXISTS zfw (
  id BIGINT PRIMARY KEY,
  path VARCHAR(1024) NULL,
  status VARCHAR(32) NULL,
  parser_path VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

---

## 9. 启动服务

在项目根目录执行：

```bash
python app.py
```

默认监听端口看 `config.py` 的 `SERVER_PORT`（当前代码默认 5005）。

---

## 10. 从命令行直接使用解析器（不经过 API）

### 10.1 解析本地日志清单

```bash
python parser_cli.py parse zfw --log-identifier zfw/1760441130
```

日志文件会按这个规则读取：

- `spider_log/zfw/1760441130.log`

该 `.log` 文件每行应是一个本地文件路径。

### 10.2 直接解析 OSS 目录

```bash
python parser_cli.py oss-parse zfw --object-key zfw/1760441250/
```

### 10.3 下载 OSS 目录到本地

```bash
python parser_cli.py oss-download-folder --folder-prefix "zfw/1760441100" --local-dir "./spider_log"
```

---

## 11. API 使用示例

### 11.1 健康检查

```http
GET /api/hello?name=test
```

### 11.2 分页查询项目表数据

```http
GET /api/parser/list?id=zfw&page=1&per_page=10
```

### 11.3 列出 OSS 子目录

```http
GET /api/oss/list?bucket=house-spider&prefix=zfw/
```

### 11.4 触发 OSS 解析并回写数据库

```http
GET /api/oss_parse?project=zfw&object_key=zfw/1760441250/&id=1760441250
```

成功时会尝试把 `zfw` 表中对应 `id` 的：

- `status` 更新为 `'1'`
- `parser_path` 更新为解析输出文件名（如果能提取到）

---

## 12. 常见报错与排查

1. **`解析器脚本不存在`**
   - 检查 `parser_script/<spider_name>.py` 是否存在。

2. **`未找到BaseParser的子类`**
   - 检查你的解析器类是否正确继承 `BaseParser`。

3. **`Table 'xxx' doesn't exist`**
   - `project` 对应的表没建。

4. **OSS 访问失败**
   - 检查 AK/SK、Endpoint、Bucket、网络连通性。

5. **API 能调但不写库**
   - 检查 `id` 是否存在、表字段是否有 `status/parser_path`、DB 权限是否足够。

6. **子进程调用失败**
   - 重点检查 `config.py` 中 `PYTHON_PATH` 和 `SCRIPT_PATH` 是否是当前机器有效绝对路径。

---

## 13. 一次性快速自检（推荐）

按这个顺序验证：

1. `python parser_cli.py -h` 能输出帮助
2. `python app.py` 能正常启动
3. `GET /api/hello` 正常返回
4. `GET /api/parser/list?id=<你的表名>` 能查到数据
5. 调用 `/api/oss_parse` 后数据库 `status` 能变更
6. `parsed_results/` 下能看到新产物

---

## 14. 安全提醒（强烈建议）

当前项目采用 `config.py` 明文配置密钥。上线前建议：

- 改为环境变量读取 OSS/MySQL 凭证
- 不要把真实密钥提交到公开仓库
- 对数据库账号最小化授权

---

如果你希望，我下一步可以继续帮你把这个 README 再补一版“**给前端同学联调**”的接口清单（参数、返回示例、错误码），直接可贴给前端使用。