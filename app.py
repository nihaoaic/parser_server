from flask import Flask, request, jsonify
import oss2
from config import OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, SPIDER_BUCKET, PARSED_RESULTS_DIR
import logging
import subprocess
import config
import os
import pymysql

# 从配置模块导入 SCRIPT_PATH
SCRIPT_PATH = config.SCRIPT_PATH
PYTHON_PATH = config.PYTHON_PATH

# 检查文件是否存在
parse_path = config.PARSED_RESULTS_DIR
if os.path.exists(parse_path):
    print(f"目录 {parse_path} 存在")
else:
    print(f"目录 {parse_path} 不存在")
    os.makedirs(parse_path)

app = Flask(__name__)

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    'host': config.DB_HOST,
    'port': config.DB_PORT,
    'user': config.DB_USER,
    'password': config.DB_PASSWORD,
    'database': config.DB_NAME,
    'charset': 'utf8mb4'
}

# 初始化OSS客户端的函数
def get_oss_client(bucket_name):
    try:
        auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
        endpoint = OSS_ENDPOINT
        bucket = oss2.Bucket(auth, endpoint, bucket_name)
        return bucket
    except Exception as e:
        logger.error(f"初始化OSS客户端失败: {str(e)}")
        return None

# 数据库连接函数
def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

@app.route('/')
def home():
    return "欢迎来到Flask应用!"

@app.route('/api/hello', methods=['GET'])
def hello():
    name = request.args.get('name', 'World')
    return jsonify({"message": f"Hello, {name}!"})

@app.route('/api/data', methods=['POST'])
def post_data():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    # 处理数据的逻辑可以在这里添加
    response = {
        "message": "Data received successfully",
        "received_data": data
    }
    return jsonify(response), 201

# 新增：查询MySQL zfw表的API（支持分页）
@app.route('/api/parser/list', methods=['GET'])
def list_zfw_records():
    """
    查询指定表中的记录，支持分页
    参数:
    - id: 表名（必填）
    - page: 页码，默认为1
    - per_page: 每页记录数，默认为10，最大100
    - status: 状态过滤条件（可选）
    """
    try:
        # 获取表名参数
        table_name = request.args.get('id')
        if not table_name:
            return jsonify({"error": "Missing required parameter: id (table name)"}), 400
        
        # 验证表名是否合法（防止SQL注入）
        if not table_name.replace('_', '').replace('-', '').isalnum():
            return jsonify({"error": "Invalid table name"}), 400
        
        # 获取分页参数
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)
        status = request.args.get('status', None, type=str)
        
        # 限制每页最大记录数
        per_page = min(per_page, 100)
        
        # 计算OFFSET
        offset = (page - 1) * per_page
        
        # 构建SQL查询
        connection = get_db_connection()
        with connection.cursor() as cursor:
            # 构建基础查询语句
            base_query = f"SELECT id, path, status FROM {table_name}"
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            
            # 添加过滤条件
            if status:
                where_clause = " WHERE status = %s"
                params = [status]
            else:
                where_clause = ""
                params = []
            
            # 完整查询语句
            full_query = base_query + where_clause + " LIMIT %s OFFSET %s"
            full_count_query = count_query + where_clause
            
            # 执行计数查询
            cursor.execute(full_count_query, params)
            total_records = cursor.fetchone()[0]
            
            # 执行数据查询
            query_params = params + [per_page, offset]
            cursor.execute(full_query, query_params)
            records = cursor.fetchall()
            
            # 格式化结果
            result_records = []
            for record in records:
                result_records.append({
                    'id': record[0],
                    'path': record[1],
                    'status': record[2]
                })
            
            # 计算分页信息
            total_pages = (total_records + per_page - 1) // per_page
            
            # 构建响应
            response = {
                'records': result_records,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total_records': total_records,
                    'total_pages': total_pages
                },
                'table': table_name
            }
            
            if status:
                response['filter'] = {'status': status}
            
            return jsonify(response), 200
            
    except pymysql.err.ProgrammingError as e:
        if "doesn't exist" in str(e):
            return jsonify({"error": f"Table '{table_name}' does not exist"}), 404
        else:
            logger.error(f"查询{table_name}表时发生错误: {str(e)}")
            return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"查询{table_name}表时发生错误: {str(e)}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500
    finally:
        try:
            connection.close()
        except:
            pass

# 新增���查询OSS目录信息的API
@app.route('/api/oss/list', methods=['GET'])
def list_oss_subdirs():
    """
    查询 OSS 指定目录下的子目录（仅返回一级目录）
    参数:
    - bucket: OSS存储桶名称 (可选，默认使用配置中的 SPIDER_BUCKET)
    - prefix: 目录前缀，用于过滤对象 (可选)            zfw/
    """
    bucket_name = request.args.get('bucket', SPIDER_BUCKET)
    prefix = request.args.get('prefix', '')
    try:
        # 获取OSS客户端
        bucket = get_oss_client(bucket_name)
        if not bucket:
            return jsonify({"error": "Failed to initialize OSS client"}), 500

        # 使用 delimiter='/' 模拟文件夹结构
        result = bucket.list_objects(prefix=prefix, delimiter='/')

        # 仅保留子目录
        subdirs = []
        if result.prefix_list:
            try:
                subdirs = [p.prefix if hasattr(p, 'prefix') else str(p) for p in result.prefix_list]
            except Exception as e:
                logger.error(f"Error processing prefix_list: {str(e)}")
                subdirs = []

        logger.info(f"Found {len(subdirs)} subdirectories under prefix '{prefix}'")

        response = {
            'bucket': bucket_name,
            'prefix': prefix,
            'subdirectories': subdirs,
            'is_truncated': result.is_truncated,
            'next_marker': result.next_marker
        }

        return jsonify(response), 200

    except oss2.exceptions.NoSuchBucket:
        return jsonify({"error": f"Bucket '{bucket_name}' does not exist"}), 404
    except Exception as e:
        logger.error(f"查询OSS子目录时发生错误: {str(e)}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@app.route("/api/oss_download_folder", methods=["GET"])
def oss_download_folder():
    """
    调用 parser_cli.py 下载 OSS 文件夹
    示例：
    GET /api/oss_download_folder?project=zfw&folder_prefix=zfw/1760441130&local_dir=./spider_log
    实际执行：
    C:/Users/26567/anaconda3/envs/big-data/python.exe .\parser_cli.py zfw oss-download-folder --folder-prefix "zfw/1760441130" --local-dir "./spider_log"
    """
    try:
        # 获取请求参数
        project = request.args.get("project")
        folder_prefix = request.args.get("folder_prefix")
        local_dir = request.args.get("local_dir", "./spider_log")

        if not project or not folder_prefix:
            return jsonify({"error": "Missing required parameters: project or folder_prefix"}), 400

        # 拼接命令
        cmd = [
            PYTHON_PATH,
            SCRIPT_PATH,
            project,
            "oss-download-folder",
            "--folder-prefix", folder_prefix,
            "--local-dir", local_dir
        ]

        print(f"[命令执行] {' '.join(cmd)}")

        # 调用 subprocess 执行命令
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=False
        )

        stdout, stderr = process.communicate()
        exit_code = process.returncode

        # 构造返回结果
        result = {
            "cmd": " ".join(cmd),
            "exit_code": exit_code,
            "stdout": stdout.strip(),
            "stderr": stderr.strip(),
        }

        if exit_code == 0:
            return jsonify({"status": "success", **result}), 200
        else:
            return jsonify({"status": "error", **result}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/oss_parse", methods=["GET"])
def oss_parse():
    """
    调用 parser_cli.py 进行线上解析
    示例：
    GET /api/oss_parse?project=zfw&object_key=zfw/1760441250/
    实际执行：
    C:/Users/26567/anaconda3/envs/big-data/python.exe parser_cli.py oss-parse zfw --object-key zfw/1760441250/
    """
    try:
        # 获取请求参数
        project = request.args.get("project")
        object_key = request.args.get("object_key")

        if not project or not object_key:
            return jsonify({"error": "Missing required parameters: project or object_key"}), 400

        # 拼接命令
        cmd = [
            PYTHON_PATH,
            SCRIPT_PATH,
            "oss-parse",
            project,
            "--object-key", object_key
        ]

        print(f"[命令执行] {' '.join(cmd)}")

        # 调用 subprocess 执行命令
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=False
        )

        stdout, stderr = process.communicate()
        exit_code = process.returncode

        # 构造返回结果
        result = {
            "cmd": " ".join(cmd),
            "exit_code": exit_code,
            "stdout": stdout.strip(),
            "stderr": stderr.strip(),
        }

        if exit_code == 0:
            return jsonify({"status": "success", **result}), 200
        else:
            return jsonify({"status": "error", **result}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
