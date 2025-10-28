from flask import Flask, request, jsonify
import oss2
from config import OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, SPIDER_BUCKET
import logging
import subprocess

app = Flask(__name__)

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PYTHON_PATH = r"C:/Users/26567/anaconda3/envs/big-data/python.exe"
SCRIPT_PATH = r"f:/parser_server/parser_cli.py"  # 根据实际路径修改


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

# 新增：查询OSS目录信息的API
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
