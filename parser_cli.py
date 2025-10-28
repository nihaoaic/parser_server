#!/usr/bin/env python3
import os
import sys
from pathlib import Path
import argparse
import importlib.util
import oss2
from oss2.exceptions import OssError
import time
import asyncio
# 尝试导入配置文件
try:
    from config import OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, OSS_BUCKET_NAME
except ImportError:
    OSS_ACCESS_KEY_ID = ''
    OSS_ACCESS_KEY_SECRET = ''
    OSS_ENDPOINT = ''
    OSS_BUCKET_NAME = ''

def find_parser_class(module_path):
    """在模块中查找BaseParser的子类"""
    spec = importlib.util.spec_from_file_location("parser_module", module_path)
    parser_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(parser_module)
    
    # 查找BaseParser的子类
    for attr_name in dir(parser_module):
        attr = getattr(parser_module, attr_name)
        if isinstance(attr, type) and hasattr(attr, '__bases__') and \
           any(base.__name__ == 'BaseParser' for base in attr.__mro__[1:]):
            return attr
    
    return None

def download_from_oss(bucket_name, object_key, local_path, access_key, secret_key, endpoint):
    """从OSS下载文件"""
    try:
        # 创建认证信息
        auth = oss2.Auth(access_key, secret_key)
        
        # 创建Bucket对象
        bucket = oss2.Bucket(auth, endpoint, bucket_name)
        
        # 下载文件
        bucket.get_object_to_file(object_key, local_path)
        print(f"成功从OSS下载文件: {object_key} -> {local_path}")
        return True
    except OssError as e:
        print(f"从OSS下载文件失败: {e}")
        return False
    except Exception as e:
        print(f"下载过程中出现错误: {e}")
        return False

def download_oss_folder(bucket_name, folder_prefix, local_dir, log_file, access_key, secret_key, endpoint):
    """从OSS下载文件夹"""
    try:
        # 创建认证信息
        auth = oss2.Auth(access_key, secret_key)
        
        # 创建Bucket对象
        bucket = oss2.Bucket(auth, endpoint, bucket_name)
        
        # 确保本地目录存在
        Path(local_dir).mkdir(parents=True, exist_ok=True)
        
        # 确保日志文件目录存在
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 列出文件夹中的所有文件
        print(f"正在列出OSS文件夹 '{folder_prefix}' 中的文件...")
        
        # 写入日志文件
        with open(log_file, 'w', encoding='utf-8') as f:
            # 遍历文件夹中的所有对象
            for obj in oss2.ObjectIterator(bucket, prefix=folder_prefix):
                if not obj.key.endswith('/'):  # 跳过文件夹对象
                    # 保持原有的OSS目录结构
                    # 移除bucket名称前缀，保留完整的路径结构
                    local_file_path = Path(local_dir) / obj.key
                    
                    # 确保本地文件的目录存在
                    local_file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # 下载文件
                    try:
                        bucket.get_object_to_file(obj.key, str(local_file_path))
                        print(f"已下载: {obj.key} -> {local_file_path}")
                        
                        # 将文件路径写入日志
                        f.write(f"{local_file_path}\n")
                    except OssError as e:
                        print(f"下载文件 {obj.key} 失败: {e}")
        
        print(f"文件列表已保存到: {log_file}")
        print(f"文件已下载到: {local_dir}")
        return True
    except Exception as e:
        print(f"下载OSS文件夹过程中出现错误: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='数据解析工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 解析命令
    parse_parser = subparsers.add_parser('parse', help='解析本地文件')
    parse_parser.add_argument('spider_name', help='爬虫名称')
    parse_parser.add_argument('--log-identifier', help='日志标识符')
    
    # 新增：OSS解析命令
    oss_parse_parser = subparsers.add_parser('oss-parse', help='直接从OSS解析文件或文件夹')
    oss_parse_parser.add_argument('spider_name', help='爬虫名称')
    oss_parse_parser.add_argument('--bucket', default=OSS_BUCKET_NAME, help='OSS存储桶名称')
    oss_parse_parser.add_argument('--object-key', required=True, help='OSS对象键（可以是单个文件或目录前缀）')
    oss_parse_parser.add_argument('--log-identifier', help='日志标识符')
    
    # 新增：OSS文件夹解析命令
    oss_parse_folder_parser = subparsers.add_parser('oss-parse-folder', help='直接从OSS文件夹解析所有文件')
    oss_parse_folder_parser.add_argument('spider_name', help='爬虫名称')
    oss_parse_folder_parser.add_argument('--bucket', default=OSS_BUCKET_NAME, help='OSS存储桶名称')
    oss_parse_folder_parser.add_argument('--folder-prefix', required=True, help='OSS文件夹前缀')
    oss_parse_folder_parser.add_argument('--log-identifier', help='日志标识符')
    
    # OSS下载命令
    oss_parser = subparsers.add_parser('oss-download', help='从OSS下载文件')
    oss_parser.add_argument('--bucket', default=OSS_BUCKET_NAME, help='OSS存储桶名称')
    oss_parser.add_argument('--object-key', required=True, help='OSS对象键')
    oss_parser.add_argument('--local-path', required=True, help='本地保存路径')
    oss_parser.add_argument('--access-key', default=OSS_ACCESS_KEY_ID, help='OSS访问密钥')
    oss_parser.add_argument('--secret-key', default=OSS_ACCESS_KEY_SECRET, help='OSS密钥')
    oss_parser.add_argument('--endpoint', default=OSS_ENDPOINT, help='OSS端点URL')
    
    # OSS文件夹下载命令
    oss_folder_parser = subparsers.add_parser('oss-download-folder', help='从OSS下载文件夹')
    oss_folder_parser.add_argument('--bucket', default=OSS_BUCKET_NAME, help='OSS存储桶名称')
    oss_folder_parser.add_argument('--folder-prefix', required=True, help='OSS文件夹前缀')
    oss_folder_parser.add_argument('--local-dir', required=True, help='本地保存目录')
    oss_folder_parser.add_argument('--log-file', help='日志文件路径')
    oss_folder_parser.add_argument('--access-key', default=OSS_ACCESS_KEY_ID, help='OSS访问密钥')
    oss_folder_parser.add_argument('--secret-key', default=OSS_ACCESS_KEY_SECRET, help='OSS密钥')
    oss_folder_parser.add_argument('--endpoint', default=OSS_ENDPOINT, help='OSS端点URL')
    
    args = parser.parse_args()
    
    # 如果是OSS下载命令
    if args.command == 'oss-download':
        # 检查必要参数
        if not args.access_key or not args.secret_key or not args.endpoint or not args.bucket:
            print("错误: 缺少OSS配置信息，请在config.py中配置或通过命令行参数提供")
            sys.exit(1)
            
        download_from_oss(
            args.bucket,
            args.object_key,
            args.local_path,
            args.access_key,
            args.secret_key,
            args.endpoint
        )
        return
    
    # 如果是OSS文件夹下载命令
    if args.command == 'oss-download-folder':
        # 检查必要参数
        if not args.access_key or not args.secret_key or not args.endpoint or not args.bucket:
            print("错误: 缺少OSS配置信息，请在config.py中配置或通过命令行参数提供")
            sys.exit(1)
        
        # 如果没有指定日志文件路径，使用默认路径
        if not args.log_file:
            # 从folder-prefix中提取爬虫名称和时间戳
            # 例如：zfw/1760441130/ -> zfw/1760441130.log
            folder_prefix = args.folder_prefix.rstrip('/')
            if '/' in folder_prefix:
                # 提取第一级和第二级目录
                parts = folder_prefix.split('/')
                if len(parts) >= 2:
                    args.log_file = f"spider_log/{parts[0]}/{parts[1]}.log"
                else:
                    args.log_file = f"spider_log/{folder_prefix}.log"
            else:
                args.log_file = f"spider_log/{folder_prefix}.log"
            
        download_oss_folder(
            args.bucket,
            args.folder_prefix,
            args.local_dir,
            args.log_file,
            args.access_key,
            args.secret_key,
            args.endpoint
        )
        return
    
    # 如果是OSS解析命令
    if args.command == 'oss-parse':
        # 获取爬虫名称
        spider_name = args.spider_name
        
        # 构建解析器脚本路径
        script_path = Path(f"./parser_script/{spider_name}.py").resolve()
        
        if not script_path.exists():
            print(f"错误: 解析器脚本不存在: {script_path}")
            sys.exit(1)
        
        # 动态加载解析器类
        parser_class = find_parser_class(script_path)
        
        if not parser_class:
            print(f"错误: 在 {script_path} 中未找到BaseParser的子类")
            sys.exit(1)
        
        # 确定日志文件路径
        if args.log_identifier:
            log_file_path = f"spider_log/{args.log_identifier}.log"
        else:
            # 从object-key中提取日志标识符
            object_key = args.object_key.rstrip('/')
            if '/' in object_key:
                parts = object_key.split('/')
                if len(parts) >= 2:
                    log_file_path = f"spider_log/{parts[0]}/{parts[1]}.log"
                else:
                    log_file_path = f"spider_log/{object_key}.log"
            else:
                log_file_path = f"spider_log/{object_key}.log"
        
        # 实例化并运行解析器
        parser_instance = parser_class(log_file_path)
        
        # 判断是解析单个文件还是整个目录
        if args.object_key.endswith('/'):
            # 如果以 / 结尾，认为是目录前缀
            parser_instance.parse_from_oss_folder(args.bucket, args.object_key)
        else:
            # 否则先尝试作为单个文件解析
            try:
                parser_instance.parse_from_oss(args.bucket, args.object_key)
            except Exception as e:
                # 如果失败，尝试作为目录前缀解析
                print(f"单文件解析失败，尝试作为目录解析: {str(e)}")
                parser_instance.parse_from_oss_folder(args.bucket, args.object_key + '/')
        return
    
    # 如果是解析命令（默认行为）
    if not args.command or args.command == 'parse':
        # 获取爬虫名称
        spider_name = args.spider_name
        
        # 构建解析器脚本路径
        script_path = Path(f"./parser_script/{spider_name}.py").resolve()
        
        if not script_path.exists():
            print(f"错误: 解析器脚本不存在: {script_path}")
            sys.exit(1)
        
        # 动态加载解析器类
        parser_class = find_parser_class(script_path)
        
        if not parser_class:
            print(f"错误: 在 {script_path} 中未找到BaseParser的子类")
            sys.exit(1)
        
        # 确定日志文件路径
        if args.log_identifier:
            log_file_path = f"spider_log/{args.log_identifier}.log"
        else:
            # 默认使用爬虫名称作为日志文件名
            log_file_path = f"spider_log/{spider_name}.log"
        
        # 实例化并运行解析器
        parser_instance = parser_class(log_file_path)
        
        # 检查解析器是否有run方法，如果没有则使用默认的异步处理方法
        if hasattr(parser_instance, 'run'):
            parser_instance.run()
        elif hasattr(parser_instance, 'process_all'):
            # 使用默认的异步处理方法
            start = time.perf_counter()
            asyncio.run(parser_instance.process_all())
            print(f"[异步解析完成] 总耗时: {time.perf_counter() - start:.2f}s")
        else:
            print(f"错误: 解析器类 {parser_class.__name__} 没有 run 或 process_all 方法")
            sys.exit(1)
        return

if __name__ == "__main__":
    main()
