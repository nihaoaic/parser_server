# 文件: f:/parser_server/test.py

import asyncio
import aiofiles
import time
from pathlib import Path
from abc import ABC, abstractmethod
import sys
import inspect
import json
import threading
from concurrent.futures import ThreadPoolExecutor
import logging

from collections import defaultdict
import datetime
import oss2
from config import OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, SPIDER_BUCKET, PARSED_RESULTS_DIR

class BaseParser(ABC):
    """异步文件解析基类"""

    def __init__(self, log_file_path: str = "spider_log/log.txt", log_identifier: str = None):
        """
        初始化解析器
        :param log_file_path: 日志文件路径，默认为 spider_log/log.txt
        :param log_identifier: 日志标识符，用于创建子目录
        """
        self.log_file_path = Path(log_file_path)
        self.log_identifier = log_identifier
        # 创建一个线程锁来保证写入的线程安全
        self.write_lock = threading.Lock()
        
        # 用于存储相同ID的数据
        self.data_buffer = defaultdict(list)
        
        # 用于OSS解析的计数器
        self.processed_count = 0
        self.error_count = 0

    async def read_file(self, file_path: Path) -> str:
        """异步读取文件内容"""
        async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
            return await f.read()

    @abstractmethod
    def parser(self, file_path: Path, content: str):
        """
        子类重写此方法即可，不必写 async。
        如果是普通函数，会自动放入线程池异步执行。
        """
        pass

    async def process_file(self, file_path: Path):
        """异步解析单个文件"""
        try:
            content = await self.read_file(file_path)

            # 如果 parser 是异步函数，直接 await
            if inspect.iscoroutinefunction(self.parser):
                result = await self.parser(file_path, content)
                # 处理异步生成器返回的结果
                if result:
                    async for item in result:
                        self.buffer_result(item)
            else:
                # 否则自动用线程池执行同步方法
                result = await asyncio.to_thread(self.parser, file_path, content)
                # 处理同步生成器返回的结果
                if result:
                    for item in result:
                        self.buffer_result(item)
        except Exception as e:
            print(f"处理文件 {file_path} 时出错: {e}")

    def buffer_result(self, data):
        """缓冲结果数据，按ID分组"""
        # 获取数据ID
        data_id = data.get('id')
        if data_id:
            # 添加更新日期标记
            data['update_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
            # 按ID分组存储
            self.data_buffer[data_id].append(data)

    def merge_and_write_results(self):
        """合并相同ID的数据并写入文件"""
        # 确保输出目录存在
        output_dir = Path(PARSED_RESULTS_DIR)
        
        # 如果有log_identifier，则创建子目录
        if self.log_identifier:
            output_dir = output_dir / self.log_identifier
            
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 使用爬虫类名作为文件名的一部分
        parser_name = self.__class__.__name__.replace('Parser', '').lower()
        output_file = output_dir / f"{parser_name}_results__{str(int(time.time() * 1000))}.json"  # 固定文件名，不使用时间戳

        # 合并相同ID的数据并写入文件
        with self.write_lock:
            try:
                with open(output_file, mode='w', encoding='utf-8') as f:  # 使用 'w' 模式
                    for data_id, data_list in self.data_buffer.items():
                        # 如果只有一个数据项，直接使用
                        if len(data_list) == 1:
                            merged_data = data_list[0]
                        else:
                            # 如果有多个数据项，合并它们
                            merged_data = data_list[0].copy()
                            # 可以选择保留最新的数据或其他合并策略
                            # 这里简单地用最后一个数据项覆盖前面的
                            for data in data_list[1:]:
                                merged_data.update(data)
                        
                        # 写入合并后的数据，每行一个JSON对象
                        json_str = json.dumps(merged_data, ensure_ascii=False)
                        f.write(json_str + '\n')
                    
                    # 确保数据被写入磁盘
                    f.flush()
            except Exception as e:
                print(f"写入数据时出错: {e}")
                # 记录错误数据以便调试
                try:
                    error_file = output_dir / f"{parser_name}_errors.log"
                    with open(error_file, mode='a', encoding='utf-8') as f:
                        f.write(f"数据写入错误: {e}\n")
                except:
                    pass

    async def process_all(self):
        """异步处理日志文件中列出的所有文件"""
        if not self.log_file_path.exists():
            print(f"日志文件不存在: {self.log_file_path}")
            return

        # 读取日志文件中的文件路径列表
        async with aiofiles.open(self.log_file_path, mode='r', encoding='utf-8') as f:
            file_paths = await f.readlines()

        tasks = []
        for file_path_str in file_paths:
            file_path_str = file_path_str.strip()
            if file_path_str:  # 忽略空行
                file_path = Path(file_path_str)
                if file_path.exists() and file_path.is_file():
                    tasks.append(asyncio.create_task(self.process_file(file_path)))
                else:
                    print(f"文件不存在或不是文件: {file_path}")

        if tasks:
            await asyncio.gather(*tasks)
        else:
            print("没有找到有效的文件路径")

        # 处理完成后，合并并写入所有缓冲的数据
        self.merge_and_write_results()

    def run(self):
        """入口函数"""
        start = time.perf_counter()
        asyncio.run(self.process_all())
        print(f"[异步解析完成] 总耗时: {time.perf_counter() - start:.2f}s")
    
    def get_oss_client(self, bucket_name=None):
        """获取OSS客户端"""
        if bucket_name is None:
            bucket_name = SPIDER_BUCKET
        try:
            auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
            endpoint = OSS_ENDPOINT
            bucket = oss2.Bucket(auth, endpoint, bucket_name)
            return bucket
        except Exception as e:
            logging.error(f"初始化OSS客户端失败: {str(e)}")
            return None
    
    def read_oss_object(self, bucket_name, object_key):
        """从OSS读取对象内容"""
        try:
            bucket = self.get_oss_client(bucket_name)
            if not bucket:
                raise Exception("无法初始化OSS客户端")
            
            # 读取对象内容
            result = bucket.get_object(object_key)
            content = result.read().decode('utf-8')
            return content
        except Exception as e:
            logging.error(f"从OSS读取对象失败: {str(e)}")
            raise
    
    def parse_from_oss(self, bucket_name, object_key):
        """直接从OSS解析对象"""
        try:
            # 从OSS读取内容
            content = self.read_oss_object(bucket_name, object_key)
            
            # 调用具体的解析方法
            results = self.parser(object_key, content)
            
            # 处理解析结果
            if results:
                for result in results:
                    # 将结果添加到缓冲区
                    self.buffer_result(result)
                    self.processed_count += 1
                
        except Exception as e:
            logging.error(f"解析OSS对象 {object_key} 失败: {str(e)}")
            self.error_count += 1
    
    def parse_from_oss_folder(self, bucket_name, folder_prefix):
        """从OSS文件夹解析所有对象"""
        try:
            bucket = self.get_oss_client(bucket_name)
            if not bucket:
                raise Exception("无法初始化OSS客户端")
            
            # 列出文件夹中的所有对象
            result = bucket.list_objects(prefix=folder_prefix)
            
            for obj in result.object_list:
                # 跳过目录对象
                if obj.key.endswith('/'):
                    continue
                    
                try:
                    # 解析每个对象
                    self.parse_from_oss(bucket_name, obj.key)
                except Exception as e:
                    logging.error(f"解析OSS对象 {obj.key} 失败: {str(e)}")
                    self.error_count += 1
            
            # 处理完所有对象后，合并并写入结果
            self.merge_and_write_results()
                    
        except Exception as e:
            logging.error(f"从OSS文件夹解析失败: {str(e)}")
            raise