import sys
from pathlib import Path
# 添加上级目录到Python路径
sys.path.append(str(Path(__file__).parent.parent))

import json
from base_parser import BaseParser
from pathlib import Path
import sys
import datetime
from utils import format_address
import hashlib

class ZfwParser(BaseParser):
    """示例解析器：同步写法，也能被异步调度"""

    def parser(self, file_path: Path, content: str):
        # 注意：这里file_path可能是OSS对象键，content是实际内容
        parts = content.split("\n",3)
        if len(parts) != 4:
            return 
        
        url = parts[0]
        content_type = parts[1]
        page = parts[2]
        data = parts[3]

        # 解析 code
        json_data = json.loads(data)
 
        code = json_data.get('code')
        if code == "0000":
            data_list = json_data.get("data",{}).get('list',[])
            for data_item in data_list:
                # 构建模型
                ods_zfw_house = {
                    'source': 'zfw',
                    'date': datetime.datetime.now().strftime("%Y-%m-%d"),
                    'tableName': 'ods_zfw_house',
                    'isFromDetail': True
                }

                # 提取 description
                description = data_item.get('content')
                if description:
                    ods_zfw_house['description'] = description
                
                # 提取 address
                address = data_item.get('address')

                # 提取 location
                location = data_item.get('location')
                if location and address:
                    address = format_address.format_address(address,location)
                    if address:
                        ods_zfw_house['address'] = address

                # 提取 经纬度
                lat = data_item.get('latitude')
                lng = data_item.get('longitude')

                # 检查经纬度是否有效
                if lat is not None and lat != 0 and lng is not None and lng != 0:
                    ods_zfw_house['map'] = {}
                    ods_zfw_house['map']['lat'] = lat
                    ods_zfw_house['map']['lng'] = lng

                # 提取 sourceId
                source_id = data_item.get('threadId')
                if source_id:
                    ods_zfw_house['sourceId'] = source_id
                    if ods_zfw_house.get('tableName'):
                        ods_zfw_house['id'] = hashlib.md5((source_id + ods_zfw_house['tableName']).encode()).hexdigest()
                        yield ods_zfw_house