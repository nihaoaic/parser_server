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

class Nuan1(BaseParser):
    name = 'nuan_1'
    def parser(self, file_path: Path, content: str):
        parts = content.split("\n",1)
        if len(parts) != 2:
            return 
        
        page = parts[0]
        data = parts[1]

        # 解析 code
        json_data = json.loads(data)

        data = json_data.get('rooms')
        if data:
            for house in data:
                ods_nuan_house = {
                    'source': 'nuan',
                    'date': datetime.datetime.now().strftime("%Y-%m-%d"),
                    'tableName': 'ods_nuan_house',
                }

                # 提取sourceid
                sourceid = house.get('id')
                if sourceid:
                    ods_nuan_house['sourceId'] = str(sourceid)
                    if ods_nuan_house.get('tableName'):
                        ods_nuan_house['id'] = hashlib.md5((str(sourceid)).encode()).hexdigest()


                        # 提取 releaseDate
                        release_data = house.get('postTime')
                        if release_data:
                            try:
                                dt = datetime.datetime.strptime(release_data, "%Y-%m-%dT%H:%M:%S.%fZ")
                                ods_nuan_house['releaseDate'] = dt.strftime("%Y-%m-%d")
                            except:
                                pass
                        
                        # 提取 apartmentType
                        room_type = house.get('roomType')
                        if room_type:
                            ods_nuan_house['apartmentType'] = room_type.strip()

                        # 提取 houseArea:
                        size = house.get('size')
                        if size:
                            ods_nuan_house['houseArea'] = {
                                'max': float(size),
                                'unit': '平方米',
                                'value': size
                            }
                        
                        # 提取 label
                        label = house.get('metaLabels')
                        if label:
                            ods_nuan_house['label'] = label

                        # 提取 租金信息
                        price = house.get('price')
                        if price:
                            ods_nuan_house['rent'] = {
                                'min': float(price),
                                'amount': price,
                                'unit': 'yuan',
                                'value': price
                            }

                        # 提取 desc
                        desc = house.get('description')
                        if desc:
                            ods_nuan_house['description'] = desc

                        # 提取 address
                        address = house.get('address')
                        location = house.get('community')
                        address_info = format_address.format_address(address,location)
                        if address_info:
                            # 提取 经纬度
                            coordsMars = house.get('coordsMars')
                            
                            if coordsMars:
                                lat, lng = coordsMars
                                address_info['map']={
                                    'lat': lat,
                                    'lng': lng
                                }
                            ods_nuan_house['address'] = address_info
                        
                        # 提取 标题
                        title = house.get('title')
                        if title:
                            ods_nuan_house['title'] = title

                if ods_nuan_house.get('id'):
                    ods_nuan_house['isFromDetail'] = True
                    yield ods_nuan_house
    