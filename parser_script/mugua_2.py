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

class Mugua(BaseParser):
    name = 'mugua_2'
    def parser(self, file_path: Path, content: str):
        parts = content.split("\n",1)
        if len(parts) != 2:
            return 
        
        source_id = parts[0]
        data = parts[1]

        # 解析 code
        json_data = json.loads(data)

        data = json_data.get('data')
        if data:
            ods_mugua_house = {
                'source': 'mugua',
                'date': datetime.datetime.now().strftime("%Y-%m-%d"),
                'tableName': 'ods_mugua_house',
                'isFromDetail': True
            }
            
            ods_mugua_contact = {
                'source': 'mugua',
                'date': datetime.datetime.now().strftime("%Y-%m-%d"),
                'tableName': 'ods_mugua_contact',
            }


            # 提取 title
            title = data.get('title')
            if title:
                ods_mugua_house['title'] = title.strip()

            # 提取 经纬度
            lat = data.get('lat')
            lng = data.get('lng')
            if lat and lng:
                ods_mugua_house['address'] = {
                   'map': {
                    'lat': float(lat),
                    'lng': float(lng)
                   }
                }

            # 提取 rent
            price = data.get('rentPrice')
            rentPriceType = data.get('rentPriceType')
            if price:
                ods_mugua_house['rent'] = {
                    'max': float(price),
                    'unit': 'month',
                    'amount': 'yuan',
                    'value': price
                }
                if rentPriceType:
                    if rentPriceType == "monthly":
                        ods_mugua_house['rent'].update({
                            'leaseTerm': 'month'
                        })
            
            # 提取 description
            description = data.get('description')
            if description:
                ods_mugua_house['description'] = description.strip()
            
            # houseArea
            area = data.get('area')
            if area:
                ods_mugua_house['houseArea'] = {
                    'max': float(area),
                    'unit': '平方米',
                    'value': area
                }

            # 提取 户型
            bedroom = data.get('bedroom')
            parlor = data.get('parlor')
            kitchen = data.get('kitchen')
            toilet = data.get('toilet')

            apartmentType = ''
            if bedroom:
                apartmentType += bedroom + '室'
            if parlor:
                apartmentType += parlor + '厅'
            if kitchen:
                apartmentType += kitchen + '厨'
            if toilet:
                apartmentType += toilet + '卫'
            if apartmentType:
                ods_mugua_house['apartmentType'] = apartmentType

            
            # 提取 floor
            floor = data.get('floorNumber')
            if floor:
                ods_mugua_house['floor'] = floor

            # 提取 towards
            towards = data.get('towardsTypeStr')
            if towards:
                ods_mugua_house['towards'] = towards
            
            # 提取 releaseDate
            releaseDate = data.get('updateTime')
            if releaseDate:
                dt = datetime.datetime.strptime(releaseDate, "%Y-%m-%d %H:%M:%S")
                ods_mugua_house['releaseDate'] = dt.strftime("%Y-%m-%d")

            # 提取 facilities
            equ = data.get('houseEquipped',{}).get("roomEquippedList")
            if equ:
                facilities = equ[0].get('equippedTypesStr')
                facilities = facilities.split('，')
                if facilities:
                    facilities = [_.strip() for _ in facilities]
                    ods_mugua_house['facilities'] = facilities

             # 提取 照片
            houseAttachments = data.get('houseAttachments')
            if houseAttachments:
                imageList = []
                for houseAttachment in houseAttachments:
                    urls = houseAttachment.get('urls')
                    if urls:
                        
                        for url in urls:
                            imageList.append({
                                'imageLink': url
                            })
                if imageList:
                    ods_mugua_house['imageList'] = imageList
            else:
                # 添加 isDownload 
                ods_mugua_house['isDownload'] = True
            # 封装 sourceId
            ods_mugua_house['sourceId'] = source_id
            ods_mugua_contact['houseId'] = source_id
            if ods_mugua_house.get('tableName'):
                ods_mugua_house['id'] = hashlib.md5((str(source_id)).encode()).hexdigest()
                yield ods_mugua_house

            # 提取 联系人
            contactPhone = data.get('contactPhone')
            if contactPhone:
                ods_mugua_contact['contact'] = contactPhone
                ods_mugua_contact['contactType'] = '电话'
                ods_mugua_contact['numberType'] = '电话'
                
            person = data.get('contactPerson')
            if person:
                ods_mugua_contact['personName'] = person.strip()


           

            
            # 增加 sourceId 以及 id
            if ods_mugua_contact.get('tableName') and ods_mugua_contact.get('houseId') and ods_mugua_contact.get('contact') and ods_mugua_contact.get('contactType') and ods_mugua_contact.get('numberType'):
                    ods_mugua_contact['sourceId'] = hashlib.md5((str(ods_mugua_contact['houseId']) + ods_mugua_contact['contact'] + ods_mugua_contact['contactType'] + ods_mugua_contact['numberType']).encode()).hexdigest()
                    ods_mugua_contact['id'] = hashlib.md5((str(ods_mugua_contact['sourceId'])).encode()).hexdigest()
                    yield ods_mugua_contact
            
            
