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
import re

class Yuxiaor2(BaseParser):
    name = 'yuxiaor_2'
    def parser(self, file_path: Path, content: str):
        
        json_data = json.loads(content)

        data = json_data.get('data')
        if data:
            ods_yuxiaor_house = {
                'source': 'yuxiaor',
                'date': datetime.datetime.now().strftime("%Y-%m-%d"),
                'tableName': 'ods_yuxiaor_house',
                'isFromDetail':True
            }     
            ods_yuxiaor_contact = {
                'source': 'yuxiaor',
                'date': datetime.datetime.now().strftime("%Y-%m-%d"),
                'tableName': 'ods_yuxiaor_contact',
            }
            sourcr_id = data.get("bizId")
            if sourcr_id:
                ods_yuxiaor_house['sourceId'] = str(sourcr_id)
                ods_yuxiaor_contact['houseId'] = str(sourcr_id)

                rent = {}
                deposit = {}
                price_info = data.get("payType")
                for price in price_info:
                    if price['payCycle'] == "月付":
                        rent['leaseTerm'] = 'month'
                        rent['unit'] = 'yuan'
                        if '押金' in price['priceDetail'] :
                            deposit_amount = re.search(r"押金(\d+)", price['priceDetail'])
                            if deposit_amount:
                                deposit['pay'] = float(deposit_amount.group(1))
                                deposit['mortgage'] = price['priceDetail']
                
                prices = data.get('price')
                if prices:
                    rent['min'] = float(prices)
                    rent['value'] = prices

                if rent:
                    ods_yuxiaor_house['rent'] = rent
                if deposit:
                    ods_yuxiaor_house['deposit'] = deposit

                # 提取设施
                pubAmenities = data.get('pubAmenities')
                facilities = []
                if pubAmenities:
                    for facility in pubAmenities:
                        if facility.get('name'):
                            facilities.append(facility['name'])
                
                if facilities:
                    ods_yuxiaor_house['facilities'] = facilities


                # 提取付款方式
                payMethod = data.get('payCycle')
                if payMethod:
                    ods_yuxiaor_house['payMethod'] = payMethod

                # 提取 floor
                floor = data.get('floor')
                if floor:
                    ods_yuxiaor_house['floor'] = floor

                # 提取 apartmentType 以及 towards 以及 houseArea
                space = data.get('space')
                if space:
                    area_data = self.give_area(space)
                    if area_data:
                        ods_yuxiaor_house['houseArea'] = area_data
                        
                layout = data.get('layout')
                if layout:
                    ods_yuxiaor_house['apartmentType'] = layout.strip()
                            

                orientation = data.get('orientation')
                if orientation:
                    ods_yuxiaor_house['towards'] = orientation.strip()

                # 提取 description
                houseDesc = data.get('houseDesc')
                if houseDesc:
                    ods_yuxiaor_house['description'] = houseDesc

                # 提取 Term
                leaseTerm = data.get('rent')
                if leaseTerm:
                    term_data = self.give_term(leaseTerm)
                    if term_data:
                        ods_yuxiaor_house['Term'] = term_data
            
                # 提取 address
                address = data.get('address')
                district = data.get('district')
                if address and district:

                    address_data = format_address.format_address(address,district)
                    if address_data:
                        # 提取 经纬度
                        lat = data.get('lat')
                        lon = data.get('lon')
                        if lat and lon:
                            address_data.update({
                                'map':{
                                    'lat':float(lat),
                                    'lng':float(lon)
                                }
                            })
                        ods_yuxiaor_house['address'] = address_data

                
                # 提取联系方式
                contact = data.get('agentPhone')
                if contact and len(contact) == 11:
                    ods_yuxiaor_contact['contact'] = contact
                    ods_yuxiaor_contact['contactType'] = '电话'
                    ods_yuxiaor_contact['numberType'] = '手机号'
                
                # 提取联系人姓名
                name = data.get('agentName')
                if name and name.strip():
                    ods_yuxiaor_contact['personName'] = name.strip()



                # 完成 house表 id
                if ods_yuxiaor_house.get('tableName') and ods_yuxiaor_house.get('sourceId'):
                    ods_yuxiaor_house['id'] = hashlib.md5((str(ods_yuxiaor_house['sourceId'])).encode()).hexdigest()
                    yield ods_yuxiaor_house


                # 提取照片
                imageList = []
                images = data.get('houseImages')
                if images:
                    for image in images:
                        url = image.get('url')
                        if url:
                            imageList.append({
                                'imageLink':url
                            })
                    if imageList:
                        ods_yuxiaor_house['imageList'] = imageList
                else:
                    # 添加 isDownload 
                    ods_yuxiaor_house['isDownload'] = True

                # 完成对 联系表的 id
                if ods_yuxiaor_contact.get('tableName') and ods_yuxiaor_contact.get('houseId') and ods_yuxiaor_contact.get('contact') and ods_yuxiaor_contact.get('contactType') and ods_yuxiaor_contact.get('numberType'):
                    ods_yuxiaor_contact['sourceId'] = hashlib.md5((str(ods_yuxiaor_contact['houseId']) + ods_yuxiaor_contact['contact'] + ods_yuxiaor_contact['contactType'] + ods_yuxiaor_contact['numberType']).encode()).hexdigest()
                    ods_yuxiaor_contact['id'] = hashlib.md5((str(ods_yuxiaor_contact['sourceId'])).encode()).hexdigest()
                    yield ods_yuxiaor_contact

    def give_area(self, origin_str):
        origin_str = origin_str.strip()
        # 使用正则表达式匹配面积信息，支持"18㎡"或"18-20㎡"等格式
        area_pattern = r'(\d+(?:\.\d+)?)(?:\s*-\s*(\d+(?:\.\d+)?))?\s*(㎡|平米|平方米|m²)'
        match = re.search(area_pattern, origin_str)
        
        if match:
            min_area = float(match.group(1))
            max_area = float(match.group(2)) if match.group(2) else min_area
            unit = match.group(3)
            
            # 标准化单位表示
            if unit in ['㎡', '平米', '平方米', 'm²']:
                unit = '㎡'
            
            return {
                'min': min_area,
                'max': max_area,
                'unit': unit,
                'value': origin_str
            }
        
        # 如果没有匹配到标准格式，但仍包含数字和面积单位
        number_pattern = r'(\d+(?:\.\d+)?)'
        numbers = re.findall(number_pattern, origin_str)
        if numbers:
            max_area = float(numbers[0])      
            return {
                'max': max_area,
                'unit': '平方米',  # 默认单位
                'value': origin_str
            }
        
        # 如果无法解析，返回None
        return None
    

    def give_term(self, origin_str):
        # "1-12月"
        origin_str = origin_str.strip()
        data = origin_str.split("-")
        if len(data) == 2:
            min_term = int(data[0].replace("月","").strip())
            max_term = int(data[1].replace("月","").strip())
            return {
                'min': min_term,
                'max': max_term,
                'unit': 'month',
                'value': origin_str
            }
        elif len(data) == 1:
            max_term = int(data[0].replace("月","").strip())
            return {
                'max': max_term,
                'unit': 'month',
                'value': origin_str
            }
        else:
            return
