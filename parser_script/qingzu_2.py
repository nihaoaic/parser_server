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

class Qingzu2(BaseParser):
    name = 'qingzu_2'
    def parser(self, file_path: Path, content: str):
        
        parts = content.split("\n",1)
        if len(parts) != 2:
            return
        
        source_id = parts[0]
        file_content = parts[1]


        json_data = json.loads(file_content)

        data = json_data.get('data',{}).get('info')
        if data:
            ods_qingzu_house = {
                'source': 'qingzu',
                'date': datetime.datetime.now().strftime("%Y-%m-%d"),
                'tableName': 'ods_qingzu_house',
                'isFromDetail':True
            }     
            if source_id:
                ods_qingzu_house['sourceId'] = str(source_id)

                # 提取 设施
                facilities = []
                furniture_list = data.get('furnitureList')
                if furniture_list:
                    for furniture in furniture_list:
                        furniture_name = furniture.get('furnitureValue')
                        if furniture_name:
                            facilities.append(furniture_name)
                
                if facilities:
                    ods_qingzu_house['facilities'] = facilities


                # 提取付款方式
                paymethod = data.get('depositTypeName')
                if paymethod:
                    ods_qingzu_house['payMethod'] = paymethod

                # 提取发布时间
                releaseDate = data.get('updateTime')
                if releaseDate:
                    try:
                        dt = datetime.strptime(releaseDate, "%Y-%m-%d %H:%M:%S")
                        date_only = dt.strftime("%Y-%m-%d")
                        ods_qingzu_house['releaseDate'] = date_only
                    except:
                        pass
                
                # 提取朝向
                towards = data.get('directionName')
                if towards:
                    ods_qingzu_house['towards'] = towards
                
                # 提取 楼层
                floor = data.get('houseFloor')
                if floor:
                    ods_qingzu_house['floor'] = floor
                
                # 提取 户型
                apartmentType = ''
                houseTypeRoom = data.get('houseTypeRoom')
                if houseTypeRoom:
                    apartmentType += '%s室' % houseTypeRoom
                houseTypeHall = data.get('houseTypeHall')
                if houseTypeHall:
                    apartmentType += '%s厅' % houseTypeHall
                houseTypeToilet = data.get('houseTypeBathroom')
                if houseTypeToilet:
                    apartmentType += '%s卫' % houseTypeToilet
                if apartmentType:
                    ods_qingzu_house['apartmentType'] = apartmentType

                # 提取面积
                houseArea = data.get('houseArea')
                area = {}
                if houseArea:
                    area.update({
                        'max': float(houseArea),
                        'unit': '平方米',
                        'value': houseArea
                    })
                if area:
                    ods_qingzu_house['houseArea'] = area

                # 提取网费信息
                netBill = data.get('networkMoney')
                if netBill and netBill != 0:
                    # 先除以 100
                    net_amount = float(netBill) / 100
                    ods_qingzu_house['netBill'] = {
                        'max': net_amount,
                        'unit': 'month',
                        'amount': 'yuan',
                        'value': netBill
                    }
                
                # 提取 elecBill
                elecBill = data.get('electricMoney')
                if elecBill and elecBill != 0:
                    # 先除以 100
                    elec_amount = float(elecBill) / 100
                    ods_qingzu_house['elecBill'] = {
                        'max': elec_amount,
                        'unit': 'month',
                        'amount': 'yuan',
                        'value': elecBill
                    }
                
                # 提取 waterBill
                waterBill = data.get('waterMoney')
                if waterBill and waterBill != 0:
                    # 先除以 100
                    water_amount = float(waterBill) / 100
                    ods_qingzu_house['waterBill'] = {
                        'max': water_amount,
                        'unit': 'month',
                        'amount': 'yuan',
                        'value': waterBill
                    }
                
                # 提取 Term
                term =  data.get('rentYearName')
                if term:
                    term = self.give_term(term)
                    if term:
                        ods_qingzu_house['Term'] = term

                # 提取 描述信息
                description = data.get('description')
                if description:
                    ods_qingzu_house['description'] = description
                
                # 提取 地址
                address = data.get('houseAddress')
                location = data.get('addressRoad')
                address = format_address.format_address(address,location)
                if address:
                    ods_qingzu_house['address'] = address
                    # 提取坐标
                    lat = data.get('lat')
                    lng = data.get('lng')
                    if lat and lng:
                        ods_qingzu_house['address'].update({
                            'map':{
                                'lat':float(lat),
                                'lng':float(lng)
                            }
                        })
                # 提取 租金
                rentmoney = data.get('rentMoney')
                if rentmoney:
                    ods_qingzu_house['rent'] = {
                        'max': float(rentmoney) / 100,
                        'unit': 'month',
                        'amount': 'yuan',
                        'value': rentmoney
                    }

                # 提取 照片
                imageList = []
                images = data.get('imgList')
                if images:
                    for image in images:
                        imageList.append({
                            'imageLink':image
                        })
                    if imageList:
                        ods_qingzu_house['imageList'] = imageList
                else:
                    ods_qingzu_house['isDownload'] = True

                # 构建id
                if ods_qingzu_house.get('sourceId'):
                    ods_qingzu_house['id'] = ods_qingzu_house['id'] = hashlib.md5((str(ods_qingzu_house.get('sourceId'))).encode()).hexdigest()
                    yield ods_qingzu_house

    def give_term(self, origin_str):
        # "1-12月"
        origin_str = origin_str.strip()
        data = origin_str.split("-")
        if len(data) == 2:
            min_term = int(data[0].replace("个月","").strip())
            max_term = int(data[1].replace("个月","").strip())
            return {
                'min': min_term,
                'max': max_term,
                'unit': 'month',
                'value': origin_str
            }
        elif len(data) == 1:
            max_term = int(data[0].replace("个月","").strip())
            return {
                'min': max_term,
                'unit': 'month',
                'value': origin_str
            }
        else:
            return
