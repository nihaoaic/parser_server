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
from lxml import etree
import html as html_tool

class Ygzf(BaseParser):
    name = 'ygzf_2'
    def parser(self, file_path: Path, content: str):
        parts = content.split("\n",1)
        if len(parts) != 2:
            return 
        
        source_id = parts[0]
        data = parts[1]

        ods_ygzf_house = {
            'source': 'ygzf',
            'date': datetime.datetime.now().strftime("%Y-%m-%d"),
            'tableName': 'ods_ygzf_house',
            'sourceId': source_id,
            'id': hashlib.md5((source_id).encode()).hexdigest(),
            'isFromDetail':True
        }
        
        
        html = etree.HTML(data)


        
        rent = {}
        # 提取租金信息
        price_nodes = html.xpath("//div[@class='fl prz']/span/text() | //div[@class='fl prz']/span/following-sibling::text()")
        if price_nodes:
            price_text = ''.join(price_nodes).strip()
            if price_text:
                # 使用正则表达式提取数字（支持小数）
                import re
                match = re.search(r"(\d+\.?\d*)", price_text)
                if match:
                    price_value = match.group(1)
                    if price_value.isdigit():
                        price = float(price_value)
                        # 解析单位以及 月份
                        unit, term = price_text.replace(price_value,'').split("/")
                        if term == "月":
                            term = 'month'
                        elif term == "年":
                            term = 'year'
                        rent.update({
                            'max': price,
                            'min': price,
                            'amount': price,
                            'leaseTerm': term,
                            'unit': unit,
                            'value': price_text  # 保存完整的字符串值
                        })
                if rent:
                    ods_ygzf_house['rent'] = rent

        address_clean = ''
        # 提取 位置
        address = html.xpath("//span[contains(text(),'房源地址')]/text()")
        # <li>房屋位置：<span style="margin-left: 4px;">荔湾区 西村街</span></li>
        location = html.xpath('//li[contains(text(),"房屋位置")]/span/text()')
        if address:
            addr_raw = address[0]
            addr_cleaned = html_tool.unescape(addr_raw).strip().replace("房源地址 :","").strip()
            if addr_cleaned:
                if '广东省广州市' not in addr_cleaned:
                    addr_cleaned = '广东省广州市' + addr_cleaned
            if location:
                location = location[0]
                address_clean = format_address.format_address(addr_cleaned,location)
            else:
                address_clean = format_address.format_address(addr_cleaned,'')
        
        if address_clean:
            ods_ygzf_house['address'] = address_clean

        # 提取 房屋面积
        house_area = html.xpath("//li[contains(text(),'房屋面积')]/span/text()")
        if house_area:
            house_area = house_area[0]
            area = house_area.replace('平米','').strip()
            area = float(area)
            ods_ygzf_house['houseArea'] = {
                'min': area,
                'max': area,
                'unit': 'm²',
                'value': house_area,
            }
        
        # 提取 户型
        apartmentType = html.xpath("//li[contains(text(),'房屋户型')]/text()")
        if apartmentType:
            apartmentType = apartmentType[0]
            ods_ygzf_house['apartmentType'] = re.sub(r'[\r\n\t]', '', html_tool.unescape(apartmentType).strip()).replace('房屋户型：','').strip()

        # 提取 floor
        floor = html.xpath("//span[contains(text(),'所处楼层')]/text()")
        if floor:

            floor = floor[0]
            floor = re.sub(r'[\r\n\t]', '', html_tool.unescape(floor).strip()).replace('所处楼层 :','').replace('楼','').strip()
            if floor and floor.isdigit():
                ods_ygzf_house['floor'] = int(floor)

        
        # 提取 towards
        toward = html.xpath("//li[contains(text(),'房屋朝向')]/text()")
        if toward:
            toward = toward[0]
            if toward and '未知' not in toward:
                toward = re.sub(r'[\r\n\t]', '', html_tool.unescape(toward).strip()).replace('房屋朝向：','').strip()
                if toward:
                    ods_ygzf_house['towards'] = toward


        # 提取 押金信息
        deposit = html.xpath("//span[contains(text(),'支付方式')]/text()")
        if deposit:
            deposit = deposit[0]
            deposit = re.sub(r'[\r\n\t]', '', html_tool.unescape(deposit).strip()).replace('支付方式 :','').strip()
            if deposit:
                # 使用正则表达式提取 “押X付Y” 中的 X 和 Y 数字
                match = re.search(r'[押](\d+)[付](\d+)', deposit)
                if match:
                    mortgage = int(match.group(1))
                    pay = int(match.group(2))
                    ods_ygzf_house['deposit'] = {
                        'mortgage': mortgage,
                        'pay': pay
                    }

        # releaseDate
        releaseDate = html.xpath("//li[contains(text(),'发布时间')]/span/text()")
        if releaseDate:
            releaseDate = releaseDate[0]
            releaseDate = re.sub(r'[\r\n\t]', '', html_tool.unescape(releaseDate).strip()).strip()
            if releaseDate:
                ods_ygzf_house['releaseDate'] = releaseDate

        # facilities
        facilities = html.xpath("//li[div[@class='pic' and ./img/@style]]/p")
        if facilities:
            facility_list = []
            for fac in facilities:
                text = fac.text.strip() if fac.text else ''
                if text:
                    facility_list.append(text)
            if facility_list:
                ods_ygzf_house['facilities'] = facility_list

        # 描述
        desc = html.xpath("//table[./th[contains(text(),'房源描述')]]/td/text()")
        if desc:
            desc = desc[0]
            desc = re.sub(r'[\r\n\t]', '', html_tool.unescape(desc).strip()).strip()
            if desc:
                ods_ygzf_house['description'] = desc

        if ods_ygzf_house.get('source') and ods_ygzf_house.get('sourceId') and ods_ygzf_house.get("id"):
            yield ods_ygzf_house
            