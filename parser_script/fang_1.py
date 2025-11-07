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

class Fang(BaseParser):
    name = 'fang_1'
    def parser(self, file_path: Path, content: str):
        parts = content.split("\n",1)
        if len(parts) != 2:
            return 
        
        page = parts[0]
        page_content = parts[1]

        html = etree.HTML(page_content)

        # 提取 标题 以及 pageUrl
        info = html.xpath('//dd[@class="info rel"]/p[1]/a')
        for i in info:
            title = i.xpath("./@title")
            page_url = i.xpath("./@href")
            if title and page_url:
                title = title[0]
                page_url = page_url[0]

                ods_fang_house = {
                    'source': 'fang',
                    'date': datetime.datetime.now().strftime("%Y-%m-%d"),
                    'tableName': 'ods_fang_house',
                }
                ods_fang_house['title'] = title
                ods_fang_house['pageUrl'] = "https://gz.zu.fang.com" + page_url
                
                ods_fang_house['sourceId'] = hashlib.md5(ods_fang_house['pageUrl'].encode()).hexdigest()
                ods_fang_house['id'] = hashlib.md5((ods_fang_house['sourceId']).encode()).hexdigest()
                if ods_fang_house.get('id') and ods_fang_house.get('sourceId'):
                    yield ods_fang_house