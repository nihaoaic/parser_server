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

class Ygzf(BaseParser):
    name = 'ygzf_1'
    def parser(self, file_path: Path, content: str):
        parts = content.split("\n",1)
        if len(parts) != 2:
            return 
        
        page = parts[0]
        data = parts[1]

        html = etree.HTML(data)
        
        # 提取房屋列表
        house_list =  html.xpath("//ul/li[@class='clearfix']/div[@class='pic fl']")
        for house in house_list:
            ods_ygzf_house = {
                'source': 'ygzf',
                'date': datetime.datetime.now().strftime("%Y-%m-%d"),
                'tableName': 'ods_ygzf_house',
            }
            # 提取 url 链接
            page_url = house.xpath("./a/@href")
            if page_url:
                page_url = page_url[0]
                ods_ygzf_house['pageUrl'] = "https://zfcj.gz.gov.cn" + page_url

                # 提取 sourceId
                source_id = page_url.split("/")[-1].split(".")[0]
                if source_id:
                    ods_ygzf_house['sourceId'] = source_id
                    ods_ygzf_house['id'] = hashlib.md5((source_id).encode()).hexdigest()
                    yield ods_ygzf_house
