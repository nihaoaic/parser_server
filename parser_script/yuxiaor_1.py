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

class Yuxiaor1(BaseParser):
    name = 'yuxiaor_1'
    def parser(self, file_path: Path, content: str):
        parts = content.split("\n",1)
        if len(parts) != 2:
            return 
        
        page = parts[0]
        data = parts[1]

        # 解析 code
        json_data = json.loads(data)

        data = json_data.get('data')
        if data:
            house_info = data.get('data')
            if house_info:
                for house in house_info:
                    ods_yuxiaor_house = {
                        'source': 'yuxiaor',
                        'date': datetime.datetime.now().strftime("%Y-%m-%d"),
                        'tableName': 'ods_yuxiaor_house',
                    }

                    # 提取sourceid
                    sourceid = house.get('houseId')
                    if sourceid:
                        ods_yuxiaor_house['sourceId'] = str(sourceid)
                        if ods_yuxiaor_house.get('tableName'):
                            ods_yuxiaor_house['id'] = hashlib.md5((str(sourceid)).encode()).hexdigest()
                        yield ods_yuxiaor_house


        