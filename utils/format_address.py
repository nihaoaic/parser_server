"""
    format_address.py
    ~~~~~~~~~~~~
    :author: rocky
    :copyright: (c) 2025, Tungee
    :date created: 2025/10/14
    :python version: 2.7
"""
import re

def format_address(address: str,location:str):
    """
    格式化地址
    :param address: 地址 location: 位置
    :return: 格式化后的地址
    """

    if not address:
        return
    
    origin_address = address

    area = {
        "越秀":{
            "street":[
                "北京",
                "洪桥",
                "六榕",
                "流花",
                "光塔",
                "人民",
                "东山",
                "农林",
                "大东",
                "大塘",
                "珠光",
                "白云",
                "建设",
                "华乐",
                "梅花村",
                "黄花岗",
                "矿泉",
                "登峰"
            ]
        },
        "海珠":{
            "street":[
                "江海",
                "赤岗",
                "新港",
                "滨江",
                "素社",
                "凤阳",
                "龙凤",
                "沙园",
                "瑞宝",
                "海幢",
                "南华西",
                "南石头",
                "江南中",
                "昌岗",
                "南洲",
                "琶洲",
                "官洲",
                "华洲"
            ]
        },
        "荔湾":{
            "street":[
            "石围塘",
                "金花",
                "沙面",
                "华林",
                "多宝",
                "昌华",
                "逢源",
                "龙津",
                "彩虹",
                "南源",
                "西村",
                "站前",
                "岭南",
                "桥中",
                "冲口",
                "花地",
                "茶滘",
                "中南",
                "东漖",
                "东沙",
                "海龙",
                "白鹤洞"
            ]
        },
        "天河":{
            "street":[
                "天园",
                "五山",
                "员村",
                "车陂",
                "沙河",
                "石牌",
                "兴华",
                "沙东",
                "林和",
                "棠下",
                "猎德",
                "冼村",
                "天河南",
                "元岗",
                "黄村",
                "龙洞",
                "长兴",
                "凤凰",
                "前进",
                "珠吉",
                "新塘"
            ]
        },
        "白云":{
            "street":[
                "景泰",
                "松洲",
                "同德",
                "黄石",
                "棠景",
                "新市",
                "三元里",
                "同和",
                "京溪",
                "永平",
                "均禾",
                "金沙",
                "石井",
                "嘉禾",
                "云城",
                "鹤龙",
                "白云湖",
                "石门",
                "龙归",
                "大源"
            ]
        },
        "黄埔":{
            "street":[
                "萝岗",
                "夏港",
                "联和",
                "永和",
                "大沙",
                "黄埔",
                "红山",
                "鱼珠",
                "文冲",
                "南岗",
                "穗东",
                "长洲",
                "长岭",
                "云埔",
                "九佛",
                "龙湖"
            ]
        },
        "花都":{
            "street":[
                "花城",
                "新华",
                "新雅",
                "秀全"
            ]
        },
        "番禺":{
            "street":[
                "市桥",
                "沙湾",
                "钟村",
                "石壁",
                "大石",
                "洛浦",
                "大龙",
                "东环",
                "桥南",
                "沙头",
                "小谷围"
            ]
        },
        "南沙":{
            "street":[
                "南沙",
                "珠江",
                "龙穴",
                "港湾"
            ]
        },
        "从化":{
            "street":[
                "街口",
                "城郊",
                "江埔"
            ]
        },
        "增城":{
        "street":[
            "荔城",
            "增江",
            "朱村",
            "永宁",
            "荔湖",
            "宁西"
        ]
    }
    }

    result = {}

    # 先解析地址
    address_match = re.search(r'广州市(.*?)区',address)
    if address_match:
        address_city = address_match.group(1)
        # 剔除 省 区 市 
        if address_city:
            result['district'] = address_city
            address = address.replace('广东省广州市%s区'%address_city,'')
            # 解析街道
            towns = area[address_city]['street']
            town_match = re.search(r'(%s)'%'|'.join(towns),address)
            if town_match:
                town = town_match.group(1)
                result['town'] = town
                detail = address.replace(town,'')
                if detail:
                    result['detail'] = detail
    
    if location and result:
        result['location'] = location
        result['value'] = origin_address
        result['province'] = '广东省'
        result['city'] = '广州市'
        return result
            

# if __name__ == "__main__":
#     print(format_address("广东省广州市天河区车陂北正大街35号","天河区车陂云公馆北(车陂北正大街东三巷东)"))