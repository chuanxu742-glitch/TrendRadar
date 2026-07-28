# coding=utf-8
"""Stable bilingual labels for monitored country and territory entities."""

from __future__ import annotations

import re
from typing import Any, Mapping


# ISO countries plus the territories and legacy aliases present in the inventory.
COUNTRY_NAMES_ZH = {
    "afghanistan": "阿富汗", "albania": "阿尔巴尼亚", "algeria": "阿尔及利亚",
    "american-samoa": "美属萨摩亚", "andorra": "安道尔", "angola": "安哥拉",
    "anguilla": "安圭拉", "antigua-and-barbuda": "安提瓜和巴布达", "argentina": "阿根廷",
    "armenia": "亚美尼亚", "aruba": "阿鲁巴", "australia": "澳大利亚",
    "austria": "奥地利", "azerbaijan": "阿塞拜疆", "bahrain": "巴林",
    "bangladesh": "孟加拉国", "barbados": "巴巴多斯", "belarus": "白俄罗斯",
    "belgium": "比利时", "belize": "伯利兹", "benin": "贝宁", "bermuda": "百慕大",
    "bhutan": "不丹", "bolivia": "玻利维亚", "bosnia-and-herzegovina": "波斯尼亚和黑塞哥维那",
    "botswana": "博茨瓦纳", "brazil": "巴西", "british-indian-ocean-territory": "英属印度洋领地",
    "british-virgin-islands": "英属维尔京群岛", "brunei-darussalam": "文莱",
    "bulgaria": "保加利亚", "burkina-faso": "布基纳法索", "burundi": "布隆迪",
    "cabo-verde": "佛得角", "cambodia": "柬埔寨", "cameroon": "喀麦隆",
    "canada": "加拿大", "canary-islands": "加那利群岛", "cayman-islands": "开曼群岛",
    "central-african-republic": "中非共和国", "chad": "乍得", "chile": "智利",
    "china": "中国", "christmas-island": "圣诞岛", "colombia": "哥伦比亚",
    "comoros": "科摩罗", "congo-democratic-republic-of-th": "刚果民主共和国",
    "congo-republic-of-the": "刚果共和国", "cook-islands": "库克群岛",
    "costa-rica": "哥斯达黎加", "cote-divoire": "科特迪瓦", "croatia": "克罗地亚",
    "cuba": "古巴", "curaco": "库拉索", "cyprus": "塞浦路斯",
    "cyprus-northern-turkish-republic-of": "北塞浦路斯", "czechia": "捷克",
    "denmark": "丹麦", "djibouti": "吉布提", "dominica": "多米尼克",
    "dominican-republic": "多米尼加共和国", "ecuador": "厄瓜多尔", "egypt": "埃及",
    "el-salvador": "萨尔瓦多", "equatorial-guinea": "赤道几内亚", "eritrea": "厄立特里亚",
    "estonia": "爱沙尼亚", "eswatini": "斯威士兰", "ethiopia": "埃塞俄比亚",
    "european-union": "欧盟", "falkland-islands-islas-malvinas": "福克兰群岛",
    "faroe-islands": "法罗群岛", "fiji": "斐济", "finland": "芬兰", "france": "法国",
    "gabon": "加蓬", "georgia": "格鲁吉亚", "germany": "德国", "ghana": "加纳",
    "gibraltar": "直布罗陀", "greece": "希腊", "greenland": "格陵兰",
    "grenada": "格林纳达", "guadeloupe": "瓜德罗普", "guam": "关岛",
    "guatemala": "危地马拉", "guernsey": "根西岛", "guinea": "几内亚",
    "guyana": "圭亚那", "haiti": "海地", "honduras": "洪都拉斯",
    "hong-kong-sar": "中国香港", "hungary": "匈牙利", "iceland": "冰岛",
    "india": "印度", "indonesia": "印度尼西亚", "iran": "伊朗", "iraq": "伊拉克",
    "ireland": "爱尔兰", "israel": "以色列", "italy": "意大利", "jamaica": "牙买加",
    "japan": "日本", "jordan": "约旦", "kazakhstan": "哈萨克斯坦", "kenya": "肯尼亚",
    "kiribati": "基里巴斯", "korea-north": "朝鲜", "korea-south": "韩国",
    "kosovo": "科索沃", "kuwait": "科威特", "kyrgyzstan": "吉尔吉斯斯坦",
    "laos": "老挝", "latvia": "拉脱维亚", "lebanon": "黎巴嫩", "lesotho": "莱索托",
    "liberia": "利比里亚", "libya": "利比亚", "liechtenstein": "列支敦士登",
    "lithuania": "立陶宛", "luxembourg": "卢森堡", "macao": "中国澳门",
    "macedonia-north": "北马其顿", "madagascar": "马达加斯加", "malawi": "马拉维",
    "malaysia": "马来西亚", "maldives": "马尔代夫", "mali": "马里", "malta": "马耳他",
    "marshall-islands": "马绍尔群岛", "mauritania": "毛里塔尼亚", "mauritius": "毛里求斯",
    "mexico": "墨西哥", "micronesia-federated-states-of": "密克罗尼西亚联邦",
    "moldova": "摩尔多瓦", "monaco": "摩纳哥", "mongolia": "蒙古",
    "montenegro": "黑山", "montserrat": "蒙特塞拉特", "morocco": "摩洛哥",
    "mozambique": "莫桑比克", "myanmar-burma": "缅甸", "namibia": "纳米比亚",
    "nauru": "瑙鲁", "nepal": "尼泊尔", "netherlands": "荷兰",
    "netherlands-antilles": "荷属安的列斯", "new-caledonia": "新喀里多尼亚",
    "new-zealand": "新西兰", "nicaragua": "尼加拉瓜", "niger": "尼日尔",
    "nigeria": "尼日利亚", "norfolk-island": "诺福克岛",
    "northern-mariana-islands": "北马里亚纳群岛", "norway": "挪威", "oman": "阿曼",
    "pakistan": "巴基斯坦", "palau": "帕劳", "panama": "巴拿马",
    "papua-new-guinea": "巴布亚新几内亚", "paraguay": "巴拉圭", "peru": "秘鲁",
    "philippines": "菲律宾", "poland": "波兰", "portugal": "葡萄牙",
    "puerto-rico": "波多黎各", "qatar": "卡塔尔", "romania": "罗马尼亚",
    "russia": "俄罗斯", "rwanda": "卢旺达", "saint-kitts-and-nevis": "圣基茨和尼维斯",
    "saint-lucia": "圣卢西亚", "saint-pierre-and-miquelon": "圣皮埃尔和密克隆",
    "saint-vincent-and-the-grenadines": "圣文森特和格林纳丁斯", "samoa": "萨摩亚",
    "san-marino": "圣马力诺", "saudi-arabia": "沙特阿拉伯", "senegal": "塞内加尔",
    "serbia": "塞尔维亚", "seychelles": "塞舌尔", "sierra-leone": "塞拉利昂",
    "singapore": "新加坡", "sint-maarten": "荷属圣马丁", "slovakia": "斯洛伐克",
    "slovenia": "斯洛文尼亚", "solomon-islands": "所罗门群岛", "somalia": "索马里",
    "south-africa": "南非", "south-sudan-republic-of": "南苏丹", "spain": "西班牙",
    "sri-lanka": "斯里兰卡", "sudan": "苏丹", "suriname": "苏里南",
    "sweden": "瑞典", "switzerland": "瑞士", "syria": "叙利亚",
    "tahiti-french-polynesia": "塔希提（法属波利尼西亚）", "taiwan": "中国台湾",
    "tajikistan": "塔吉克斯坦", "tanzania": "坦桑尼亚", "thailand": "泰国",
    "the-bahamas": "巴哈马", "the-gambia": "冈比亚", "timor-leste": "东帝汶",
    "togo": "多哥", "tonga": "汤加", "trinidad-and-tobago": "特立尼达和多巴哥",
    "tunisia": "突尼斯", "turkiye": "土耳其", "turkmenistan": "土库曼斯坦",
    "turks-and-caicos-islands": "特克斯和凯科斯群岛", "uganda": "乌干达",
    "ukraine": "乌克兰", "united-arab-emirates": "阿拉伯联合酋长国",
    "united-kingdom": "英国", "united-states": "美国", "uruguay": "乌拉圭",
    "uzbekistan": "乌兹别克斯坦", "vanuatu": "瓦努阿图", "venezuela": "委内瑞拉",
    "vietnam": "越南", "virgin-islands": "维尔京群岛", "yemen": "也门",
    "zambia": "赞比亚", "zimbabwe": "津巴布韦",
}

_SUBDIVISION_PARENTS = ("united-states", "united-kingdom", "australia", "canada")


def english_name(value: Any) -> str:
    return " ".join(
        part[:1].upper() + part[1:]
        for part in re.split(r"[-_\s]+", str(value or "").strip())
        if part
    )


def country_name_zh(key: str) -> str:
    if key in COUNTRY_NAMES_ZH:
        return COUNTRY_NAMES_ZH[key]
    for parent in _SUBDIVISION_PARENTS:
        prefix = f"{parent}-"
        if key.startswith(prefix):
            child = english_name(key.removeprefix(prefix))
            return f"{COUNTRY_NAMES_ZH[parent]}·{child}"
    return ""


def entity_names(
    kind: str,
    key: str,
    metadata: Mapping[str, Any],
    configured: Mapping[str, Mapping[str, Any]],
) -> tuple[str, str, str]:
    raw_configured = configured.get(f"{kind}:{key}", {})
    configured_names = raw_configured if isinstance(raw_configured, Mapping) else {}
    name_zh = str(
        metadata.get("entity_name_zh")
        or configured_names.get("name_zh")
        or (country_name_zh(key) if kind == "country" else "")
    ).strip()
    raw_name_en = str(
        metadata.get("entity_name_en")
        or configured_names.get("name_en")
        or english_name(key)
    ).strip()
    name_en = (
        english_name(raw_name_en)
        if raw_name_en.islower() or re.search(r"[-_]", raw_name_en)
        else raw_name_en
    )
    label = f"{name_zh} ({name_en})" if name_zh and name_en else name_zh or name_en or "待归类"
    return name_zh, name_en, label
