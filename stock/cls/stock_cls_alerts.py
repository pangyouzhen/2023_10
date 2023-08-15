import json
from datetime import datetime

import pandas as pd
import requests

"""
Date: 2021/5/29
Desc: 财联社今日快讯
https://www.cls.cn/searchPage?keyword=%E5%BF%AB%E8%AE%AF&type=all
"""

cls_url = "https://www.cls.cn/api/sw?app=CailianpressWeb&os=web&sv=7.5.5"

cls_headers = {
    "Host": "www.cls.cn",
    "Connection": "keep-alive",
    "Content-Length": "112",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://www.cls.cn",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}

def stock_zh_a_roll_cls() -> pd.DataFrame:
    """
    财联社电报加红
    https://www.cls.cn/telegraph/
    :return: 时间,标题,简讯
    :rtype: pandas.DataFrame
    只抓取当天200条内的
    """
    url = "https://www.cls.cn/v1/roll/get_roll_list?app=CailianpressWeb&category=red&os=web&refresh_type=1&rn=200&sv=7.7.5"

    payload={}

    response = requests.request("GET", url, headers=cls_headers, data=payload)

    today = datetime.today()
    today_start = today.replace(hour=0, minute=0, second=0, microsecond=0)
    js = response.json()
    roll_data = js["data"]["roll_data"]
    df = pd.DataFrame(roll_data)
    df = df[["ctime","title","brief"]]
    df["ctime"] = df["ctime"].apply(datetime.fromtimestamp)
    df = df[df["ctime"] > today_start]
    df.columns = ["时间","标题","简讯"]
    return df


if __name__ == "__main__":
    print(stock_zh_a_roll_cls())
