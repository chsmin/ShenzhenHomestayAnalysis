import requests
import json
import time
import random

# 设置headers和Token
# Cookie和user-agent要及时更换 # 记得登录
headers = {
    "Cookie": "",
    "Referer": "https://minsu.dianping.com/shenzhen/pn2/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
}
TOKEN = ""
url_template = "https://minsu.dianping.com/api/phx/cprod/products?cityPinyin=shenzhen&pageNow={}&isInternal=false&_token={}"



# 遍历每一页数据
for number in range(1, 201):
    try:
        time.sleep(random.uniform(5, 10))
        url = url_template.format(number, TOKEN)  # 动态替换pageNow和TOKEN
        res = requests.get(url, headers=headers)
        res.raise_for_status()  # 检查请求状态
        data = res.json()  # 使用 res.json() 解析
        items = data.get('data', {}).get('list', [])

        with open('./MeituanGuesthouse.txt', 'a+', encoding='utf-8') as f:
            for item in items:

                productId = item.get('productId',"0")
                title = item.get('title', '').replace("\r", "").replace("\n", " ").replace(" ", "")
                cityName = item.get('cityName','未知城市')
                districtName = item.get('districtName','未知区域')
                locationArea = item.get('locationArea', '未知位置')
                # 第五个
                starRating = item.get('starRating','0')
                starRatingDesc = item.get('starRatingDesc','评分描述空缺')
                commentNumber = item.get('commentNumber','0')
                distanceDesc = item.get('distanceDesc', '未知')
                coverImage = item.get('coverImage')
                # 第十个
                favCount = item.get('favCount', '0')
                bedCount =item.get('bedCount', '0') #没什么用 favCountDesc = item.get('favCountDesc')
                productUserCount = item.get('productUserCount', '0')
                consumeDesc = item.get('consumeDesc', '0人消费')
                avgFinalPrice = item.get('avgFinalPrice','0')
                # 第十五个
                layoutRoom =item.get('layoutRoom','0') #layoutRoom更好
                guestNumberDesc = item.get('guestNumberDesc', '未知人数')
                ugcDesc = item.get('ugcDesc',"评论描述空缺").replace("“", "").replace("”", "")
                available = item.get('available', '是否可用未知')
                isSuperHost = item.get('isSuperHost', '是否超赞房东未知')
                #第二十个
                tags = "_".join([tag['tagName'] for tag in item.get('productTagModelList', [])])
                layoutDesc = item.get('layoutDesc','无标签描述')


                line = (f"{productId}-{cityName}-{title}-{districtName}-{locationArea}-"
                        f"{starRating}-{starRatingDesc}-{commentNumber}-{distanceDesc}-{coverImage}-"
                        f"{favCount}-{bedCount}-{productUserCount}-{consumeDesc}-{avgFinalPrice}-"
                        f"{layoutRoom}-{guestNumberDesc}-{ugcDesc}-{available}-{isSuperHost}-"
                        f"{tags}-{layoutDesc}")
                f.write(line + "\n")

    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"Error fetching data for page {number}: {e}")
