# -*- coding: utf-8 -*-
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from functools import wraps

import pymysql
import requests
from lxml import etree
from fake_useragent import UserAgent

from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_CHARSET

# UA = UserAgent(path='user_agents.json')
UA = UserAgent()

lock = Lock()

mysql_conn = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB,
    charset=MYSQL_CHARSET
)
cursor = mysql_conn.cursor()


def logging(f):
    """ 打印日志的装饰器 """

    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            print('执行{0}失败，错误：{1}'.format(f.__name__, str(e)))
            return None

    return wrapper


def request(url, **kwargs):
    """ 简单封装一下GET请求 """

    kwargs.setdefault('headers', {})
    kwargs['headers'].setdefault('User-Agent', UA.random)
    r = requests.get(url, **kwargs)
    r.encoding = r.apparent_encoding
    return r


@logging
def save(**kwargs):
    """ 保存数据 """

    # 多线程中执行要加锁
    # with lock:
    # sql00 = "truncate table hotrows;"
    # cursor.execute(sql00)
    sql = "insert into hotrows (`data`, `data_type`, `name`) values (%s, %s, %s)"
    cursor.execute(sql, (kwargs.get('data'), kwargs.get('data_type'), kwargs.get('name')))
    mysql_conn.commit()


@logging
def get_v2ex():
    """ V2EX, 建议科(fang-)学(-zhi)上(he-)网(-xie) """

    data_type = 'V2EX'
    name = 'V2EX'
    url = 'https://www.v2ex.com/?tab=hot'
    host = 'https://www.v2ex.com'

    r = request(url, timeout=30)
    html = etree.HTML(r.text)
    hot_list = html.xpath('//a[@class="topic-link"]')

    all_data = []
    for hot in hot_list:
        all_data.append({
            'title': hot.xpath('string()'),
            'url': host + hot.get('href')
        })
    return {'data': json.dumps(all_data, ensure_ascii=False), 'data_type': data_type, 'name': name}


@logging
def get_ithome():
    """ IT之家"""

    data_type = 'ithome'
    name = 'IT之家'
    url = 'https://www.ithome.com/'

    r = request(url)
    html = etree.HTML(r.text)
    hot_list = html.xpath('//div[@id="rank"]/ul[2]/li/a')

    all_data = []
    for hot in hot_list:
        all_data.append({
            'title': hot.xpath('string()'),
            'url': hot.get('href')
        })

    return {'data': json.dumps(all_data, ensure_ascii=False), 'data_type': data_type, 'name': name}


def get_zhihu():
    """ 知乎热榜 """

    data_type = 'zhihu'
    name = '知乎'
    url = 'https://www.zhihu.com/hot'
    # TODO: 替换cookie
    headers = {
        'Cookie': '_zap=d67980f6-024f-4cb9-84f2-b85d5595e3c3; d_c0="ACCegLUAVhSPTnjcDAm3bZpj2CygaUyOSFc=|1642148931"; _9755xjdesxxd_=32; YD00517437729195%3AWM_TID=26DFzuOvMWxFFFEERVc7rIYiPTEhb0kb; captcha_session_v2=2|1:0|10:1675147035|18:captcha_session_v2|88:K1k4MzE5SXdxUFNzKzBsaWtFVm1oQUVTclJRdGlBTlFyUTU4ZitFa0R5TGpYMnpwNGJWTFRPZ2szQXlvWkY0Qw==|34facac093fcef30609c17f1f531d7b34a56644a3d2ee350a18492672773d9e1; __snaker__id=Nbt1mXhacxbtguUY; gdxidpyhxdE=9P0ltL%2Fbqdjp7fPSqhHeUCqQ1HanKXYBAJ4nLLM1Arfe8%2Fcce%2BeTGid1ozqJ1QivHP9urhax1Ih7dKN7kVb2OKlSgDMuznVSWNGxByVvy6seG7Tj8GbOH7ixOV8HNPXqN7biyYknGdWvPPgtkItABLDnDBsxy98o85%2FPvBvuQZOR%2Bk8l%3A1675147935596; YD00517437729195%3AWM_NI=QHatOebU1PUwn8GdM623UC%2BEzqfKD6Wg8NCWTiN8t7OfYHEpM5TIC1KxK6jqSSClcJ%2BiHLTxG6YD7fFyNVgByp0QCi8UVYYX32PX2gN39d4OuPT4VLHXt2uXZpVBuMkISWc%3D; YD00517437729195%3AWM_NIKE=9ca17ae2e6ffcda170e2e6eed1f56a85bfafd3d54ab1ac8aa7c55b869b9f82c448928fc088dc40a5959698d72af0fea7c3b92a9398af95ce6fa28bffb1d87daee88985d03ef2ed9ba4aa4fbaafaaacc73e95b9f8afc25d98b782d9f73d8dba97a9c87c8feb9caac23b89a885d7e97b918d8d8de242e9b788add77485ea97a5b66e86a782b7e64abbb000b2c263fc8b9abadc54f1eea0b9b16d8eedf785b745b7909cd5d5488c88bcd3f8549ab09aa3f847a9979db8e237e2a3; q_c1=c9b585f12ca64ff295571107450578ed|1675147049000|1675147049000; z_c0=2|1:0|10:1675240371|4:z_c0|92:Mi4xbFNKVUNRQUFBQUFBSUo2QXRRQldGQmNBQUFCZ0FsVk5LUVhHWkFCZl8ycVNNVGMwVC00cWNYdmVqM2lnSVlMODZn|2779750472e685b111a6c0ef9cd1149e386a79a1a6c85c69381c156cf2f97a6d; _xsrf=055ff5e2-c710-43fb-8af2-adfa6b61338b; Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49=1673317890,1675147034,1675654422; arialoadData=false; Hm_lpvt_98beee57fd2ef70ccdd5ca52b9740c49=1675739317; tst=h; SESSIONID=9hQUk6LdtVx9M5yRN6MmbkpPfGu3bJz8umkmfS2L6WV; JOID=VVESA05zHs9CllisI3US36lj6sE3RHiZEv9g7kAFRrl83m_bf8TXLCaaWKUhcrTsvevgfx2HbvHlxN3VSKSMarM=; osd=V18dBkJxEMBHmlqiLHAe3ads7801SnecHv1u4UUJRLdz22PZccvSICSUV6AtcLrjuOficRKCYvPry9jZSqqDb78=; KLBRSID=fb3eda1aa35a9ed9f88f346a7a3ebe83|1675739967|1675739316',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }

    r = request(url, headers=headers)
    html = etree.HTML(r.text)
    hot_list = html.xpath('//div[@class="HotItem-content"]/a')

    all_data = []
    for hot in hot_list:
        all_data.append({
            'title': hot.xpath('h2')[0].text,
            'url': hot.get('href')
        })
    return {'data': json.dumps(all_data, ensure_ascii=False), 'data_type': data_type, 'name': name}


@logging
def get_weibo():
    """ 微博热搜 """

    name = '微博'
    data_type = 'weibo'
    url = 'https://s.weibo.com/top/summary'
    host = 'https://s.weibo.com'
    # TODO: 替换cookie
    headers = {
        'Cookie': 'UOR=m.weibo.cn,api.weibo.com,m.weibo.cn; SINAGLOBAL=7507379988060.437.1661925381896; ALF=1697852522; SCF=AiNO10gFihMaVHeWX54b7PJ-Uuacitq56B0PMxg2XvzuGAemM4dk-pRHzOqHZgzlHsvEATf3eJfJlHW0ReTQhe4.; SUB=_2AkMUvU21f8NxqwJRmP4RzG3iaIxxyQnEieKi4bxuJRMxHRl-yT9jqk5btRB6Pz1jWhcjOKaRk_3DKzTa_6l18x1rF1p-; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9W566Bc9IOn.E7hH1kxalCY9; XSRF-TOKEN=8zokVilwinvXfkW1BCjEjSIG; WBPSESS=kErNolfXeoisUDB3d9TFH-K-I-8EyzVs0so3klZlHmHfdxHpy-l_ulJqlFE_DGzC8e8viF2uGUTyQ046XqNgy9dOK0qo-GbvFDoWMCeNKl_iDM0aUuMjTbMW7kOIPAaT; _s_tentry=passport.weibo.com; Apache=4256230568523.405.1675739779619; ULV=1675739779632:2:1:1:4256230568523.405.1675739779619:1661925381910',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }

    r = request(url, headers=headers)
    html = etree.HTML(r.text)
    hot_list = html.xpath('//td[@class="td-02"]/a')
    # print('hot_list', hot_list)
    all_data = []
    for hot in hot_list:
        all_data.append({
            'title': hot.text,
            'url': host + hot.get('href')
        })
    return {'data': json.dumps(all_data, ensure_ascii=False), 'data_type': data_type, 'name': name}


@logging
def get_tieba():
    """ 百度贴吧 """

    name = '贴吧'
    data_type = 'tieba'
    url = 'http://tieba.baidu.com/hottopic/browse/topicList'

    json_data = request(url).json()
    hot_list = json_data.get('data').get('bang_topic').get('topic_list')

    all_data = []
    for hot in hot_list:
        all_data.append({
            'title': hot['topic_name'],
            'url': hot['topic_url']
        })
    return {'data': json.dumps(all_data, ensure_ascii=False), 'data_type': data_type, 'name': name}


@logging
def get_douban():
    """ 豆瓣小组-讨论精选 """

    name = '豆瓣'
    data_type = 'douban'
    url = 'https://www.douban.com/group/explore'
    r = request(url)
    r.encoding = 'utf-8'
    html = etree.HTML(r.text)
    hot_list = html.xpath('//div[@class="bd"]/h3/a')

    all_data = []
    for hot in hot_list:
        all_data.append({
            'title': hot.text,
            'url': hot.get('href')
        })
    return {'data': json.dumps(all_data), 'data_type': data_type, 'name': name}


@logging
def get_tianya():
    """ 天涯排行 """

    name = '天涯'
    data_type = 'tianya'
    url = 'http://bbs.tianya.cn/list.jsp?item=funinfo&grade=3&order=1'

    html = etree.HTML(request(url).text)
    hot_list = html.xpath('//table/tbody/tr/td[1]/a')

    all_data = []
    for hot in hot_list:
        all_data.append({
            'title': hot.text.strip(),
            'url': 'http://bbs.tianya.cn' + hot.get('href')
        })
    return {'data': json.dumps(all_data, ensure_ascii=False), 'data_type': data_type, 'name': name}


@logging
def get_hupu():
    """ 虎扑步行街热帖 """

    name = '虎扑'
    data_type = 'hupu'
    url = 'https://bbs.hupu.com/all-gambia'
    # TODO: 替换cookie
    headers = {
        'Cookie': 'smidV2=20220127135018a3ecd99d09ec76c5abe174d75f493bef002f639e01388f240; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2217e9a1543f2695-0b6f1be1f0b06d-f791539-2073600-17e9a1543f3dd2%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22%24device_id%22%3A%2217e9a1543f2695-0b6f1be1f0b06d-f791539-2073600-17e9a1543f3dd2%22%7D',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    }
    r = request(url, headers=headers)
    # print(r)
    html = etree.HTML(r.text)
    # print(html)
    hot_list = html.xpath('//span[@class="textSpan"]/a')
    # print(hot_list)

    all_data = []
    for hot in hot_list:
        all_data.append({
            'title': hot.get('title'),
            'url': 'https://bbs.hupu.com' + hot.get('href')
        })
    return {'data': json.dumps(all_data, ensure_ascii=False), 'data_type': data_type, 'name': name}


@logging
def get_github():
    """ GitHub, 网络问题可能会超时, 建议科(fang-)学(-zhi)上(he-)网(-xie) """

    name = 'GitHub'
    data_type = 'github'
    url = 'https://github.com/trending'

    r = request(url)
    html = etree.HTML(r.text)
    hot_list = html.xpath('//h1[@class="h3 lh-condensed"]/a')

    all_data = []
    for hot in hot_list:
        span = hot.xpath('span')[0]
        all_data.append({
            'title': span.text.strip() + span.tail.strip(),
            'url': 'https://github.com' + hot.get('href'),
            'desc': hot.xpath('string(../../p)').strip()
        })
    return {'data': json.dumps(all_data), 'data_type': data_type, 'name': name}


@logging
def get_baidu():
    """ 百度风云榜 """

    name = '百度'
    data_type = 'baidu'
    url = 'http://top.baidu.com/buzz?b=341&c=513&fr=topbuzz_b1'

    r = request(url)
    html = etree.HTML(r.text)
    hot_list = html.xpath('//td[@class="keyword"]/a[1]')

    all_data = []
    for hot in hot_list:
        all_data.append({
            'title': hot.text.strip(),
            'url': hot.get('href')
        })
    return {'data': json.dumps(all_data, ensure_ascii=False), 'data_type': data_type, 'name': name}


@logging
def get_():
    """ template """

    name = ''
    data_type = ''
    url = ''

    html = etree.HTML(request(url).text)
    hot_list = html.xpath('')

    all_data = []
    for hot in hot_list:
        all_data.append({
            'title': hot.text,
            'url': hot.get('href')
        })
    return {'data': json.dumps(all_data, ensure_ascii=False), 'data_type': data_type, 'name': name}


def main():
    try:
        all_func = [
            get_v2ex, get_ithome, get_zhihu, get_weibo, get_tieba, get_douban,
            get_tianya, get_hupu, get_github, get_baidu
        ]

        # 线程池
        with ThreadPoolExecutor(min(len(all_func), 10)) as executor:
            # 线程池执行任务
            all_task = [executor.submit(func) for func in all_func]

            # 同步保存结果
            for future in as_completed(all_task):
                result = future.result()
                if result:
                    print(result)
                    save(**result)
    except:
        import traceback
        traceback.print_exc()

    finally:
        cursor.close()
        mysql_conn.close()


if __name__ == '__main__':
    sql00 = "truncate table hotrows;"
    cursor.execute(sql00)
    print('清空表')
    mysql_conn.commit()
    # 定时执行
    main()


    # print(get_zhihu())
    # print(get_v2ex())
    # print(get_weibo())
    # print(get_baidu())
    # print(get_hupu())
