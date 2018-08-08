# coding:utf-8

# coding:utf-8
from bs4 import BeautifulSoup
import pymysql
import requests
import gc
import threadpool

mysql_conf = {
 'host':'127.0.0.1',
 'port':3306,
 'user':'root',
 'passwd':'123456',
 'db':'lottery',
 'charset':'utf8'
}
url = 'http://kaijiang.500.com/dlt.shtml'
#url = 'http://kaijiang.500.com/shtml/dlt/18090.shtml'
user_headers = {'Host': 'kaijiang.500.com',
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
'Accept-Encoding': 'gzip, deflate',
'Referer': 'http://kaijiang.500.com/shtml/dlt/18089.shtml',
'Cookie': 'WT_FPC=id=undefined:lv=1533522990657:ss=1533522986454; bdshare_firstime=1531189049841; _jzqa=1.171251094167433250.1531189050.1532943905.1533522987.3; _jzqy=1.1531189050.1533522987.1.jzqsr=baidu.-; Hm_lvt_4f816d475bb0b9ed640ae412d6b42cab=1531189050,1532943905,1532943917,1533522987; __utma=63332592.729008393.1531189052.1532943907.1533522988.3; __utmz=63332592.1533522988.3.3.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; ck_RegFromUrl=http%3A//kaijiang.500.com/; _qzja=1.1455858615.1531189049937.1532943905489.1533522986732.1533522986732.1533522990685.0.0.0.9.3; ck_regchanel=baidu; regfrom=0%7Cala%7Cbaidu; sdc_session=1533522986456; seo_key=baidu%7C%7Chttps://www.baidu.com/link?url=UNKDk8pOu0GxNAqqTau1Qu1F7e9N9rVjpy6zDDU-OtUbLp67SIk7hBngOMuej-jcVmX_vYaOodUSI_uyOfjDsP-awfXFaDnQmdfyPunCm7q&wd=&eqid=a19665f900002f8b000000065b67b426; _jzqc=1; _jzqckmp=1; _qzjc=1; _qzjto=2.1.0; Hm_lpvt_4f816d475bb0b9ed640ae412d6b42cab=1533522991; __utmc=63332592; motion_id=1533537034478_0.7166958161827561; ck_RegUrl=kaijiang.500.com; CLICKSTRN_ID=218.18.91.235-1531189051.281723::A2644D8ADD09DDAF557C61C5B36C1FF8',
'Connection': 'keep-alive',
'Upgrade-Insecure-Requests': '1'}

def conn_mysql(conn_conf):
    try:
        conn = pymysql.connect(host=conn_conf['host'],port=conn_conf['port'],user=conn_conf['user'],passwd=conn_conf['passwd'],db=conn_conf['db'],charset=conn_conf['charset'])
    except Exception as e:
        print(e)
    else:
        return conn

def start_http(url,head):
    rsp = requests.request('GET',url,headers=head)
    rsp.encoding = 'utf-8'
    return rsp.text

def get_one_result(date,url,heads):
    result_dic = {
    'red_1':0,
    'red_2':0,
    'red_3':0,
    'red_4':0,
    'red_5':0,
    'blue_1':0,
    'blue_2':0}
    kaijiang_rsp = start_http(url, head=heads)
    kaijiang_soup = BeautifulSoup(kaijiang_rsp, 'html.parser')
    dlt_reslut = kaijiang_soup.find('div', class_='ball_box01')
    dlt_reslut_red = dlt_reslut.find_all('li', class_='ball_red')
    dlt_reslut_blue = dlt_reslut.find_all('li', class_='ball_blue')
    count = 1
    for i in dlt_reslut_red:
        result_dic['red_'+str(count)] = int(i.text)
        if count == 5:
            count = 1
        else:
            count = count + 1

    for i in dlt_reslut_blue:
        result_dic['blue_'+str(count)] = int(i.text)
        if count == 2:
            count = 1
        else:
            count = count + 1
    print(date,result_dic)
    sql = "REPLACE INTO kj_result(`date`,  `red_1`,  `red_2`,  `red_3`,  `red_4`,  `red_5`,  `blue_1`,  `blue_2`) VALUES (%d,%d,%d,%d,%d,%d,%d,%d)" % (date,result_dic['red_1'],result_dic['red_2'],result_dic['red_3'],result_dic['red_4'],result_dic['red_5'],result_dic['blue_1'],result_dic['blue_2'])
    mysql_conn = conn_mysql(mysql_conf)
    mysql_conn.autocommit(False)
    cursor = mysql_conn.cursor()
    try:
        cursor.execute(sql)
        mysql_conn.commit()
    except Exception as e:
        print(e)
        mysql_conn.rollback()
    cursor.close()
    mysql_conn.close()
    return date,result_dic


def main():
    rsp = start_http(url,head=user_headers)
    soup = BeautifulSoup(rsp,'html.parser')
    url_list = [] #所有开奖的url
    all_dlt_url = soup.find('div', class_='iSelectList')
    for i in all_dlt_url.find_all('a'):
        one_url_list = [int(i.text),i['href'],user_headers]
        one_thread_args = (one_url_list,None)
        url_list.append(one_thread_args)


    w_pool = threadpool.ThreadPool(10)
    requestss = threadpool.makeRequests(get_one_result, url_list)
    [w_pool.putRequest(req) for req in requestss]
    w_pool.wait()





if __name__ == "__main__":
    main()