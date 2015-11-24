# -*- coding: utf-8 -*-
"""
Created on Sun Nov 22 21:28:18 2015

@author: wangjianlong
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
#from selenium import webdriver
import time 
import re
import random
from selenium import webdriver
#定义请求头
head={
'Host':'www.dianping.com',
'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:42.0) Gecko/20100101 Firefox/42.0',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Language':'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
'Accept-Encoding':'gzip, deflate',
'Referer':'http://www.dianping.com/guangzhou',
'Cookie':'showNav=#nav-tab|0|0; _hc.v="\"ba878feb-f478-430f-88d3-0ea5e870a49a.1448199166\""; JSESSIONID=4C9DD28312804C85E46770BE86FA9DCA; cy=4; cye=guangzhou; __utma=1.793377611.1448199168.1448199168.1448199168.1; __utmb=1.6.10.1448199168; __utmc=1; __utmz=1.1448199168.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; PHOENIX_ID=0a650e71-1512f663e63-28edd8c; s_ViewType=10; aburl=1',
#'Connection: keep-alive
'Cache-Control':'max-age=0'
}


columns=['name','stars','tasty','environment','service','contents','date','url']
def get_dataframe(soup):
    #取得一页所有的会员评论信息
    df_info1=pd.DataFrame(pd.Series([0,0,0,0,0,0,0,0],index=columns)).T
    df_info1=df_info1.drop(0)
    for each in soup.find_all('li',id=re.compile('rev_\d{5,10}')):
        info=get_info(each)
        df_info1=pd.concat([df_info1,info],axis=0)
    return df_info1
    
def get_info(each):
    #取得单个会员评论的所有信息
    strs=str(each)
    soup1=BeautifulSoup(strs)
    url=soup1.find('a',href=True)['href']#会员地址
    name=soup1.find('p',class_='name').get_text()#名字
    if re.findall('class="item-rank-rst irr-star\d{2}',strs):
        stars=re.findall('class="item-rank-rst irr-star\d{2}',strs)[0][-2].decode('utf-8')#星级
    else:
        stars=''
    if soup1.find(text=re.compile(u'口味\d')):
        tasty_num=soup1.find(text=re.compile(u'口味\d'))[-1]#品味评分
    else:
        tasty_num=''
    if soup1.find(text=re.compile(u'环境\d')):
        environment_num=soup1.find(text=re.compile(u'环境\d'))[-1]#环境评分
    else:
        environment_num=''
    if soup1.find(text=re.compile(u'服务\d')):
        service_num=soup1.find(text=re.compile(u'服务\d'))[-1]#服务评分呢
    else:
        service_num=''
    comment=soup1.find('div',class_="J_brief-cont").get_text()#评论内容
    date=soup1.find('span',class_="time").get_text()#评论日期
    return pd.DataFrame(pd.Series([name,stars,tasty_num,environment_num,service_num,comment,date,url],index=columns)).T
    
#21,22
def get_comments():
    #定义一个空数据框
    df_info0=pd.DataFrame(pd.Series([0,0,0,0,0,0,0,0],index=columns)).T
    df_info0=df_info0.drop(0)
    #实现翻页抓取
    try:
        for num in range(1,23):
            try:
                error_url=[]
                url='http://www.dianping.com/shop/2833498/review_all?pageno='+str(num)
                resp=requests.get(url,headers=head)
                if resp.status_code == 200:
                    soup=BeautifulSoup(resp.text)
                    info0=get_dataframe(soup)
                    df_info0=pd.concat([df_info0,info0],axis=0)
                else:
                    error_url.append(url)
                print u'抓取网页====>',url
                t=random.randint(13, 24)
                time.sleep(t)
            except Exception:
                print u'抓取失败，休息一会重新抓取'
                time.sleep(60)
                error_url.append(url)            
                continue
    finally:
        df_info0.to_csv('/home/wangjianlong/files/python/comment_1.csv',encoding='utf-8')
    n_err=len(error_url)
    if n_err !=0 :
        for n in range(n_err):
            resp=requests.get(error_url[n],headers=head)
            if resp.status_code == 200:
                error_url.pop(n)
                soup=BeautifulSoup(resp.text)
                info0=get_dataframe(soup)
                df_info0=pd.concat([df_info0,info0],axis=0)
    with open('/home/wangjianlong/files/python/error_urls.txt','w') as f:
        f.write(','.join(error_url))
    print u'抓取完毕'
    df_info0.to_csv('/home/wangjianlong/files/python/comment_2.csv')
    
#get_comments()
#####################################抓取个人信息###########


#个人信息表格的字段名
per_columns=['name','sex','city','atten_num','fan_num','flower_num','contribution_num','grade','register_time','all_badge','more_information']

def get_personalinfo(soup3):
    #提取个人信息数据
    name=soup3.find(class_='name').get_text()
    if soup3.find('i',class_=re.compile('[w]*[o]*man')):
        sex=soup3.find('i',class_=re.compile('[w]*[o]*man'))['class'][0]
    else:
        sex=''
    if soup3.find(class_='user-groun'):
        city= soup3.find(class_='user-groun').get_text()
    else:
        city=''
    #提取关注、粉丝、鲜花数
    atten=soup3.find(class_='user_atten')
    soup4=BeautifulSoup(str(atten))
    nums=[]
    for num in soup4.find_all('strong'):
        nums.append(num.get_text())
    atten_num=nums[0]#关注数
    fan_num=nums[1]#粉丝
    flower_num=nums[2]#鲜花数
    #提取使用帐号信息
    user_time=soup3.find('div',class_='user-time')
    soup5=BeautifulSoup(str(user_time))
    nums_1=[]
    for num in soup5.find_all('p'):
        nums_1.append(num.get_text())
    contribution_num=nums_1[0].split(u'：')[1]
    grade=nums_1[1].split(u'：')[1]
    register_time=nums_1[2].split(u'：')[1]
    ##抓取徽章名称
    if soup3.find_all('a',class_='badge-img'):
        badg=[]
        for badge in soup3.find_all('a',class_='badge-img'):
            badg.append(badge['title'])
        all_badge=','.join(badg)
    else:
        all_badge=''
    if soup3.find('div',class_="user-message"):
        more_info=[]
        soup6=soup3.find('div',class_="user-message")
        soup7=BeautifulSoup(str(soup6))
        for one in soup7.find_all('li'):
            more_info.append(one.get_text())
        more_information=','.join(more_info)
    else:
        more_information=''
    ##抓取更多个人信息
    return pd.DataFrame(pd.Series([name,sex,city,atten_num,fan_num,flower_num,contribution_num,grade,register_time,all_badge,more_information],index=per_columns)).T
     

 ##加载评论人的url
all_data=pd.read_csv('/home/wangjianlong/files/python/comment_2.csv')
per_urls=all_data['url']
per_info0=pd.DataFrame(pd.Series([0,0,0,0,0,0,0,0,0,0,0],index=per_columns)).T
per_info0=per_info0.drop(0)
web=webdriver.PhantomJS()
#需要后台开启phantomjs  主要为了抓取更多的个人信息
per_url='http://www.dianping.com/member/6369560'
try:
    for every in per_urls:
        try:
            error_perurl=[]
            per_url='http://www.dianping.com'+every
            web.get(per_url)
            time.sleep(4)
            print web.title
            try:
                if web.find_element_by_class_name('more-info'):
                    web.find_element_by_class_name('more-info').click()
                    time.sleep(2)
                    print u'有更多信息'
                    soup3=BeautifulSoup(web.page_source)
                    per_info1=get_personalinfo(soup3)
                    per_info0=pd.concat([per_info0,per_info1],axis=0)
                    t=random.randint(6, 12)
                    time.sleep(t)
                    print u'抓取网页====>%s'% per_url
                    t=random.randint(6, 12)
                    time.sleep(t)
            except Exception:
                print u'没有更多信息'
                soup3=BeautifulSoup(web.page_source)
                per_info1=get_personalinfo(soup3)
                per_info0=pd.concat([per_info0,per_info1],axis=0)
                t=random.randint(6, 12)
                time.sleep(t)
                print u'抓取网页====>%s'% per_url
                continue
        except Exception:
            print u'抓取失败，休息一会重新抓取'
            time.sleep(50)
            error_perurl.append(per_url)            
            continue
finally:
    per_info0.to_csv('/home/wangjianlong/files/python/info_1_3.csv',encoding='utf-8')
n_err_per=len(error_perurl)
if n_err_per !=0 :
    for n in range(n_err_per):
        web.get(error_perurl[n])
        time.sleep(4)
        print web.title
        try:
            if web.find_element_by_class_name('more-info'):
                web.find_element_by_class_name('more-info').click()
                time.sleep(2)
        except Exception:
            print u'没有更多信息'
            error_perurl.pop(n)
            soup=BeautifulSoup(web.page_source)
            per_info1=get_personalinfo(soup)
            per_info0=pd.concat([per_info0,per_info1],axis=0)
            continue
try:
    if len(error_perurl) !=0:
        with open('/home/wangjianlong/files/python/error_per_urls.txt','w') as f:
            f.writelines(error_perurl)
    else:
        print u'抓取完毕'
finally:
    print u'抓取完毕'
    web.quit
    per_info0.to_csv('/home/wangjianlong/files/python/info_2.csv',encoding='gb2312')



