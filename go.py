#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import re,unirest
import treq
import psycopg2

header={'host':'www.onetonline.org',
        'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:43.0) Gecko/20100101 Firefox/43.0'}

def get_more(list_urls):
    try:
            for url in list_urls:
                each=url[0]
                try:
                    r=requests.get(each,headers=header)
                    soup=BeautifulSoup(r.text)
                    yes=soup.find('a',href=re.compile('^/search/task/compare/.*?'))
                    if yes:
                        more_url='http://www.onetonline.org'+yes['href']
                        #time.sleep(1)
                        r=requests.get(each,headers=header)
                        soup1=BeautifulSoup(r.text)
                        content=get_each(soup1)
                        for one in content:
                            code=one[0]
                            task=one[1]
                            for two in task:
                                cur.execute('insert into related_code_content(related_url,field,code,related_content) values(%s,\'task\',%s,%s)',[each,code,two])
                                conn.commit()

                    else:
                        content=get_each(soup)
                        if content:
                            for one in content:
                                code=one[0]
                                task=one[1]
                                for two in task:
                                    cur.execute('insert into related_code_content(related_url,field,code,related_content) values(%s,\'task\',%s,%s)',[each,code,two])
                                conn.commit()

                        else:
                            cur.execute('insert into related_code_content(related_url,field) values(%s,\'task\')',[each])
                            conn.commit()

                except Exception as e:
                    print each,e
                    sql='select related_url from error_url where related_url = \'%s\' ' % each
                    cur.execute(sql)
                    a=cur.fetchone()
                    if not a:
                        cur.execute('insert into error_url(related_url,type) values(%s,\'task\')' ,[each])
                        conn.commit()
                    continue
    except Exception as e :
            print e
            
            
    ##  带相关内容的 需要点击  返回字典格式
def get_each(soup):
    codes=[each.get_text() for each in soup.find_all('td',class_='occcode')]
    #all_content=[each.get_text() for each in soup.find_all('a',href=re.compile('^http://www.onetonline.org/link/summary/\d.*?'))]
    #nums= [ int(each.get_text().strip(' closely related')) for each in soup.find_all('a',class_='toggle')]
    all_tasks=[each.get_text().strip('\n') for each in soup.find_all('ul',class_='toggle')]
    temp=[]
    i=0
    for each in all_tasks:
        temp1=each.split('\n')
        temp.append(temp1)
    return zip(codes,temp)

conn=psycopg2.connect(database="resultdb", user="postgres",password="", host="10.1.36.183", port="5432")
cur = conn.cursor()
conn.commit()
sql='select related_url from tasks;'
cur.execute(sql)
row=cur.fetchall()

if __name__=='__main__':
    get_more(row)


