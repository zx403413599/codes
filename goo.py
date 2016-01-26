#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import re,unirest
import treq
import psycopg2

header={'host':'www.onetonline.org',
        'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:43.0) Gecko/20100101 Firefox/43.0'}

def get_more_touch(list_content,types):
    for each in list_content:
        url=each[0]
        try:
            r=requests.get(url,headers=header)
            soup=BeautifulSoup(r.text)
            a=soup.find('a',href=re.compile('^/search/t2/occs/\d{4,15}'))
            if a:
                more_url='http://www.onetonline.org'+a['href']
                r=requests.get(more_url,headers=header)
                soup1=BeautifulSoup(r.text)
                #time.sleep(1)
                content=get_more_codes(soup1)
                for one in content:
                    code=one[0]
                    task=one[1]
                    cur.execute('insert into related_code_content(related_url,field,code,related_content) values(%s,%s,%s,%s)',[each,types,code,task])
                    conn.commit()
            else:
                content=get_more_codes(soup)
                if content:
                    for one in content:
                        code=one[0]
                        task=one[1]
                        cur.execute('insert into related_code_content(related_url,field,code,related_content) values(%s,%s,%s,%s)',[each,types,code,task])
                        conn.commit()
                else:
                        cur.execute('insert into related_code_content(related_url,field) values(%s,%s)',[each,types])
                        conn.commit()
        except Exception as e:
            print each,e
            sql='select related_url from error_url where related_url = \'%s\' ' % each
            cur.execute(sql)
            a=cur.fetchone()
            if not a:
                cur.execute('insert into error_url(related_url,type) values(%s,%s)' ,[each,types])
                conn.commit()
            continue
                
    
def get_more_codes(soup):
    codes=[each.get_text() for each in soup.find_all('td',class_='reportrtd')]
    content=[each.get_text() for each in soup.select('td.report2ed div')]
    return zip(codes,content)

if __name__=='__main__':
    conn=psycopg2.connect(database="resultdb", user="postgres",password="", host="10.1.36.183", port="5432")
    cur = conn.cursor()
    all_type=['tool_content','technology_content']
    each='tool_content'
    for each in all_type:
        sql='select related_url from %s;'% each
        cur.execute(sql)
        row=cur.fetchall()
        conn.commit()
        get_more_touch(row,each)