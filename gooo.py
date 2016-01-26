#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import re,unirest
import treq
import psycopg2

header={'host':'www.onetonline.org',
        'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:43.0) Gecko/20100101 Firefox/43.0'}

def get_more_touchs(list_content,types):
    for each in list_content:
        url=each[0]
        try:

            r=requests.get(url,headers=header)
            soup=BeautifulSoup(r.text)
            websites={
            "detailed_work_activities" :"/search/dwa/compare/.*?g=Continue",
               "work_context":"^/find/descriptor/result/.*?",
               "work_values_content":"^/explore/workvalues/.*?",
               "work_styles_content":"^/find/descriptor/result/.*?",
               "work_activities":"^/find/descriptor/result/.*?",
               "skills_content":"^/find/descriptor/result/.*?",
                "knowledge_content":"^/find/descriptor/result/.*?",
                "interests":"^/explore/interests/.*?",
                  "abilities":"^/explore/interests/.*?"
            }
            re_type=websites[types]
            yes=soup.find('a',href=re.compile(re_type))
            if yes:
                more_url='http://www.onetonline.org'+yes['href']
                r=requests.get(more_url,headers=header)
                soup1=BeautifulSoup(r.text)
                if types!='detailed_work_activities':
                    content=get_more_code(soup1)
                else:
                    content=[each.get_text() for each in soup1.find_all('td',class_='occcode')]
                for code in content:
                        cur.execute('insert into related_code(related_url,field,code) values(%s,%s,%s)',[url,types,code])
                        conn.commit()
            else:
                codes=[each.get_text() for each in soup.find_all('td',class_='occcode')]
                if codes:
                    for code in codes:
                        cur.execute('insert into related_code(related_url,field,code) values(%s,%s,%s)',[url,types,code])
                        conn.commit()
                else:
                        cur.execute('insert into related_code(related_url,field) values(%s,%s)',[url,types])
                        conn.commit()
        except Exception as e:
            conn.commit()
            print each,e
            sql='select related_url from error_url where related_url = \'%s\' ' % each
            cur.execute(sql)
            a=cur.fetchone()
            if not a:
                print url
                cur.execute('insert into error_url(related_url,type) values(%s,%s)' ,[url,types])
                conn.commit()
            continue
    
def get_more_code(soup):
    #返回一个列表r eportrt
    codes=[each.get_text() for each in soup.find_all('td',class_='reportrt')]
    return codes



if __name__=='__main__':
    conn=psycopg2.connect(database="resultdb", user="postgres",password="", host="10.1.36.183", port="5432")
    cur = conn.cursor()
    conn.commit()
    all_type=["detailed_work_activities","work_context",   "work_values_content",  
    "work_styles_content", "work_activities",  "skills_content",  "knowledge_content", "interests",  "abilities"]  
    for each in all_type:
        print each
        sql='select related_url from %s;'% each
        cur.execute(sql)
        row=cur.fetchall()
        row=row[1:5] 
        conn.commit()
        get_more_touchs(row,each)
