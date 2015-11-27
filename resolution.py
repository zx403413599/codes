# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 13:28:01 2015

@author: wangjianlong
"""
import pandas as pd
import json
import psycopg2
data=pd.read_csv('/home/wangjianlong/files/programs/data_center/onetoonline.csv')


##将字典带分数对象转换成数据库表
#abilities! interests_content! 
#knowledge_content!  skills_content!
#work_activities!  work_styles_content! work_values_content
#

##不带分数的
#technology_content !tool_content!
#job_zone_content!
#related_occupations

##字典多个values
##work_context 


##列表
#(应该放到主表)  all_items #major_description code title
#detailed_work_activities education_content  sample_of_job_titles
#
#带分数的列表 
#task_content_list!

#wages_and_employment_content!(带字典的)

#数据库名 abilities interests knowledge skills work_activities 
#work_styles work_values technology tool job_zone related_occupations
#work_context tasks wages_and_employment detailed_work_activities
#education sample_of_job_titles major_table


##major_table 
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE major_table\
       (url varchar(49) PRIMARY KEY,\
       code  character varying not null ,\
       title character varying not null,\
       all_items  character varying null ,\
       major_description  character varying null)')
conn.commit()

for num in range(1110):
    try:
        url=data.ix[num,'url']
        all_items_0=data.ix[num,'all_items']
        if '["' in all_items_0:
            json_content=json.loads(all_items_0)
            all_items=';'.join(json_content)
        else:
            all_items=''
        major_description=data.ix[num,'major_description']
        code=data.ix[num,'code']
        title=data.ix[num,'title']
        cur.execute("INSERT INTO major_table VALUES(%s,%s,%s,%s,%s)",
                     [url,code,title,all_items,major_description])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()


##sample_of_job_titles 
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE sample_of_job_titles\
       (url1 character varying PRIMARY KEY,\
       url varchar(49) ,\
       job_title character varying null)')
conn.commit()

for num in range(1110):
    i=0
    url=data.ix[num,'url']
    try:
        jsons=data.ix[num,'sample_of_job_titles']#这里需要改
        if '["' in jsons:
            json_content=json.loads(jsons)
            for each in json_content:
                i=i+1
                url1=url+'_'+str(i)
                title=each
            ##这里需要改
                cur.execute("INSERT INTO sample_of_job_titles VALUES(%s,%s,%s)",
                 [url1,url,title])
        else:
            url1=url+'_'+str(0)
            cur.execute("INSERT INTO sample_of_job_titles VALUES(%s,%s,%s)",
                     [url1,url,''])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()



##education_content 
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE education\
       (url1 character varying PRIMARY KEY,\
       url varchar(49) ,\
       degress character varying null,\
       score int)')
conn.commit()

for num in range(1110):
    i=0
    url=data.ix[num,'url']
    try:
        jsons=data.ix[num,'education_content']#这里需要改
        if '["' in jsons:
            json_content=json.loads(jsons)
            for each in json_content:
                i=i+1
                url1=url+'_'+str(i)
                degree=each[0]
                score=each[1]
                print degree
                print score
            ##这里需要改
                cur.execute("INSERT INTO education VALUES(%s,%s,%s,%s)",
                 [url1,url,degree,score])
        else:
            url1=url+'_'+str(0)
            cur.execute("INSERT INTO education VALUES(%s,%s,%s,%s)",
                     [url1,url,'','0'])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()






##detailed_work_activities
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE detailed_work_activities\
       (url1 character varying PRIMARY KEY,\
       url varchar(49) ,\
       activies character varying null)')
conn.commit()

for num in range(1110):
    i=0
    url=data.ix[num,'url']
    try:
        jsons=data.ix[num,'detailed_work_activities']#这里需要改
        if '["' in jsons:
            json_content=json.loads(jsons)
            for each in json_content:
                i=i+1
                url1=url+'_'+str(i)
                activies=each
            ##这里需要改
                cur.execute("INSERT INTO detailed_work_activities VALUES(%s,%s,%s)",
                 [url1,url,activies])
        else:
            url1=url+'_'+str(0)
            cur.execute("INSERT INTO detailed_work_activities VALUES(%s,%s,%s)",
                     [url1,url,''])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()



data_1=pd.read_csv('/home/wangjianlong/下载/test_1.csv')

##wages_and_employment_content
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE wages_and_employment\
       (url varchar(49) PRIMARY KEY,\
        employment_2012 character varying null,\
        median_wages_2012 character varying null,\
        projected_growth_2012_2022  character varying null,\
        projected_job_openings_2012_2022 character varying null,\
        top_industries_2012 character varying null)')
conn.commit()

for num in range(1110):
    url=data_1.ix[num,'url']
    try:
        jsons=data_1.ix[num,'wages_and_employment_content']#这里需要改
        if '{"' in jsons:
            json_content=json.loads(jsons)
            num_employees=json_content[u'Employment (2012)']
            median_wages=json_content[u'Median wages (2014)']
            projected_growth=json_content[u'Projected growth (2012-2022)']
            projected_job_openings=json_content[u'Projected job openings (2012-2022)']
            top_industries=json_content[u'Top industries (2012)']
            ##这里需要改
            cur.execute("INSERT INTO wages_and_employment VALUES(%s,%s,%s,%s,%s,%s)",
                 [url,num_employees,median_wages,projected_growth,projected_job_openings,top_industries])
        else:
            url1=url+'_'+str(0)
            cur.execute("INSERT INTO wages_and_employment VALUES(%s,%s,%s,%s,%s,%s)",
                     [url,'','','','',''])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()




##work_context
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE work_context\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying null,\
        questions character varying null,\
        score int null,\
        explanation character varying null)')
conn.commit()

for num in range(1110):
    i=0
    j=0
    url=data.ix[num,'url']
    try:
        jsons=data.ix[num,'work_context']#这里需要改
        if '{' in jsons:
            json_content=json.loads(jsons)
            for keys,values in json_content.items():
                i=i+1
                question=values[0]
                for value in values[1]:
                    j=j+1
                    score=value[0]
                    explanation=value[1]
                    url1=url+'_'+str(i)+'_'+str(j)
            ##这里需要改
                    cur.execute("INSERT INTO work_context VALUES(%s,%s,%s,%s,%s,%s)",
                 [url1,url,keys,question,score,explanation])
        else:
            url1=url+'_'+str(0)
            cur.execute("INSERT INTO work_context VALUES(%s,%s,%s,%s,%s,%s)",
                     [url1,url,'','','0',''])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()












##related_occupations
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE related_occupations\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying null,\
        content character varying null)')
conn.commit()

for num in range(1110):
    i=0
    url=data.ix[num,'url']
    try:
        jsons=data.ix[num,'related_occupations']#这里需要改
        if '{' in jsons:
            json_content=json.loads(jsons)
            for keys,values in json_content.items():
                i=i+1
                url1=url+'_'+str(i)
                ##这里需要改
                cur.execute("INSERT INTO related_occupations VALUES(%s,%s,%s,%s)",
                     [url1,url,keys,values])
        else:
            url1=url+'_'+str(1)
            cur.execute("INSERT INTO related_occupations VALUES(%s,%s,%s,%s)",
                     [url1,url,' ',' '])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()


##job_zone_content
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE job_zone\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying null,\
        content character varying null)')
conn.commit()

for num in range(1110):
    i=0
    url=data.ix[num,'url']
    try:
        jsons=data.ix[num,'job_zone_content']#这里需要改
        if '{' in jsons:
            json_content=json.loads(jsons)
            for keys,values in json_content.items():
                i=i+1
                url1=url+'_'+str(i)
                ##这里需要改
                cur.execute("INSERT INTO job_zone VALUES(%s,%s,%s,%s)",
                     [url1,url,keys,values])
        else:
            url1=url+'_'+str(1)
            cur.execute("INSERT INTO job_zone VALUES(%s,%s,%s,%s)",
                     [url1,url,' ',' '])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()





##tool_content
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE tool\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying null,\
        content character varying null)')
conn.commit()

for num in range(1110):
    i=0
    url=data.ix[num,'url']
    try:
        jsons=data.ix[num,'tool_content']
        if '{' in jsons:
            json_content=json.loads(jsons)#这里需要改
            for keys,values in json_content.items():
                i=i+1
                url1=url+'_'+str(i)
                ##这里需要改
                cur.execute("INSERT INTO tool VALUES(%s,%s,%s,%s)",
                     [url1,url,keys,values])
        else:
            url1=url+'_'+str(1)
            cur.execute("INSERT INTO tool VALUES(%s,%s,%s,%s)",
                     [url1,url,' ',' '])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()






##technology_content
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE technology\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying null,\
        content character varying null)')
conn.commit()

for num in range(1110):
    i=0
    url=data.ix[num,'url']
    try:
        jsons=data.ix[num,'technology_content']
        if '{' in jsons:
            json_content=json.loads(jsons)#这里需要改
            for keys,values in json_content.items():
                i=i+1
                url1=url+'_'+str(i)
                ##这里需要改
                cur.execute("INSERT INTO technology VALUES(%s,%s,%s,%s)",
                     [url1,url,keys,values])
        else:
            url1=url+'_'+str(1)
            cur.execute("INSERT INTO technology VALUES(%s,%s,%s,%s)",
                     [url1,url,' ',' '])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()




##work_values_content
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE work_values\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying null,\
        content character varying null,\
        score int null)')
conn.commit()

for num in range(1110):
    i=0
    url=data.ix[num,'url']
    try:
        jsons=data.ix[num,'work_values_content']
        if '{' in jsons:
            json_content=json.loads(jsons)#这里需要改
            for keys,values in json_content.items():
                i=i+1
                url1=url+'_'+str(i)
                ##这里需要改
                cur.execute("INSERT INTO work_values VALUES(%s,%s,%s,%s,%s)",
                     [url1,url,keys,values[0],values[1]])
        else:
            url1=url+'_'+str(1)
            cur.execute("INSERT INTO work_values VALUES(%s,%s,%s,%s,%s)",
                     [url1,url,' ',' ','0'])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()



##work_styles_content
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE work_styles\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying null,\
        content character varying null,\
        score int null)')
conn.commit()

for num in range(1110):
    i=0
    url=data.ix[num,'url']
    try:
        jsons=data.ix[num,'work_styles_content']
        if '{' in jsons:
            json_content=json.loads(jsons)#这里需要改
            for keys,values in json_content.items():
                i=i+1
                url1=url+'_'+str(i)
                ##这里需要改
                cur.execute("INSERT INTO work_styles VALUES(%s,%s,%s,%s,%s)",
                     [url1,url,keys,values[0],values[1]])
        else:
            url1=url+'_'+str(1)
            cur.execute("INSERT INTO work_styles VALUES(%s,%s,%s,%s,%s)",
                     [url1,url,' ',' ','0'])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()



##work_activities
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE work_activities\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying null,\
        content character varying null,\
        score int null)')
conn.commit()

for num in range(1110):
    i=0
    url=data.ix[num,'url']
    try:
        jsons=data.ix[num,'work_activities']
        if '{' in jsons:
            json_content=json.loads(jsons)#这里需要改
            for keys,values in json_content.items():
                i=i+1
                url1=url+'_'+str(i)
                ##这里需要改
                cur.execute("INSERT INTO work_activities VALUES(%s,%s,%s,%s,%s)",
                     [url1,url,keys,values[0],values[1]])
        else:
            url1=url+'_'+str(1)
            cur.execute("INSERT INTO work_activities VALUES(%s,%s,%s,%s,%s)",
                     [url1,url,' ',' ','0'])
    except Exception as e:
        print url,e
        conn.commit()
        continue
conn.commit()


##skills_content
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE skills\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying,\
        content character varying,\
        score int)')
conn.commit()
for num in range(1110):
    i=0
    url=data.ix[num,'url']
    try:
        json_content=json.loads(data.ix[num,'skills_content'])#这里需要改
        for keys,values in json_content.items():
            i=i+1
            url1=url+'_'+str(i)
            ##这里需要改
            cur.execute("INSERT INTO skills VALUES(%s,%s,%s,%s,%s)",
                 [url1,url,keys,values[0],values[1]])
    except Exception:
        print url
        continue
conn.commit()







##skills_content
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE skills\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying,\
        content character varying,\
        score int)')
conn.commit()
for num in range(1110):
    i=0
    url=data.ix[num,'url']
    try:
        json_content=json.loads(data.ix[num,'skills_content'])#这里需要改
        for keys,values in json_content.items():
            i=i+1
            url1=url+'_'+str(i)
            ##这里需要改
            cur.execute("INSERT INTO skills VALUES(%s,%s,%s,%s,%s)",
                 [url1,url,keys,values[0],values[1]])
    except Exception:
        print url
        continue
conn.commit()




##knowledge_content
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE knowledge\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying,\
        content character varying,\
        score int)')
conn.commit()
for num in range(1110):
    i=0
    url=data.ix[num,'url']
    json_content=json.loads(data.ix[num,'knowledge_content'])#这里需要该
    for keys,values in json_content.items():
        i=i+1
        url1=url+'_'+str(i)
        ##这里需要改
        cur.execute("INSERT INTO knowledge VALUES(%s,%s,%s,%s,%s)",
             [url1,url,keys,values[0],values[1]])
conn.commit() 






##knowledge_content
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
#这里需要改
cur.execute('CREATE TABLE knowledge\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying,\
        content character varying,\
        score int)')
conn.commit()
for num in range(1110):
    i=0
    url=data.ix[num,'url']
    json_content=json.loads(data.ix[num,'knowledge_content'])#这里需要该
    for keys,values in json_content.items():
        i=i+1
        url1=url+'_'+str(i)
        ##这里需要改
        cur.execute("INSERT INTO knowledge VALUES(%s,%s,%s,%s,%s)",
             [url1,url,keys,values[0],values[1]])
conn.commit() 






##interests
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
cur.execute('CREATE TABLE interests\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying,\
        content character varying,\
        score int)')
conn.commit()
for num in range(1110):
    i=0
    url=data.ix[num,'url']
    json_content=json.loads(data.ix[num,'interests_content'])
    for keys,values in json_content.items():
        i=i+1
        url1=url+'_'+str(i)
        cur.execute("INSERT INTO interests VALUES(%s,%s,%s,%s,%s)",
             [url1,url,keys,values[0],values[1]])
conn.commit()  



##ability
##创建表
conn=psycopg2.connect(database="testdb", user="postgres",password="wszgrwhja1", host="10.1.36.183", port="5432")
cur = conn.cursor()
cur.execute('CREATE TABLE abilities\
       (url1 character varying PRIMARY KEY,\
        url varchar(49),\
        type character varying,\
        content character varying,\
        score int)')
conn.commit()
for num in range(1110):
    i=0
    url=data.ix[num,'url']
    json_content==json.loads(data.ix[num,'abilities'])
    for keys,values in json_content.items():
        i=i+1
        url1=url+'_'+str(i)
        cur.execute("INSERT INTO abilities VALUES(%s,%s,%s,%s,%s)",
             [url1,url,keys,values[0],values[1]])
conn.commit()        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
##解析数据majortitle
columns=['url','abilities','all_items','detailed_work_activities','education_content','interests_content','job_zone_content','knowledge_content','major_description','major_title','related_occupations','sample_of_job_titles','skills_content','task_content_list','technology_content','tool_content','wages_and_employment_content','work_activities','work_context','work_styles_content','work_values_content','code','title']
online1=pd.DataFrame([range(23)],columns=columns)
online1=online1.drop(0)
for num in range(1110):
    online0=data.ix[num,:]
    major_title=data.ix[num,'major_title']
    if  ' - ' in major_title:
        title=major_title.split(' - ')[1].strip().encode("utf-8")
        code=major_title.split(' - ')[0].strip().encode("utf-8")
    else:
        title=''
        code=''
    online0['code']=code
    online0['title']=title
    online2=pd.DataFrame(online0).T
    online1=pd.concat([online1,online2],axis=0)
online1.to_csv('/home/wangjianlong/files/programs/data_center/onetoonline.csv',encoding='utf-8')