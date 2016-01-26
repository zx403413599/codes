conn=psycopg2.connect(database="resultdb", user="postgres",password="", host="", port="")
cur = conn.cursor()
from pyspider.database import connect_database
resultdb = connect_database("sqlalchemy+postgresql+resultdb://postgres:@10.1.36.183:5432/resultdb")
#result=resultdb.select('test6').next()
#row_result = result['result']
#url=row_result['wages_and_employment_content']
#print type(url),url

##抓数据


##抓取相关的数据 并存到数据库中  

##获取进一步的链接 并返回列表 列表是一个字典 （带相关内容的）
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
            sql='select related_url from error_url_test where related_url = \'%s\' ' % each
            cur.execute(sql)
            a=cur.fetchone()
            if not a:
                print url
                cur.execute('insert into error_url_test(related_url,type) values(%s,%s)' ,[url,types])
                conn.commit()
            continue
    
def get_more_code(soup):
    #返回一个列表r eportrt
    codes=[each.get_text() for each in soup.find_all('td',class_='reportrt')]
    return codes

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
                    cur.execute('insert into related_code_content_test(related_url,field,code,related_content) values(%s,%s,%s,%s)',[each,types,code,task])
                    conn.commit()
            else:
                content=get_more_codes(soup1)
                if content:
                    for one in content:
                        code=one[0]
                        task=one[1]
                        cur.execute('insert into related_code_content_test(related_url,field,code,related_content) values(%s,%s,%s,%s)',[each,types,code,task])
                        conn.commit()
                else:
                        cur.execute('insert into related_code_content_test(related_url,field) values(%s,%s)',[each,types])
                        conn.commit()
        except Exception as e:
            conn.commit()
            print each,e
            sql='select related_url from error_url_test where related_url = \'%s\' ' % each
            cur.execute(sql)
            a=cur.fetchone()
            if not a:
                cur.execute('insert into error_url_test(related_url,type) values(%s,%s)' ,[each,types])
                conn.commit()
            continue
                
    
def get_more_codes(soup):
    codes=[each.get_text() for each in soup.find_all('td',class_='reportrtd')]
    content=[each.get_text() for each in soup.select('td.report2ed div')]
    return zip(codes,content)















##针对task的
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






###导数据
conn.commit()
for result in  resultdb.select('test6'):
# major_title
    try:
        row_result = result['result']
        url=row_result['current_url']
        sql='select url_id from major_table where url = \'%s\' ' % url
        cur.execute(sql)
        a=cur.fetchone()
        if a:
            url_id=a[0]
            wages_and_employment_content =row_result['wages_and_employment_content']##这里和下面得改
            if  wages_and_employment_content :##这里得该
                median_wages=wages_and_employment_content[0]#这里得修改
                median_wages[0]=median_wages[0].encode('utf-8')
                if '(' in median_wages[0]:
                    median_wages[0]=median_wages[0].split('(')[1].strip(' )')
                median_wages=':'.join(median_wages)
                employment=wages_and_employment_content[1]#这里得修改
                employment[0]=employment[0].encode('utf-8')
                if '(' in employment[0]:
                    employment[0]=employment[0].split('(')[1].strip(' )')
                employment=':'.join(employment)
                projected_growth=wages_and_employment_content[2]#这里得修改
                projected_growth[0]=projected_growth[0].encode('utf-8')
                if '(' in projected_growth[0]:
                    projected_growth[0]=projected_growth[0].split('(')[1].strip(' )')
                projected_growth=':'.join(projected_growth)
                projected_job_openings=wages_and_employment_content[3]#这里得修改
                projected_job_openings[0]=projected_job_openings[0].encode('utf-8')
                if '(' in projected_job_openings[0]:
                    projected_job_openings[0]=projected_job_openings[0].split('(')[1].strip(' )')
                projected_job_openings=':'.join(projected_job_openings)
                top_industries=wages_and_employment_content[4]#这里得修改
                top_industries[0]=top_industries[0].encode('utf-8')
                if '(' in top_industries[0]:
                    top_industries[0]=top_industries[0].split('(')[1].strip(' )')                
                top_industries=':'.join(top_industries)
                cur.execute( "INSERT INTO wages_and_employment_content (url_id,median_wages,employment,projected_growth,projected_job_openings,top_industries) VALUES (%s,%s,%s,%s,%s,%s)",
                  [url_id, median_wages,employment,projected_growth,projected_job_openings,top_industries]) ##这里得改
                conn.commit()
            else:
                cur.execute( "INSERT INTO wages_and_employment_content (url_id,median_wages,employment,projected_growth,projected_job_openings,top_industries) VALUES (%s,%s,%s,%s,%s,%s)",
                      [url_id,'','','','',''])  ##这里得改
                conn.commit()
        else:
            print url
    except Exception as e:
        print url, e
        conn.commit()
        break


##wages_and_employment_content
       if a:
            url_id=a[0]
            wages_and_employment_content =row_result['wages_and_employment_content']##这里和下面得改
            if  wages_and_employment_content :##这里得该
                median_wages=wages_and_employment_content[0]#这里得修改
                median_wages[0]=median_wages[0].encode('utf-8')
                if '(' in median_wages[0]:
                    median_wages[0]=median_wages[0].split('(')[1].strip(' )')
                median_wages=':'.join(median_wages)
                employment=wages_and_employment_content[1]#这里得修改
                employment[0]=employment[0].encode('utf-8')
                if '(' in employment[0]:
                    employment[0]=employment[0].split('(')[1].strip(' )')
                employment=':'.join(employment)
                projected_growth=wages_and_employment_content[2]#这里得修改
                projected_growth[0]=projected_growth[0].encode('utf-8')
                if '(' in projected_growth[0]:
                    projected_growth[0]=projected_growth[0].split('(')[1].strip(' )')
                projected_growth=':'.join(projected_growth)
                projected_job_openings=wages_and_employment_content[3]#这里得修改
                projected_job_openings[0]=projected_job_openings[0].encode('utf-8')
                if '(' in projected_job_openings[0]:
                    projected_job_openings[0]=projected_job_openings[0].split('(')[1].strip(' )')
                projected_job_openings=':'.join(projected_job_openings)
                top_industries=wages_and_employment_content[4]#这里得修改
                top_industries[0]=top_industries[0].encode('utf-8')
                if '(' in top_industries[0]:
                    top_industries[0]=top_industries[0].split('(')[1].strip(' )')                
                top_industries=':'.join(top_industries)
                cur.execute( "INSERT INTO wages_and_employment_content (url_id,median_wages,employment,projected_growth,projected_job_openings,top_industries) VALUES (%s,%s,%s,%s,%s,%s)",
                  [url_id, median_wages,employment,projected_growth,projected_job_openings,top_industries]) ##这里得改
                conn.commit()
            else:
                cur.execute( "INSERT INTO wages_and_employment_content (url_id,median_wages,employment,projected_growth,projected_job_openings,top_industries) VALUES (%s,%s,%s,%s,%s,%s)",
                      [url_id,'','','','',''])  ##这里得改
                conn.commit()
##  if tool_content:##这里得该
if tool_content:##这里得该
     for key,value in tool_content.items():#
            cur.execute( "INSERT INTO tool_content (url_id, tool,content,related_url) VALUES(%s,%s,%s,%s)",
              [url_id, key,value[0],value[1]])  ##这里得改
            conn.commit()
else:
    cur.execute( "INSERT INTO tool_content (url_id, tool,content,related_url) VALUES(%s,%s,%s,%s)",
          [url_id, '','',''])  ##这里得改
    conn.commit()
##job_zone_content
if job_zone_content:##这里得该
     for key,value in job_zone_content.items():#
            cur.execute( "INSERT INTO job_zone_content (url_id, type,content) VALUES(%s,%s,%s)",
              [url_id, key,value])  ##这里得改
            conn.commit()
else:
    cur.execute( "INSERT INTO job_zone_content (url_id, type,content) VALUES(%s,%s,%s)",
      [url_id, '',''])  ##这里得改
    conn.commit()
##related_occupations
if related_occupations:
    for key in related_occupations.keys():##这里和下面得改
            cur.execute( "INSERT INTO related_occupations (url_id, code) VALUES(%s,%s)",
              [url_id, key])  ##这里得改
            conn.commit()
else:
    cur.execute( "INSERT INTO related_occupations (url_id, code) VALUES(%s,%s)",
      [url_id, ''])  ##这里得改
    conn.commit()
#work_context
            work_context=row_result['work_context']##这里和下面得改
            if work_context:
                for key,value in work_context.items():##这里和下面得改
                    questions=value[0]
                    answer=value[1]
                    related_url=value[2]
                    for each in answer:
                        #url_id	context	questions	score	explanation	related_url
                        score=each[0]
                        explanation=each[1]
                        cur.execute("INSERT INTO work_context (url_id, context,questions,score,explanation,related_url) VALUES(%s,%s,%s,%s,%s,%s)",
                         [url_id, key,questions,score,explanation,related_url])##这里得改
                    conn.commit()
            else:
                cur.execute("INSERT INTO work_context (url_id) VALUES(%s)",
                         [url_id])
##detailed_work_activities
            detailed_work_activities=row_result['detailed_work_activities']##这里和下面得改
            if detailed_work_activities:
                for key,value in detailed_work_activities.items():##这里和下面得改
                        #url_id	context	questions	score	explanation	related_url
                        cur.execute( "INSERT INTO detailed_work_activities (url_id, activies,related_url) VALUES(%s,%s,%s)",
                          [url_id, key,value])  ##这里得改
                        conn.commit()
#education_content
url_id=a[0]
education_content=row_result['education_content']
for each in education_content:
    if each[1]=='Not available':
        score=0
    else:
        score=each[1]
    cur.execute("INSERT INTO education_content (url_id, degree,score) VALUES(%s,%s,%s)",
         [url_id, each[0],score])
    conn.commit()
# sample_of_job_titles
    row_result = result['result']
    url=row_result['current_url']
    sql='select url_id from major_table where url = \'%s\' ' % url
    cur.execute(sql)
    a=cur.fetchone()
    if a:
        url_id=a[0]
        sample_of_job_titles=row_result['sample_of_job_titles']
        cur.execute("INSERT INTO sample_of_job_titles (url_id, job_title) VALUES(%s,%s)",
                 [url_id, sample_of_job_titles])
        conn.commit()
#major table 
try:
    row_result = result['result']
    url=row_result['current_url']
    all_items=row_result['all_items']
    if all_items:
        all_items=';'.join(all_items)       
    #all_items=
    major_title=row_result['major_title']
    title=major_title.split(' - ')[1].strip().encode("utf-8")
    code=major_title.split(' - ')[0].strip().encode("utf-8")
    major_description=row_result['major_description']
#sql='insert into major_table(url, all_items ,code,title,major_description)'
    cur.execute("INSERT INTO major_table(url, all_items ,code,title,major_description) VALUES(%s,%s,%s,%s,%s)",
                 [url, all_items ,code,title,major_description])
    conn.commit()
except Exception as e:
    print url,e,type(all_items)
    conn.commit()
##error_url
sql='''CREATE TABLE error_url 
       (id serial not null PRIMARY KEY,
       related_url varchar(155) not null   ,
       type varchar(50) not null   ,
       time  timestamp  not null default current_timestamp
)'''

##related_code_content
sql='''CREATE TABLE related_code_content 
       (id serial not null PRIMARY KEY,
       related_url varchar(90) not null   ,
       field  varchar(50)  null,
       code  varchar(10)  null,
       related_content  character varying  null
)'''
##related_code
sql='''CREATE TABLE related_code 
       (id serial not null PRIMARY KEY,
       related_url varchar(90) not null   ,
       field  varchar(50)  null,
       code  varchar(10)   null
)'''
##tasks

sql='''CREATE TABLE tasks 
       (id serial not null PRIMARY KEY,
       url_id int not null  references major_table(url_id) ,
       task  character varying  null,
        score int null,
        related_url varchar(90)   null
)'''
cur.execute(sql)
conn.commit()
##work_values_content work_styles_content  work_activities skills_content knowledge_content  interests abilities tasks
sql='''CREATE TABLE work_values_content 
       (id serial not null PRIMARY KEY,
       url_id int not null  references major_table(url_id) ,
       work_value  character varying not null,
       content  character varying not null,
        score int null,
        related_url varchar(90)  not null

)'''
##tool_content technology_content
sql='''CREATE TABLE tool_content 
       (id serial not null PRIMARY KEY,
       url_id int not null  references major_table(url_id) ,
       tool  character varying not null,
       content  character varying not null,
        related_url varchar(90)  not null

)'''
##job_zone_content
sql='''CREATE TABLE job_zone_content 
       (id serial not null PRIMARY KEY,
       url_id int not null  references major_table(url_id) ,
       type  character varying not null,
       content  character varying not null
)'''
##related_occupations
sql='''CREATE TABLE related_occupations 
       (id serial not null PRIMARY KEY,
       url_id int not null  references major_table(url_id) ,
       code  varchar(10)    null 
)'''
##work_context
sql='''CREATE TABLE work_context 
       (id serial not null PRIMARY KEY,
       url_id int not null  references major_table(url_id) ,
        context character varying null,
        questions character varying null,
        score int null,
        explanation character varying null,
         related_url varchar(90)   null
)'''
##wages_and_employment_content
sql='''CREATE TABLE wages_and_employment_content 
       (
       url_id int not null  references major_table(url_id) PRIMARY KEY,
        employment character varying null,
        median_wages character varying null,
        projected_growth  character varying null,
        projected_job_openings character varying null,
        top_industries character varying null
)'''
##detailed_work_activities
sql='''CREATE TABLE detailed_work_activities 
       (id serial not null PRIMARY KEY,
       url_id int not null references major_table(url_id),
       activies character varying  null ,
       related_url varchar(90)   null
)'''
##education_content
sql='''CREATE TABLE education_content 
       (id serial not null PRIMARY KEY,
       url_id int not null references major_table(url_id),
       degree character varying  null ,
       score  int null
)'''
##sample_of_job_titles 
sql='''CREATE TABLE sample_of_job_titles 
       (id serial not null PRIMARY KEY,
       url_id int not null references major_table(url_id),
       job_title  character varying  null 
)'''
##major_table
sql='''CREATE TABLE major_table
       (url_id serial not null PRIMARY KEY,
       url varchar(49) ,
       code  character varying not null ,
       title character varying not null,
       all_items  character varying null ,
       major_description  character varying null)'''