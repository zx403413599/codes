mport re
from pyspider.libs.base_handler import *
import time
from bs4 import BeautifulSoup
import requests

class Handler(BaseHandler):

    @every(minutes=24 * 60)
    def on_start(self):
       cat={'bright':'b','career':'c','green':'n','industry':'i','family':'f','zone':'z','stem':'t'}
#each=cat[0]
       for key_name,id_name in cat.items():
            url='http://www.onetonline.org/find/'+key_name+'?'+id_name+'=0&g=Go'
            respon=requests.get(url)
            time.sleep(3)
            soup=BeautifulSoup(respon.text)
            for one in soup.find_all('a',href=True):
                if one['href'].startswith('http://www.onetonline.org/link/summary/'):
                     url=one['href'].replace('summary','details')
                     self.crawl(url, callback=self.detail_page)

    @config(age=24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            if re.match("http://www.onetonline.org/link/details/\d{2}-\d{4}.\d{2}", each.attr.href):
                self.crawl(each.attr.href, priority=9, callback=self.detail_page)


    def detail_page(self, response):
        return {
            "current_url": self.get_current_url(response),
            "major_title": self.get_major_title(response),
            "major_description": self.get_major_description(response),
            "sample_of_job_titles": self.get_sample_of_job_titles(response),
            "all_items": self.get_all_items(response),
            "task_content_list": self.get_task_content_list(response),
            "tool_content":self.get_tools(response),
             "technology_content":self.get_technology(response),
            "abilities": self.get_abilities_content_list(response),
           "work_activities":self.get_work_activities_content_list(response),
            "detailed_work_activities":self.get_detailed_work_activities_content_list(response),
            "work_context":self.get_work_context_content_list(response),
            "knowledge_content":self.get_knowledge(response),
            "skills_content":self.get_skills_content_list(response),
             "interests_content":self.get_interests_content_list(response),
            "work_values_content":self.get_work_values_content_list(response),
            "job_zone_content":self.get_job_zone_content_list(response),
            "education_content":self.get_education_content_list(response),
            "work_styles_content":self.get_work_styles_content_list(response),
            "related_occupations":self.get_related_occupations_list(response),
            "wages_and_employment_content":self.get_wages_and_employment_content_list(response),
        }
    
    #获取当前的URL地址 OK
    def get_current_url(self, response):
        return response.url
    
    #获得此类专业名称 OK 
    def get_major_title(self,response):
        return response.doc('.titleb').text()
    
    #获得此类专业的描述 OK
    def get_major_description(self, response):
        try:
           content=response.doc('#realcontent > div > p').eq(0).text()
        except Exception:
            content=None
        finally:
            return content
    
    #获得此类专业相关工作的头衔名称 OK
    def get_sample_of_job_titles(self, response):
        try:
            content=response.doc('#realcontent>div#content>p').eq(1).text().split(':')[1].split(',')
        except Exception:
            content=None
        finally:
            return content
    
    #获得所有分类 OK
    def get_all_items(self, response):
        try:
           content=[x.text() for x in response.doc('.sm').eq(0).items()][0].split('|')
        except Exception:
            content=None
        finally:
            return content
    
    #获得任务目标的主要内容 先目标后内容 OK  
    def get_task_content_list(self, response):
        try:
            list_content=[x.text() for x in response.doc('.section_Tasks div').items()][3:]
            list_score=[x.text() for x in response.doc('.section_Tasks b').items()]
            content=zip(list_content,list_score)
        except Exception:
            content=None
        finally:
            return content
    
    #获得工具 OK
    def get_tools(self,response):
        try:
            content=handle_special_unicode_string_dict([x.text() for x in response.doc('ul.moreinfo').eq(0).children('li').items()])
        except Exception:
            content=None
        finally:
            return content
        
    #获得技术的相关内容 Ok
    def get_technology(self,response):
        try:
            content=handle_special_unicode_string_dict([x.text() for x in response.doc('ul.moreinfo').eq(1).children('li').items()])
        except Exception:
            content=None
        finally:
            return content
    #获得能力的主要内容 OK
    def get_abilities_content_list(self,response):
        try:
            list_details=  handle_special_unicode_string([x.text() for x in response.doc('.section_Abilities div').items()][5:])
              #title_list =  [x.text() for x in response.doc('.section_Abilities b').items()]
            list_all =  [x.text() for x in response.doc('.section_Abilities b').items()]
            content=handle_list(list_all,list_details)
        except Exception:
            content=None
        finally:
            return content
        
    #获得工作活动的主要内容  OK
    def get_work_activities_content_list(self,response):
        try:
             list_all =  [x.text() for x in response.doc('.section_WorkActivities b').items()]
             list_details =  handle_special_unicode_string([x.text() for x in response.doc('.section_WorkActivities div').items()][5:])
             content=handle_list(list_all,list_details)
        except Exception:
            content=None
        finally:
            return content                
    
    #获得详细工作能力的主要内容 OK
    def get_detailed_work_activities_content_list(self, response):
        try:
            content=[x.text() for x in response.doc('ul.moreinfo').eq(2).children('li').items()]
        except Exception:
            content=None
        finally:
            return content
        
    #获得工作环境的主要内容 dict(zip(title_list,handle_special_unicode_string(origin_content_list)))
    #字典格式 key为context values 为列表;列表为两个元素，第一个为问题;第二个为回复结果的一个列表 该列表个数不确定 列表中的组成元素也是列表  第一个元素为百分比  第二个元素为 对应内容
    def get_work_context_content_list(self,response):#OK
        try:
             title_list =  [x.text() for x in response.doc('.section_WorkContext div.moreinfo').items()]
             keys=handle_special_unicode_string_key(title_list)
             values=handle_special_unicode_string(title_list)
             #responses=[x.text() for x in response.doc('.section_WorkContext').children('td').items()]
             responses_per=[x.text() for x in response.doc('.section_WorkContext td.report2a').items()]
             responses_con=[x.text() for x in response.doc('.section_WorkContext td.report2al').items()]
             list_all=[]
             temp_list = []
             for each in zip(responses_per,responses_con)[1:]:
                    if  each != ('', ''):
                        temp_list.append(each)
                    else:
                        list_all.append(temp_list)
                        temp_list = []
             #origin_content_list =  [x.text() for x in response.doc('.section_WorkContext li').items()]
             content=dict(zip(keys,zip( values,list_all)))
        except Exception:
            content=None
        finally:
            return content                  
    
    #获得工作类型的主要内容 ok
    def get_work_styles_content_list(self,response):
        try:
            list_detailes=handle_special_unicode_string([ x.text() for x in response.doc('.section_WorkStyles div.moreinfo').items()])
            list_key=handle_special_unicode_string_key([ x.text() for x in response.doc('.section_WorkStyles div.moreinfo').items()])
            list_values=[ x.text() for x in response.doc('.section_WorkStyles td.report2a b').items()]
            content=dict(zip(list_key,zip(list_detailes,list_values)))
        except Exception:
            content=None
        finally:
            return content   
        
    #获得相关职位的主要内容ok
    def get_related_occupations_list(self,response):
        try:
            title_list =  [x.text() for x in response.doc('.occcode').items()]
            origin_content_list =  [x.text() for x in response.doc('.occtitle').items()]
            content=dict(zip(title_list,origin_content_list))
        except Exception:
            content=None
        finally:
            return content    
   
    #获取知识的主要内容 OK
    def get_knowledge(self,response):
        try:
           # list_detailes=handle_special_unicode_string([ x.text() for x in response.doc('section_Knowledge div.moreinfo').items()])
            list_detailes=handle_special_unicode_string([ x.text() for x in response.doc('.section_Knowledge div.moreinfo').items()])
            list_key=handle_special_unicode_string_key([ x.text() for x in response.doc('.section_Knowledge div.moreinfo').items()])
            list_values=[ x.text() for x in response.doc('.section_Knowledge  td.report2a b').items()]
            content=dict(zip(list_key,zip(list_detailes,list_values)))
        except Exception:
            content=None
        finally:
            return content
    
    #获取技能的主要内容 OK
    def get_skills_content_list(self,response):
        try:
            list_detailes=handle_special_unicode_string([ x.text() for x in response.doc('.section_Skills div.moreinfo').items()])
            list_key=handle_special_unicode_string_key([ x.text() for x in response.doc('.section_Skills div.moreinfo').items()])
            list_values=[ x.text() for x in response.doc('.section_Skills  td.report2a b').items()]
            content=dict(zip(list_key,zip(list_detailes,list_values)))
        except Exception:
            content=None
        finally:
            return content
        
    #获取兴趣的主要内容 OK
    def get_interests_content_list(self,response):
        try:
            list_key=handle_special_unicode_string_key([x.text() for x in response.doc('div#content>div>table').eq(2).find('.moreinfo').items()])
            list_detailes=handle_special_unicode_string([x.text() for x in response.doc('div#content>div>table').eq(2).find('.moreinfo').items()])
            list_values=[x.text() for x in response.doc('div#content>div>table').eq(2).find('.report2a').items()]
            content=dict(zip(list_key,zip(list_detailes,list_values)))
        except Exception:
            content=None
        finally:
            return content
        
    #获取技能的主要内容 OK
    def get_work_values_content_list(self,response):
        try:
            list_detailes=handle_special_unicode_string([ x.text() for x in response.doc('div#content>div>table').eq(3).find('div.moreinfo').items()])
            list_key=handle_special_unicode_string_key([ x.text() for x in response.doc('div#content>div>table').eq(3).find('div.moreinfo').items()])
            #list_key=handle_special_unicode_string_key([ x.text() for x in response.doc('.section_Skills div.moreinfo').items()])
            list_values=[ x.text() for x in response.doc('div#content>div>table').eq(3).find('.report2a').items()]
            content=dict(zip(list_key,zip(list_detailes,list_values)))
        except Exception:
            content=None
        finally:
            return content        
    
    #获取工作地区的主要内容 OK
    def get_job_zone_content_list(self,response):
        try:
            job_zone_title_list = [x.text() for x in self.get_job_education_wages_content_list(response).eq(0).find('.report2b').items()]
            job_zone_description_list = [x.text() for x in self.get_job_education_wages_content_list(response).eq(0).find('.report2').items()]
            content=dict(zip(job_zone_title_list,job_zone_description_list))
        except Exception:
            content=None
        finally:
            return content   
        
    #获取教育的主要内容 OK
    def get_education_content_list(self,response):
        try:
            list_per=[ x.text() for x in response.doc('div#content>div>table').eq(1).find('.report2a').items()]
            list_details=[ x.text() for x in response.doc('div#content>div>table').eq(1).find('.report2').items()]
            content=zip(list_details,list_per)
        except Exception:
            content=None
        finally:
            return content
        
    def get_job_education_wages_content_list(self,response):
        try:
            content=response.doc('#realcontent > div > div > table')
        except Exception:
            content=None
        finally:
            return content    
    
    #获取工资和雇主情况的主要内容
    def get_wages_and_employment_content_list(self,response):
        #处理非图片内容
        try:
            list_key=[x.text() for x in response.doc('div#content>div>table').eq(5).find('.report2b').items()]
            list_detailes=[x.text() for x in response.doc('div#content>div>table').eq(5).find('.report2').items()]
            content=dict(zip(list_detailes,list_detailes))
        except Exception:
            content=None
        finally:
            return content
        #合并
#处理utf-8破折号的函数，返回单列表      html body div#allcontent div#realcontent div#content div table tbody tr td.report2b

def handle_special_unicode_string(origin_list):
    new_list = []
    tag = u'\u2014'
    #处理相关的内容，只要破折号后面的内容
    for content in origin_list:
        if(isinstance(content,unicode) == True):
            #unicode转换成string
            new_list.append(content.split(tag)[1].strip().encode("utf-8"))
        else:
            new_list.append("None")
    return new_list

#处理utf-8破折号的函数，返回单列表      列表为key
def handle_special_unicode_string_key(origin_list):
    new_list = []
    tag = u'\u2014'
    #处理相关的内容，只要破折号后面的内容
    for content in origin_list:
        if(isinstance(content,unicode) == True):
            #unicode转换成string
            new_list.append(content.split(tag)[0].strip().encode("utf-8"))
        else:
            new_list.append("None")
    return new_list



    
#处理utf-8破折号的函数，返回字典
def handle_special_unicode_string_dict(origin_list):
    tag = u'\u2014'
    title_list = []
    description_list = []
    for each in origin_list:
        if (isinstance(each,unicode) == True):
            title_list.append(each.split(tag)[0].strip().encode("utf-8"))
            description_list.append(each.split(tag)[1].strip().encode("utf-8"))
        else:
            title_list.append(each.split(tag)[0].strip().encode("utf-8"))
            description_list.append('')
    return dict(zip(title_list,description_list))
    
    
#处理三个列表 成为一个字典
def  handle_list(list_all,list_details):
    len_list=(len(list_all)+1)/2
    keys=[ ]
    values=[ ]
    for num in range(len_list):
        tem_list=[ ]
        tem_list.append(list_details[num])
        tem_list.append(list_all[2*num])
        keys.append(list_all[2*num+1])
        values.append(tem_list)
    return dict(zip(keys,values))
        
    
    
    
    
    
    

