!SLIDE
## python爬虫入门

###**报告人**: 王剑龙 孙瑞娜

###**学院**: 统计学院 信息学院

###**邮箱**:wjlkevin@163.com srn672836152@163.com

!SLIDE

##爬虫可以做哪些有趣的事情？
> ###**追女神和欧巴可以用!**
> 1. 当你的女神或者欧巴在知乎或者微博更新了消息，你是第一个知道的，而且是最快评论和关注的,那你要关注的女神是不是很感动，两人在一起是不是成功率更高一些？
> 2. 比如在知乎，用爬虫爬取关注对象关注的问题，给什么问题点赞了，回答了什么问题等等，一旦有更新，爬虫就可以第一时间通过邮件发送到你手机了，妈妈再也不用担心我找不到对象了。

!SLIDE
##爬虫可以做哪些有趣的事情？
> ###**喜欢看电影的孩子有福了!**
> 1. 你可以把大众点评、美团猫眼、淘宝电影、豆瓣电影的数据爬取下来，做一个简单的可视化页面后就可以及时查询：
> 2. 这部最新的007电影哪家电影院最便宜，以前的007系列评价如何，评分最高的电影有哪些？是不是找电影就不用几个客户端来回看了？

!SLIDE
##爬虫可以做哪些有趣的事情？
> ###**购物就是买优惠。**
> 可以用爬虫爬取我爱白菜网、超值分享汇、惠惠购物、今日聚超值、留住你、买手党、买个便宜货、什么值得买、天上掉馅饼、一分网、折800值得买、值值值等网站的折扣信息，有自己中意的商品是不是可以很优惠的买到？

!SLIDE
##爬虫可以做哪些有趣的事情？
> ###**金融数据随我拿。**
> 1. 自己想做个量化投资什么的没有数据可不行，而新浪财经、巨潮网是不是有很多股票基金数据和财务数据，我们就可以用爬虫爬取这些财经网站的数据为自己所用，供自己分析，
> 2. 如果想弄得更高端一点。还可以抓取所有新闻网站的财经新闻，做个文本挖掘，将最火最热的题材给最新挖掘出来。
> 3. 或者将新浪微博等社交网站的数据抓下来，分析大众的关注热点和情绪倾向，做个股票预测的可不可以？

!SLIDE
##爬虫可以做哪些有趣的事情？
> ###**好工作来找我。**
> 1. 现在什么工作最赚钱，最近的工作形势怎么样？是不是可以把一大堆招聘网站的招聘数据抓下来，然后做个对比分析？
> 2. 这种基于大量数据的分析才不会让你只见树木不见森林。好工作随你找。
> 3.除了上面的小应用以外，爬虫还有好多好多用处，只要你脑洞大开，什么的都可以做做。说这么多，只是想说明一点，爬虫在我们生活中**很有用！**

!SLIDE
##浏览网页

爬虫抓取网页的过程其实和咱们平时使用浏览器浏览网页的道理是一样的。

- 浏览器的地址栏中输入地址，比如**www.baidu.com**;
- 浏览器会查找域名对应的IP地址，向IP对应的服务器发送请求;
- 百度那边的服务器响应请求，发回网页内容(html代码);
- 浏览器解析网页内容,然后将原始的代码转变成我们直接看到的网站页面。

!SLIDE
## 爬虫介绍

爬虫就是用计算机代码实现模拟浏览器的功能，给定url，将请求的html下载下来，这个过程就是抓**数据**，而不需要人工参与。

学习python网络爬虫主要分3个大的版块：抓取，分析，存储

###**firebug**
爬虫必备神器  
### 抓取 ( requests库)
1. get(headers)
2. post(模拟登入、和cookies)
3. 模拟浏览器(selenium)#看时间
###html解析
BeautifulSoup解析 得到结构化数据
### 存储
1. 文本、数据框 (pandas)保存为csv、txt比较多
2. 图片、pdf、视频保存（requests)
3. 存入到数据库中
###反爬虫机制

!SLIDE
##爬虫小助手firebug

> ###展示firebug :chrome safari 
+ 有类似的工具存在 右键 看看有没有审查元素 
+ Firebug如同一把精巧的瑞士军刀，从各个不同的角度剖析Web页面内部的细节层面,我们可以利用它提高我们写爬虫的效率。怎么剖析待会结合实例讲。

!SLIDE
##抓取之get方法（一）
抓取就是刚才说的向服务器发送请求并得到网页内容的一个过程。
请求的方法有主要有两种get和post:

1. get 方法就是你向服务器请求网页内容，服务器把网页内容发给你
2. 我们用requests get两个网页试一试
3. 为什么大众点评的get不到？

!SLIDE
##抓取之get方法（二）
大众点评网站不返回请求的网页内容，其实这涉及到http请求头。

###利用firebug查看：
  **host user-agent Cookie**

### 定制请求头，伪装成浏览器。
在防爬虫层面，通常是可以从这里区别你是爬虫还是正常用户。如果你没有请求头,会拒绝给你发送信息。
利用firebug查看请求头信息，然后自己用python字典的格式模仿即可。
### 请求状态码 （http相应状态)
1. 成功（2字头）：200 OK
2. 请求错误（4字头）：**403 Forbidden**（服务器理解但是拒绝) **404 Not Found** (请求失败，要么失败要么服务器没有该资源）

### 其他请求类或库
1. python自带：urllib2、urllib3
2. pycurl urllib3 RoboBrowser

!SLIDE
##抓取之post方法
get方法能请求大部分简单的网站了，但是有些网站需要浏览器发送数据给服务器才能发送信息。
比如登入网页等。

1. firebug查看 
2. 模拟登入脉脉
3. 保持登入
那是不是请求一个网页就得post呢，当然不是了。
当你post方法登入以后，服务器会返回给你cookie,也就是身份信息了。得到这个东西说明可以认为是特定用户在浏览，我们在header中加入cookie,就不需要每次很麻烦的post了。

!SLIDE

##html代码解析之解析包介绍
我们会发现网页返还给我们的一大堆东西，不管对我们有用没用，全给你了。而我们根据需求只要特定内容，而且希望得到结构化的数据，也就是整理的好好的数据。这样看起来才不累。这时候就需要我们的解析工具登场了。
###解析包介绍

1. **BeautifulSoup** ：
支持css 正则表达式 ，非常人性化的一个包，简单好用 。但是纯python 实现的 ，解析效率稍微弱一些。一般情况够用
2. **lxml包：**
支持xpath 语法 ，用C语言写的 非常高效。学习xpath 语法可以 去[w3school ]（http://www.w3school.com.cn/），免费的web技术教程网站 ，那里还有htmi xml javasccript 等语言的介绍，我没事常会去学习会。
3. **pyquery** 
支持jquery选择器
4. **cssselect**   
支持css选择器 
5. **re (正则表达式)**
当然也可以使用python 自带的re 正则表达式模块 ，小型爬虫还好，写多了会有点崩溃。当上述方式得不到数据的时候可以结合正则表达式。

总之，应根据自己需求选择解析包即可，没有绝对的好坏。初学者建议使用BeautifulSoup 因为好学容易上手。
 下面的讲解主要基于，这里主要用到BeautifulSoup4的解析包。

 
!SLIDE
##BeautifulSoup4解析

还是拿大众点评来进行讲解

+ find 
+ find_all 

!SLIDE

##存储
+ 文本类型
根据自己需要进行选择了，小批量的数据我们可以保存在电脑硬盘里，文本的我们可以保存为txt、数据框的可以保存为txt、csv， 比如单个店面评论的数据。
+ 图片、pdf、视频
这些数据请求有点不同。requests的官方文档里会有介绍，大家有需要可以查询，因为时间关系，这里就不介绍了。
+ 数据库存储
这里指的是把抓来的数据直接放到数据库里，如Mysql。这里的内容大家有需要根据自己情况现学就行。

!SLIDE

##防爬虫机制

大数据时代，数据就是一个公司的核心资产。你想抓数据，那边公司肯定会千方百计的设计防爬虫的机制。你有机制，我有机智。把刚才涉及的伪装浏览器的东西总结下，再加上一些需要注意的地方。

1. header 
http请求头信息伪装,这个还是比较容易的，用firebug就可以查询到。
2. cookie 
个人身份信息。刚才也讲到了，也可以用firebug获取，加入到header里面
3. time 
如果你爬取速度太快，且好多爬虫同时进行，也就是多线程时，会给对方服务器造成巨大压力，甚至造成瘫痪，所以，很多公司会对爬取速度做出限定。因为正常人不会访问速度不会这么快和频繁。你需要每抓取一个页面，然后等待几秒钟。
4. 利用javascript(js)加密网页内容
有些数据是动态变化的，它不会把所有的数据一下子加载到网页里，有的需要点击一下才能出现，这时候很多爬虫就束手无策了。
有两种解决方案，一种是调用浏览器去点击它，比如selenium ,它主要用来做自动化测试的，但是也可以用来抓数据，待会会演示。但由于调用浏览器占内存，对性能要求较高。另一种就是封装个无界面浏览器，如phantomjs,执行js代码进行抓取。
5. 验证码
你浏览次数过多或很频繁，就会跑出验证码，爬虫一般识别不了，多次输入错误，你的IP就会被封了。一种方法是人工填写，一种方法就是抓取大量验证码，然后利用机器学习中数字识别的算法就行判别，这个要求就很高了。
还有好多，比如内容放到图片里，不让你抓。 


!SLIDE

##爬虫框架介绍
爬虫框架与小型爬虫相比 就如电瓶车与汽车。
新手不推荐用框架，因为它隐藏了很多细节，不利于学习。
自己多写几个小型爬虫，框架就很容易上手了。

### scrapy 
python最著名的爬虫框架 ，但是不支持python3
###pyspider
这是国人开发的一款爬虫框架，有web页面，很强大很好用
###cola 
这也是国人开发的，支持分布式抓取，也就是可以支持多台电脑同时抓取
###grab
支持python3 非常全面的爬虫框架

!SLIDE left

##谢谢观看!
