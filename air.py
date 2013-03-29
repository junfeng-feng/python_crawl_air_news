#coding=utf-8

import urllib
import time
import re
import threading
import string
import urllib2
import chardet
import MySQLdb
import base64
import os
import sys
from pyquery import PyQuery
reload(sys)
sys.setdefaultencoding("utf-8")

#新闻最早时间
earliestTime = "2010-06-01"
mutex = threading.Lock()

def printf(obj):
    """多线程的输出"""
    global mutex
    mutex.acquire()
    print obj
    mutex.release()
    

def mysqlDataBase_init(database="jipiao"):
    """连接mysql
返回conn,cursor"""
    conn = MySQLdb.connect(host="localhost", user="root", passwd="", db=database, unix_socket="/opt/lampp/var/mysql/mysql.sock")
    cursor = conn.cursor()
    cursor.execute("SET NAMES utf8")
    cursor.execute("SET CHARACTER_SET_CLIENT=utf8")
    cursor.execute("SET CHARACTER_SET_RESULTS=utf8")
    conn.commit()
    return (conn, cursor)       

def countNewsUrl(cursor, newsUrl):
    "count  newsUrl 个数"
    selectSql = "select count(*) from jipiaonews where newsUrl='%s'" % newsUrl
    cursor.execute(selectSql)
    result = cursor.fetchone()
    return result[0]
    pass

def checkNewsUrl(cursor, newsUrl):
    """检查newsUrl
返回是否已经存在"""
    #已经有的url，不需要抓取了
    if countNewsUrl(cursor, newsUrl) > 0:
        return 1
    
    #多线程输出
    global mutex
    mutex.acquire()
    print newsUrl
    mutex.release()
    return 0

def executeSql(cursor, title, postDate, content, airport, source, newsUrl):
    """执行sql语句"""
    #替换不友好字符
    content = content.replace("'", "")
    content = content.replace("\"", "")
    
    insertsql = ("insert into jipiaonews (title,postDate,content,airport,source,newsUrl)" 
            " values('%s','%s','%s','%s','%s','%s')"
            % (title, postDate, content, airport, source, newsUrl))    
    insertsql = insertsql.encode("utf-8")
    cursor.execute(insertsql)    
    pass

def parseDate(postDate):
    """转换日期"""
    return postDate.replace("年", "-").replace("月", "-").replace("日", "")
    pass

def xiamenhangkong():
    """厦门航空"""
    airport = "厦门航空"
    matchPostDate=re.compile(r"[\d]+-[\d]+-[\d]")
    
    conn,cursor=mysqlDataBase_init()
    page=1
    while page!=-1:
        newsPageUrl="http://www.xiamenair.com.cn/newslist/news-%s.shtml"%page
        page+=1
        
        try:
            pq=PyQuery(newsPageUrl)
        except:
            break
        i=0
        while True:
            a= pq("table.artlist a:eq(%s)"%i)
            i+=1
            
            newsUrl=a.attr("href")
            if not newsUrl:
                #第一个新闻都没有，则退出
                if i==1:
                    page=-1
                break
            newsUrl="http://www.xiamenair.com.cn"+newsUrl
            if checkNewsUrl(cursor, newsUrl):
                page=-1
                break
            title=a.text()
            tr=pq("table.artlist tr:eq(%s)"%i)
            postDate = matchPostDate.search(tr.text()).group()
            newpq=PyQuery(newsUrl)
            content=newpq("div.artcon").text()

            if content.find("）")!=-1:
                content=content[content.find("）")+1:]
            content=content.strip() 
            source="" 
            
            executeSql(cursor, title, postDate, content, airport, source, newsUrl)
            pass
        pass
    conn.commit()
    conn.close()
#    newsBasePage = "http://www.xiamenair.com.cn/about_news.aspx?Page=%s"
#    #由于新闻少，所以时间向前推了
#    ownEarliestTime = "2010-01-01"
#    #页码
#    pages = 1
#    while pages != -1:
#        newsBaseUrl = newsBasePage % pages    
#        pages += 1
#        
#        pq = PyQuery(newsBaseUrl)
#        table = pq("div#header table:eq(25)")
#        conn, cursor = mysqlDataBase_init()
#        index = 0
#        while True:
#            a = table.find("a:eq(%s)" % index)
#            index += 1
#
#            newsUrl = a.attr("href")
#            if not newsUrl:
#                break
#            newsUrl = "http://www.xiamenair.com.cn/" + newsUrl    
#            title = a.text()
#            if checkNewsUrl(cursor, newsUrl):
#                pages=-1#结束
#                break
#            newspq = PyQuery(newsUrl)
#            newsTable = newspq("table:eq(3)")    
#            postDate = newsTable("div:eq(1)").html()  
#            #函数终止条件
#            if postDate < ownEarliestTime:
#                pages = -1#结束
#                break  
#            content = newsTable("table:eq(1)").text()
#            source = "" 
#            postDate = postDate.encode("utf8")
#            
#            executeSql(cursor, title, postDate, content, airport, source, newsUrl)           
#            pass
#        conn.commit()
#        conn.close()
#        pass    
#    pass

def chongqinghangkong():
    """重庆航空"""
    airport = "重庆航空"
    
    newsBaseUrl = "http://www.flycq.com/info/news/index.html"
    pq = PyQuery(newsBaseUrl) 
    conn, cursor = mysqlDataBase_init()
    #使用正则表达式，查找日期
    patternDate = re.compile(r"[\d]{6}")
    i = 0
    while i!=-1:
        a = pq("div#newsContent div ul li:eq(%s)" % i).find("a")
        newsUrl = a.attr("href")
        i += 1
        if not newsUrl:
            break
        if newsUrl.startswith("http"):
            continue
        #使用正则表达式，查找日期
        postDate = "20" + patternDate.search(newsUrl).group()
        #需要修改个别的网址
        newsUrl = newsUrl[newsUrl.rfind('/') + 1:]       
        try:         
            newsUrl = "http://www.flycq.com/info/news/" + newsUrl
            if checkNewsUrl(cursor, newsUrl):
                i=-1#结束
                break
            newspq = PyQuery(newsUrl)
        except urllib2.HTTPError:
            printf(newsUrl + "   illegal url!")
            continue            
        content = newspq("div#newsContent").text()
        if content.find("InstanceBeginEditable") != -1:
            content = content[len("InstanceBeginEditable name=\"content\" "):]
        if content.find("InstanceEndEditable") != -1:
            content = content[:-len("InstanceEndEditable")]

        title = a.text()
        source = ""

        executeSql(cursor, title, postDate, content, airport, source, newsUrl)   
        pass   
    conn.commit()
    conn.close()    
    pass

def hannanhangkong():
    """海南航空"""
    airport = "海南航空"
    
    matchUrl = re.compile(r"/.*\.html")
    matchPostDate = re.compile(r"[\d]{4}-[\d]+-[\d]+")
    
    page = 0
    while page != -1:
        if page == 0:
            newsPageUrl = "http://www.hnair.com/gyhh/hhxw/index.html" 
        else:
            newsPageUrl = "http://www.hnair.com/gyhh/hhxw/index_%s.html" % page
        page += 1
        
        conn, cursor = mysqlDataBase_init()
        pq = PyQuery(newsPageUrl)
        i = 0
        while True:
            a = pq("div.body a:eq(%s)" % i)
            newsUrl = a.attr("onclick")
            if not newsUrl:
                break
            newsUrl = matchUrl.search(newsUrl).group()
            newsUrl = "http://www.hnair.com/gyhh/hhxw" + newsUrl
            li = pq("div.body li:eq(%s)" % i)
            i += 1
            
            postDate = matchPostDate.search(li.text()).group()
            if postDate < earliestTime:
                page = -1#结束标记
                break
            if checkNewsUrl(cursor, newsUrl):
                page = -1#结束标记
                break
            title = li.text()[:-(len(postDate) + 2)]                    
            newspq = PyQuery(newsUrl)
            content = newspq("div.body").text()
            if content.find("本报讯") != -1 and content.find("）") != -1:
                content = content[content.find("）") + len("）"):]
            source = ""

            executeSql(cursor, title, postDate, content, airport, source, newsUrl)
            pass
        conn.commit()
        conn.close()
        pass

def xingfuhangkong():
    """幸福航空"""
    airport = "幸福航空"
    newsBasePage = "http://www.joy-air.com/AboutUs/NewsCenter/List.aspx?id=1"
    pq = PyQuery(newsBasePage)
    
    url = pq("div#newslist").find("a").attr("href") 
    id = int(re.search(r"[\d]+", url).group())
    newsBaseUrl = "http://www.joy-air.com/AboutUs/NewsCenter/Detail.aspx?id="
    
    conn, cursor = mysqlDataBase_init()
    while True:
        newsUrl = newsBaseUrl + str(id)
        id -= 1
        
        if checkNewsUrl(cursor, newsUrl):
            break                   
        try:
            newspq = PyQuery(newsUrl)
        except urllib2.HTTPError:
            continue
        postDate = newspq("div.dynamicmessagesbox span:eq(1)").text()
        #函数终止条件
        if postDate < earliestTime:        
            break 
        content = newspq("div.dynamicmessagesbox").text()
        title = newspq("div.dynamicmessagesbox span:eq(0)").text()
        source = ""
            
        executeSql(cursor, title, postDate, content, airport, source, newsUrl)
        pass
    conn.commit()
    conn.close()
    pass

def shenzhenhangkong():
    """深圳航空"""
    airport = "深圳航空"
    newsPageUrl = "http://www.shenzhenair.com/more.jsp?category=423afc988259499faa94bbaf8b6983b5&title=%E6%B7%B1%E8%88%AA%E6%96%B0%E9%97%BB&lan=1" 
    
    sourceFile = urllib.urlopen(newsPageUrl).read()
    pq = PyQuery(sourceFile.decode("gbk"))
    
    conn, cursor = mysqlDataBase_init()
    newspqs = pq("table:eq(17)")
    match = re.compile(r"</script>.*")
    i = 0
    while True:
        a = newspqs("a:eq(%s)" % i)
        i += 1
        
        newsUrl = a.attr("href")
        title = a.text()
        if not newsUrl:
            break
        newsUrl = newsUrl[:newsUrl.rfind("&")]
        newsUrl = "http://www.shenzhenair.com/" + newsUrl   
        title = match.search(title).group()[len(r"</script>"):].strip()
        if checkNewsUrl(cursor, newsUrl):
            break        
        sourceFile = urllib.urlopen(newsUrl).read()
        newspq = PyQuery(sourceFile.decode("gbk"))
        content = newspq("table:eq(2)").text()
        #没有找到时间
        postDate = time.strftime("%Y%m%d")
        source = ""
        begin = content.find("】")
        begin = content.find("】", begin + 1)
        end = content.rfind("【")
        if begin == -1:
            begin = 0
        content = content[begin + 2:end]
  
        executeSql(cursor, title, postDate, content, airport, source, newsUrl)
        if i % 20 == 0:
            time.sleep(5)
        pass
    conn.commit()
    conn.close()
    pass

def chunqiuhangkong():
    """春秋航空
采用的是新闻页的，比首页的新闻还少"""
    airport = "春秋航空"
    newsPageUrl = "http://www.china-sss.com/Static/New_information"

    conn, cursor = mysqlDataBase_init()
    sourceFile = urllib.urlopen(newsPageUrl).read()
    pq = PyQuery(unicode(sourceFile, "utf-8"))
    i = 1
    while True:
        span = pq("div.New_information span:eq(%s)" % i)
        a = pq("div.New_information a:eq(%s)" % i)
        i += 1

        newsUrl = a.attr("href")
        if not newsUrl:
            break 
        title = a.text()
        postDate = span.text()
        newsUrl = "http://www.china-sss.com" + newsUrl
        if checkNewsUrl(cursor, newsUrl):
            break            
        sourceFile = urllib.urlopen(newsUrl).read()
        newspq = PyQuery(unicode(sourceFile, "utf-8"))
        content = newspq("dl#content_top dd").text()
        source = ""
          
        executeSql(cursor, title, postDate, content, airport, source, newsUrl)
        pass
    conn.commit()
    conn.close()
    pass

def dongfanghangkong():
    """东方航空"""
    airport = "东方航空"
    newsPageUrl = "http://www.ce-air.com/mu/main/gydh/xwgg/index.html"
    index = 0
    while index != -1:  
        if index != 0:
            newsPageUrl = ("http://www.ce-air.com/"
                         "mu/main/gydh/xwgg/index_%s.html" % index)
             
        index += 1#下一个页面    
        pq = PyQuery(newsPageUrl)
        conn, cursor = mysqlDataBase_init()
        i = 0
        while True:
            a = pq("div.s_xw_list ul li:eq(%s)" % i).find("a")
            i += 1

            href = a.attr("href")
            if not href:
                break
            title = a.text()
            newsUrl = "http://www.ce-air.com/mu/main/gydh/xwgg/" + href[2:]    
            postDate = re.search(r"[\d]{8}", newsUrl).group()
            if postDate < earliestTime.replace("-", ""):
                index = -1#外层循环结束标记
                break 
            if checkNewsUrl(cursor, newsUrl):
                index = -1#外层循环结束标记
                break                      
            newspq = PyQuery(newsUrl)
            content = newspq("div.body p:eq(1)").text()
            if content == None:
                content = newspq("div.body p:eq(0)").text()
            if len(content) == 0:
                    content = newspq("div.body").text()
                    content = content[content.rfind("}") + 1:]
                
            table = newspq("div.body table").text()           
            source = re.search("'.*'", table).group()[1:-1]
            
            executeSql(cursor, title, postDate, content, airport, source, newsUrl)
            pass
        conn.commit()
        conn.close()
        pass
    pass

def shanghaihangkong():
    """上海航空
没有新闻，只有快讯"""
    airport = "上海航空"
    newsPageUrl = "http://www.shanghai-air.com/cmsweb/sale.aspx?pageIndex=0"
    sourceFile = urllib.urlopen(newsPageUrl).read()
    pq = PyQuery(unicode(sourceFile, "utf-8"))
    
    conn, cursor = mysqlDataBase_init()
    i = 0
    while True:
        a = pq("div.items li:eq(%s)" % i).find("a")
        i += 1

        newsUrl = a.attr("href")
        title = a.attr("title")
        if not newsUrl:
            break
        newsUrl = "http://www.shanghai-air.com/cmsweb/" + newsUrl
        if checkNewsUrl(cursor, newsUrl):
            break            
        sourceFile = urllib.urlopen(newsUrl).read()
        newspq = PyQuery(unicode(sourceFile, "utf-8"))
        content = newspq("div.nowtext").text()
        if not content:
            content = ""
        postDate = time.strftime("%Y%m%d")
        source=""
        
        executeSql(cursor, title, postDate, content, airport, source, newsUrl)
        pass
    conn.commit()
    conn.close()
    pass

    """上海航空旧版的新闻"""
    airport = "上海航空"
    
    newsPageUrl = "http://ww1.shanghai-air.com/news/saldt.asp?ttt="
    matchPostDate = re.compile(r"[\d]+-[\d]+-[\d]+")
    
    conn, cursor = mysqlDataBase_init()
    pq = PyQuery(newsPageUrl)
    i = 0
    while True:
        a = pq("a:eq(%s)" % i)
        i += 1
        newsUrl = a.attr("href")
        if not newsUrl:
            break
        if not newsUrl.find("PublicInfo") != -1:
            continue
        newsUrl = "http://ww1.shanghai-air.com" + newsUrl
        if checkNewsUrl(cursor, newsUrl):
            break
        title = a.text()[1:]
        postDate = parseDate(matchPostDate.search(a.text()).group())
        title = title[:-(len(postDate) + 2)]
        newspq = PyQuery(newsUrl)
        content = newspq("div.texttext").text()
        source = ""
        if not content:
            continue
        
        executeSql(cursor, title, postDate, content, airport, source, newsUrl)
        pass
    conn.commit()
    conn.close()
    pass
def zhongguohangkong():
    """中国航空
有时会出现urlopen error"""
    airport = "中国航空"
    newsPageUrl = "http://ffp.airchina.com.cn/cms/ffp/jszx/xwxx/default.html"
    
    conn, cursor = mysqlDataBase_init()
    count=0
    while count!=10:
        try:    
            pq = PyQuery(newsPageUrl)
        except urllib2.URLError:
            count+=1
            time.sleep(5)
        else:
            break   
    i = 0
    while True:
        newsUrl = pq("a:eq(%s)" % i).attr("href")   
        if not newsUrl:            
            break
        span = pq("span:eq(%s)" % i).text()
        i += 1
        postDate = re.search(r"[\d]{4}-[\d]{2}-[\d]{2}", span).group()
        if postDate < earliestTime:
            break
        title = span[:-14]
        
        newsUrl = "http://ffp.airchina.com.cn" + newsUrl   
        if checkNewsUrl(cursor, newsUrl):
            break   
        count=0   
        while count!=10:
            try:    
                newspq = PyQuery(newsUrl)
            except urllib2.URLError:
                count+=1
                time.sleep(5)
            else:
                break          
        content = newspq("body").text()
        source = ""   
             
        executeSql(cursor, title, postDate, content, airport, source, newsUrl)        
        pass
    conn.commit()
    conn.close()
    pass

def sichuanhangkong():
    """四川航空"""
    airport = "四川航空"
    newsPageUrl = "http://www.scal.com.cn/ScalB2CWeb/News/More_News.aspx?code=NC00391"
    
    pq = PyQuery(urllib.urlopen(newsPageUrl).read())
    ul = pq("ul.news_n")
    
    conn, cursor = mysqlDataBase_init()
    match = re.compile(r"[\d]{4}")
    i = 0
    while True:
        a = pq("ul.news_n a:eq(%s)" % i)
        i += 1
        id = a.attr("onclick")
        if not id:
            break
        
        id = match.search(id).group()
        newsUrl = "http://www.scal.com.cn/Scal.WebMaster/FileUpLoad/htmlpage/%s.html"
        newsUrl = newsUrl % id  
        if checkNewsUrl(cursor, newsUrl):
            break           
        newspq = PyQuery(newsUrl)
        title = newspq("span.NewsTitle").text()
        postDate = newspq("span.NewsInfo_Time").text()
        postDate = parseDate(postDate)[5:]
        source = newspq("span.NewsInfo_Publisher").text()
        content = newspq("div.NewsContent").text()
        
        executeSql(cursor, title, postDate, content, airport, source, newsUrl)
        pass
    conn.commit()
    conn.close()
    pass
    
def tianjinhangkong():
    """天津航空"""
    airport = "天津航空"
    
    #天津航空官网上的的新闻
    url = "http://www.tianjin-air.com/index/ann!doAjaxQuery.action"
    pq = PyQuery(urllib.urlopen(url).read())
    match = re.compile("'.*'")
    conn, cursor = mysqlDataBase_init()
    i = 0
    while True:
        a = pq("a:eq(%s)" % i)
        i += 1
        title = a.attr("title")
        newsUrl = a.attr("href")
        if not newsUrl:
            break
        
        newsUrl = match.search(newsUrl).group()[1:-1]
        newsUrl = "http://www.tianjin-air.com" + newsUrl
        if checkNewsUrl(cursor, newsUrl):
            break             
        content = urllib.urlopen(newsUrl).read()
        begin = content.find("Date:")
        postDate = content[begin:content.find("\n", begin)][6:-6].strip()
        try:
            postDate = time.strftime("%Y-%m-%d", time.strptime(postDate, "%a, %m %b %Y %H:%M:%S"))
        except:
            matchp=re.search(r"[\d]+-[\d]+-[\d]+", postDate)
            if matchp:
                postDate=matchp.group()
            else:
                postDate=time.strftime("%Y%m%d")
        
        content = content[content.find("\n", content.find("X-MimeOLE")):].strip() 
        #若是base64需要解码
        content = base64.decodestring(content)  
        try:
            newspq = PyQuery(content)
        except:
            continue
        content = newspq("body").text()
        if not content:
            continue
        source=""
        
        executeSql(cursor, title, postDate, content, airport, source, newsUrl)
        pass
    conn.commit()
    conn.close()
    pass
def shandonghangkong():
    """山东航空"""
    airport = "山东航空"
    
    headerString = """Accept:*/*
Accept-Charset:GBK,utf-8;q=0.7,*;q=0.3
Accept-Encoding:gzip,deflate,sdch
Accept-Language:zh-CN,zh;q=0.8
Connection:keep-alive
Content-Length:176
Content-Type:application/x-www-form-urlencoded
Cookie:JSESSIONID=0000Dva3t-EvL-J6jQ5uEa8YppU:-1; GUEST_LANGUAGE_ID=zh_CN; COOKIE_SUPPORT=true; __ozlvd671=1302764071; __ozlvd=1302764071
Host:www.shandongair.com
Method:POST /c/portal/render_portlet HTTP/1.1
Origin:http://www.shandongair.com
Referer:http://www.shandongair.com/web/shair_zh/news
User-Agent:Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16"""
    headerString = headerString.split("\n")

    dataString = """p_l_id:89978
p_p_id:journal_news_INSTANCE_1h76
p_p_action:0
p_p_state:normal
p_p_mode:view
p_p_col_id:column-3
p_p_col_pos:2
p_p_col_count:3
currentURL:/web/shair_zh/news"""  
    dataString = dataString.split("\n")

    dataDict = {}
    for line in dataString:
        line = line.split(":")
        dataDict[line[0]] = line[1]
    
    
    url = "http://www.shandongair.com/c/portal/render_portlet"
    request = urllib2.Request(url)
    for line in headerString:
        line = line.strip().split(":")
        request.add_header(line[0], line[1])  
        
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor()) 
    
    page = 1
    while page != -1:
        dataDict["cur"] = "%s" % page
        data = urllib.urlencode(dataDict)
        page += 1
        
        sourceFile = opener.open(request, data).read()    
        conn, cursor = mysqlDataBase_init()
        pq = PyQuery(unicode(sourceFile, "utf8"))
        i = 0
        while True:
            a = pq("table.taglib-search-iterator a:eq(%s)" % i)
            span = pq("table.taglib-search-iterator span:eq(%s)" % i)
            i += 1
            
            newsUrl = a.attr("href")
            if not newsUrl:
                break
            newsUrl = "http://www.shandongair.com" + newsUrl
            if checkNewsUrl(cursor, newsUrl):
                page = -1#终止条件
                break
            title = a.text()
            postDate = span.text()
            if postDate < earliestTime:
                page = -1#终止条件
                break
            sourceFile = urllib.urlopen(newsUrl).read()
            newspq = PyQuery(unicode(sourceFile, "utf8"))
            content = newspq("td.main").text()
            source = ""
            
            executeSql(cursor, title, postDate, content, airport, source, newsUrl)
            pass
        conn.commit()
        conn.close()        
        pass 


    #山东航空的首页新闻
    newsPageUrl = "http://www.shandongair.com/"
    conn, cursor = mysqlDataBase_init()
    pq = PyQuery(newsPageUrl)
    i = 0
    while True:
        a = pq("div.ind_news_box_center a:eq(%s)" % i)
        i += 1
        newsUrl = a.attr("href")
        if not newsUrl:
            break
        if checkNewsUrl(cursor, newsUrl):
            break 
        sourceFile = urllib.urlopen(newsUrl).read()
        sourceFile = unicode(sourceFile, "utf-8")
        newspq = PyQuery(sourceFile)
        content = newspq("body table:eq(1)").text()[:-6]
        title = newspq("title").text()
        
        m = re.search(r"[\d]{4}年[\d]+月[\d]+日", content)
        if m:
            postDate = m.group()
            postDate = parseDate(postDate)
        else:
            postDate = time.strftime("%Y%m%d")
        source = ""
    
        executeSql(cursor, title, postDate, content, airport, source, newsUrl)
        pass 
    conn.commit()
    conn.close()
    pass

def jixianghangkong():
    """吉祥航空
没有看见新闻，就找了促销信息"""

    airport = "吉祥航空" 
    page = 1
    while page != -1:
        newsPageUrl = "http://www.juneyaoair.com/JY_Airlines_dtlist.jsp?page=%s&menu_no=203" % page
        page += 1
        
        try:
            pq = PyQuery(newsPageUrl) 
        except:
            break
        conn, cursor = mysqlDataBase_init()
        table = pq("table:eq(18)")
        trindex = 1
        while True:
            tr = table("tr:eq(%s)" % trindex)
            trindex += 1
            if not tr:
                break          
            a = tr("td a")
            newsUrl = "http://www.juneyaoair.com/" + a.attr("href")
            
            title = a.text()
            source = tr("td:eq(3)").text()
            postDate = tr("td:eq(4)").text()
            if not postDate:
                break
            if postDate < earliestTime:
                page = -1#终止条件
                break
            if checkNewsUrl(cursor, newsUrl):
                page = -1#终止条件
                break           
            newspq = PyQuery(newsUrl)
            content = newspq("table:eq(6)").text()

            executeSql(cursor, title, postDate, content, airport, source, newsUrl)
            pass
        conn.commit()
        conn.close()
        pass
    pass

def nanfanghangkong():
    """南方航空
还有一定的问题"""
    airport = "南方航空"
    
    year = int(time.strftime("%Y"))
    match = re.compile(r"[\d]{4}-[\d]{2}-[\d]{2}")
    
    conn, cursor = mysqlDataBase_init()
    while str(year) >= earliestTime[:4]:
        newsBasePage = "http://www.csair.com/cn/aboutcsn/04/newsCompany/%s/list_news_%s.asp" % (year, year)
        sourceFile = urllib.urlopen(newsBasePage).read()
        pq = PyQuery(unicode(sourceFile, "utf8"))
        liindex = 0
        while True:
            li = pq("li:eq(%s)" % liindex)
            liindex += 1
            a = li("a")
            newsUrl = a.attr("href")
            if not newsUrl:
                break
            newsUrl = "http://www.csair.com" + newsUrl    
            postDate = match.search(li.text()).group()
            if postDate < earliestTime:
                return
            if checkNewsUrl(cursor, newsUrl):
                return             
            newspq = PyQuery(newsUrl) 
            content = newspq("#content").text()       
            title = content.split()[2]
            source = ""    
            if content.find("InstanceBeginEditable") != -1:
                content = content[len("InstanceBeginEditable name=\"con\" "):]
            if content.find("InstanceEndEditable") != -1:
                content = content[:-len("InstanceEndEditable")]

            executeSql(cursor, title, postDate, content, airport, source, newsUrl)
            pass
        year -= 1
        pass
    conn.commit()
    conn.close()
    pass

def chunqiuhangkongwang():
    """春秋航空网"""
    airport = "春秋航空"
    source = "春秋航空网"
    newsBasePageUrl = "http://info.china-sss.com/article_ps.do?xsl=topic_first_ilist_e&pt=1&ps=30&pi=%s&sf=seq_num&sfdt=number&st=ascending&cid=8069761218&bid=1&qtopic=3&qtit=&qct1=&qct2=#"

    conn, cursor = mysqlDataBase_init()
    sourceMatch = re.compile(r"来源：.*\[")
    page = 1
    while page != -1:
        newsPageUrl = newsBasePageUrl % page       
        page += 1
        
        sourceFile = urllib.urlopen(newsPageUrl).read() 
        pq = PyQuery(unicode(sourceFile, "gbk"))
        i = 0
        while True:
            a = pq("a:eq(%s)" % i)
            tr = pq("td#container1 tr:eq(%s)" % i)
            i += 1
            
            newsUrl = a.attr("href")
            if not newsUrl:
                break
            if len(newsUrl) == 1:
                continue    
            title = a.text()       
            newsUrl = "http://info.china-sss.com" + newsUrl
            postDate = "20" + tr("td:eq(2)").text()
            if postDate < earliestTime:
                page = -1
                break    
            if checkNewsUrl(cursor, newsUrl):
                page = -1
                break  
            sourceFile = urllib.urlopen(newsUrl).read()
            newspq = PyQuery(unicode(sourceFile, "gbk"))
            content = newspq("td.fontmain1").text()
            if content.find("function showContent()") != -1:
                content = content[:content.find("function showContent()")]
            
            sourceInContent = sourceMatch.search(content)
            if sourceInContent:
                source = sourceInContent.group()[len("来源："):-1]
            if source.find("】") != -1:
                source = source[:-len("】")]
            content = content.replace("[/链接]", "")
            if content.find("[链接 地址=") != -1:
                content = content[:-content.find("[链接 地址=")]        
            
            executeSql(cursor, title, postDate, content, airport, source, newsUrl)
            #恢复默认
            source = "春秋航空网"
            pass 
        pass
    conn.commit()
    conn.close()
    pass


def minhangziyuanwang():
    source = "民航资源网"
    matchPostDate = re.compile(r"[\d]+-[\d]+-[\d]+")
    conn, cursor = mysqlDataBase_init()
    
    au = ["天津航空", "吉祥航空", "厦门航空", "四川航空", "中国航空"]

    for airport in au:
        url = "http://news.carnoc.com/search.jsp?key=%s&querytype=0&page=1"    
        url = url % (urllib.quote(airport.decode("utf8").encode("gb2312")))

        sourceFile = urllib.urlopen(url).read()
        pq = PyQuery(sourceFile)
        index = 0
        while True:
            li = pq("div.text li:eq(%s)" % index)
            index += 1
            if not li:
                break
            newsUrl = li("a").attr("href")

            title = li("a").text()
            match = matchPostDate.search(li.text())
            if not match:
                continue
            postDate = match.group()
            if newsUrl.find("2000") != -1:
                continue
            if checkNewsUrl(cursor, newsUrl):
                break
            
            sourceFile = urllib.urlopen(newsUrl).read()
            newspq = PyQuery(sourceFile)
            content = newspq("div#newstext").text()
            
            if not content:
                continue
            if len(content) < 100:
                continue
            executeSql(cursor, title, postDate, content, airport, source, newsUrl)
            pass 
    
    #恢复默认   
    source = ""
    conn.commit()
    conn.close()
    pass

def crawl_news():
    startTime = time.time()
    #最总版本使用的
    import jipiao
    threads = []
    #添加线程
    threads.append(threading.Thread(target=minhangziyuanwang))
    for f in dir(jipiao):
        if f.find("hangkong") != -1:
            exec("threads.append(threading.Thread(target=%s))" % f)
    #启动线程
    for t in threads:
        t.start()
    #等待线程结束
    for t in threads:
        t.join()
        
    printf((time.time() - startTime))    
    pass

if __name__ == "__main__":
    startTime = time.time()
    crawl_news()
    
    #写日志
    if not os.path.exists("logs"):
        os.mkdir("logs")
    logs = file("./logs/logs.txt", "a")
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    logs.write("%s\t耗时:%sms\n" % (now, time.time() - startTime))
    logs.close()
    
    printf(time.time() - startTime)

#    chongqinghangkong()
#    chunqiuhangkong()
#    chunqiuhangkongwang()#春秋航空网
#    dongfanghangkong()
#    hannanhangkong()
#    jixianghangkong()
#    nanfanghangkong()
#    shandonghangkong()#ajax不易处理,使用post
#    shanghaihangkong()
#    shanghaihangkong_old()#上海航空旧版
#    shenzhenhangkong()
#    sichuanhangkong()
#    tianjinhangkong()
#    xiamenhangkong()
#    xingfuhangkong()
#    zhongguohangkong()
#    minhangziyuanwang()
