#coding:utf8
import requests
from requests.exceptions import RequestException,ProxyError
import re
import json
from multiprocessing import Pool
import time
import pymongo
from pyquery import PyQuery as pq


MONGO_URL='localhost'
MONGO_DB='豆瓣电影'
MONGO_TABLE='黄晓明短评1'

client = pymongo.MongoClient(MONGO_URL, connect=False)
db = client[MONGO_DB]




#def test_proxy():


headers = {
'Host': 'movie.douban.com',
'Connection': 'keep-alive',
'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
#'User-Agent':'Baiduspider+',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'zh-CN,zh;q=0.9',
'Cookie': 'll="118162"; bid=RNuK8oASKq4; __yadk_uid=FJheMmH1hXbXJn6Ta0xgRCRrZtwFr3eR; ps=y; ap=1; __utmz=30149280.1513067045.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmz=223695111.1513086177.3.2.utmcsr=accounts.douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/login; push_noty_num=0; push_doumail_num=0; _vwo_uuid_v2=13AA31808F64A19471155E62A0969DD6|84a577a1ede4556bf6cee44bc67fffd7; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1514298853%2C%22https%3A%2F%2Faccounts.douban.com%2Flogin%3Falias%3D17707943066%26redir%3Dhttps%253A%252F%252Fmovie.douban.com%252F%26source%3DNone%26error%3D1013%22%5D; _pk_ses.100001.4cf6=*; __utma=30149280.1778443675.1513067045.1513086132.1514298853.4; __utmb=30149280.0.10.1514298853; __utmc=30149280; __utma=223695111.1870772461.1513067045.1513086177.1514298853.4; __utmb=223695111.0.10.1514298853; __utmc=223695111; as="https://movie.douban.com/"; dbcl2="161299904:koJiGKYhxOE"; ck=siNR; _pk_id.100001.4cf6=a461103e9e201478.1513067041.4.1514298869.1513086746.; report=ref=%2F&from=mv_a_pst'
}#通过requests方法获取网页内容




def get_request(url):
    try:
        response=requests.get(url)
      #  print(response.text)
        if response.status_code==200:
            return(response.text)
        else:
            return None
    except requests.RequestException:
        return None


def parse_actor_movie_page(content):
    doc=pq(content)
    content=doc('.grid_view .sortby')
    sibling=content.siblings()
#    print(str(sibling))
    guize=re.compile('<li.*?<a.*?href="(.*?)">.*?title="(.*?)".*?</a>.*?<dl>.*?<dd>(.*?)</dd>.*?<dd>(.*?)</dd>\s*?</dl>.*?<span>(.*?)</span>.*?</li>',re.S)
    items=re.findall(guize,str(sibling))
    print(items)
    for item in items:
        print('url%s',item[0])
        print('name%s',item[1])
        print('导演%s',item[2])
        print('演员%s',item[3])
        print('评分%s',item[4])
        guize2=re.compile('https.*?subject/(.*?)/')
        urlId=re.findall(guize2,item[0])  #正则表达式匹配出的是是一个列表，其中匹配到的符合条件的重复项是放在元组中类似{(),(),(),()},需要遵循迭代规则获取其中的每一项圆组
        for Id in urlId:
            print(Id)
            yield{'url':item[0],'moviename':item[1],'导演':item[2],'演员':item[3],'评分':item[4],'Id':Id}

proxy_pool_url = 'http://120.78.81.105:8000/first'

#请求代理页
def get_proxy():
    try:
        response=requests.get(proxy_pool_url)
        print(response.status_code)
        if response.status_code==200:
            print(response.text)
            return response.text
        else:
            #time.sleep(9)
            return None
    except ConnectionError:
        return None



max_count=50

#请求网页返回请求返回值
def get_url(url,count=1):
    print('Crawling',url)
    print('Trying Count',count)
#    proxy="121.231.151.236:8888"
    proxy=get_proxy()
#    global  proxy
    if count>max_count:
        print('crawing too many times')
        return None
    try:
        if proxy:
            print('using proxy%s right now'%proxy)
            proxies = {'https': 'https://' + proxy}
            response = requests.get(url, allow_redirects=False, headers=headers, proxies=proxies)
        if response.status_code == 200:
            return response.text
        elif response.status_code == 302:
            #           time.sleep(4)
            # ip被封锁，需要使用代理
            print('302')
            proxy = get_proxy()
            if proxy:
                #                print(proxy)
                #               count+=1
                print('Using Proxy:', proxy)
                return get_url(url)
            else:
                print('Getting proxy Error')
                return None
        else:
            get_url(url)
    except ProxyError as e:  #代理无法使用说明正好adsl服务器在重新发送IP
        print('Error Occurred',e.args)
        time.sleep(5)   #停止5秒再获取代理
        proxy=get_proxy()
        count+=1
        return get_url(url,count)




#爬取详情页面的精彩评论
def get_detail(content):
    try:
        guize=re.compile('<div class="comment">.*?<span class="votes">(.*?)</span>.*?<p.*?>(.*?)\s*?</p>',re.S)
        information=re.findall(guize,content)
        if information:
            for info in information:

                yield({'点赞':info[0],'评论':info[1]})
    except ValueError:
        pass



#将爬取的信息保存到文件
def save_file(info):
    with open('黄晓明豆瓣所有电影短评.txt','a') as f:
    #    f.write('\n')
        f.write(json.dumps(info,ensure_ascii=False)+'\n')


def save_to_mongo(data):
    if db[MONGO_TABLE].insert(data):
        print('Successfully save to mongo')
        return True
    else:
        return False





def main(id,num,url,moviename,director,actor,pingfen):
    try:
        url='https://movie.douban.com/subject/'+str(id)+'/comments?start='+str(num)+'&limit=20&sort=new_score&status=P'
        print(url)
        print('爬取第%d页'%(num/20))
        content=get_url(url)
    #    print(content)
        for info in  get_detail(content):
            print(info)
 #           save_file(info)
            info['url']=url
            info['moviename']=moviename
            info['director']=director
            info['actor']=actor
            info['pingfen']=pingfen
            save_to_mongo(info)
    except ValueError:
        pass




if __name__=='__main__':
    for num in range(0,10):
        num=num*10;
        Actor_url = 'https://movie.douban.com/celebrity/1041404/movies?start='+str(num)+'&format=pic&sortby=vote&'
        ActorContent = get_request(Actor_url)
        movieInfo = parse_actor_movie_page(ActorContent)
        for movieId in movieInfo:
            id = movieId['Id']
            url = movieId['url']
            moviename = movieId['moviename']
            director = movieId['导演']
            actor = movieId['演员']
            pingfen=movieId['评分']
            for i in range(0, 25):
             #   if i==5:
            #        time.sleep(4)
                if i>9 and i%10==0: #每爬取10页评论休息2秒
                    time.sleep(2)
                num=i*20
                main(id,num,url,moviename,director,actor,pingfen)