from selenium import webdriver ##메인 코드
import urllib.request
from urllib.parse import quote_plus
from urllib.request import HTTPError
from bs4 import BeautifulSoup
import time
import os
import pymysql
import boto3
from pyvirtualdisplay import Display
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.base import JobLookupError
import logging

log = logging.getLogger(__name__)
def job():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.headless = True
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(3)
    baseurl='https://www.venturesquare.net/'
    s3=boto3.client('s3')
    db=pymysql.connect(host='aivory-db-1-instance-1.cihordk6uhul.ap-northeast-2.rds.amazonaws.com',port=3306,user='assistance',passwd='ZzFqox6Ek#HmrH' , db='venture_crawler' ,charset='utf8')
    curs=db.cursor(pymysql.cursors.DictCursor)
    html1=urllib.request.urlopen(baseurl).read() #자동화 코드
    soup1=BeautifulSoup(html1,'html.parser')
    first=soup1.select('.item.large-item')
    b=[]
    for i in first:
        b.append(i.a['href'])
    BASE_DIR=os.path.dirname(os.path.abspath('/home/ubuntu/'))
    latest =b[0][30:37]

    with open(os.path.join(BASE_DIR,'latest.txt'),'r+') as f_read:
        before=f_read.readline()
        if before != latest:
            for i in range(before,latest):
                url=baseurl+str(i)
                try:
                        html=urllib.request.urlopen(url).read()
                except Exception as e:
                        log.exception(e)
                        continue
                try:
                        soup=BeautifulSoup(html,'html.parser')
                        driver.get(url)
                except Exception as e:
                        log.exception(e)
                else:
                        source="venturesquare"
                        originalid=str(i)
                try:
                        title=driver.find_element_by_css_selector('h1.post-title.item.fn').text
                        writer=str(driver.find_element_by_css_selector('span.reviewer').text)
                except:
                        log.exception(e)
                        continue
                else:
                        article=soup.select("article")
                        for j in article:
                                contentlen=len(j.find_all('p'))
                        content=""
                        for v in range(contentlen):
                                content+=str(j.find_all('p')[v])
                try:
                        created=modified=j.time.attrs['datetime']
                except Exception:
                        log.exception(e)
                else:
                    try:
                        photoa=j.select_one('.description').img['src']
                        Photor=j.select_one('.description').a['href']
                        imgurl=j.select_one('.description').img['src']
                        originalfilename='image{}.jpg'.format(str(i))
                        originalid1=str(i)
                        URL='https://ap-northeast-2.amazonaws.com/aivory-demo-babyitems.nerdfactory.ai/policy/'+quote_plus('image')+str(i)+'.jpg'
                        try:
                                sql1="insert into core_attach(url,original_filename,original_id) values(%s,%s,%s)"
                                val1=(URL,originalfilename,originalid1)
                                curs.execute(sql1,val1)
                        except:
                                log.exception(e)
                                continue
                        urllib.request.urlretrieve(imgurl,'/home/nerdfactory/Projects/venture_crawling/venv/img/'+'image'+str(i)+'.jpg')
                        filename='/home/nerdfactory/Projects/venture_crawling/venv/img/'+'image'+str(i)+'.jpg'
                        bucket_name='aivory-demo-babyitems.nerdfactory.ai'
                        s3.upload_file(filename,bucket_name,'policy/'+'image'+str(i)+'.jpg')
                except:
                        photoa=None
                        Photor=None
            try:
                    sql="insert into core_data(source,original_id,title,writer,content,created,modified,photo_reference) values(%s,%s,%s,%s,%s,%s,%s,%s)"
                    val=(source,originalid,title,writer,content,created,modified,Photor)
                    curs.execute(sql,val)
            except Exception:
                    log.exception(e)
            db.commit()
        db.close()
        f_read.close()

    with open(os.path.join(BASE_DIR,'latest.txt'),'w+') as f_write:
        f_write.write(latest)
        f_write.close()
sched =BackgroundScheduler()
sched.start()
sched.add_job(job,'cron',hour='01',minute='58',id='crawling_01')                                                                                                                                                                                                 89,1         바닥
