# -*- coding utf-8 -*-
__author__ = 'xiaxichen'

import requests, mysql.connector, time, json, threading, copy
from TT_setting import *
from DBUtils.PooledDB import PooledDB


# from concurrent import futures
# from queue import Queue
def extract_meta(raw):
    item = {}
    return json.dumps(item, ensure_ascii=False)


class StreamDB:
    def __init__(self):
        self.conn = mysql.connector.connect(host=MYSQL_HOST,
                                            user=MYSQL_USER,
                                            password=MYSQL_PASSWORD,
                                            database=MYSQL_DBNAME,
                                            charset=MYSQL_CHARSET, )
        self.cursor = self.conn.cursor()
        self.mutex = threading.Lock()

    def get_keyword(self):
        with self.mutex:
            current_ts = time.time() - COMMENT_BETWEEN_TIME
            set_ts = current_ts + 600  # 这个600就是表示如果出问题，600秒后会重试
            args = (current_ts, set_ts, '', '', '')
            self.conn.ping(True)
            rst = self.cursor.callproc('get_keyword_comment', args)
            self.conn.commit()
            _, _, artId, media_id, url = rst
            parms = (artId, media_id, url)
            return parms

    def get_insert_sql(self, item, pageNum):
        url = item["url"]
        crawl_timestamp = int(time.time())
        crawl_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(crawl_timestamp))
        raw = item["raw"]
        media_id = item["media_id"]
        article_id = item["article_id"]
        meta = item["meta"]
        params = (
            url, crawl_timestamp, crawl_time, media_id, article_id, pageNum, raw, meta)
        insert_sql = """
                    insert into comments(url, crawl_timestamp, crawl_time, media_id,article_id,page_num, raw, meta)values(%s, %s, %s, 
                    %s, %s, %s, COMPRESS(%s), COMPRESS(%s))
                    ON DUPLICATE KEY UPDATE crawl_time=VALUES(crawl_time),crawl_timestamp=VALUES(crawl_timestamp),raw=values(raw),
                    meta=values(meta);
                    """
        with self.mutex:
            self.conn.ping(True)
            self.cursor.execute(insert_sql, params)
            self.conn.commit()

    def retry_lose(self, artId, pageNum):
        search_sql = "select retry from articles where article_id=%s"
        data = (artId,)
        with self.mutex:
            self.conn.ping(True)
            self.cursor.execute(search_sql, data)
            retry = self.cursor.fetchall()
        retry = retry[0][0]
        update_sql = "update articles set retry=%s,page_num=%s where article_id=%s"
        data = (retry + 1, pageNum, artId)
        with self.mutex:
            self.conn.ping(True)
            self.cursor.execute(update_sql, data)
            self.conn.commit()
        print("爬取失败当前文章为(%s) 页数%s 失败次数%s" % (artId, pageNum, retry + 1))

    def update_over_ts(self, id):
        with self.mutex:
            update_sql = "update articles set last_ts=%s,retry=0 where id=%s"
            data = (int(time.time()), id)
            self.conn.ping(True)
            self.cursor.execute(update_sql, data)
            self.conn.commit()

    def retry_update(self):
        with self.mutex:
            upd_sql = "update articles set retry=0 WHERE retry>9"
            self.conn.ping(True)
            self.cursor.execute(upd_sql)
            self.conn.commit()

    def next_update(self, id):
        with self.mutex:
            data = (id,)
            update_sql = "update articles set retry=0 where id=%s"
            self.conn.ping(True)
            self.cursor.execute(update_sql, data)
            self.conn.commit()


class Spider(threading.Thread):
    def __init__(self, headers, artId, media_id, article_url, sem, db):
        super().__init__()
        self.headers = headers
        self.artId = artId
        self.media_id = media_id
        self.url = "https://lf.snssdk.com/article/v1/tab_comments/"
        self.article_url = article_url
        self.sem = sem
        self.db = db
        self.item = {}

    def run(self):
        # self.sem.acquire()
        pageNum = 1
        while True:
            is_over = self.get_url(pageNum)
            if is_over == 1:

                try:
                    self.db.get_insert_sql(self.item, pageNum)
                    pageNum += 1
                    time.sleep(1)
                except Exception as e:
                    print("插入失败！文章Id为%s 用户Id为%s" % (self.artId, self.media_id))
                    print(e)
                    self.db.retry_lose(pageNum)
                self.db.next_update(self.artId)
            elif is_over == 0:
                self.db.retry_lose(self.artId, pageNum)
                break
            elif is_over == 2:
                self.db.update_over_ts(self.artId)
                print("当前文章评论完成 文章Id(%s) 页数%s" % (self.artId, pageNum))
                break
        time.sleep(1)
        self.sem.release()

    def get_url(self, pageNum):
        try:
            RETRY = RETRY_NUM
        except:
            print("RETRY_NUM 未设置 已采用默认值3")
            RETRY = 3
        while True:
            try:
                proxy_dict = json.loads(requests.get(PROXY_URL, timeout=3).text)
                proxies = {
                    "http": "http" + "://" + str(
                        proxy_dict["data"]["host"]) + ":" +
                            str(proxy_dict["data"]["port"]),
                    "https": "http" + "://" + str(
                        proxy_dict["data"]["host"]) + ":" +
                             str(proxy_dict["data"]["port"]),
                }
            except Exception as e:
                proxies = {}
                print("代理获取失败")
                print(e)
            # proxies = {}
            try:
                data = {
                }
                url = self.url
                headers = copy.copy(self.headers)
                headers["X-SS-REQ-TICKET"] = str(int(time.time() * 1000))
                response = requests.get(url, headers=headers, params=data, timeout=TIME_OUT, proxies=proxies)
                break
            except requests.exceptions.ReadTimeout:
                RETRY -= 1
                print("网页读取超时 当前用户为%s,文章为%s\n重试开始重试剩余次数%s 网页链接%s" % (self.media_id, self.artId, RETRY, self.url))
            except requests.exceptions.ConnectTimeout:
                RETRY -= 1
                print("网页连接超时 当前用户为%s,文章为%s\n重试开始重试剩余次数%s 网页链接%s" % (self.media_id, self.artId, RETRY, self.url))
            except Exception as e:
                print(e)
                print("获取网页失败")
                return 0
            if RETRY < 0:
                return 0
        if RETRY > 0:
            try:
                asn = json.loads(response.text)
                if type(asn) == str:
                    asn = json.loads(asn)
            except Exception as e:
                print("json解析失败")
                print(e)
                self.db.retry_lose(pageNum)
                return 0
            try:
                message = asn["message"]
            except:
                self.db.retry_lose(pageNum)
                return 0
            if message == "success":
                data_list = asn["data"]
                if data_list:
                    self.item["raw"] = response.text
                    self.item["meta"] = extract_meta(data_list)
                    self.item["url"] = self.article_url
                    self.item["media_id"] = self.media_id
                    self.item["article_id"] = self.artId
                    if asn["has_more"] == True:
                        return 1
                    else:
                        return 2
                elif asn["has_more"] == False:
                    return 2
                else:
                    return 0
            else:
                return 0


if __name__ == '__main__':
    pool = PooledDB(mysql.connector, mincached=5, host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASSWORD,
                    db=MYSQL_DBNAME,
                    port=3306,
                    charset=MYSQL_CHARSET, setsession=['SET AUTOCOMMIT = 1'])
    # 5为连接池里的最少连接数，setsession=['SET AUTOCOMMIT = 1']是用来设置线程池是否打开自动更新的配置，0为False，1为True
    # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
    sem = threading.Semaphore(CONCURRENT_REQUESTS_COMMENT)
    now_time = int(time.time())
    db = StreamDB()
    while True:
        row = db.get_keyword()
        if row[0]:
            artId, media_id, article_url = row
            sem.acquire()
            Spider_crawl = Spider(headers=HEADERS_COMMENT, artId=artId, media_id=media_id, article_url=article_url,
                                  sem=sem, db=db)
            Spider_crawl.setDaemon(False)
            Spider_crawl.start()
        else:
            print("评论爬虫完成时间 耗时 %s 秒" % (time.time() - now_time))
            now_time = int(time.time())
            time.sleep(60)
