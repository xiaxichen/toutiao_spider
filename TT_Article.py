# -*- coding utf-8 -*-
__author__ = 'xiaxichen'

import requests, mysql.connector, time, json, threading, copy
from queue import Queue
from urllib import parse
from other import strip_tags
from TT_setting import *


def extract_meta(text, userId, articleId, others):
    item = {}
    try:
        data = json.loads(text)["data"]
    except Exception as e:
        print(e)
        print(text)
    try:
        item["url"] = "https://www.toutiao.com/a%s/" % articleId
        item["create_timestamp"] = data['publish_time']
        item["create_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(item["create_timestamp"]))
        item["commentsCnt"] = others[0]
        item["title"] = data["title"]
        item["raw"] = text
        item["status"] = 1
        item["pageNum"] = others[3]
        item["media_id"] = userId
        item["article_id"] = articleId
        item["content"] = strip_tags.strip_tags(parse.unquote(data["content"]))
        item_other = {}
        item_other["title"] = item["title"]
        item_other["comment_num"] = data["comment_count"]
        item_other["content"] = item["content"]
        try:
            item_other["images"] = data['large_image']
        except:
            item_other["images"] = ""
        item_other["author"] = others[1]
        item["meta"] = json.dumps(item_other, ensure_ascii=False)
        return item
    except Exception as e:
        print(e)
        print(data)
        return 0


def extract_meta_weibo(asn_content, userId, keyword):
    item = {}
    try:
        item["url"] = asn_content["share_url"]
        item["title"] = asn_content["abstract"]
        item["raw"] = asn_content
        if "create_time" in asn_content:
            item["create_timestamp"] = asn_content["create_time"]
        else:
            item["create_timestamp"] = asn_content["behot_time"]
        item["create_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(item["create_timestamp"]))
        item["status"] = 1
        item["media_id"] = userId
        try:
            item["article_id"] = asn_content["id"]
        except:
            item["article_id"] = asn_content['thread_id']
        item_other = {}
        item_other["title"] = asn_content["abstract"]
        try:
            item_other["content"] = asn_content["content"]
        except:
            item_other["content"] = ""
        item_other["article_id"] = item["article_id"]
        item_other["media_name"] = asn_content["verified_content"]
        item_other["article_keyword"] = keyword
        item_other["like_num"] = asn_content['digg_count']
        item_other["comment_num"] = asn_content['comment_count']
        try:
            item_other["share_num"] = asn_content['default_text_line']
        except:
            item_other["share_num"] = 0
        images = []
        thumbnails = []
        for image_dict in asn_content['large_image_list']:
            images.append(image_dict["url"])
            thumbnails.append(image_dict["url_list"])
        item_other["images"] = str(images)
        item_other["thumbnails"] = str(thumbnails)
        item["meta"] = json.dumps(item_other, ensure_ascii=False)
        return item
    except Exception as e:
        print(e)
        print(asn_content)
        return 0


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
            current_ts = time.time() - ARTICLE_BETWEEN_TIME
            set_ts = current_ts + 600  # 这个600就是表示如果出问题，600秒后会重试
            args = (current_ts, set_ts, '', '', '', '')
            self.conn.ping(True)
            rst = self.cursor.callproc('get_keyword', args)
            self.conn.commit()
            _, _, url, media_id, name, pageNum = rst
            parms = (url, media_id, name, pageNum)
            return parms

    def get_install_sql(self, item):
        try:
            url = item["url"]
            crawl_timestamp = int(time.time())
            crawl_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(crawl_timestamp))
            create_timestamp = int(item["create_timestamp"])
            create_time = item["create_time"]
            raw = item["raw"]
            status = item["status"]
            media_id = item["media_id"]
            article_id = item["article_id"]
            meta = item["meta"]
            parms = (
                url, crawl_timestamp, crawl_time, create_timestamp, create_time, raw, status, media_id, article_id,
                meta)
            insert_sql = """
                        insert into articles(url, crawl_timestamp, crawl_time, create_timestamp, create_time, raw, status,
                        media_id, article_id, meta)values(%s, %s, %s, %s, %s, COMPRESS(%s), %s, %s, %s, COMPRESS(%s))
                        ON DUPLICATE KEY UPDATE url=VALUES(url), crawl_timestamp=VALUES(crawl_timestamp), crawl_time=VALUES(crawl_time),
                        meta=values(meta);
                        """
            with self.mutex:
                self.conn.ping(True)
                self.cursor.execute(insert_sql, parms)
                self.conn.commit()
        except Exception as e:
            print(e)
            return None

    def get_install_sql_weibo(self, item):
        try:
            url = item["url"]
            crawl_timestamp = int(time.time())
            crawl_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(crawl_timestamp))
            create_timestamp = int(item["create_timestamp"])
            create_time = item["create_time"]
            raw = item["raw"]
            status = item["status"]
            media_id = item["media_id"]
            article_id = item["article_id"]
            meta = item["meta"]
            parms = (
                url, crawl_timestamp, crawl_time, create_timestamp, create_time, raw, status, media_id, article_id,
                meta)
            insert_sql = """
                        insert into articles(url, crawl_timestamp, crawl_time, create_timestamp, create_time, raw, status,
                        media_id, article_id, meta)values(%s, %s, %s, %s, %s, COMPRESS(%s), %s, %s, %s, COMPRESS(%s))
                        ON DUPLICATE KEY UPDATE url=VALUES(url), crawl_timestamp=VALUES(crawl_timestamp), crawl_time=VALUES(crawl_time),
                        meta=values(meta);
                        """
            with self.mutex:
                self.conn.ping(True)
                self.cursor.execute(insert_sql, parms)
                self.conn.commit()
        except Exception as e:
            print(e)
            return None

    def retry_lose(self, id, pageNum, keyword):
        with self.mutex:
            self.conn.ping(True)
            data = (id,)
            search_sql = "select retry from user_profiles where media_id=%s"
            self.cursor.execute(search_sql, data)
            retry = self.cursor.fetchall()[0][0]
            data = (retry + 1, id)
            update_sql = "update user_profiles set retry=%s where media_id=%s"
            self.cursor.execute(update_sql, data)
            self.conn.commit()
            print("爬取失败当前用户为(%s) 页数%s 失败次数%s" % (keyword, pageNum, retry + 1), "\n", "当前关用户id%s" % id)

    def update_over_ts(self, id):
        with self.mutex:
            update_sql = "update user_profiles set last_ts=%s,retry=0,page_num=0 where media_id=%s"
            data = (int(time.time()), id)
            self.conn.ping(True)
            self.cursor.execute(update_sql, data)
            self.conn.commit()

    def retry_update(self):
        with self.mutex:
            upd_sql = "update user_profiles set retry=0 WHERE retry>9"
            self.conn.ping(True)
            self.cursor.execute(upd_sql)
            self.conn.commit()

    def next_update(self, id):
        with self.mutex:
            data = (id,)
            update_sql = "update user_profiles set retry=0 where media_id=%s"
            self.conn.ping(True)
            self.cursor.execute(update_sql, data)
            self.conn.commit()

    def insert_lost_html(self, url, userId, articleId, pageNum, keyword):
        search_sql = "select article_id from article_lose where article_id=%s and page_num=%s"
        data = (articleId, pageNum)
        try:
            with self.mutex:
                self.cursor.execute(search_sql, data)
                a = self.cursor.fetchall()
                if a:
                    print("获取网页失败,已存在article_lose表中")
                else:
                    parms = (
                        url, userId, articleId, pageNum, keyword)
                    insert_sql = """
                                insert into article_lose(url,media_id, article_id,page_num,keyword)values(%s,%s,%s,%s,%s)
                                """
                    self.cursor.execute(insert_sql, parms)
                    self.conn.commit()
                    print("获取网页失败加入article_lose表中")
        except Exception as e:
            print(e)


class Spider(threading.Thread):
    def __init__(self, headers, keyword, sem, db, detail_url_queue, ip_ownership):
        super().__init__()
        self.headers = headers
        self.keyword = keyword[0]
        self.id = keyword[1]
        self.name = keyword[2]
        self.pageNum = keyword[3]
        self.detail_url_queue = detail_url_queue
        self.url = "https://lf.snssdk.com/api/feed/profile/v1/"
        self.sem = sem
        self.db = db
        self.ip_ownership = ip_ownership

    def run(self):
        while True:
            is_over = self.get_url(self.pageNum)
            if is_over == 1:
                self.db.next_update(self.id)
                time.sleep(1)
            elif is_over == 0:
                self.db.retry_lose(self.id, self.pageNum, self.name)
                break
            elif is_over == 2:
                print("当前用用户完成 用户Id(%s) 页数%s" % (self.name, self.pageNum), "\n",
                      "当前当前用用户Id%s" % self.id)
                self.db.update_over_ts(self.id)
                break
        time.sleep(1)
        self.sem.release()

    def get_url(self, pageNum):
        try:
            RETRY = RETRY_NUM
        except Exception as e:
            print("RETRY_NUM 未设置 已采用默认值3")
            RETRY = 3
        while True:
            try:
                proxy_dict = json.loads(requests.get(PROXY_URL_OWNERSHIP % self.ip_ownership, timeout=3).text)
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
                headers = copy.copy(self.headers)
                headers['X-SS-REQ-TICKET'] = str(int(time.time() * 1000))
                headers['X-Khronos'] = str(int(time.time()))
                url = self.url
                response = requests.get(url, headers=headers, params=data, timeout=TIME_OUT, proxies=proxies)
                asn = json.loads(response.text)
                break
            except requests.exceptions.ReadTimeout:
                RETRY -= 1
                print("网页读取超时 当前关键词为%s,序号为%s\n重试开始重试剩余次数%s" % (self.keyword, self.id, RETRY))
            except requests.exceptions.ConnectTimeout:
                RETRY -= 1
                print("网页连接超时 当前关键词为%s,序号为%s\n重试开始重试剩余次数%s" % (self.keyword, self.id, RETRY))
            except Exception as E:
                print(E)
                RETRY -= 1
            if RETRY <= 0:
                break
        if RETRY > 0:
            message = asn["message"]
            if message == "success":
                self.pageNum = asn["offset"]
                if asn["data"]:
                    data_list = asn["data"]
                    for data in data_list:
                        try:
                            asn_content = json.loads(data['content'])
                            try:
                                if "Abstract" in asn_content and "app_download" not in asn_content:
                                    now_time = int(time.time() - 172800)
                                    if asn_content['publish_time'] > now_time:
                                        summary = asn_content["Abstract"]
                                        article_id = asn_content["id"]
                                        media_name = asn_content['ugc_recommend']['reason']
                                        article_url = asn_content["article_url"]
                                        parms = (
                                            article_id, self.id,
                                            (summary, media_name, article_url, pageNum, self.keyword))
                                        self.detail_url_queue.put(parms)
                                    else:
                                        return 2
                            except Exception as e:
                                print(e)
                                # print(asn_content)
                            # else:
                            #     try:
                            #         now_time = int(time.time() - 172800)
                            #         try:
                            #             create_time = asn_content['create_time']
                            #         except:
                            #             create_time = asn_content['publish_time']
                            #         if create_time > now_time:
                            #             item = extract_meta_weibo(asn_content, self.id, self.keyword)
                            #
                            #         else:
                            #             return 2
                            #         if item == 0:
                            #             return 0
                            #         else:
                            #             self.db.get_install_sql_weibo(item)
                            #     except Exception as e:
                            #         print(e)
                            #         # print(asn_content)
                        except Exception as e:
                            print(e)
                            return 0
                    try:
                        if asn["has_more"] is True:
                            self.pageNum = asn["offset"]
                            return 1
                        else:
                            return 2
                    except Exception as e:
                        print(e)
                        return 0
                else:
                    return 2
            else:
                return 0
        else:
            return 0


class Spider_Html(threading.Thread):
    def __init__(self, headers, detail_url_queue, db):
        super().__init__()
        self.headers = headers
        self.detail_url_queue = detail_url_queue
        self.db = db
        self.url = "https://a3.pstatp.com/article/full/lite/14/1/%s/%s/2/0/"

    def run(self):
        while True:
            parms = self.detail_url_queue.get()
            articleId = parms[0]
            userId = parms[1]
            others = parms[2]
            pageNum = others[3]
            keyword = others[4]
            url = others[2]
            is_over = self.get_url(userId, articleId, others)
            if is_over == 2:
                print("完成爬取文章 文章Id为%s 用户Id为%s" % (articleId, userId))
            if is_over == 0:
                self.db.insert_lost_html(url, userId, articleId, pageNum, keyword)
            time.sleep(1)

    def get_url(self, userId, articleId, others):
        # RETRY = RETRY_NUM
        try:
            RETRY = RETRY_NUM
        except Exception as e:
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
                RETRY -= 1
            # proxies = {}
            try:
                data = {
                }
                url = self.url % (articleId, articleId)
                headers = copy.copy(self.headers)
                headers["X-SS-REQ-TICKET"] = str(int(time.time() * 1000))
                response = requests.get(url, headers=headers, params=data, timeout=TIME_OUT, proxies=proxies)
                break
            except requests.exceptions.ReadTimeout:
                RETRY -= 1
                print("网页读取超时 当前用户为%s,文章为%s\n重试开始重试剩余次数%s 网页链接%s" % (userId, articleId, RETRY, url))
            except requests.exceptions.ConnectTimeout:
                RETRY -= 1
                print("网页连接超时 当前用户为%s,文章为%s\n重试开始重试剩余次数%s 网页链接%s" % (userId, articleId, RETRY, url))
            except Exception as e:
                print(e)
                print("获取网页失败")
                RETRY -= 1
            if RETRY < 0:
                return 0
        if RETRY > 0:
            text = json.dumps(json.loads(response.text), ensure_ascii=False)
            item = extract_meta(text, userId, articleId, others)
            if item == 0:
                return 0
            else:
                try:
                    self.db.get_install_sql(item)
                except Exception as e:
                    return 0
                return 2
        else:
            return 0


if __name__ == '__main__':
    now_time = int(time.time())
    detail_url_queue = Queue(maxsize=1000)
    db = StreamDB()
    for i in range(CONCURRENT_REQUESTS_ARTICLE_HTML):
        Spider_Crawl_Html = Spider_Html(headers=HEADERS_ARTICLE, detail_url_queue=detail_url_queue, db=db)
        Spider_Crawl_Html.setDaemon(False)
        Spider_Crawl_Html.start()
    sem = threading.Semaphore(CONCURRENT_REQUESTS_ARTICLE)
    ip_ownership = 1
    while True:
        keyword = db.get_keyword()
        if keyword[0]:
            if ip_ownership >= CONCURRENT_REQUESTS_PROFILES_PROFILE:
                ip_ownership = 1
            sem.acquire()
            Spider_crawl = Spider(headers=HEADERS_ARTICLE_LIST, keyword=keyword, sem=sem, db=db,
                                  detail_url_queue=detail_url_queue, ip_ownership=ip_ownership)
            Spider_crawl.setDaemon(False)
            Spider_crawl.start()
            ip_ownership += 1
            # Spider_crawl.join()
        else:
            print("用户列表爬虫完成时间 耗时 %s 秒" % (time.time() - now_time))
            now_time = int(time.time())
            db.retry_update()
            time.sleep(60)
