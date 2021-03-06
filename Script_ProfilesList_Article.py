# -*- coding utf-8 -*-
from queue import Queue

__author__ = 'xiaxichen'
import requests, mysql.connector, time, json, threading
from TT_setting import *


def extract_meta(data, keyword):
    item = {}
    try:
        item["createTime"] = data["create_time"]
        item["weiboUrl"] = data["source_url"]
        item["status"] = data['user_type']
        try:
            item["user_id"] = data["media_id"]
        except:
            item["user_id"] = ""
        item["media_id"] = data["user_id"]
        item["name"] = data["name"]
        try:
            item["description"] = data["user_auth_info"]["auth_info"]
        except Exception as e:
            item["description"] = ""
        try:
            item["follow_Num"] = data["follow_count"]
        except:
            item["follow_Num"] = 0
        item["data"] = data
        item_other = {}
        item_other["media_name"] = item["name"]
        item_other["keyswords"] = keyword
        item_other["summary"] = item["description"]
        item_other["images"] = [data["avatar_url"], ]
        item["meta"] = json.dumps(item_other, ensure_ascii=False)
        return item
    except Exception as e:
        print(e)
        print(data)
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
            self.conn.ping(True)
            current_ts = time.time() - BETWEEN_TIME
            set_ts = current_ts + 600  # 这个600就是表示如果出问题，600秒后会重试
            args = (current_ts, set_ts, '', '')
            rst = self.cursor.callproc('get_keyword_profile', args)
            self.conn.commit()
            _, _, keyword, id = rst
            row = (keyword, id)
            return row

    def get_search_sql(self, userId):
        search_sql = """
        select count(*) from user_profiles where user_id=%s 
        """
        params = (userId,)
        with self.mutex:
            self.conn.ping(True)
            self.cursor.execute(search_sql, params)
            result = self.cursor.fetchall()[0][0]
        if result == 0:
            return True
        else:
            return False

    def get_insert_sql(self, item):
        weiboUrl = item["weiboUrl"]
        crawl_timestamp = int(time.time())
        crawl_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(crawl_timestamp))
        create_timestamp = int(item["createTime"])
        create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(create_timestamp))
        data = json.dumps(item["data"], ensure_ascii=False)
        meta = item["meta"]
        user_Id = item["user_id"]
        if item["status"] == 1:
            status = 0
        else:
            status = 1
        media_id = item["media_id"]
        name = item["name"]
        description = item["description"]
        follow_Num = item["follow_Num"]
        params = (
            weiboUrl, crawl_timestamp, crawl_time, create_time, create_timestamp, status, media_id, name,
            description, follow_Num, data, meta, user_Id)
        insert_sql = """
                    insert into user_profiles(url, crawl_timestamp, crawl_time, create_time, create_timestamp, status, media_id, name, description,
        follow_num,raw,meta,user_id)values(%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,COMPRESS(%s),COMPRESS(%s),%s)
                    ON DUPLICATE KEY UPDATE crawl_time=VALUES(crawl_time), crawl_timestamp=VALUES(crawl_timestamp), status=VALUES(status),name=VALUES(name),
                    description=VALUES(description),follow_num=VALUES(follow_num),raw=values(raw),meta=values(meta);
                    """
        with self.mutex:
            self.conn.ping(True)
            self.cursor.execute(insert_sql, params)
            self.conn.commit()

    def retry_lose(self, id, pageNum, keyword):
        with self.mutex:
            data = (id,)
            search_sql = "select retry from key_word where id=%s"
            self.cursor.execute(search_sql, data)
            retry = self.cursor.fetchall()[0][0]
            data = (retry + 1, id)
            update_sql = "update key_word set retry=%s where id=%s"
            self.cursor.execute(update_sql, data)
            self.conn.commit()
            print("爬取失败当前关键词为(%s) 页数%s 失败次数%s" % (keyword, pageNum, retry + 1), "\n", "当前关键词序号%s" % id)

    def update_over_ts(self, id):
        with self.mutex:
            update_sql = "update key_word set last_ts=%s,retry=0 where id=%s"
            data = (int(time.time()), id)
            self.conn.ping(True)
            self.cursor.execute(update_sql, data)
            self.conn.commit()

    def retry_update(self):
        with self.mutex:
            upd_sql = "update key_word set retry=0 WHERE retry>9"
            self.conn.ping(True)
            self.cursor.execute(upd_sql)
            self.conn.commit()

    def next_update(self, id):
        with self.mutex:
            data = (id,)
            update_sql = "update key_word set retry=0 where id=%s"
            self.conn.ping(True)
            self.cursor.execute(update_sql, data)
            self.conn.commit()


class Spider(threading.Thread):
    def __init__(self, headers, keyword, sem, db, detail_url_queue):
        super().__init__()
        self.headers = headers
        self.keyword = keyword[0]
        self.id = keyword[1]
        self.url = "https://lf.snssdk.com/api/search/content/"
        self.sem = sem
        self.db = db
        self.detail_url_queue = detail_url_queue
        self.search_id = ""
        self.pageNum_article = 0

    def run(self):
        pageNum = 0
        while True:
            is_over = self.get_url(pageNum)
            if is_over == 1:
                pageNum += 1
                self.pageNum_article += 10
                self.db.next_update(self.id)
                time.sleep(1)
            elif is_over == 0:
                self.db.retry_lose(self.id, pageNum, self.keyword)
                break
            elif is_over == 2:
                self.db.update_over_ts(self.id)
                break
        time.sleep(1)
        self.sem.release()

    def get_url(self, pageNum):
        data = {
        }
        if pageNum == 0:
            data["search_id"] = ""
        else:
            data["search_id"] = str(self.search_id)
        url = self.url
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
                print(e)
            # proxies = {}
            try:
                response = requests.get(url, headers=self.headers, params=data, timeout=TIME_OUT, proxies=proxies)
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
            asn = json.loads(response.text)
            message = asn["message"]
            if message == "success":
                try:
                    data_list = asn["data"]
                    if data_list:
                        for data in data_list:
                            if "abstract" in data:
                                try:
                                    try:
                                        user_id = data["media_creator_id"]
                                    except:
                                        user_id = data["user_id"]
                                    name = data["media_name"]
                                    on_db = self.db.get_search_sql(user_id)
                                    if on_db:
                                        self.get_profile_url(user_id, name)
                                except Exception as e:
                                    print(e)
                                    print(data)
                        if asn["has_more"] == 1:
                            self.search_id = asn["request_id"]
                            self.pageNum_article = self.pageNum_article
                            return 1
                        else:
                            return 2
                except Exception as e:
                    print(e)
                    return 0
                else:
                    print("当前关键词完成 关键词(%s) 页数%s" % (self.keyword, pageNum / 20), "\n", "当前关键词序号%s" % self.id)
                    return 2
            elif message == "from is too large":
                print("当前关键词完成 关键词(%s) 页数%s" % (self.keyword, pageNum / 20), "\n", "当前关键词序号%s" % self.id)
                return 2
            else:
                return 0
        else:
            return 0

    def get_profile_url(self, userId, name):
        url = "https://www.toutiao.com/search_content/?offset=%s&format=json&keyword=%s&autoload=true&count=20&cur_tab=4&from=media&pd=" % (
            0, name)
        headers = {
            'Host': 'www.toutiao.com',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv64.0) Gecko/20100101 Firefox/64.0',
            'Accept': 'application/json, text/javascript',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.toutiao.com/search/?keyword=%E7%81%AB%E7%AE%AD',
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Connection': 'Close'}
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
                response = requests.get(url, headers=headers, timeout=TIME_OUT, proxies=proxies)
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
        try:
            response = requests.get(url, headers=headers, timeout=TIME_OUT, proxies=proxies)
        except requests.exceptions.ReadTimeout:
            print("网页读取超时 当前关键词为%s,序号为%s\n重试开始重试剩余次数" % (self.keyword, self.id))
        except requests.exceptions.ConnectTimeout:
            print("网页连接超时 当前关键词为%s,序号为%s\n重试开始重试剩余次数" % (self.keyword, self.id))
        except Exception as E:
            print(E)
            return 0
        if RETRY > 0:
            asn = json.loads(response.text)
            message = asn["message"]
            if message == "success":
                try:
                    data_list = asn["data"]
                    if data_list:
                        for data in data_list:
                            try:
                                data_userId = data['user_id']
                                if int(userId) == int(data_userId):
                                    item = extract_meta(data, self.keyword)
                                    if item == 0:
                                        pass
                                    else:
                                        self.db.get_insert_sql(item)
                                        print("录入用户%s userId %s" % (name, userId))
                                    break
                            except:
                                pass

                except:
                    pass


def worker():
    pass


if __name__ == '__main__':
    # 5为连接池里的最少连接数，setsession=['SET AUTOCOMMIT = 1']是用来设置线程池是否打开自动更新的配置，0为False，1为True
    # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
    now_time = int(time.time())
    detail_url_queue = Queue(maxsize=10)
    db = StreamDB()
    sem = threading.Semaphore(CONCURRENT_REQUESTS_PROFILES_LIST)
    while True:
        keyword = db.get_keyword()
        if keyword[0]:
            sem.acquire()
            Spider_crawl = Spider(headers=HEADERS_PROFILE_LIST, keyword=keyword, sem=sem, db=db,
                                  detail_url_queue=detail_url_queue)
            Spider_crawl.setDaemon(False)
            Spider_crawl.start()
            # Spider_crawl.join()
        else:
            print("用户列表爬虫完成时间 耗时 %s 秒" % (time.time() - now_time))
            now_time = int(time.time())
            db.retry_update()
            time.sleep(60)
