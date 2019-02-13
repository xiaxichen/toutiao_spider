# -*- coding utf-8 -*-
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
            rst = self.cursor.callproc('get_keyword_profile_article', args)
            self.conn.commit()
            _, _, keyword, id = rst
            row = (keyword, id)
            return row

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
                    description=VALUES(description),follow_num=VALUES(follow_num),raw=values(raw),meta=values(meta),user_id=values(user_id);
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
    def __init__(self, headers, keyword, sem, db):
        super().__init__()
        self.headers = headers
        self.keyword = keyword[0]
        self.id = keyword[1]
        self.url = "https://www.toutiao.com/search_content/"
        self.sem = sem
        self.db = db

    def run(self):
        pageNum = 0
        while True:
            is_over = self.get_url(pageNum)
            if is_over == 1:
                pageNum += 20
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
        url = self.url + "?offset=%s&format=json&keyword=%s&autoload=true&count=20&cur_tab=4&from=media&pd=" % (
            pageNum, self.keyword)
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
                response = requests.get(url, headers=self.headers, timeout=TIME_OUT, proxies=proxies)
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
                            item = extract_meta(data, self.keyword)
                            if item == 0:
                                return 0
                            else:
                                self.db.get_insert_sql(item)
                        return 1
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


def worker():
    pass


if __name__ == '__main__':
    # 5为连接池里的最少连接数，setsession=['SET AUTOCOMMIT = 1']是用来设置线程池是否打开自动更新的配置，0为False，1为True
    # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
    now_time = int(time.time())
    db = StreamDB()
    sem = threading.Semaphore(CONCURRENT_REQUESTS_PROFILES)
    while True:
        keyword = db.get_keyword()
        if keyword[0]:
            sem.acquire()
            Spider_crawl = Spider(headers=HEADERS_PROFILESLIST, keyword=keyword, sem=sem, db=db)
            Spider_crawl.setDaemon(False)
            Spider_crawl.start()
            # Spider_crawl.join()
        else:
            print("用户列表爬虫完成时间 耗时 %s 秒" % (time.time() - now_time))
            now_time = int(time.time())
            db.retry_update()
            time.sleep(60)
