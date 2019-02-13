import pymysql, time
from TT_setting import *

conn = pymysql.connect(host=MYSQL_HOST, port=3306, user=MYSQL_USER,
                       passwd=MYSQL_PASSWORD, db=MYSQL_DBNAME, charset='utf8')
cursor = conn.cursor()
search_sql = "select article_id from articles where crawl_timestamp<=%s" % (int(time.time()) - MYSQL_SAVE_TIME)
cursor.execute(search_sql)
row = cursor.fetchall()
if row:
    row_list = len(row)
    for i in row:
        artId = i[0]
        del_sql = "DELETE FROM article WHERE article_id=%s;" % artId
        cursor.execute(del_sql)
        del_sql = "DELETE FROM comments WHERE article_id=%s;" % artId
        cursor.execute(del_sql)
        conn.commit()
    print("当前时间%s 删除过期数据完成 删除文章%s条" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), row_list))
print("当前时间%s 没有数据删除" % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
