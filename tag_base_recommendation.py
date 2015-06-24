#!/usr/bin/python2.7
# -*- coding:utf-8 -*-

import MySQLdb
import numpy as np
import time

# default settings
TAGS = """
    JPY_news,JPY_norm,JPY_analy,CHF_news,CHF_norm,CHF_analy,USD_news,USD_norm,USD_analy,
    EUR_news,EUR_norm,EUR_analy,GBP_news,GBP_norm,GBP_analy,AUD_news,AUD_norm,AUD_analy,
    CAD_news,CAD_norm,CAD_analy,RMB_news,RMB_norm,RMB_analy,gold,silver,crude,bond
    """
MIN_READ = 5.0
MAX_READ = 300.0
USERS_DB = 'usersPool'
NEWS_DB = 'newsPool'

def user_portrait(user_ID, tags=TAGS, min_read=MIN_READ, max_read=MAX_READ,
                  users_db=USERS_DB):
    conn = MySQLdb.connect(host='localhost', user='newSpider', passwd='123456',
                           db=users_db, charset='utf8', use_unicode=True)
    cursor = conn.cursor()
    sql = "select %s from user_read where user_id=\'%s\'" % (tags, user_ID)
    cursor.execute(sql)
    dataset = cursor.fetchone() #read time under every tag
    cursor.close()
    conn.close()
    dataset_min_max_norm = []
    for data in dataset:
        if data > max_read:
            dataset_min_max_norm.append(1)
        elif data < min_read:
            dataset_min_max_norm.append(0)
        else:
            dataset_min_max_norm.append((data-min_read)/max_read)
    return np.array(dataset_min_max_norm)


def news_user_attraction(user_ID, news_ID, news_table, tags=TAGS, news_db=NEWS_DB):
    conn = MySQLdb.connect(host='localhost', user='newSpider', passwd='123456',
                           db=news_db, charset='utf8', use_unicode=True)
    cursor = conn.cursor()
    sql = "select %s from %s where id=\'%s\'" % (tags, news_table, news_ID)
    cursor.execute(sql)
    data_news_tag = np.array(cursor.fetchone()) # tags that every news has
    cursor.close()
    conn.close()
    data_portrait = user_portrait(user_ID)
    tags_sum = data_news_tag.sum()
    if tags_sum != 0:
        attraction = np.dot(data_news_tag, data_portrait) / tags_sum
        return attraction
    else:
        return 0.5


def recommendation(user_ID, news_table, N_recommendation, tags=TAGS, news_db=NEWS_DB):
    conn = MySQLdb.connect(host='localhost', user='newSpider', passwd='123456',
                           db=news_db, charset='utf8', use_unicode=True)
    cursor = conn.cursor()
    time_current = int(time.time())
    time_7days_before = time_current - 604800
    sql1 = "select id from %s where time>%s and time<%s" % (news_table, time_7days_before, time_current)
    cursor.execute(sql1)
    news_list = [x[0] for x in cursor.fetchall()]
    attraction_list = []
    for news_ID in news_list:
        attraction = news_user_attraction(user_ID, news_ID, news_table)
        attraction_list.append((attraction, news_ID))
    attraction_list.sort(reverse=True)
    attraction_id_list = [id_x.encode('utf-8') for (attr_x, id_x) in attraction_list[:N_recommendation]]
    attraction_id_tuple = tuple(attraction_id_list)
    sql2 = "select title,time,content from %s where id in %s" % (news_table, attraction_id_tuple)
    print sql2
    cursor.execute(sql2)
    recommendation_tuple = cursor.fetchall()
    cursor.close()
    conn.closr()
    return recommendation_tuple


if __name__ == '__main__':
    c = recommendation('001', 'fx_news', 5)
#   c = recommendation('001', 'wallstreet_realtime_news', 5)

