#!/usr/bin/python2.7
#-*-coding:utf-8-*-

from tag_base_recommendation import recommendation
import MySQLdb

CONN = MySQLdb.connect(host='localhost', user='newSpider', passwd='123456',
                           db='usersPool', charset='utf8', use_unicode=True)
CURSOR = CONN.cursor()

def main(table_name, cursor):
    sql_read_scan = 'select user_id from user_read'
    cursor.execute(sql_read_scan)
    user_list = [x[0] for x in cursor.fetchall()]

    for user_ID in user_list:
        if existsTest(user_ID, table_name, cursor):
            updateRecommen(user_ID, table_name, cursor)
        else:
            insertRecommen(user_ID, table_name, cursor)
            updateRecommen(user_ID, table_name, cursor)

def existsTest(user_id, table_name, cursor):
    sql_user_exists = 'select time from %s where user_id=\'%s\'' % (table_name, user_id)
    if cursor.execute(sql_user_exists) != None:
        return True
    else:
        return False

def insertRecommen(user_id, table_name, cursor):
    sql_insert = "insert into %s (user_id, news) values(\'%s\', \'\')" % (table_name, user_id)
    cursor.execute(sql_insert)

def updateRecommen(user_id, table_name, cursor, N_recommen=5):
    news_recommen_list = recommendation(user_id, table_name, N_recommen)
    news_recommen_str = ','.join(news_recommen_list)
    sql_update = "update %s set news=\'%s\' where user_id=\'%s\'" % (table_name, news_recommen_str, user_id)
    cursor.execute(sql_update)

if __name__ == '__main__':
    main('fx_recomm', CURSOR)
    main('wallstreet_recomm', CURSOR)
