#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# #
#  Copyright (c) 2014 xxxx.com, Inc. All Rights Reserved
#
#
"""
:authors: wanghongrui
:create_date: 2011-1-23
:description:
"""

import pymysql
import time
import redis
import schedule
import logging
import pymongo
import gzip
import subprocess

from bson.objectid import string_type
from aduspider_web import settings
from geopy.distance import geodesic

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# 更新adulabel表
def adu_update_table(taskid, planid):
    """
    添加绑定任务
    """
    if planid.isdigit():
        try:
            client = pymysql.connect(host=settings.ADULB_MYSQL_HOST, user=settings.ADULB_MYSQL_USER,
                                     passwd=settings.ADULB_MYSQL_PASSWD, db=settings.ADULB_MYSQL_DBNAME,
                                     port=settings.ADULB_MYSQL_PORT)
            cur = client.cursor()
            # 查询符合条件的任务
            sql = "update {0} set adb_task_id='{1}', status=2 where id={2} and adb_task_id='' ".format(
                settings.ADULB_MYSQL_TABLE, str(taskid), str(planid))
            logger.info('Execute SQL statements:%s' % sql)
            cur.execute(sql)
            client.commit()
            cur.close()
            client.close()
        except Exception as e:
            logger.info('Updating SQL failed for error reasons:%s' % e)


# 查询msql中的任务
def get_taskid_for_mysql():
    """
    Query eligible tasks from MySQ
    :return:
    """
    try:
        client = pymysql.connect(host='', user='', passwd='',
                                 db='', port=, charset='utf8')
        cur = client.cursor()
        # 查询符合条件的任务
        sql = "SELECT taskid, formatname, coll_plan  FROM vehicle_datatask WHERE whereis ='redis'"
        cur.execute(sql)
        thumb = cur.fetchall()
        for td in thumb:
            get_redis_past_time(td)
        cur.close()
        client.close()
    except Exception as e:
        logger.info(e)


# 查询redis中key对应的过期时间
def get_redis_past_time(td):
    """
    get redis past time
    :param td:
    :return:
    """
    pool = redis.ConnectionPool(host=settings.REDIS_HOST, port=settings.REDIS_PORT,
                                password=settings.REDIS_PASSWORD, db=settings.REDIS_DATA_DB)
    r = redis.Redis(connection_pool=pool)
    past_time = r.ttl(td[1])
    if past_time < 60 * 60 * (24 * 2 + 12) and past_time != -2:
        try:
            x = r.get(td[1]).decode('utf8')
            distance = 0
            e_time = 0
            travel_time = 0
            pos = 0
            for l in eval(x):
                if pos == 0:
                    pos = (l[0], l[1])
                    travel_time = l[4]
                else:
                    cur_distance = geodesic((l[0], l[1]), pos,).m
                    c_time = l[4] - travel_time
                    if cur_distance < c_time * 36:
                        distance += cur_distance
                        e_time += c_time
                        travel_time = l[4]
                        pos = (l[0], l[1])
            obj_id = save_data_mongodb(gzip_data(x))

            if obj_id != 'error':
                distance = round(distance / 1000, 2)
                e_time = round(e_time / 3600, 2)
                sql_status = change_mileage(obj_id, e_time, distance, td[1])
                if sql_status != 'error':
                    r.delete(td[1])
                    adu_update_table(td[0], td[2])
        except Exception as e:
            logger.info(e)


# 连接mongodb，插入数据
def save_data_mongodb(datas):
    """
    save datas to mongodb
    :param datas:
    :return:
    """
    try:
        client = pymongo.MongoClient(settings.MONGO_DB_URI)
        db = client.aduspider.data
        jsonData = {'MapData': datas}
        object_id = db.insert(jsonData)
        object_id = string_type(object_id)
        client.close()
        return object_id
    except Exception as e:
        logger.info('Mongodb connection failed for the following reasons: {}'.format(e))
        return 'error'


# 使用gzip压缩数据
def gzip_data(datas):
    """
    Compressing data with gzip
    :param datas: 传入未压缩的数据
    :return: 返回压缩的数据
    """
    if not isinstance(datas, str):
        datas = str(datas)
    com_datas = gzip.compress(datas.encode('utf8'))
    return com_datas


# 更改sql表中对应任务的里程数
def change_mileage(formatname, coll_time, travel_mileage, rediskey):
    """
    Update SQL table corresponding data
    :param formatname:
    :param coll_time:
    :param travel_mileage:
    :param rediskey:
    :return:
    """
    try:
        client = pymysql.connect(host='', user='', passwd='',
                                 db='', port=, charset='utf8')
        cur = client.cursor()
        # 查询符合条件的任务
        sql = "update vehicle_datatask set whereis='mongodb', formatname='{0}', coll_time='{1}'," \
              "travel_mileage='{2}', end_mode='异常' where whereis='redis' and formatname='{3}'".format(formatname,
                                                        coll_time, travel_mileage, rediskey)
        cur.execute(sql)
        client.commit()
        logger.info('Executing SQL statements ：%s' % sql)
        cur.close()
        client.close()
        return 'ok'
    except Exception as e:
        logger.info(e)
        return 'error'


# 计算task的可用率
def calculating_availability():
    """
    Calculating Availability
    :return:
    """
    subprocess.Popen('source /home/aduspider/py2env/bin/activate && '
                     'python /home/aduspider/aduspider_web/Data_quality.py >> logs/data_quality.log 2>&1 &',
                     shell=True)


# 数据入库
def data_tolabel():
    """
    data_tolabel
    :return:
    """
    subprocess.Popen('source /home/aduspider/py2env/bin/activate && '
                     'python /home/aduspider/aduspider_web/Data_tolabel.py >> logs/data_tolabel.log 2>&1 &',
                     shell=True)


# 每隔一分钟执行一次
schedule.every(1).minutes.do(get_taskid_for_mysql)
# 每隔一小时执行一次
# schedule.every().hour.do(get_taskid_for_mysql)
schedule.every().hour.do(calculating_availability)
schedule.every().hour.do(data_tolabel)

while True:
    schedule.run_pending()
    time.sleep(1)


# https://blog.csdn.net/liao392781/article/details/80521194