#!/usr/bin/env python
# -*- coding: utf-8 -*
# #############################################################
#
#  Copyright (c) 2014 xxxx.com, Inc. All Rights Reserved
#
# #############################################################

"""
:authors: wanghongrui
:create_date: 2019-4-4
:description:
"""

import logging
import os
import sys
import re
import signal
import subprocess
import time
import ConfigParser
import requests
import pymysql

from itertools import groupby

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# 重写ConfigParser 因为源码中自动会将大写全部转为小写
class MyConfigParser(ConfigParser.ConfigParser):
    """
        ConfigParser
    """
    def __init__(self, defaults=None):
        ConfigParser.ConfigParser.__init__(self, defaults=defaults)

    def optionxform(self, optionstr):
        return optionstr


def with_ret_and_output(cmd, timeout=0):
    """
        with ret code and output, like:
        0, (stdout, stderr)
    """
    try:
        process = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except Exception as e:
        return 'error', e
    t_beginning = time.time()
    retcode = None
    output = ""
    err = ""
    while True:
        retcode = process.poll()
        if retcode is not None:
            break
        else:
            tmp_out, tmp_err = process.communicate()
            if tmp_out:
                output += tmp_out
            if tmp_err:
                err += tmp_err
        time.sleep(0.01)
    # print "with_ret_and_output: %s, %s, %s" % (retcode, output, err)
    return retcode, (output, err)


def with_output_or_exception(cmd, timeout=0):
    """
        with output when success, otherwise throw an exception
    """
    ret, out = with_ret_and_output(cmd, timeout)
    if ret:
        return 'error'
    return out[0]


# 执行mysql查询
def get_taskid_form_adulabel(host, port, user, passwd, db, sql):
    """
    select mysql
    :return:
    """
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)
    cursor = conn.cursor()
    cursor.execute(sql)
    # print cursor.fetchall()
    task_list = [['' if t is None else t for t in l] for l in cursor.fetchall()]
    cursor.close()
    conn.close()
    return task_list


# MKZ049 MKZ111 MKZ121 MKZ123指定分支
def designated_vehicle(ti, output):
    """
    designated vehicle
    :return:
    """
    x = re.findall('meta:(.+)', output.replace('\t', ' '))[0].split(' ')
    dic = {}
    for l in x:
        if l != '':
            dic[l.split('=')[0]] = eval(l.split('=')[1])
    # print dic
    sql1 = 'select weather,coll_time,travel_mileage,personnel from ' \
           'vehicle_datatask where taskid="{0}"'.format(ti[1])
    get_tasks_info1 = get_taskid_form_adulabel(host=aduspider_host, port=aduspider_port,
                                               user=aduspider_user, passwd=aduspider_passwd,
                                               db=aduspider_db, sql=sql1)
    weather_dict = {'晴天': 1, '多云': 2, '阴天': 3, '小雨': 4, '中雨': 5, '大雨': 6, '小雪': 7,
                    '中雪': 8, '大雪': 9, '暴风雪': 10, '雾天': 11, '霜降': 12, '雨夹雪': 13,
                    '沙尘暴': 14, '烟雾天': 15}
    if get_tasks_info1:
        # 天气
        weather = weather_dict[get_tasks_info1[0][0]]
        # task任务id
        data_url = ti[1]
        # task任务大小
        data_size = round(float(dic['data_size']) / 1024 / 1024 / 1024, 2)
        # 采集总时长
        duration = get_tasks_info1[0][1]
        # 采集里程
        mileage = get_tasks_info1[0][2]
        # 采集车辆
        vehicle = dic['car_id']
        sensor = 'sensors'
        start_time_stamp = time.mktime(time.strptime(ti[1].split('_')[1], '%Y%m%d%H%M%S'))
        # 格式化后开始采集时间
        start_time_format = time.strftime("%Y-%m-%d %H:%M:%S",
                                          time.localtime(start_time_stamp))
        end_time_stamp = start_time_stamp + float(duration) * 3600
        # 格式化后的结束采集时间
        end_time_format = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time_stamp))
        # 司机
        driver = ti[6]
        # 　采集任务id
        collect_task_id = ti[0]
        # 采集需求id
        collect_need_id = ti[5]
        # 任务类型
        data_type = ti[3]
        status = 1
        # remark = '' if ti[2] == '' else ti[2]
        remark = ti[2]
        sql2 = 'select alias from tb_district where id={0}'.format(ti[4])
        get_tasks_info2 = get_taskid_form_adulabel(host=adulabel_host, port=adulabel_port,
                                                   user=adulabel_user, db=adulabel_db,
                                                   passwd=adulabel_passwd, sql=sql2)
        # 查询task是否已经存在表中
        select_task = 'select id from tb_collect_data where data_url="{0}"'.format(ti[1])
        get_task_id = get_taskid_form_adulabel(host=adulabel_host, port=adulabel_port,
                                               user=adulabel_user, db=adulabel_db,
                                               passwd=adulabel_passwd, sql=select_task)
        if get_task_id:
            logger.info('Task:{0} Existing please update manually'.format(ti[1]))
        else:
            # print get_tasks_info2
            district_alias = get_tasks_info2[0][0]
            sql3 = 'INSERT INTO tb_collect_data (data_url, data_size, duration, mileage, ' \
                   'vehicle, sensor, start_time, end_time, driver, collect_task_id, ' \
                   'collect_need_id, data_type, district_alias, weather, status, remark) ' \
                   'VALUES ("{0}", {1}, {2}, {3}, "{4}", "{5}", "{6}", "{7}", "{8}", ' \
                   '{9}, {10}, {11},"{12}",{13},{14},"{15}")'.format(data_url, data_size,
                                                                     duration, mileage,
                                                                     vehicle, sensor,
                                                                     start_time_format,
                                                                     end_time_format, driver,
                                                                     collect_task_id,
                                                                     collect_need_id, data_type,
                                                                     district_alias, weather,
                                                                     status, remark)
            conn = pymysql.connect(host=adulabel_host, port=adulabel_port, user=adulabel_user,
                                   passwd=adulabel_passwd, db=adulabel_db)
            cursor = conn.cursor()
            cursor.execute(sql3)
            e = cursor.rowcount
            # conn.commit()
            if e != 0:
                sql4 = 'UPDATE tb_collect_task SET status=4 WHERE adb_task_id="{0}"'.format(
                    ti[1])
                cursor.execute(sql4)
                r = cursor.rowcount
                if r != 0:
                    conn.commit()
                    logger.info('Task:{0} The state has changed'.format(ti[1]))
            cursor.close()
            conn.close()
    return 0


# 其他车辆分支
def other_vehicles(ti, output):
    """
    OTHER VEHICLES
    :return:
    """
    x = re.findall('meta:(.+)', output.replace('\t', ' '))[0].split(' ')
    dic = {}
    for l in x:
        if l != '':
            dic[l.split('=')[0]] = eval(l.split('=')[1])
    # 天气
    weather = 0
    # task任务id
    data_url = ti[1]
    # task任务大小
    data_size = round(float(dic['data_size']) / 1024 / 1024 / 1024, 2)
    # 采集总时长
    duration = round(float(dic['duration']) / 3600, 2)
    if duration <= 0:
        logger.info('Task:{0} There is an abnormal acquisition time. Please handle it manually\n'.format(
            ti[1]))
        return 0
    # 采集里程
    mileage = round(float(dic['miles']), 2)
    # 采集车辆
    vehicle = dic['car_id']
    sensor = 'sensors'
    start_time_stamp = time.mktime(time.strptime(ti[1].split('_')[1], '%Y%m%d%H%M%S'))
    # 格式化后开始采集时间
    start_time_format = time.strftime("%Y-%m-%d %H:%M:%S",
                                      time.localtime(start_time_stamp))
    end_time_stamp = start_time_stamp + float(duration) * 3600
    # 格式化后的结束采集时间
    end_time_format = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time_stamp))
    # 司机
    driver = ti[6]
    # 　采集任务id
    collect_task_id = ti[0]
    # 采集需求id
    collect_need_id = ti[5]
    # 任务类型
    data_type = ti[3]
    status = 1
    remark = ti[2]
    sql2 = 'select alias from tb_district where id={0}'.format(ti[4])
    get_tasks_info2 = get_taskid_form_adulabel(host=adulabel_host, port=adulabel_port,
                                               user=adulabel_user, db=adulabel_db,
                                               passwd=adulabel_passwd, sql=sql2)
    # 查询task是否已经存在表中
    select_task = 'select id from tb_collect_data where data_url="{0}"'.format(ti[1])
    get_task_id = get_taskid_form_adulabel(host=adulabel_host, port=adulabel_port,
                                           user=adulabel_user, db=adulabel_db,
                                           passwd=adulabel_passwd, sql=select_task)
    if get_task_id:
        logger.info('Task:{0} Existing please update manually'.format(ti[1]))
    else:
        # print get_tasks_info2
        district_alias = get_tasks_info2[0][0]
        sql3 = 'INSERT INTO tb_collect_data (data_url, data_size, duration, mileage, ' \
               'vehicle, sensor, start_time, end_time, driver, collect_task_id, ' \
               'collect_need_id, data_type, district_alias, weather, status, remark) ' \
               'VALUES ("{0}", {1}, {2}, {3}, "{4}", "{5}", "{6}", "{7}", "{8}", ' \
               '{9}, {10}, {11},"{12}",{13},{14},"{15}")'.format(data_url, data_size,
                                                                 duration, mileage,
                                                                 vehicle, sensor,
                                                                 start_time_format,
                                                                 end_time_format, driver,
                                                                 collect_task_id,
                                                                 collect_need_id, data_type,
                                                                 district_alias, weather,
                                                                 status, remark)
        conn = pymysql.connect(host=adulabel_host, port=adulabel_port, user=adulabel_user,
                               passwd=adulabel_passwd, db=adulabel_db)
        cursor = conn.cursor()
        cursor.execute(sql3)
        e = cursor.rowcount
        #conn.commit()
        if e != 0:
            sql4 = 'UPDATE tb_collect_task SET status=4 WHERE adb_task_id="{0}"'.format(
                ti[1])
            cursor.execute(sql4)
            r = cursor.rowcount
            if r != 0:
                conn.commit()
                logger.info('Task:{0} The state has changed'.format(ti[1]))
        cursor.close()
        conn.close()
    return 0


if __name__ == '__main__':

    # adulabel 数据库配置
    adulabel_host = ''
    adulabel_port = 
    adulabel_user = ''
    adulabel_passwd = ''
    adulabel_db = ''

    # aduspider 数据库配置
    aduspider_host = ''
    aduspider_port = 
    aduspider_user = ''
    aduspider_passwd = ''
    aduspider_db = ''

    # adb_client 路径配置
    adb_client_path = '/home/aduspider/adb_client/bin/adb_client'

    # 执行sql 拿到task表中的部分信息
    sql = "select id, adb_task_id, remark, task_type, district_id, collect_need_id, drivers " \
          "from tb_collect_task WHERE status!=4 AND deleted=0"
    get_tasks_info = get_taskid_form_adulabel(host=adulabel_host, port=adulabel_port,
                                              user=adulabel_user, passwd=adulabel_passwd,
                                              db=adulabel_db, sql=sql)
    # get_tasks_info = [[1, 'MKZ111_20190326132702']]
    # 根据不同车型规定不同的路径
    for ti in get_tasks_info:
        logger.info('Discovery task: {0}'.format(ti[1]))
        if ti[1] != '':
            # ti[1] = 'MKZ121_20190321145805'
            if ti[1][:6] in ['MKZ149', 'MKZ111', 'MKZ121', 'MKZ123']:
                table_url = 'l3collect/auto_car/task_keydata'
            elif ti[1][:3] in ['KL']:
                table_url = 'kinglong/auto_car/task_keydata'
            elif ti[1][:5] in ['EQ019']:
                table_url = 'neolix/auto_car/task_keydata'
            else:
                table_url = 'auto_car/task_keydata'

            if ti[1][:6] in ['MKZ149', 'MKZ111', 'MKZ121', 'MKZ123']:
                # 获取所需要的信息
                cmd = "{0} scan -t {1} -w 'task_id=\"{2}\"'".format(
                    adb_client_path, table_url, ti[1])
                output = with_output_or_exception(cmd)
                if output == 'error' or 'meta:' not in output:
                    continue
                designated_vehicle(ti, output)
                    # other_vehicles(ti, output)
            else:
                cmd = "{0} scan -t {1} -w 'task_id=\"{2}\"'".format(
                    adb_client_path, table_url, ti[1])
                output = with_output_or_exception(cmd)
                if output == 'error' or 'meta:' not in output:
                    continue
                other_vehicles(ti, output)
        # break
    # print get_tasks_info