#!/usr/bin/env python
# -*- coding: utf-8 -*
# #############################################################
#
#  Copyright (c) 2014 xxxx.com, Inc. All Rights Reserved
#
# #############################################################

"""
:authors: wanghongrui
:create_date: 2011-4-1
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
import logging

from itertools import groupby

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# 重写ConfigParser 因为源码会自动将key中的大写转为小写
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


# 处理数据
def data_processing(taskid):
    """
    data processing
    """
    t = time.time()
    adb_collect_path = '/home/aduspider/adb_client'
    # 将整个task任务的所有bag包都下载来
    local_path = os.path.join('/home/aduspider/output_data', taskid)
    if os.path.exists(local_path):
        subprocess.check_output('rm -rf {0}'.format(local_path), shell=True)
    table_url1 = 'l3collect/auto_car/app_data'
    cmd = "%s/bin/adb_client scan -t %s -w 'task_id=\"%s\" and data_type=\"bag_info\" ' " \
          "--export --output-dir %s --name-by file_name" % (adb_collect_path,
                                                            table_url1, taskid, local_path)
    download_status = with_output_or_exception(cmd)
    if download_status == 'error':
        return 'error'

    # 获取传感器对应的正常帧率
    config_parser = MyConfigParser()
    config_parser.read('rate.ini')
    topic_dict = dict(config_parser.items("rate"))

    # 移除下载下来的task任务所产生的.meta文件
    subprocess.check_output('rm -f {}/*.meta'.format(local_path), shell=True)
    rate_file_list = os.listdir(local_path)

    rate_datas = {"rateBag": {}, "available": {}, "taskId": taskid}
    # 各个传感器可用的时间戳
    sensor_available_seconds = {}
    for key in topic_dict.keys():
        sensor_available_seconds[key] = []

    # 所有bag包的开始和结束时间
    bag_start_time_lists = []
    bag_end_time_lists = []
    for bag in rate_file_list:
        path = os.path.join(local_path, bag)
        with open(path, 'r') as f:
            # 取出bag包中所有数据
            packet_datas = eval(f.read())
            packet_datas_start_time = packet_datas['start_time']
            packet_datas_end_time = packet_datas['end_time']
            bag_start_time_lists.append(packet_datas_start_time)
            bag_end_time_lists.append(packet_datas_end_time)
            bag_rate_data = {'start_time': packet_datas_start_time, 'end_time': packet_datas_end_time}
            bag_available_data = {'start_time': packet_datas_start_time,
                                  'end_time': packet_datas_end_time}
            # 只取出task包中关于传感器的帧率信息
            for k, v in packet_datas['fps'].items():
                bag_rate_data[k] = [[l, v[l]] for l in range(1, len(v))]
                #
                if k in topic_dict.keys():
                    bag_available_data[k] = []
                    # 相机传感器上下10% 其余传感器上下5%
                    if '/sensor/camera/' in k:
                        for l in range(len(v)):
                            if float(topic_dict[k]) * 0.9 <= v[l] <= float(topic_dict[k]) * 1.1:
                                bag_available_data[k].append(l)
                                sensor_available_seconds[k].append(int(packet_datas_start_time) + l)
                    else:
                        for l in range(len(v)):
                            if float(topic_dict[k]) * 0.95 <= v[l] <= float(topic_dict[k]) * 1.05:
                                bag_available_data[k].append(l)
                                sensor_available_seconds[k].append(int(packet_datas_start_time) + l)
        rate_datas['rateBag'][bag] = bag_rate_data
        rate_datas['available'][bag] = bag_available_data
        # break

    # 所有传感器可以使用集合
    sensor_available_set = {}
    for sensor in sensor_available_seconds.keys():
        sensor_available_seconds[sensor].sort()
        available_time_interval = []
        for k, g in groupby(enumerate(sensor_available_seconds[sensor]), lambda (p, y): y - p):
            available_time = []
            for i, v in g:
                available_time.append(v)
            if len(available_time) > 1:
                available_time_interval.append([available_time[0], available_time[-1]])
            else:
                available_time_interval.append(available_time)
            sensor_available_set[sensor] = available_time_interval
    file_path = '%s/%s.txt' % (local_path, taskid)
    with open(file_path, 'wb') as f:
        f.write(str(rate_datas))

    # task信息 开始时间 结束时间 总时长 可用时长
    task_start_time = min(bag_start_time_lists)
    task_end_time = max(bag_end_time_lists)
    task_total_time = task_end_time - task_start_time

    # 计算task的可用率
    standard_sensors = dict(config_parser.items("standard")).keys()
    if taskid[:6] not in ['MKZ049', 'MKZ111']:
        standard_sensors.remove('/sensor/velodyne64/VelodyneScan')
    availability_set = ''
    for l in standard_sensors:
        if availability_set == '':
            availability_set = sensor_available_seconds[l]
        else:
            availability_set = list(set(availability_set) & set(sensor_available_seconds[l]))
        logger.info('%s, %s' % (l, len(availability_set)))
    task_available_rate = round(float(len(availability_set)) / float(task_total_time) * 100, 2)

    # for l in sensor_available_set.keys():
    #     print sys.getsizeof(str(sensor_available_set[l])) / 1024
    # subprocess.check_output('rm -rf /home/black/Desktop/output/', shell=True)

    task_time = {}
    task_time['taskid'] = taskid
    task_time['start_coll_time'] = task_start_time
    task_time['end_coll_time'] = task_end_time
    task_time['total_time'] = task_total_time
    task_time['available_proportion'] = task_available_rate
    task_time['available_time'] = len(availability_set)
    task_time['time_interval'] = 2 if taskid[-6:] > '180100' or taskid[-6:] < '055900' else 1

    # 获取task的任务分类及采集人员
    conn = pymysql.connect(host=adulabel_host, port=adulabel_port, user=adulabel_user,
                           passwd=adulabel_passwd, db=adulabel_db)
    cursor = conn.cursor()
    task_info_sql = 'select task_type,drivers from tb_collect_task where adb_task_id="{0}"'.format(
        taskid)
    cursor.execute(task_info_sql)
    task_info = cursor.fetchall()
    if task_info is not None:
        task_time['task_type'] = task_info[0][0]
        task_time['personnel'] = task_info[0][1]
    else:
        task_time['task_type'] = 0
        task_time['personnel'] = ''
    cursor.close()
    conn.close()

    # 需要传输的数据
    data = {'sensors': sensor_available_set, 'times': task_time}
    op_data = open(file_path, "rb")
    files = {"file": op_data}
    r_date = requests.post(aduspider_interface, files=files,
                           data={'data': str(data), 'key': 'xxxxxxxxxxxxxxxxxxxxx'})
    op_data.close()
    if r_date.json()['result'] == 'ok':
        subprocess.check_output('rm -rf {0}'.format(local_path), shell=True)
        subprocess.check_output('rm -f {0}'.format(file_path), shell=True)
        logger.info('Task {0} is processed and takes {1} seconds\n'.format(taskid, time.time() - t))
    else:
        logger.info('Task {0} is error\n'.format(taskid))


# 执行mysql查询
def get_taskid_form_adulabel(host, port, user, passwd, db, sql):
    """
    select mysql
    :return:
    """
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)
    cursor = conn.cursor()
    cursor.execute(sql)
    task_list = [l[0] for l in cursor.fetchall()]
    cursor.close()
    conn.close()
    return task_list


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
    aduspider_passwd = '&'
    aduspider_db = ''

    # aduspider 接口
    aduspider_interface = 'http://127.0.0.1:8000/quality/data_quality/'


    sql = "select adb_task_id FROM tb_collect_task WHERE id in(" \
          "SELECT collect_task_id FROM tb_collect_data WHERE start_time > '2018-12-30 00:00:00')"
    total_taskid = get_taskid_form_adulabel(host=adulabel_host, port=adulabel_port,
                                            user=adulabel_user, passwd=adulabel_passwd,
                                            db=adulabel_db, sql=sql)
    sql1 = "SELECT taskid FROM quality_dataquality"
    few_taskid = get_taskid_form_adulabel(host=aduspider_host, port=aduspider_port,
                                          user=aduspider_user, passwd=aduspider_passwd,
                                          db=aduspider_db, sql=sql1)
    task_list = list(set(total_taskid) - set(few_taskid))
    task_list.sort(key=lambda i:int(i[7:]))
    for l in task_list:
        ll = ['MKZ049', 'MKZ111', 'MKZ121', 'MKZ123']
        for q in ll:
            if q in l:
                logger.info('-------------任务{0}已经开始-------------'.format(l))
                try:
                    pr_status = data_processing(l)
                    if pr_status == 'error':
                        logger.info('-------------任务{0}解析失败-------------'.format(l))
                except Exception as e:
                    logger.info('Error reason %s' % e)
