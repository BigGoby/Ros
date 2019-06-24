#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# #############################################################
#
#  Copyright (c) 2014 xxxx.com, Inc. All Rights Reserved
#
# #############################################################

"""
:authors: wanghongrui
:create_date: 2018/10/11
:description:
    monitor and alarm
"""

import rospy
import time
import io
import subprocess
import ConfigParser
import rostopic
import json
import sys
import os
import threading
import Queue
import commands
import psutil
import requests
import sqlite3

from app.config import config


reload(sys)
sys.setdefaultencoding('utf-8')


# 监控传感器各项的值
def topic_monitor_threads(topic_key, topic_object, topic_values):
    """
       monitor topic value
    """
    rospy.sleep(3)
    times = sum(topic_object.times)
    messages = len(topic_object.times)
    rate = round(messages / times if times > 0. else 0, 2)
    rospy.loginfo("%s rate: %s" % (topic_values, rate))
    if rate == 0:

        data_return['sensors'][topic_key] = [0, 1]
    else:
        data_return['sensors'][topic_key] = [rate, 0]


# 监控radar各项的值
def radar_monitor_threads(topic_key, topic_object, topic_values):
    """
       monitor radar value
    """
    rospy.sleep(3)
    times = sum(topic_object.times)
    messages = len(topic_object.times)
    rate = round(messages / times if times > 0. else 0, 2)
    rospy.loginfo("%s rate: %s" % (topic_values, rate))
    if rate == 0:
        data_return['sensors'][topic_key] = [0, 1]
    else:
        data_return['sensors'][topic_key] = [rate, 0]


# 监控car_info各项的值
def car_info_monitor_threads(topic_key, topic_object, topic_values):
    """
       monitor car_info value
    """
    rospy.sleep(3)
    times = sum(topic_object.times)
    messages = len(topic_object.times)
    rate = round(messages / times if times > 0. else 0, 2)
    rospy.loginfo("%s rate: %s" % (topic_values, rate))
    if rate == 0:
        data_return['sensors'][topic_key] = [0, 1]
    else:
        data_return['sensors'][topic_key] = [rate, 0]


# 监控nvme磁盘的信息
def nvme_monitor_threads():
    """
       monitor disk space
    """
    disk_total_storage = 0
    current_available_space = 0
    lines = commands.getoutput("mount -v | grep nvme").split('\n')
    if lines == ['']:
        data_return['system']['nvme'] = [0, 0, 1]
    else:
        points = map(lambda line: line.split()[2], lines)
        # 获取磁盘的总空间
        for di in points:
            disk_total_storage += round(float(psutil.disk_usage(di).total) / 1024 / 1024 / 1000, 2)
            # f_bavail非超级用户可获取的块数,f_bsize传输块大小
            disk = os.statvfs(di)
            current_available_space += disk.f_bsize * disk.f_bavail / 1024 / 1024 / 1000
        data_return['system']['nvme'] = [disk_total_storage, current_available_space, 0]


# 监控系统cpu使用率
def cpu_monitor_threads():
    """
        monitor cpu
    """
    # 获取cpu的线程数
    cpu_thread_count = psutil.cpu_count()
    # cup 使用率
    cpu_usage_rate = round(float(sum(psutil.cpu_percent(interval=1, percpu=True))) /
                           cpu_thread_count, 2)
    # cup 1,5,15平均负载
    cpu_loadavg = os.getloadavg()
    data_return['system']['cpu'] = [str(100 - cpu_usage_rate)+'%', cpu_loadavg[0], cpu_loadavg[1],
                                    cpu_loadavg[2]]


# 监控系统内存使用率
def memory_monitor_threads():
    """
        monitor memory
    """
    # 获取物理内存信息
    memory_info = psutil.virtual_memory()
    # 获取内存利用率
    memory_percent = memory_info.percent
    # 获取内存大小
    memory_total = round(float(memory_info.total) / 1024 / 1024 / 1000, 2)
    # 获取已经使用的内存大小
    memory_used = round(float(memory_info.total - memory_info.available) / 1024 / 1024 / 1000,
                        2)
    data_return['system']['memory'] = [memory_total, memory_used, memory_percent]


# 监控系统盘信息
def disk_monitor_threads():
    """
            monitor disk
    """
    # 磁盘分区信息
    disk_mountpoint = psutil.disk_partitions()
    disk_info = psutil.disk_usage(disk_mountpoint[0].mountpoint)
    # 磁盘总容量
    disk_size = round(disk_info.total / 1024 / 1024 / 1000, 2)
    # 磁盘剩余容量
    disk_free = round(disk_info.free / 1024 / 1024 / 1000, 2)
    # 磁盘利用率
    disk_percent = disk_info.percent
    data_return['system']['disk'] = [disk_size, disk_free, disk_percent]


# 开启线程任务
def start_monitor():
    threads = []
    # 初始化监测ros节点
    rospy.init_node("topic_monitor_node", log_level=rospy.DEBUG)

    # 报警配置
    config_parser = ConfigParser.ConfigParser()
    config_parser.read(config.CONFIG_INI_PATH)

    # 协程运行监控camera、ublox等的状态
    others_topic = dict(config_parser.items("topic_others"))
    for topic_key, topic in others_topic.items():
        topic_object = rostopic.ROSTopicHz(-1)
        rospy.Subscriber(topic, rospy.AnyMsg, topic_object.callback_hz)
        threads.append(threading.Thread(target=topic_monitor_threads,
                                        args=(topic_key, topic_object, topic,)))
    # 协程运行监控car_info各个传感器的状态
    radar_topic = dict(config_parser.items("topic_radar"))
    for topic_key, topic in radar_topic.items():
        topic_object = rostopic.ROSTopicHz(-1)
        rospy.Subscriber(topic, rospy.AnyMsg, topic_object.callback_hz)
        threads.append(threading.Thread(target=radar_monitor_threads,
                                        args=(topic_key, topic_object, topic,)))

    # 协程运行监控car_info各个传感器的状态
    car_topic = dict(config_parser.items("topic_car_info"))
    for topic_key, topic in car_topic.items():
        topic_object = rostopic.ROSTopicHz(-1)
        rospy.Subscriber(topic, rospy.AnyMsg, topic_object.callback_hz)
        threads.append(threading.Thread(target=car_info_monitor_threads,
                                        args=(topic_key, topic_object, topic,)))

    # 创建监控nvme磁盘的线程
    threads.append(threading.Thread(target=nvme_monitor_threads))

    # 创建监控cpu的线程
    threads.append(threading.Thread(target=cpu_monitor_threads))

    # 创建监控内存的线程
    threads.append(threading.Thread(target=memory_monitor_threads))

    # 创建监控disk的线程
    threads.append(threading.Thread(target=disk_monitor_threads))

    # 开启线程任务
    for t in threads:
        t.setDaemon(True)
        t.start()
    for t in threads:
        t.join()


# 外部调用函数
def external_use():
    global data_return
    data_return = {'sensors': {}, 'system': {}}

    # 多线程跑传感器监控
    start_monitor()

    # 将结果写入文件
    with io.open('data_return.json', 'wb') as f1:
        f1.writelines(str(data_return).replace("'", '"'))
    return data_return


if __name__ == '__main__':
    external_use()