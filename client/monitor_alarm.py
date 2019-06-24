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


# 向hi推送报警
def alarm_publish(topic_value=None, begin_time=None):
    """
       publish alarm from baidu-hi
    """
    if topic_value == "no_nvme":
        alarm_msg = "车牌号:%s\nNVME盘挂载失败" % car_id
    elif topic_value == 'cpu_load':
        alarm_msg = '%s\ncpu负载过高\n车牌号:%s' % (begin_time, car_id)
    elif topic_value == 'memory_percent':
        alarm_msg = "%s\n内存使用率过高\n车牌号:%s" % (begin_time, car_id)
    elif topic_value == 'lack_space':
        alarm_msg = "%s\n系统磁盘剩余空间不足\n车牌号:%s" % (begin_time, car_id)
    elif topic_value == 'nvme_space':
        alarm_msg = "%s\nnvme盘剩余空间不足\n车牌号:%s" % (begin_time, car_id)
    elif topic_value in topic_dict.values():
        alarm_msg = "%s\n传感器异常:%s\n车牌号:%s" % (begin_time, topic_value, car_id)
    else:
        alarm_msg = "%s---\n数据落盘异常\n车牌号:%s" % (begin_time, car_id)

    for driver_hi in drivers_hi:
        hi_msg = {'access_token': config.ACCESS_TOKEN, 'to': driver_hi, 'msg_type': config.MSG_TYPE,
                  'content': alarm_msg}
        try:
            res = requests.post(config.HI_URL, data=hi_msg, timeout=1).json()['result']
        except Exception as e:
            res = e
        if res != 'error':
            rospy.loginfo("publish alarm message of %s successfully!" % topic_value)
        else:
            rospy.loginfo("publish alarm message of %s failed! Reasons：%s" % (topic_value, res))


# 移除hi的报警
def alarm_remove(topic_value=None, duration=None, end_time=None):
    """
       remove alarm from baidu-hi
    """
    if topic_value in topic_dict.values():
        alarm_msg = "%s\n传感器恢复正常:%s\n车牌号:%s\n持续时间:%d秒" % (end_time, topic_value, car_id,
                                                          duration)
    elif topic_value == 'cpu_load':
        alarm_msg = "%s\ncpu负载恢复正常\n车牌号:%s\n持续时间:%d秒" % (end_time, car_id, duration)
    elif topic_value == 'memory_percent':
        alarm_msg = "%s\n内存使用率恢复正常\n车牌号:%s\n持续时间:%d秒" % (end_time, car_id, duration)
    elif topic_value == 'lack_space':
        alarm_msg = "%s\n磁盘剩余空间正常\n车牌号:%s\n持续时间:%d秒" % (end_time, car_id, duration)
    else:
        alarm_msg = "%s\n数据落盘恢复正常\n车牌号:%s\n持续时间:%d秒" % (end_time, car_id, duration)

    for driver_hi in drivers_hi:
        hi_msg = {'access_token': config.ACCESS_TOKEN, 'to': driver_hi, 'msg_type': config.MSG_TYPE,
                  'content': alarm_msg}
        try:
            res = requests.post(config.HI_URL, data=hi_msg, timeout=1).json()['result']
        except Exception as e:
            res = e
        if res != 'error':
            rospy.loginfo("remove alarm message of %s successfully!" % topic_value)
        else:
            rospy.loginfo("remove alarm message of %s failed! Reasons：%s" % (topic_value, res))


# 异常处理:kill该topic的pid，并restart该topic
def handle_anomally(topic_key):
    """
    handle anomally
    """
    # 获取topic对应的启动命令
    config_parser = ConfigParser.ConfigParser()
    config_parser.read(config.CONFIG_INI_PATH)
    sensorlaunch_dict = dict(config_parser.items("launch"))

    # 查看topic是否在config.ini里面
    if topic_key == 'inspva':
        topic_key = 'novatel'
    if topic_key in sensorlaunch_dict:
        topic_command = sensorlaunch_dict[topic_key]
        # 获取到topic的pid
        pid_shell = "ps -ef | grep '{0}' |grep -v grep|awk '{1}'".format(
            topic_command.split(' >>')[0], '{print $2}')
        topic_pids = subprocess.check_output(pid_shell, shell=True)
        # 如果存在且不为“”的话，循环杀死pid
        if topic_pids:
            # 获取topic的pid
            rospy.loginfo("PID needs to be killed - > {}".format(topic_pids.split('\n')))
            # 切分获得的pid
            for pid in topic_pids.split('\n'):
                if pid:
                    subprocess.Popen('kill -9 {}'.format(pid), shell=True)
        # 主程序不会自动等待子进程完成
        subprocess.Popen('{}'.format(topic_command), shell=True)
        rospy.loginfo("Restarting - > {}".format(topic_key))


# 监控传感器各项的值
def topic_monitor_threads(topic_key, topic_object, topic_values):
    """
       monitor topic value
    """
    data = {}
    rospy.sleep(3)
    times = sum(topic_object.times)
    messages = len(topic_object.times)
    rate = round(messages / times if times > 0. else 0, 2)
    rospy.loginfo("%s rate: %s" % (topic_values, rate))

    if topic_key in data_set.keys():
        # 帧率为零，且满足间隔时间,重启次数小于某值
        if rate == 0:
            inspect_restart_time = time.time()
            # 满足重启条件和发送报警的条件
            if data_set[topic_key]['restart_time'] < inspect_restart_time - config.SENSOR_WAIT_TIME\
                    and data_set[topic_key]['inspect_counts'] < 3:
                handle_anomally(topic_key)
                data_set[topic_key]['inspect_counts'] += 1
                data_set[topic_key]['restart_time'] = inspect_restart_time
            elif data_set[topic_key]['restart_time'] < inspect_restart_time -\
                    config.SENSOR_SENDALERT_TIME and data_set[topic_key]['inspect_counts'] >= 3:
                new_time = time.strftime('%y/%m/%d %X', time.localtime(time.time()))
                data_set[topic_key]['restart_time'] = inspect_restart_time
                alarm_publish(topic_value=topic_values, begin_time=new_time)
            data_return['sensors'][topic_key] = [0, 1]
        # 帧率正常，解除警报，删除文件中的topic_key
        else:
            if data_set[topic_key]['inspect_counts'] >= 3:
                inspect_restart_time = time.time()
                duration_time = round(inspect_restart_time - data_set[topic_key]['start_time'], 2)
                new_time = time.strftime('%y/%m/%d %X', time.localtime(inspect_restart_time))
                alarm_remove(topic_value=topic_values, duration=duration_time, end_time=new_time)
                data_return['sensors'][topic_key] = [rate, 0]
                del data_set[topic_key]
            else:
                data_return['sensors'][topic_key] = [rate, 0]
                del data_set[topic_key]
    else:
        if rate == 0:
            data['start_time'] = time.time()
            data['restart_time'] = time.time()
            data['inspect_counts'] = 1
            data_set.update({topic_key: data})
            handle_anomally(topic_key)
            data_return['sensors'][topic_key] = [0, 1]
        else:
            data_return['sensors'][topic_key] = [rate, 0]


# 监控radar各项的值
def radar_monitor_threads(topic_key, topic_object, topic_values):
    """
       monitor radar value
    """
    data = {}
    rospy.sleep(3)
    times = sum(topic_object.times)
    messages = len(topic_object.times)
    rate = round(messages / times if times > 0. else 0, 2)
    rospy.loginfo("%s rate: %s" % (topic_values, rate))

    if topic_key in data_set.keys():
        # 帧率为零，且满足间隔时间,重启次数小于某值
        if rate == 0:
            inspect_restart_time = time.time()
            # 满足重启和发送报警的条件
            if data_set['radar']['restart_time'] < inspect_restart_time -\
                    config.RADAR_INTERVAL_TIME and data_set['radar']['radar_inspect_counts'] < 3:
                q.put(['radar_inspect_counts', 1])
                q.put(['radar_restart', time.time()])
            elif data_set['radar']['restart_time'] < inspect_restart_time -\
                    config.RADAR_SENDALERT_TIME and data_set['radar']['radar_inspect_counts'] >= 3:
                new_time = time.strftime('%y/%m/%d %X', time.localtime(time.time()))
                #q.put(['radar_restart', time.time()])
                alarm_publish(topic_value=topic_values, begin_time=new_time)
            data_return['sensors'][topic_key] = [0, 1]
        # 帧率正常，解除警报，删除文件中的topic_key
        else:
            if data_set['radar']['radar_inspect_counts'] >= 3:
                inspect_restart_time = time.time()
                duration_time = round(inspect_restart_time - data_set[topic_key]['start_time'], 2)
                new_time = time.strftime('%y/%m/%d %X', time.localtime(inspect_restart_time))
                alarm_remove(topic_value=topic_values, duration=duration_time, end_time=new_time)
                data_return['sensors'][topic_key] = [rate, 0]
                del data_set[topic_key]
            else:
                data_return['sensors'][topic_key] = [rate, 0]
                del data_set[topic_key]
    else:
        if rate == 0:
            data['start_time'] = time.time()
            data_set.update({topic_key: data})
            if 'radar' not in data_set.keys():
                data_set['radar'] = {'radar_inspect_counts': 1}
            q.put(['radar_restart', time.time()])
            data_return['sensors'][topic_key] = [0, 1]
        else:
            data_return['sensors'][topic_key] = [rate, 0]


# 监控car_info各项的值
def car_info_monitor_threads(topic_key, topic_object, topic_values):
    """
       monitor car_info value
    """
    data = {}
    rospy.sleep(3)
    times = sum(topic_object.times)
    messages = len(topic_object.times)
    rate = round(messages / times if times > 0. else 0, 2)
    rospy.loginfo("%s rate: %s" % (topic_values, rate))

    if topic_key in data_set.keys():
        # 帧率为零，且满足间隔时间,重启次数小于某值
        if rate == 0:
            inspect_restart_time = time.time()
            # 满足重启和报警的条件
            if data_set['car_info']['restart_time'] < inspect_restart_time -\
                    config.CARINFO_INTERVAL_TIME and \
                    data_set['car_info']['car_info_inspect_counts'] < 3:
                q.put(['car_info_inspect_counts', 1])
                q.put(['car_info_restart', time.time()])
            elif data_set['car_info']['restart_time'] < inspect_restart_time -\
                    config.CARINFO_SENDALERT_TIME and data_set['car_info']['car_info_inspect_counts'] >= 3:
                new_time = time.strftime('%y/%m/%d %X', time.localtime(time.time()))
                #q.put(['car_info_restart', time.time()])
                alarm_publish(topic_value=topic_values, begin_time=new_time)
            data_return['sensors'][topic_key] = [0, 1]
        # 帧率正常，解除警报，删除文件中的topic_key
        else:
            if data_set['car_info']['car_info_inspect_counts'] >= 3:
                inspect_restart_time = time.time()
                duration_time = round(inspect_restart_time - data_set[topic_key]['start_time'], 2)
                new_time = time.strftime('%y/%m/%d %X', time.localtime(inspect_restart_time))
                alarm_remove(topic_value=topic_values, duration=duration_time, end_time=new_time)
                data_return['sensors'][topic_key] = [rate, 0]
                del data_set[topic_key]
            else:
                data_return['sensors'][topic_key] = [rate, 0]
                del data_set[topic_key]
    else:
        if rate == 0:
            data['start_time'] = time.time()
            data_set.update({topic_key: data})
            if 'car_info' not in data_set.keys():
                data_set['car_info'] = {'car_info_inspect_counts': 1}
            q.put(['car_info_restart', time.time()])
            data_return['sensors'][topic_key] = [0, 1]
        else:
            data_return['sensors'][topic_key] = [rate, 0]


# 监控nvme磁盘的信息
def nvme_monitor_threads():
    """
       monitor disk space
    """
    data = {}
    disk_total_storage = 0
    current_available_space = 0
    lines = commands.getoutput("mount -v | grep nvme").split('\n')
    inspect_restart_time = time.time()
    new_time = time.strftime('%y/%m/%d %X', time.localtime(inspect_restart_time))
    if lines == ['']:
        if 'nvme' in data_set.keys():
            # 发送nvme盘挂载失败
            if data_set['nvme']['restart_time'] < inspect_restart_time - config.NVME_INTERVAL_TIME:
                data_set['nvme']['restart_time'] = time.time()
                alarm_publish(topic_value='no_nvme', begin_time=new_time)
        else:
            data['disk_total_storage'] = disk_total_storage
            data['current_available_space'] = current_available_space
            data['start_time'] = inspect_restart_time
            data['restart_time'] = inspect_restart_time
            data_set.update({'nvme': data})
        data_return['system']['nvme'] = [0, 0, 1]
    else:
        points = map(lambda line: line.split()[2], lines)
        # 获取磁盘的总空间
        for di in points:
            disk_total_storage += round(float(psutil.disk_usage(di).total) / 1024 / 1024 / 1024, 2)
            # f_bavail非超级用户可获取的块数,f_bsize传输块大小
            disk = os.statvfs(di)
            current_available_space += round(float(disk.f_bsize * disk.f_bavail) / 1024 / 1024 / 1024, 2)

        if 'nvme' in data_set.keys():
            # 报警条件1：当检测到的此次磁盘可用空间比上一次的大或者相等时报警
            if current_available_space >= data_set['nvme']['current_available_space']:
                if 'nvme_inspect_time' not in data_set['nvme'].keys():
                    data_set['nvme']['nvme_inspect_time'] = inspect_restart_time
                    alarm_publish(begin_time=new_time)
                elif data_set['nvme']['nvme_inspect_time'] < inspect_restart_time -\
                        config.NVME_SPACE_INTERVAL_TIME:
                    data_set['nvme']['nvme_inspect_time'] = inspect_restart_time
                    alarm_publish(begin_time=new_time)
            else:
                if 'nvme_inspect_time' in data_set['nvme'].keys():
                    # 解除报警
                    duration_time = round(inspect_restart_time -
                                          data_set['nvme']['nvme_inspect_time'], 2)
                    del data_set['nvme']['nvme_inspect_time']
                    alarm_remove(end_time=new_time, duration=duration_time)
                # 报警条件2：当磁盘空间利用率到达某值时触发报警 剩余/全部 < 0.1
                if current_available_space / disk_total_storage < config.NVME_SPACE_FREE:
                    if 'nvme_inspect1_time' not in data_set['nvme'].keys():
                        data_set['nvme']['nvme_inspect1_time'] = inspect_restart_time
                        alarm_publish(topic_value='nvme_space', begin_time=new_time)
                    elif data_set['nvme']['nvme_inspect1_time'] < inspect_restart_time -\
                            config.NVME_SPACE_FREE:
                        data_set['nvme']['nvme_inspect1_time'] = inspect_restart_time
                        alarm_publish(topic_value='nvme_space', begin_time=new_time)
                elif 'nvme_inspect1_time' in data_set['nvme'].keys():
                    # 解除警报
                    duration_time = round(inspect_restart_time -
                                          data_set['nvme']['nvme_inspect1_time'], 2)
                    del data_set['nvme']['nvme_inspect1_time']
                    alarm_remove(duration=duration_time, end_time=new_time)
        else:
            data['disk_total_storage'] = disk_total_storage
            data['current_available_space'] = current_available_space
            data_set.update({'nvme': data})
        data_set['nvme']['current_available_space'] = current_available_space
        data_return['system']['nvme'] = [disk_total_storage, current_available_space, 0]


# 监控系统cpu使用率
def cpu_monitor_threads():
    """
        monitor cpu
    """
    inspect_restart_time = time.time()
    new_time = time.strftime('%y/%m/%d %X', time.localtime(inspect_restart_time))
    # 获取cpu的线程数
    cpu_thread_count = psutil.cpu_count()
    # cup 使用率
    cpu_usage_rate = round(float(sum(psutil.cpu_percent(interval=1, percpu=True))) /
                           cpu_thread_count, 2)
    # cup 1,5,15平均负载
    cpu_loadavg = os.getloadavg()

    # 当cup使用率超过一定值的时候触发报警
    if cpu_usage_rate > config.CPU_USAGE_RATE:
        if 'cpu_restart_time' not in data_set.keys():
            data_set['cpu_start_time'] = inspect_restart_time
            data_set['cpu_restart_time'] = inspect_restart_time
            alarm_publish(topic_value='cpu_load', begin_time=new_time)
        elif inspect_restart_time < data_set['cpu_restart_time'] - config.CPU_INTERVAL_TIME:
            data_set['cpu_restart_time'] = inspect_restart_time
            # 发送报警
            alarm_publish(topic_value='cpu_load', begin_time=new_time)

    else:
        if 'cpu_start_time' in data_set.keys():
            duration_time = round(inspect_restart_time - data_set['cpu_start_time'], 2)
            del data_set['cpu_start_time']
            del data_set['cpu_restart_time']
            alarm_remove(topic_value='cpu_load', end_time=new_time, duration=duration_time)
    data_return['system']['cpu'] = [str(100-cpu_usage_rate)+'%', cpu_loadavg[0], cpu_loadavg[1],
                                    cpu_loadavg[2]]


# 监控系统内存使用率
def memory_monitor_threads():
    """
        monitor memory
    """
    inspect_restart_time = time.time()
    new_time = time.strftime('%y/%m/%d %X', time.localtime(inspect_restart_time))
    # 获取物理内存信息
    memory_info = psutil.virtual_memory()
    # 获取内存利用率
    memory_percent = memory_info.percent
    # 获取内存大小
    memory_total = round(float(memory_info.total) / 1024 / 1024 / 1000, 2)
    # 获取已经使用的内存大小
    memory_used = round(float(memory_info.total - memory_info.available) / 1024 / 1024 / 1000,
                        2)
    # 当内存利用率大于某值时触发报警
    if memory_percent > config.MEMORY_UTILIZE_RATE:
        if 'memory_restart_time' not in data_set.keys():
            data_set['memory_start_time'] = inspect_restart_time
            data_set['memory_restart_time'] = inspect_restart_time
            alarm_publish(topic_value='memory_percent', begin_time=new_time)
        elif inspect_restart_time < data_set['memory_restart_time'] - config.MEMORY_INTERVAL_TIME:
            data_set['memory_restart_time'] = inspect_restart_time
            # 发送报警
            alarm_publish(topic_value='memory_percent', begin_time=new_time)
    else:
        if 'memory_start_time' in data_set.keys():
            # 发送解除报警
            duration_time = round(inspect_restart_time - data_set['cpu_start_time'], 2)
            del data_set['memory_start_time']
            del data_set['memory_restart_time']
            alarm_remove(topic_value='memory_percent', end_time=new_time, duration=duration_time)
    data_return['system']['memory'] = [memory_total, memory_used, memory_percent]


# 监控系统盘信息
def disk_monitor_threads():
    """
            monitor disk
    """
    inspect_restart_time = time.time()
    # 磁盘分区信息
    disk_mountpoint = psutil.disk_partitions()
    new_time = time.strftime('%y/%m/%d %X', time.localtime(inspect_restart_time))
    disk_info = psutil.disk_usage(disk_mountpoint[0].mountpoint)
    # 磁盘总容量
    disk_size = round(disk_info.total / 1024 / 1024 / 1000, 2)
    # 磁盘剩余容量
    disk_free = round(disk_info.free / 1024 / 1024 / 1000, 2)
    # 磁盘利用率
    disk_percent = disk_info.percent
    # 当磁盘利用率超过一定值时报警
    if disk_percent > config.MEMORY_UTILIZE_RATE:
        if 'disk_restart_time' not in data_set.keys():
            data_set['disk_start_time'] = inspect_restart_time
            data_set['disk_restart_time'] = inspect_restart_time
            alarm_publish(topic_value='lack_space', begin_time=new_time)
        elif inspect_restart_time < data_set['disk_start_time'] - config.DISK_INTERVAL_TIME:
            data_set['disk_restart_time'] = inspect_restart_time
            # 发送报警
            alarm_publish(topic_value='lack_space', begin_time=new_time)
    else:
        if 'disk_start_time' in data_set.keys():
            # 发送解除报警
            duration_time = round(inspect_restart_time - data_set['cpu_start_time'], 2)
            del data_set['disk_start_time']
            del data_set['disk_restart_time']
            alarm_remove(topic_value='lack_space', end_time=new_time, duration=duration_time)
    data_return['system']['disk'] = [disk_size, disk_free, disk_percent]


# 处理queue队列中，radar和car_info的值
def executor_queue():
    """
        Resolve tasks in queues
    """
    is_radar_restart = False
    is_car_info_restart = False
    car_info_restart_count = 0
    radar_restart_count = 0
    if q.qsize() != 0:
        for i in range(q.qsize()):
            sql_block = q.get()
            if sql_block[0] == 'radar_restart' and is_radar_restart == False:
                data_set['radar']['restart_time'] = sql_block[1]
                is_radar_restart = True
                handle_anomally('radar')
            elif sql_block[0] == 'radar_inspect_counts' and radar_restart_count == 0:
                data_set['radar']['radar_inspect_counts'] += 1
                radar_restart_count = 1
            elif sql_block[0] == 'car_info_restart' and is_car_info_restart == False:
                data_set['car_info']['restart_time'] = sql_block[1]
                is_car_info_restart = True
                handle_anomally('car_info')
            elif sql_block[0] == 'car_info_inspect_counts' and car_info_restart_count == 0:
                data_set['car_info']['car_info_inspect_counts'] += 1
                car_info_restart_count = 1
    else:
        rospy.loginfo("time:{0} Queue is empty".format(time.time()))


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
    car_topic = dict(config_parser.items("topic_radar"))
    for topic_key, topic in car_topic.items():
        topic_object = rostopic.ROSTopicHz(-1)
        rospy.Subscriber(topic, rospy.AnyMsg, topic_object.callback_hz)
        threads.append(threading.Thread(target=radar_monitor_threads,
                                        args=(topic_key, topic_object, topic,)))

    # 协程运行监控car_info各个传感器的状态
    radar_topic = dict(config_parser.items("topic_car_info"))
    for topic_key, topic in radar_topic.items():
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


# 从数据库中获取司机姓名
def get_sql_drive():
    """
        get driver name
    """
    conn = sqlite3.connect(config.SQLITE_PATH)
    cursor = conn.cursor()
    # 获取input_info表中最后一个司机
    get_drive = "SELECT username FROM INPUT_INFO order by id desc LIMIT 1;"
    current_drive_name = cursor.execute(get_drive).fetchone()[0]
    cursor.close()
    conn.close()
    return current_drive_name


# 外部调用函数
def external_use():
    global data_return, q, data_set, car_id, drivers_hi, topic_dict
    data_return = {'sensors': {}, 'system': {}}
    q = Queue.Queue()

    # hi报警获取司机
    car_id = os.getenv("CARID", default="MKZ000")
    config_parser = ConfigParser.ConfigParser()
    config_parser.read(config.CONFIG_INI_PATH)
    topic_dict = dict(config_parser.items("topic"))
    drivers_hi = config.ADMIN_HI
    drive_name = get_sql_drive()
    if drive_name in config.DRIVER_HI.keys():
        drivers_hi.append(config.DRIVER_HI[drive_name])

    # 获取上一次记录的状态
    if os.path.exists('aduspider.json'):
        with io.open('aduspider.json', 'r+', encoding='utf8') as f:
            data_set = eval(f.readlines()[0].decode('utf8'))
    else:
        data_set = {'none': 0}

    # 多线程跑传感器监控
    start_monitor()

    # 执行队列中的任务
    executor_queue()

    # radar 计数清零
    radar_count = 0
    radar_dict = dict(config_parser.items("topic_radar"))
    for radar_topic in radar_dict.keys():
        if data_return['sensors'][radar_topic][0] != 0:
            radar_count += 1
    if radar_count == len(radar_dict) and 'radar' in data_set.keys():
        data_set['radar']['radar_inspect_counts'] = 0 

    # car_info 计数清零
    carinfo_count = 0
    carinfo_dict = dict(config_parser.items("topic_car_info"))
    for carinfo_topic in carinfo_dict.keys():
        if data_return['sensors'][carinfo_topic][0] != 0:
            carinfo_count += 1
    if carinfo_count == len(carinfo_dict) and 'car_info' in data_set.keys():
        data_set['car_info']['car_info_inspect_counts'] = 0

    # 将本次的记录存放在文件里面
    if data_set != {'none': 0}:
        with io.open('aduspider.json', 'wb') as f:
            f.writelines(str(data_set))

    # 将结果写入文件
    with io.open('data_return.json', 'wb') as f1:
        f1.writelines(str(data_return).replace("'", '"'))
    print data_return
    return data_return


if __name__ == '__main__':
    external_use()
