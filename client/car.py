#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# #
#  Copyright (c) 2014 xxxx.com, Inc. All Rights Reserved
#
#
"""
:authors: wanghongrui
:create_date: 2019-1-23
:description:

"""

import sqlite3
import sys
import subprocess
import requests
import time
import os
import base64
import gps
import logging

from multiprocessing import Pool
from geopy.distance import geodesic

from app.config import config


reload(sys)
sys.setdefaultencoding('utf-8')
sys.setrecursionlimit(10000)


# 获取info表中需要回传的信息
def get_task_info():
    """
        get driver name
    """
    conn = sqlite3.connect(config.SQLITE_PATH)
    data = {}
    cursor = conn.cursor()
    # 获取input_info表中最后一个司机
    get_drive = "SELECT username, weather, shifts, taskId, planId FROM " \
                "INPUT_INFO order by id desc LIMIT 1;"
    task_details = list(cursor.execute(get_drive).fetchone())
    if task_details:
        data['username'] = task_details[0]
        data['weather'] = task_details[1]
        data['time'] = task_details[2]
        data['taskid'] = task_details[3]
        data['planid'] = task_details[4]
    cursor.close()
    conn.close()
    return data


# 获取info表中需要回传的信息
def get_end_task_info():
    """
        get driver name
    """
    conn = sqlite3.connect(config.SQLITE_PATH)
    data = {}
    cursor = conn.cursor()
    # 获取input_info表中最后一个司机
    get_drive = "SELECT username, weather, shifts, taskId, planId, travel_time, distance FROM " \
                "INPUT_INFO order by id desc LIMIT 1;"
    task_details = list(cursor.execute(get_drive).fetchone())
    if task_details:
        data['username'] = task_details[0]
        data['weather'] = task_details[1]
        data['time'] = task_details[2]
        data['taskid'] = task_details[3]
        data['planid'] = task_details[4]
        if task_details[5] is not None:
            data['travel_time'] = round(float(task_details[5]) / 3600, 2)
        else:
            data['travel_time'] = 0
        if task_details[6] is not None:
            data['distance'] = round(float(task_details[6]) / 1000, 2)
        else:
            data['distance'] = 0
    cursor.close()
    conn.close()
    return data


# 获取车辆的当前位置
def current_location():
    """
    get car location
    :return:
    """
    # 每2秒获取一次坐标点，获取5次坐标点 返回值[[],[],[],...] 二维列表
    location_point_set = gps.DataColl().star(5)
    r_datas = subprocess.check_output('cat data_return.json', shell=True)
    location_point_set = [lp + [[r_datas]] for lp in location_point_set]
    return location_point_set


# 获取任务的task id
def get_task_id():
    """
    get task id
    :return:
    """
    start_time = time.time()
    conn = sqlite3.connect(config.SQLITE_PATH)
    cursor = conn.cursor()
    # 获取input_info表中的任务id，如果任务id为0000则不向服务端发送数据
    get_plan_id = "SELECT planId FROM INPUT_INFO order by id desc LIMIT 1;"
    plan_id = list(cursor.execute(get_plan_id).fetchone())
    conn.commit()
    cursor.close()
    conn.close()
    if plan_id[0] == '0000':
        return 1
    while True:
        for filename in os.listdir(config.NVME_DATA_DISK_PATH):
            ll = os.path.join(config.NVME_DATA_DISK_PATH, filename)
            # 获取nvme 落盘所在目录，遍历文件夹获取创建时间
            if os.path.isdir(ll):
                if 'MKZ' in str(filename):
                    y = filename.split('_')[1]
                    x = time.mktime(time.strptime(y, '%Y%m%d%H%M%S'))
                    if x > start_time:
                        # 更新sqlite中的唯一任务id
                        conn_td = sqlite3.connect(config.SQLITE_PATH)
                        cursor_td = conn_td.cursor()
                        get_only_id = "UPDATE INPUT_INFO set taskId='{0}' where ID=1;".format(filename)
                        cursor_td.execute(get_only_id)
                        conn_td.commit()
                        conn_td.close()
                        return 'ok'
        time.sleep(10)


# 向车端post数据版本2
def data_transmission(data, taskid):
    """
    1.向车端post数据，如果异常则写入文件，下次发送数据时不发送post数据，而是先写入文件，在将文件post
    到server端
    2.如果发送post文件成功，则下次发送post数据，如此反复
    :param data:
    :return:
    """
    # 检查是否存在超时文件，存在则发送超时文件
    tfile_path = os.path.join(os.getcwd(), 'timeout/{}'.format(taskid))
    if os.path.exists(tfile_path) is True:
        with open(tfile_path, 'a+') as f:
            f.writelines(base64.b64encode(str(data)) + '\n')
        files = {"field1": open(tfile_path, "rb")}
        try:
            r = requests.post(config.SEND_TIMEOUT_FILE, files=files,
                              data={'key': config.SUBMI_SECRET_KEY}, timeout=3)
            if r.json()['result'] == 'ok':
                os.remove(tfile_path)
        except Exception as e:
            logging.info(e)
    else:
        try:
            r = requests.post(config.SEND_PUSH_DATA, data={'data': base64.b64encode(str(data)),
                                                           'key': config.SUBMI_SECRET_KEY},
                              timeout=3)
            if r.json()['result'] != 'ok':
                with open(tfile_path, 'a+') as f:
                    f.writelines(base64.b64encode(str(data)) + '\n')
        except Exception as e:
            logging.info(e)
            with open(tfile_path, 'a+') as f:
                f.writelines(base64.b64encode(str(data)) + '\n')


# 计算实时位置
def reckon_position():
    """
    Reference: https://geopy.read the docs.io/en/latest/#module-geopy.distance

    ellipsoid可选参数：--
                  model             major (km)   minor (km)     flattening
    ELLIPSOIDS = {'WGS-84':        (6378.137,    6356.7523142,  1 / 298.257223563),
                  'GRS-80':        (6378.137,    6356.7523141,  1 / 298.257222101),
                  'Airy (1830)':   (6377.563396, 6356.256909,   1 / 299.3249646),
                  'Intl 1924':     (6378.388,    6356.911946,   1 / 297.0),
                  'Clarke (1880)': (6378.249145, 6356.51486955, 1 / 293.465),
                  'GRS-67':        (6378.1600,   6356.774719,   1 / 298.25),}
    [[],[],[],...] 二维列表位置对应信息：[纬度,经度,海拔,状态,时间戳]
    """

    distance = 0
    start_time = time.time()
    last_position = 0
    while True:
        # 2秒获取一次位置点，返回的值为[[]] 二维列表
        datas = gps.DataColl().star(1)
        if datas != []:
            if last_position != 0:
                current_distance = round(geodesic((datas[0][0], datas[0][1]), last_position).m, 2)
                if current_distance < (datas[0][4] - last_position[2]) * 36:
                    distance += current_distance
                    travel_time = time.time() - start_time
                    conn = sqlite3.connect(config.SQLITE_PATH)
                    cursor = conn.cursor()
                    # 更新实时里程数和采集时长
                    get_drive = "UPDATE INPUT_INFO set distance='{0}',travel_time='{1}' " \
                                "where ID = 1;".format(distance, travel_time)
                    cursor.execute(get_drive)
                    conn.commit()
                    cursor.close()
                    conn.close()
                    last_position = (datas[0][0], datas[0][1], datas[0][4])
                else:
                    travel_time = time.time() - start_time
                    conn = sqlite3.connect(config.SQLITE_PATH)
                    cursor = conn.cursor()
                    # 更新实时里程数和采集时长
                    get_drive = "UPDATE INPUT_INFO set travel_time='{0}' where ID = 1;".format(travel_time)
                    cursor.execute(get_drive)
                    conn.commit()
                    cursor.close()
                    conn.close()
            else:
                last_position = (datas[0][0], datas[0][1], datas[0][4])
            del datas[:]


# 停止落盘后发送停止数据
def stop_push_data():
    """
    stop push data
    :return:
    """
    car_info = get_end_task_info()
    car_info['end_sign'] = 'true'
    car_info['car_id'] = os.getenv("CARID", default="MKZ000")
    try:
        r = requests.post(config.SEND_STOP_PUSH_DATA, timeout=3, data={'taskinfo': str(car_info),
                                                                    'key': config.SUBMI_SECRET_KEY})
        if r.json()['result'] != 'ok':
            tfile_path = os.path.join(os.getcwd(), 'timeout/{}'.format(car_info['taskid']))
            with open(tfile_path, 'a+') as f:
                f.writelines(base64.b64encode(str(car_info)) + '\n')
    except Exception as e:
        tfile_path = os.path.join(os.getcwd(), 'timeout/{}'.format(car_info['taskid']))
        with open(tfile_path, 'a+') as f:
            f.writelines(base64.b64encode(str(car_info)) + '\n')
        logging.info(e)


# 向服务端发送数据
def send_data_to_server():
    """
    send data to server
    1.先检测项目根目录下是否存在上次遗留下的超时文件
    2.如果存在，向服务端发送超时文件，根据发送状态判断是否网络环境异常
    3.网络环境正常，向服务端post数据
    3.1 post数据思路：发送一个包，如果接受失败抛出timeout异常，则断定
    网络环境异常，下次post数据发送的是超时文件，如果接收成功，下次发送则
    发送post数据
    :return:
    """
    # 检查是否存在超时文件，存在则发送超时文件
    folder_path = os.path.join(os.getcwd(), 'timeout')
    # 条件：timeout超时文件夹下是否存在文件
    if os.listdir(folder_path):
        # 获取固定常量
        drive_info = get_task_info()
        car_id = os.getenv("CARID", default="MKZ000")
        if drive_info:
            drive_info['car_id'] = car_id
            # 获取car的位置信息
            car_locations = current_location()
            car_locations.append(drive_info)
            tfile_path = os.path.join(os.getcwd(), 'timeout/{}'.format(drive_info['taskid']))
            with open(tfile_path, 'a+') as f:
                f.writelines(base64.b64encode(str(car_locations)) + '\n')
                print car_locations
            del car_locations[:]
        for l in os.listdir(folder_path):
            file_path = os.path.join(folder_path, l)
            if l == 'None':
                os.remove(file_path)
            else:
                timeout_data = open(file_path, "rb")
                files = {"field1": timeout_data}
                try:
                    r = requests.post(config.SEND_TIMEOUT_FILE, files=files,
                                      data={'key': config.SUBMI_SECRET_KEY}, timeout=3)
                    if r.json()['result'] == 'ok':
                        os.remove(file_path)
                except Exception as e:
                    print e
                timeout_data.close()
        send_data_to_server()

    else:
        # 获取固定常量
        drive_info = get_task_info()
        car_id = os.getenv("CARID", default="MKZ000")
        if drive_info:
            drive_info['car_id'] = car_id
            while True:
                # 获取car的位置信息
                car_locations = current_location()
                if car_locations != []:
                    # 添加常量
                    car_locations.append(drive_info)
                    # 向车端post数据
                    data_transmission(car_locations, drive_info['taskid'])
                    del car_locations[:]
                else:
                    logging.info('inspva Not opened')
        else:
            time.sleep(2)
            send_data_to_server()


# 开始停止发送数据实例化类
class CarData(object):
    """
     Start Data Sending Class
     方法1：start_push 和 stop_push 是通过一个进程去跑这个函数，
     把主父进程pid保存到文件里通过父进程的pid杀死全部的父进程和子进程，
     此方式存在弊端，如果外部调用会阻塞外部调用的进程
     方法2：star 和 stop 是通过执行shell命令运行这个文件，关闭原理和方法一相同
    """

    def start_push(self):
        """
         Write the main thread PID to the file
        """
        subprocess.Popen('rm aduspider.json', shell=True)
        task_id = get_task_id()
        if task_id == 'ok':
            p = Pool(2)
            p.apply_async(send_data_to_server)
            p.apply_async(reckon_position)
            p.close()
            p.join()

    # 外部异步调用
    def star(self):
        """
        start push car datas
        :return: 0
        """
        subprocess.Popen('python car.py >> logs/car_gps.log 2>&1 &', shell=True)

    # 发送停止信号，kill 进程
    def stop(self):
        """
        stop push car datas
        :return: 0
        """
        pid_shell = "ps -ef | grep 'python car.py' |grep -v grep|awk '{print $2}'"
        topic_pids = subprocess.check_output(pid_shell, shell=True).replace('\n', ' ')
        subprocess.check_output('kill -9 {}'.format(topic_pids), shell=True)
        # 先停止car.py 然后在发送停止落盘信号，否则有可能引发超时文件最后一行不是停止落盘的数据
        stop_push_data()
        subprocess.Popen('rm aduspider.json', shell=True)


if __name__ == '__main__':
    CarData().start_push()
