#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# #############################################################
#
#  Copyright (c) 2014 xxxx.com, Inc. All Rights Reserved
#
# #############################################################
"""
:authors: wanghongrui
:create_date: 2018/1/23
:description: Get GPS data and return
"""

import rospy
import rostopic
import time
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


import types
import copy_reg


def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


copy_reg.pickle(types.MethodType, _pickle_method)




class DataColl(object):
    """
    External Call: After instantiation, call the Star class and return a two-dimensional
    list of [[], [], [],...]. The default is to grab one data in 2 seconds at a time.
    """

    def __init__(self):
        self.datas = []
        self.cun = 0

    def callback(self, data):
        if self.cun == 0:
            if data:
                # if data.insstatus == 3:
                data_one = [data.latitude, data.longitude, data.altitude, data.insstatus, time.time()]
                self.cun = 1
                self.datas.append(data_one)

    def listener(self):
        rospy.init_node(rostopic.NAME, anonymous=True)
        topic_type, real_topic, msg_eval = rostopic.get_topic_type('/sensor/novatel/inspva')
        # Check whether there is a topic
        if real_topic is None:
            return 'error'
        msg_class, real_topic, msg_eval = rostopic.get_topic_class('/sensor/novatel/inspva',
                                                                   blocking=True)
        rospy.Subscriber("/sensor/novatel/inspva", msg_class, self.callback)
        # spin() If the node does not exit, the python program will not exit, blocking operation
        # rospy.spin()

    def star(self, num):
        count = 0
        while count < num:
            self.cun = 0
            self.listener()
            count += 1
            time.sleep(2)
        return self.datas

    def insstatus(self):
        ins_status = self.star(1)
        if ins_status == []:
            ins_status = 0
        else:
            ins_status = ins_status[0][3]
        return ins_status