# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : qq_msg.py
# @Software: PyCharm
# @content : qq相关信息

import argparse
import subprocess
from py4j.java_gateway import JavaGateway


if __name__ == '__main__':
    getaway = JavaGateway()
    random = getaway.jvm.java.util.Random()
    print random.nextInt(10)


