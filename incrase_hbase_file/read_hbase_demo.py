# Author:Dengwenxing
# -*- coding: utf-8 -*-
# @Time     :2019/12/30 15:09
# @Site     :
# @fILE     : hbaseReader.py
# @Software :

from thrift.transport import TSocket,TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *


transport = TSocket.TSocket('24.1.17.3',9090)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)
transport.open()

def gettables():
    print client.getTableNames()


if __name__ == '__main__':
    gettables()
