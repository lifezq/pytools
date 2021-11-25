#!/usr/bin/python
# -*- coding: UTF-8 -*-
# version: python3.6

from functools import partial

import nsq
import tornado.ioloop

nsqdServer = "192.168.1.84:4150"


def producer(topic, message, callback=None):
    # 当writer连接nsqd成功后, 再推送数据
    if not writer.conns:
        return io_loop.add_callback(
            partial(producer, *(topic, message, callback))
        )

    # 推送数据
    writer.pub(topic, bytes(message, encoding="UTF-8"), callback)


def on_finish(conn, status_code):
    # 打印操作返回的结果
    print(conn, status_code)

    # 退出程序
    io_loop.stop()


# 实例化Writer
writer = nsq.Writer([nsqdServer])
# 实例化IOLoop
io_loop = tornado.ioloop.IOLoop.instance()


def pushNsqMessage(topic: str, message: str):
    print("nsq pub msg topic:%s msg:%s" % (topic, message))
    # 将producer函数加入到io_loop的callbacks列表中
    io_loop.add_callback(
        partial(producer, *(topic, message, on_finish))
    )
    # 启动IOLoop, 它会在内部执行callbacks列表中的函数
    io_loop.start()


if __name__ == '__main__':
    pushNsqMessage("test", "test value")
