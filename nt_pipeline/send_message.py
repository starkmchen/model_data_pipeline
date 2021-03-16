# -*- coding: utf-8 -*-
import urllib
import requests
import json
import sys
import socket



# 获取钉钉消息
def extractionMessage(message):
    # 拼接需要发送的消息
    return "钉！ <font color=orange> "+ message +" </font>"


# 发送钉钉消息
def sendDingDingMessage(url, data):
    headers = {"Content-Type": "application/json; charset=utf-8"}
    requests.post(url, data=json.dumps(data), headers=headers)


# 主函数
def main(message):
    posturl = 'https://oapi.dingtalk.com/robot/send?access_token=db031407266a93b74dd262408c2dc1422690b53c7268d323b17b95ffabab09b2'
    data = {"msgtype": "markdown", "markdown": {"text": extractionMessage(message), "title": "NTModel", "isAtAll": "false"}}
    sendDingDingMessage(posturl, data)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        MESSAGE = sys.argv[1]
    else:
        MESSAGE = "This\'s null message"
    main(MESSAGE)
