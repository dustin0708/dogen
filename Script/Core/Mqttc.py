#-*-coding:utf-8-*-

import sys
import time
import traceback
import paho.mqtt.client as mqttclient
import paho.mqtt.publish as mqttpub

MQTTC_ERR_SUCCESS = 0
MQTTC_ERR_FAILURE = 1

def on_message(client, endpt, message):
    """paho回调接口, 用于分发消息
    """
    endpt.dispatch_message(message)

def on_publish(client, endpt, mid):
    pass

class endpoint(object):
    def __init__(self, client_id=None, clean_session=True, transport='tcp'):
        """初始化MQTT客户端数据
        """
        self.client_id = client_id
        self.clean_session = clean_session
        self.transport = transport
        
        self.mqttc = None
        self.cb_notify_pool = {}
        self.do_connect = False

    def __create_mqttc(self):
        try:
            mqttc = mqttclient.Client(client_id=self.client_id, clean_session=self.clean_session, userdata=self, transport=self.transport)
            mqttc.on_message = on_message
            mqttc.on_publish = on_publish
            return mqttc
        except Exception:
            traceback.print_exc()
            return None
        pass

    def set_server(self, host='127.0.0.1', port=1883, do_connect=False):
        """设置（连接MQTT服务器）
        """
        self.do_connect = do_connect
        self.server = host
        self.port = port
        if (not self.do_connect):
            return MQTTC_ERR_SUCCESS

        self.mqttc = self.__create_mqttc()
        if (self.mqttc is None):
            return MQTTC_ERR_FAILURE

        try:
            rst = self.mqttc.connect(host=self.server, port=self.port)
            if (mqttclient.MQTT_ERR_SUCCESS != rst):
                return MQTTC_ERR_FAILURE
        except Exception:
            traceback.print_exc()
            return MQTTC_ERR_FAILURE

        return MQTTC_ERR_SUCCESS

    def clr_client(self):
        """清理客户端资源
        """
        if (self.mqttc is None):
            return MQTTC_ERR_SUCCESS
        try:
            rst = self.mqttc.disconnect()
            if (mqttclient.MQTT_ERR_SUCCESS != rst):
                self.mqttc = None
                return MQTTC_ERR_FAILURE
        except Exception:
            self.mqttc = None
            traceback.print_exc()
            return MQTTC_ERR_FAILURE

        self.mqttc = None
        return MQTTC_ERR_SUCCESS

    def publish(self, topic, message=None, qos=0, retain=False):
        """客户端发布消息
        """
        if (self.mqttc is not None):
            try:
                (rst, mid) = self.mqttc.publish(topic=topic, payload=message, qos=qos, retain=retain)
                if (mqttclient.MQTT_ERR_SUCCESS != rst):
                    return MQTTC_ERR_FAILURE
            except Exception:
                traceback.print_exc()
                return MQTTC_ERR_FAILURE
        else:
            try:
                mqttpub.single(topic, payload=message, qos=qos, retain=retain, hostname=self.server, port=self.port, client_id=self.client_id, transport=self.transport)
            except Exception:
                traceback.print_exc()
                return MQTTC_ERR_FAILURE
                
        return MQTTC_ERR_SUCCESS

    def subscribe(self, topic, cb_recv_message, shared=False, group=None, qos=0):
        """客户端订阅主题
        """
        self.cb_notify_pool[topic] = cb_recv_message
        if (shared):
            if (group is not None):
                topic = "$share/" + group + "/" + topic
            else:
                topic = "$queue/" + topic
        try:
            (rst, mid) = self.mqttc.subscribe(topic, qos=qos)
            if (mqttclient.MQTT_ERR_SUCCESS != rst):
                return MQTTC_ERR_FAILURE
        except Exception:
            traceback.print_exc()
            return MQTTC_ERR_FAILURE
        
        return MQTTC_ERR_SUCCESS

    def unsubscribe(self, topic):
        """取消订阅
        """
        del self.cb_notify_pool[topic]
        try:
            (rst, mid) = self.mqttc.unsubscribe(topic)
            if (mqttclient.MQTT_ERR_SUCCESS != rst):
                return MQTTC_ERR_FAILURE
        except Exception:
            traceback.print_exc()
            return MQTTC_ERR_FAILURE
        
        return MQTTC_ERR_SUCCESS

    def dispatch_message(self, message):
        """根据消息主题分发消息
        """
        try:
            self.cb_notify_pool[message.topic](self, message.topic, message.payload)
        except Exception:
            pass
        return

    def loop_forever(self):
        """循环调度
        """
        if (self.mqttc is not None):
            self.mqttc.loop_forever()

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.")
