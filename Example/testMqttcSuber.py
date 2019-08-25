from Script.Core import Mqttc
import time

suber = Mqttc.endpoint(client_id='suber')

print(suber.set_server(host='47.97.214.213', do_connect=True))

def print_message(suber, topic, message):
    print(topic + " " + str(message))

print(suber.subscribe('/test', print_message))


suber.loop_forever()