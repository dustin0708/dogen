from Script.Core import Mqttc
import time

puber = Mqttc.endpoint(client_id='puber')

print(puber.set_server(host='47.97.214.213', do_connect=True))
print(puber.publish('/test', message='Hello, the world!'))
print(puber.clr_client())
