import os, time
import random
from paho.mqtt import client as mqtt_client


rootpath = "/data/"
paths = ["raw/phone/accel/", "raw/phone/gyro/", "raw/watch/accel/", "raw/watch/gyro/"]
WAIT_TIME = 0.5
MAX_LINES = 1000000000 #connect mosquitto
i = 0 
time.sleep(20)
#os.system('curl -d @/home/oier/Documents/kafka/config/connect-mqtt-source.json -H "Content-Type:application/json" -X POST http://localhost:8083/connectors') ##HEMEN ZURE PATH-A JARRI
broker = 'mosquitto'
port = 1883
client = mqtt_client.Client()
client.connect(broker,port)
for path in paths:
	data_files = sorted(os.listdir(rootpath+path))
	for data_file in data_files:
		with open(rootpath+path+data_file, "r") as f:
			maxl = 0
			while True:
				try:
					line = f.readline()
				except UnicodeDecodeError:
					continue
				if not line or len(line) == 1:
					break
				line = f.readline().split(",")
				try:
					if line[-1][-1] == ";":
						line[-1] = line[-1][:-1]
					elif line[-1][-1] == "\n":
						line[-1] = line[-1][:-2]
				except:
					continue
				line = '{"usid":' + line[0] + ',"action": "' + line[1] + '","ts": ' + line[2] + ',"x":' + line[3] + ',"y":' + line[4] + ',"z":' + line[5] + '}'
				 #publish egin
				time.sleep(WAIT_TIME)
				result = client.publish("smart",line)
				maxl += 1
				if maxl > MAX_LINES:
					break
				i += 1
