#testPostUrlLib
import urllib.request
import urllib.response
import json
import csv
import math
from math import sin, cos, sqrt, atan2, radians
import time

userName = "user"
passWord  = "password"
top_level_url = "http://193.161.184.130:8181/adapter/telemetry/payload"

# create an authorization handler
p = urllib.request.HTTPPasswordMgrWithDefaultRealm()
p.add_password(None, top_level_url, userName, passWord);

auth_handler = urllib.request.HTTPBasicAuthHandler(p)
opener = urllib.request.build_opener(auth_handler)

urllib.request.install_opener(opener)

payload_count = 0
headers = {
    'Content-Type': 'application/json',
    'Accept': 'text/plain',
}
payload_model = open('payload.json')
payload_file = json.load(payload_model)

with open('waypoints_nmea.txt') as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        lat=row[2]
        orient = row[3]
        lon = row[4]
        direction = row[5]
        altitude = round(float(row[8]))
        latdec = round(math.floor(float(lat) / 100) + (float(lat) % 100) / 60, 6)
        if orient == 'S':
            lat_decimal = latdec * -1
        londec = round(math.floor(float(lon) / 100) + (float(lon) % 100) / 60, 6)
        if direction =='W':
            lon_decimal = londec*-1
        print(lat_decimal)
        print(lon_decimal)

        #Passando as coordenadas em decimal para o json enviado
        payload_file['d']['message']['latitude'] = lat_decimal
        payload_file['d']['message']['longitude'] = lon_decimal
        payload_file['d']['message']['altitude'] = altitude

        #Preparando arquivo para enviar
        payload_out_name = str(payload_count) + ' - payload_out.json'
        payload_out = open(payload_out_name, 'w')
        json.dump(payload_file, payload_out)
        try:
        	req = urllib.request.Request(top_level_url, data=payload_out, headers={'Content-Type': 'application/json'})
        	result = opener.open(req)
        	messages = result.read()
        	print (messages)
        except IOError as e:
        	print(e)
        payload_out.close()
        time.sleep(0.75)
       