import json
import csv
import math
import time
import asyncio
import aiohttp
import datetime
import random

start = False
running = False
endTrip = False
running_counter = 0
payload_count = 0
num_lines = 0

headers = {
    'Content-Type': 'application/json',
}
payload_model = open('payload.json')
payload_file1 = json.load(payload_model)
payload_model.close()
payload_model2 = open('payload.json')
payload_file2 = json.load(payload_model2)
file_name1 = 'zooGuar-zooSP.txt'
file_name2 = 'jabaquara-ericsson.txt'
payload_model.close()


def determinePosition(row):
    lat = row[2]
    orient = row[3]
    lon = row[4]
    direction = row[5]
    altitude = round(float(row[8]))
    latdec = round(math.floor(float(lat) / 100) + (float(lat) % 100) / 60, 6)
    if orient == 'S':
        lat_decimal = latdec * -1
    londec = round(math.floor(float(lon) / 100) + (float(lon) % 100) / 60, 6)
    if direction == 'W':
        lon_decimal = londec * -1
    return lat_decimal,lon_decimal

async def fetch(session, url,payload_file):
    start_time = datetime.datetime.now()
    async with session.post(url, json = payload_file) as response:
        return await response.text()

async def main(payload_list):
    base_url1 = "http://localhost:8080/api/v1/GPSONIBUS/telemetry"
    base_url2 = "http://localhost:8080/api/v1/GPSCARRO/telemetry"
    urls = [base_url1,base_url2]
    tasks = []
    #print(payload_list)
    async with aiohttp.ClientSession() as session:
        for payload,url in zip(payload_list,urls):
            tasks.append(fetch(session, url, payload))
            #print(payload,url)
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    while not running:
        try:
            with open(file_name1) as csvFile1,open(file_name2) as csvFile2:
                readCSV1 = csv.reader(csvFile1, delimiter=',')
                readCSV2 = csv.reader(csvFile2, delimiter=',')
                for row1,row2 in zip(readCSV1,readCSV2):
                    if running == False:
                        print(row1,row2)
                        x = random.random() #variavel para randomização da velocidade
                        y = random.random() #variavel para randomização da velocidade
                        payload_file1['latitude'] = determinePosition(row1)[0]
                        payload_file1['longitude'] = determinePosition(row1)[1]
                        payload_file1['speed'] = abs(80 - (x*1000/12))
                        payload_file2['latitude'] = determinePosition(row2)[0]
                        payload_file2['longitude'] = determinePosition(row2)[1]
                        payload_file2['speed'] = abs(80 - (y*1000/12))
                        payload_list = [payload_file1, payload_file2]
                        print(payload_list)
                        print("Payload de running " + str(running_counter))
                        running_counter = running_counter + 1
                        payload_count = payload_count + 1
                        time.sleep(2)
                        loop = asyncio.get_event_loop()
                        loop.run_until_complete(main(payload_list))
                    else:
                        continue
        except IOError as e:
            print(e)