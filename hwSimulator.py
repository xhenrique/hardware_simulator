import json
import csv
import math
import time
import asyncio
import aiohttp
import datetime
import random

class Simulator:
    def __init__(self):
        self.start = False
        self.running = False
        self.endTrip = False
        self.running_counter = 0
        self.payload_count = 0
        self.num_lines = 0

        self.headers = {
            'Content-Type': 'application/json',
        }
        self.payload_model = open('payload.json')
        self.payload_file1 = json.load(self.payload_model)
        self.payload_model.close()
        self.payload_model2 = open('payload.json')
        self.payload_file2 = json.load(self.payload_model2)
        self.file_name1 = 'zooGuar-zooSP.txt'
        self.file_name2 = 'jabaquara-ericsson.txt'
        self.payload_model.close()

    async def fetch(self,session, url, payload_file):
        start_time = datetime.datetime.now()
        async with session.post(url, json=payload_file) as response:
            return await response.text()

    async def prepareReq(self,payload_list):
        base_url1 = "http://localhost:8080/api/v1/GPSONIBUS/telemetry"
        base_url2 = "http://localhost:8080/api/v1/GPSCARRO/telemetry"
        urls = [base_url1, base_url2]
        tasks = []
        print(payload_list)
        async with aiohttp.ClientSession() as session:
            for payload, url in zip(payload_list, urls):
                tasks.append(self.fetch(session, url, payload))
            await asyncio.gather(*tasks)

    def determinePosition(self,row):
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
        return lat_decimal, lon_decimal


    def startProcess(self):
        while not self.running:
            try:
                with open(self.file_name1) as csvFile1,open(self.file_name2) as csvFile2:
                    readCSV1 = csv.reader(csvFile1, delimiter=',')
                    readCSV2 = csv.reader(csvFile2, delimiter=',')
                    for row1,row2 in zip(readCSV1,readCSV2):
                        if not self.running:
                            x = random.random() #variavel para randomização da velocidade
                            y = random.random() #variavel para randomização da velocidade
                            self.payload_file1['latitude'] = self.determinePosition(row1)[0]
                            self.payload_file1['longitude'] = self.determinePosition(row1)[1]
                            self.payload_file1['vehicleType'] = 'ONIBUS'
                            self.payload_file1['fuel'] = abs(100 - (self.running_counter)/100)
                            self.payload_file1['speed'] = abs(80 - (x*100/80))
                            self.payload_file2['latitude'] = self.determinePosition(row2)[0]
                            self.payload_file2['longitude'] = self.determinePosition(row2)[1]
                            self.payload_file2['vehicleType'] = 'CARRO'
                            self.payload_file2['fuel'] = abs(100 - self.running_counter/100)
                            self.payload_file2['speed'] = abs(80 - (y*100/80))
                            self.payload_list = [self.payload_file1, self.payload_file2]
                            #print(self.payload_list)
                            #print("Payload de running " + str(self.running_counter))
                            self.running_counter = self.running_counter + 1
                            self.payload_count = self.payload_count + 1
                            time.sleep(1)
                            loop = asyncio.get_event_loop()
                            loop.run_until_complete(self.prepareReq(self.payload_list))
                        else:
                            continue
            except IOError as e:
                print(e)

testClass = Simulator()

testClass.startProcess()