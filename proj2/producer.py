"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import csv
import json
import os
from confluent_kafka import Producer
from employee import Employee
import confluent_kafka
import pandas as pd
from confluent_kafka.serialization import StringSerializer
import psycopg2
import pymysql
import time

employee_topic_name = "bf_employee_cdc"

class cdcProducer(Producer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
        self.running = True
        self.last_fetched_id = 0  

    def fetch_cdc(self, encoder = StringSerializer('utf-8')):
        try:
            conn = pymysql.connect(
                host='localhost',         # If running inside Docker, change to 'proj2-db_source-1' or proper service name
                port=3308,                # Your Docker-exposed port
                user='user',
                password='password',
                database='source_db',
                cursorclass=pymysql.cursors.DictCursor
            )
            cur = conn.cursor()
            #your logic should go here
            with conn.cursor() as cursor:
                        query = """
                            SELECT cdc_id, emp_id, first_name, last_name, dob, city, action
                            FROM emp_cdc
                            WHERE cdc_id > %s
                            ORDER BY cdc_id ASC;
                        """
                        cursor.execute(query, (self.last_fetched_id,))
                        rows = cursor.fetchall()            
            
            if rows:
                print(f"[CDC] Fetched {len(rows)} new changes.")

            max_cdc_id = self.last_fetched_id
            for row in rows:
                # Optional: Convert date to string
                if row['dob']:
                    row['dob'] = str(row['dob'])

                # Send to Kafka as JSON
                self.produce(
                    topic= employee_topic_name,
                    key= encoder(str(row["emp_id"])),
                    value= encoder(json.dumps(row)),
                    callback = self.delivery_report
                )
                max_cdc_id = max(max_cdc_id, row["cdc_id"])
                self.flush()
                self.last_fetched_id = max_cdc_id 
            conn.close()
        except Exception as err:
            print(f"[Error] Failed fetching CDC: {err}")

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"[Kafka Error] Message delivery failed: {err}")
        else:
            print(f"[Kafka] Message delivered to {msg.topic()} [{msg.partition()}]")
    

if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    producer = cdcProducer()
    
    while producer.running:
        producer.fetch_cdc()
        time.sleep(5) 
    
