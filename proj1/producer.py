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
import math


from confluent_kafka import Producer
from employee import Employee
import confluent_kafka
import pandas as pd
from confluent_kafka.serialization import StringSerializer


employee_topic_name = "bf_employee_salary"
csv_file = 'Employee_Salaries.csv'

#Can use the confluent_kafka.Producer class directly
class salaryProducer(Producer):
    #if connect without using a docker: host = localhost and port = 29092
    #if connect within a docker container, host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all',
                          'retries': 5}
        super().__init__(producerConfig)
    
    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
     
class DataHandler:
    '''
    Your data handling logic goes here. 
    You can also implement the same logic elsewhere. Your call
    '''
    # Data Handler set up to read the csv file and process based on criteria 
    def __init__(self, csv_file, **kwargs):
        self.csv_file = csv_file
        self.allowed_departments = kwargs.get('allowed_departments', {'ECC', 'CIT', 'EMS'})
        self.min_hire_year = kwargs.get('min_hire_year', 2010)
        self.salary_rounding = kwargs.get('salary_rounding', True)

    def process(self):
        with open(self.csv_file, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)  # Skip header

            for line in csv_reader:
                emp = Employee.from_csv_line(line)

                # Dynamic Department Filter
                if self.allowed_departments and emp.emp_dept not in self.allowed_departments:
                    continue

                # Dynamic Year Filter
                try:
                    hire_year = int(emp.hire_date[-4:])
                except ValueError:
                    continue

                if hire_year < self.min_hire_year:
                    continue

                # Salary rounding
                if self.salary_rounding:
                    emp.emp_salary = math.floor(float(emp.emp_salary))

                yield emp

if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    reader = DataHandler(
        csv_file=csv_file,
        allowed_departments={'ECC', 'CIT', 'EMS'},
        min_hire_year=2010,
        salary_rounding=True
    )
    producer = salaryProducer()
     
    for emp in reader.process():
        producer.produce(
            topic=employee_topic_name,
            key=encoder(emp.emp_dept),
            value=encoder(emp.to_json()),
            callback=salaryProducer.delivery_report
        )
        producer.poll(0)  # Poll to trigger delivery callbacks if any

    # Make sure all messages are sent before exiting
    producer.flush()
    