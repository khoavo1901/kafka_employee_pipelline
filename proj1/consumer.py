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

import json
import random
import string
import sys
from datetime import datetime
import pymysql
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from producer import employee_topic_name #you do not want to hard copy it

class SalaryConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        
        #self.consumer = Consumer(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
        #implement your message processing logic here. Not necessary to follow the template. 
        try:
            self.subscribe(topics)
            while self.keep_runnning:
                msg = self.poll(timeout=1.0)
                if not msg:
                    pass
                elif msg.error():
                    print("error")
                else:
                    #can implement other logics for msg
                    processing_func(msg)
        finally:
            # Close down consumer to commit final offsets.
            self.close()

#or can put all functions in a separte file and import as a module
class ConsumingMethods:
    @staticmethod
    def add_salary(msg):
        e = Employee(**(json.loads(msg.value())))
        print(e)
        try:
            conn = pymysql.connect(
                host='localhost',
                port=3306,
                user='alex',
                password='mysql',
                database='proj1'
            )
            cur = conn.cursor()
            #your logic goes here
            insert_employee = """
                                INSERT INTO Department_Employee (department, department_division, position_title, hire_date, salary)
                                VALUES (%s, %s, %s, %s, %s)
                                """
            update_dept_salary =  """
                                    INSERT INTO Department_Employee_Salary (department, total_salary)
                                    VALUES (%s, %s)
                                    ON DUPLICATE KEY UPDATE total_salary = total_salary + %s
                                    """
            cur.execute(insert_employee, (
                e.emp_dept,
                e.emp_dept_div,
                e.pos_title,
                datetime.strptime(e.hire_date, '%d-%b-%Y'),
                e.emp_salary
            ))
            print("Inserted employee salary into database")
            cur.execute("""
                INSERT INTO Department_Employee_Salary (department, total_salary)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE total_salary = total_salary + %s
            """, (e.emp_dept, e.emp_salary, e.emp_salary))
            conn.commit()  
            cur.close()
            conn.close()
        except Exception as err:
            print({err})

if __name__ == '__main__':
    consumer = SalaryConsumer(group_id='salary-consumer-group') 
    consumer.consume(["bf_employee_salary"],ConsumingMethods.add_salary) 