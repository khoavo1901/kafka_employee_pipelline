import json
import random
import string
import sys
from datetime import datetime
import pymysql
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from producer import employee_topic_name
from config import KAFKA_CONFIG, MYSQL_CONFIG


class SalaryConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        self.conf = {
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'],
            'group.id': KAFKA_CONFIG['consumer_group_id'],
            'enable.auto.commit': KAFKA_CONFIG['enable_auto_commit'],
            'auto.offset.reset': KAFKA_CONFIG['auto_offset_reset']
        }
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
                host=MYSQL_CONFIG['host'],
                port=MYSQL_CONFIG['port'],
                user=MYSQL_CONFIG['user'],
                password=MYSQL_CONFIG['password'],
                database=MYSQL_CONFIG['database']
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