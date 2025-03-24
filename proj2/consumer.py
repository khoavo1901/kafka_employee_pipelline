import json
import random
import string
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from employee import Employee
from producer import employee_topic_name
import pymysql
from config import KAFKA_CONFIG, MYSQL_DEST_CONFIG

class cdcConsumer(Consumer):
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
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
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
            self.close()

def update_dst(msg):
    data = json.loads(msg.value())
    try:
        conn = pymysql.connect(
            host=MYSQL_DEST_CONFIG['host'],
            port=MYSQL_DEST_CONFIG['port'],
            user=MYSQL_DEST_CONFIG['user'],
            password=MYSQL_DEST_CONFIG['password'],
            database=MYSQL_DEST_CONFIG['database'],
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cur:
            action = data.get("action")
            emp_id = data.get("emp_id")

            if action == 'insert':
                insert_query = """
                    INSERT INTO employees (emp_id, first_name, last_name, dob, city)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE first_name=%s, last_name=%s, dob=%s, city=%s;
                """
                cur.execute(insert_query, (
                    emp_id, data.get("first_name"), data.get("last_name"), data.get("dob"), data.get("city"),
                    data.get("first_name"), data.get("last_name"), data.get("dob"), data.get("city")
                ))
                print(f"[DB] Inserted/Updated employee emp_id: {emp_id}")

            elif action == 'update':
                update_query = """
                    UPDATE employees
                    SET first_name=%s, last_name=%s, dob=%s, city=%s
                    WHERE emp_id=%s;
                """
                cur.execute(update_query, (
                    data.get("first_name"), data.get("last_name"), data.get("dob"), data.get("city"), emp_id
                ))
                print(f"[DB] Updated employee emp_id: {emp_id}")

            elif action == 'delete':
                delete_query = "DELETE FROM employees WHERE emp_id = %s;"
                cur.execute(delete_query, (emp_id,))
                print(f"[DB] Deleted employee emp_id: {emp_id}")

            else:
                print(f"[Warning] Unknown action: {action}")

            conn.commit()
    except Exception as err:
        print(f"[DB Error] {err}")

if __name__ == '__main__':
    consumer = cdcConsumer(group_id= 'cdc_consumer_group') 
    consumer.consume([employee_topic_name], update_dst) 