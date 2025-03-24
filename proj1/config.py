KAFKA_CONFIG = {
    # If running outside Docker, use 'localhost:29092'
    # If running inside Docker, use 'kafka:9092'
    'bootstrap_servers': 'localhost:29092',
    'topic_name': 'bf_employee_salary',
    'num_partitions': 3,
    'replication_factor': 1,
    'acks': 'all',
    'retries': 5,
    'consumer_group_id': 'salary-consumer-group',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
}

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'alex',
    'password': 'mysql',
    'database': 'proj1',
}

CSV_CONFIG = {
    'csv_file_path': 'Employee_Salaries.csv',
    'allowed_departments': {'ECC', 'CIT', 'EMS'},
    'min_hire_year': 2010,
    'salary_rounding': True
}

# Optional: Add general app settings
APP_SETTINGS = {
    'log_level': 'DEBUG',  
    'max_poll_timeout': 1.0,  
}

