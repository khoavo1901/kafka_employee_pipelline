# Kafka Configurations
KAFKA_CONFIG = {
    'bootstrap_servers': 'localhost:29092',      # Change to 'kafka:9092' if running inside Docker
    'employee_cdc_topic': 'bf_employee_cdc',
    'num_partitions': 3,
    'replication_factor': 1,
    'acks': 'all',
    'retries': 5,
    'consumer_group_id': 'cdc_consumer_group',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'poll_timeout': 1.0  # seconds
}

# MySQL Source DB Config (for CDC)
MYSQL_SOURCE_CONFIG = {
    'host': 'localhost',            # Docker users may change to service name (ex: 'proj2-db_source-1')
    'port': 3308,
    'user': 'user',
    'password': 'password',
    'database': 'source_db',
    'cursor_class': 'DictCursor'
}

# MySQL Destination DB Config (for Consumer updating target DB)
MYSQL_DEST_CONFIG = {
    'host': 'localhost',
    'port': 3307,
    'user': 'user',
    'password': 'password',
    'database': 'dest_db',
    'cursor_class': 'DictCursor'
}

# Optional App Settings
APP_SETTINGS = {
    'cdc_fetch_interval': 5,   # in seconds
    'log_level': 'DEBUG'       # DEBUG, INFO, WARNING, ERROR
}
