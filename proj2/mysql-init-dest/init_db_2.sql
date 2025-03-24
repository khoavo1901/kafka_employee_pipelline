USE dest_db;

CREATE TABLE employees( 
    emp_id SERIAL, 
    first_name VARCHAR(100), 
    last_name VARCHAR(100), 
    dob DATE, 
    city VARCHAR(100));