USE source_db;

GRANT TRIGGER ON source_db.* TO 'user'@'%';
FLUSH PRIVILEGES;

CREATE TABLE employees (
    emp_id SERIAL, 
    first_name VARCHAR(100), 
    last_name VARCHAR(100), 
    dob DATE, 
    city VARCHAR(100)
);

CREATE TABLE emp_cdc (
    cdc_id INT AUTO_INCREMENT PRIMARY KEY,  -- unique CDC event id
    emp_id INT,                             -- refers to employees.emp_id
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    action VARCHAR(10),                     -- 'insert', 'update', 'delete'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELIMITER $$

CREATE TRIGGER employee_insert_trigger
AFTER INSERT ON employees
FOR EACH ROW
BEGIN
    INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, action)
    VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, 'insert');
END$$

CREATE TRIGGER employee_update_trigger
AFTER UPDATE ON employees
FOR EACH ROW
BEGIN
    INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, action)
    VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, 'update');
END$$

CREATE TRIGGER employee_delete_trigger
AFTER DELETE ON employees
FOR EACH ROW
BEGIN
    INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, action)
    VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, 'delete');
END$$

DELIMITER ;
