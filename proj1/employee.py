import json
import csv
from datetime import date

class Employee:
    def __init__(self,  emp_dept: str = '', emp_dept_div: str = '', pos_title: str = '', hire_date: str = '', emp_salary: int = 0):
        self.emp_dept = emp_dept
        self.emp_dept_div = emp_dept_div
        self.pos_title = pos_title
        self.hire_date = hire_date
        self.emp_salary = emp_salary
        
    @staticmethod
    def from_csv_line(line):
        return Employee(
            emp_dept=line[0],
            emp_dept_div=line[1],
            pos_title=line[3],
            hire_date=line[5],
            emp_salary=line[7]
    )

    def to_json(self):
        return json.dumps(self.__dict__)

if __name__ ==  '__main__':
    with open('Employee_Salaries.csv', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        
        # Skip header if the CSV has one
        next(csv_reader, None)
        counter = 0
        for line in csv_reader:
            counter += 1
            emp = Employee.from_csv_line(line)
            print(emp.to_json())
            if counter == 20:
                break
 