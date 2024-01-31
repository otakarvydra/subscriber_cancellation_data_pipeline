import unittest
from EnhancedPandas import EnhancedPandas as ep
import pandas as pd
import sqlite3

#Connecting to the Database
con = sqlite3.connect('databases/cademycode.db')
cur = con.cursor()

#Creating the DataFrames
students_df = pd.read_sql_query("SELECT * FROM cademycode_students", con)
paths_df = pd.read_sql_query("SELECT * FROM cademycode_courses", con)
jobs_df = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", con)
con.close()

#Creating the TestCases
class TableTest(unittest.TestCase):
    def test_columns(self):
        columns_set = set(self.table.columns)
        self.assertTrue(columns_set == self.desired_column_set, 'this tests if the students table has the correct columns')

    def test_check_headers(self):
        columns_with_headers = ep.check_headers(self.table)
        self.assertTrue(columns_with_headers == [], 'this tests if there are column headers in the columns')

    def test_check_empty_values(self):
        for column in self.table.columns:
            with self.subTest():
                self.assertListEqual(ep.check_empty_values(self.table, column), [], '''this tests if there are other strings such as ('empty', 'unknown') indicating empty values in any of the column''') 

class StudentsTest(TableTest):            
    table = students_df
    desired_column_set = set(['uuid', 'name', 'dob', 'sex', 'contact_info', 'job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs'])

class PathsTest(TableTest):
    table = paths_df
    desired_column_set = set(['career_path_id', 'career_path_name', 'hours_to_complete'])

class JobsTest(TableTest):
    table = jobs_df
    desired_column_set = set(['job_id', 'job_category', 'avg_salary'])

#Running the Tests and Loggging the results
suite = unittest.TestLoader().loadTestsFromTestCase(StudentsTest)
unittest.TextTestRunner().run(suite)

suite = unittest.TestLoader().loadTestsFromTestCase(PathsTest)
unittest.TextTestRunner().run(suite)

suite = unittest.TestLoader().loadTestsFromTestCase(JobsTest)
unittest.TextTestRunner().run(suite)
