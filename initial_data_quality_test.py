import unittest
from EnhancedPandas import EnhancedPandas as ep
import pandas as pd
import sqlite3

#Connecting to the Database
con = sqlite3.connect('C:/Users/Dell/OneDrive/Dokumenty/Data Engineering/Codecademy Course/subscriber_cancellation_pipeline/subscriber-pipeline/subscriber_cancellation_pipeline/databases/cademycode.db')
cur = con.cursor()

#Creating the DataFrame
students_df = pd.read_sql_query("SELECT * FROM cademycode_students", con)

#Creating the TestCases
class StudentsTest(unittest.TestCase):
    def test_columns(self):
        columns_set = set(students_df.columns)
        desired_set = {'uuid', 'name', 'dob', 'sex', 'contact_info', 'job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs'}
        self.assertTrue(columns_set == desired_set)

    def test_check_headers(self):
        columns_with_headers = ep.check_headers(students_df)
        self.assertTrue(columns_with_headers == [])

    def test_check_empty_values(self):
        for column in students_df.columns:
            with self.subTest():
                self.assertListEqual(ep.check_empty_values(students_df, column), []) 

    def test_sex_column(self):
        sex_column_set = set(students_df['sex'].unique())
        assert_set = {'M', 'N', 'F'}
        self.assertSetEqual(sex_column_set, assert_set)

unittest.main()           