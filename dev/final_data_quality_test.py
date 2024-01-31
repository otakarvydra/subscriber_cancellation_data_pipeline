#Imports
import unittest
from EnhancedPandas import EnhancedPandas as ep
import pandas as pd
import logging
import sys

#Setting up the Log
logger = logging.getLogger()
stream_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler('application_log.log')
logger.addHandler(file_handler)
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)
file_handler.setLevel(logging.DEBUG)
stream_handler.setLevel(logging.INFO)

#Importing the DataFrames
students_df = pd.read_csv('temporary_files/students.csv')
jobs_df = pd.read_csv('temporary_files/jobs.csv')
paths_df = pd.read_csv('temporary_files/paths.csv')

class ReferentialIntegrityCheck(unittest.TestCase):
    def test_job_id_ref_integrity_check(self):        
        job_ids_from_students = set(students_df['job_id'].unique())        
        job_ids_from_paths = set(jobs_df['job_id'])
        self.assertTrue((job_ids_from_students.issubset(job_ids_from_paths) or job_ids_from_paths == job_ids_from_students), 'this tests if there is referential integrity in the job_id column')

    def test_path_id_ref_integrity_check(self):
        career_path_ids_from_students = set(students_df['current_career_path_id'].unique())
        career_path_ids_from_paths = set(paths_df['career_path_id'])
        self.assertTrue((career_path_ids_from_students.issubset(career_path_ids_from_paths) or career_path_ids_from_paths == career_path_ids_from_students), 'this tests if there is referential integrity in the current_career_path_id column')

class StudentsTest(unittest.TestCase):
    def test_sex_column(self):
        sex_column_set = set(students_df['sex'].unique())
        sex_set = {'M', 'F', 'N'}
        self.assertTrue((sex_column_set.issubset(sex_set) or sex_column_set == sex_set), 'this tests if the values in the sex column are only M, N or F') 

    def test_name_for_bool(self):
        result = ep.check_bool(students_df, 'name')
        self.assertListEqual(result, [], 'this tests if there is a boolean value in the students table and name column')

    def test_name_for_numeric(self):
        result = ep.check_numeric(students_df, 'name')
        self.assertListEqual(result, [], 'this tests if there is a numeric value in the students table and name column')

    def test_email_for_bool(self):
        result = ep.check_bool(students_df, 'email')
        self.assertListEqual(result, [], 'this tests if there is a boolean value in the students table and email column')

    def test_email_for_numeric(self):
        result = ep.check_numeric(students_df, 'email')
        self.assertListEqual(result, [], 'this tests if there is a numeric value in the students table and email column')

    def test_street_for_bool(self):
        result = ep.check_bool(students_df, 'street')
        self.assertListEqual(result, [], 'this tests if there is a boolean value in the students table and street column')

    def test_street_for_numeric(self):
        result = ep.check_numeric(students_df, 'street')
        self.assertListEqual(result, [], 'this tests if there is a numeric value in the students table and street column')
    
    def test_city_for_bool(self):
        result = ep.check_bool(students_df, 'city')
        self.assertListEqual(result, [], 'this tests if there is a boolean value in the students table and city column')

    def test_city_for_numeric(self):
        result = ep.check_numeric(students_df, 'city')
        self.assertListEqual(result, [], 'this tests if there is a numeric value in the students table and city column')

    def test_state_for_bool(self):
        result = ep.check_bool(students_df, 'state')
        self.assertListEqual(result, [], 'this tests if there is a boolean value in the students table and state column')

    def test_state_for_numeric(self):
        result = ep.check_numeric(students_df, 'state')
        self.assertListEqual(result, [], 'this tests if there is a numeric value in the students table and state column')
   
class JobsTest(unittest.TestCase):
    def test_job_id_dtype(self):
        job_id_type = jobs_df['job_id'].dtype
        self.assertTrue(job_id_type == 'int64', 'this tests the correct data type in the job_id column')

    def test_job_category_dtype(self):
        job_category_type = jobs_df['job_category'].dtype
        self.assertTrue(job_category_type == 'object', 'this tests the correct data type in the job_category column')
        
      
    def test_avg_salary_dtype(self):
        avg_salary_type = jobs_df['avg_salary'].dtype
        self.assertTrue(avg_salary_type == 'int64', 'this tests the correct data type in the avg_salary_type column')
        
class PathsTest(unittest.TestCase):
    def test_career_path_id_dtype(self):
        career_path_id_type = paths_df['career_path_id'].dtype
        self.assertTrue(career_path_id_type == 'int64', 'this tests the correct data type in the career_path_id column')

    def test_career_path_name_dtype(self):
        career_path_name_type = paths_df['career_path_name'].dtype
        self.assertTrue(career_path_name_type == 'object', 'this test the correct data type in the career_path_name column')

    def test_hours_to_complete_dtype(self):
        hours_to_complete_type = paths_df['hours_to_complete'].dtype
        self.assertTrue(hours_to_complete_type == 'int64', 'this tests the correct data type in the hours_to_complete column')

print('''\nAutomatic Data Tests will be run, now please visually inspect the following outputs of the sort_check function. 
The function sorts the column and displays 3 values from the top and bottom of the sorted list, 
if there is any unorthodox character or a string, it will be visible''')

print('\nRunning the check on the students table - name column')
ep.sort_check(students_df, 'name', 3)
var = input('Press any key to continue ')
print('\nRunning the check on the students table - email column')
ep.sort_check(students_df, 'email', 3)
var = input('Press any key to continue ')
print('\nRunning the check on the students table - street column')
ep.sort_check(students_df, 'street', 3)
var = input('Press any key to continue ')
print('\nRunning the check on the students table - city column')
ep.sort_check(students_df, 'city', 3)
var = input('Press any key to continue ')
print('\nRunning the check on the students table - state column')
ep.sort_check(students_df, 'state', 3)
var = input('Press any key to continue and run the final tests ')

unittest.main()