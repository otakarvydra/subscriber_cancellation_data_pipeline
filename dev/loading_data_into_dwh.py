#Importing Packages
import sqlite3
import pandas as pd
import numpy as np
from EnhancedPandas import EnhancedPandas as ep
from datetime import datetime
import logging
import sys

#Setting up The Log
logger = logging.getLogger()
stream_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler('application_log.log')
logger.addHandler(file_handler)
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)
file_handler.setLevel(logging.DEBUG)
stream_handler.setLevel(logging.INFO)

#Function Definition
def initial_load():
        #Creating dataframes from the temporary_files
        students_dim_dwh_df = students_db_df[['uuid', 'name', 'dob', 'age', 'sex', 'email', 'street', 'city', 'state', 'postcode', 'time_spent_hrs', 'num_course_taken']]
        cancellations_fact_dwh_df = students_db_df[['uuid', 'job_id', 'current_career_path_id']]
        jobs_dim_dwh_df = jobs_db_df[['job_id', 'job_category', 'avg_salary']]
        jobs_dim_dwh_df['row_effective_date'] = np.datetime64('today', 'D')
        jobs_dim_dwh_df['row_expiration_date'] = np.nan
        jobs_dim_dwh_df['current_row_indicator'] = True
        paths_dim_dwh_df = paths_db_df[['career_path_id', 'career_path_name', 'hours_to_complete']]
        paths_dim_dwh_df['row_effective_date'] = np.datetime64('today', 'D')
        paths_dim_dwh_df['row_expiration_date'] = np.nan
        paths_dim_dwh_df['current_row_indicator'] = True

        #Renaming the dataframes to match the columns in the dwh
        students_dim_dwh_df = students_dim_dwh_df.rename(columns={'uuid': 'student_id', 'dob': 'date_of_birth', 'time_spent_hrs': 'time_spent_week_hrs'})
        cancellations_fact_dwh_df = cancellations_fact_dwh_df.rename(columns = {'uuid': 'student_id', 'current_career_path_id': 'career_path_id'})
        jobs_dim_dwh_df = jobs_dim_dwh_df.rename(columns = {'job_category': 'category', 'avg_salary': 'salary'})
        paths_dim_dwh_df = paths_dim_dwh_df.rename(columns = {'career_path_id': 'path_id', 'career_path_name': 'path_name'})

        #Inserting the Tables into the Database
        students_dim_dwh_df.to_sql('students_dim', con, index = False, if_exists = 'append')
        cancellations_fact_dwh_df.to_sql('cancellations_fact', con, index = False, if_exists = 'append')
        jobs_dim_dwh_df.to_sql('jobs_dim', con, index = False, if_exists = 'append')
        paths_dim_dwh_df.to_sql('paths_dim', con, index = False, if_exists = 'append')

def update_jobs_dimension(dwh_table, db_table):
    #Defining lambda functions
    current_row_indicator_depreciate = lambda row: False if (row.job_category != row.category or row.avg_salary != row.salary) else True
    expiration_date_depreciate = lambda row: np.datetime64('today', 'D') if (row.job_category != row.category or row.avg_salary != row.salary) else row.row_expiration_date
    
    dwh_depreciator_1 = lambda row: False if row.current_row_indicator_y == False else row.current_row_indicator_x
    dwh_depreciator_2 = lambda row: row.row_expiration_date_y if row.current_row_indicator_y == False else row.row_expiration_date_x

    #Merging jobs_dimension table and jobs_db table
    dwh_table_actual = dwh_table[dwh_table['current_row_indicator'] == True]
    jobs_merged = pd.merge(db_table, dwh_table_actual, how='left', left_on='job_id', right_on='job_id')

    #Defining df for rows to be inserted
    rows_insert = pd.DataFrame(columns=['job_id', 'category', 'salary', 'row_effective_date', 'row_expiration_date', 'current_row_indicator'])
    
    #Identifying and inserting new rows
    if jobs_merged['category'].isnull().sum() != 0:
        new_jobs = jobs_merged[jobs_merged['category'].isna() == True]
        
        new_jobs['row_effective_date'] = np.datetime64('today', 'D')
        new_jobs['row_expiration_date'] = np.nan
        new_jobs['current_row_indicator'] = True

        new_jobs = new_jobs[['job_id', 'job_category', 'avg_salary', 'row_effective_date', 'row_expiration_date', 'current_row_indicator']]
        new_jobs = new_jobs.rename(columns={'job_category': 'category', 'avg_salary': 'salary'})
        rows_insert = pd.concat([rows_insert, new_jobs], axis=0, ignore_index=True)

    #Depreciating old records in the dwh table
    jobs_merged = jobs_merged[jobs_merged['category'].isna() != True]
    jobs_merged['current_row_indicator'] = jobs_merged.apply(current_row_indicator_depreciate, axis = 1)
    jobs_merged['row_expiration_date'] = jobs_merged.apply(expiration_date_depreciate, axis = 1)

    depreciated_jobs = jobs_merged[jobs_merged['current_row_indicator'] == False]

    if not len(depreciated_jobs) == 0:
        depreciated_jobs = depreciated_jobs[['record_id', 'row_expiration_date', 'current_row_indicator']]
        dwh_table_merged = pd.merge(dwh_table, depreciated_jobs, how='left', left_on='record_id', right_on='record_id')
        dwh_table_merged['current_row_indicator_x'] = dwh_table_merged.apply(dwh_depreciator_1, axis = 1)
        dwh_table_merged['row_expiration_date_x'] = dwh_table_merged.apply(dwh_depreciator_2, axis = 1)
        dwh_table_merged = dwh_table_merged[['record_id', 'job_id', 'category', 'salary', 'row_effective_date', 'row_expiration_date_x', 'current_row_indicator_x']]
        dwh_table_merged = dwh_table_merged.rename(columns={'row_expiration_date_x': 'row_expiration_date', 'current_row_indicator_x': 'current_row_indicator'})
        cur.execute('''
        DELETE FROM jobs_dim
        ''')       
        dwh_table_merged.to_sql('jobs_dim', con, index = False, if_exists = 'append')

        #Updating the changed rows 
        changed_rows_1 = jobs_merged[jobs_merged['job_category'] != jobs_merged['category']]
        changed_rows_2 = jobs_merged[jobs_merged['avg_salary'] != jobs_merged['salary']]
        changed_rows = pd.concat([changed_rows_1, changed_rows_2], axis=0, ignore_index=True)
        changed_rows = changed_rows.drop_duplicates()  
        changed_rows['category'] = changed_rows['job_category']
        changed_rows['salary'] = changed_rows['avg_salary']
        changed_rows['row_effective_date'] = np.datetime64('today', 'D')
        changed_rows['row_expiration_date'] = np.nan
        changed_rows['current_row_indicator'] = True
        changed_rows = changed_rows[['job_id', 'category', 'salary', 'row_effective_date', 'row_expiration_date', 'current_row_indicator']]
        rows_insert = pd.concat([rows_insert, changed_rows], axis=0, ignore_index=True)

    #Inserting new rows into the dwh
    rows_insert.to_sql('jobs_dim', con, index=False, if_exists='append')

def update_paths_dimension(dwh_table, db_table):
    #Defining lambda functions
    current_row_indicator_depreciate = lambda row: False if (row.career_path_name != row.path_name or row.hours_to_complete_x != row.hours_to_complete_y) else True
    expiration_date_depreciate = lambda row: np.datetime64('today', 'D') if (row.career_path_name != row.path_name or row.hours_to_complete_x != row.hours_to_complete_y) else row.row_expiration_date

    dwh_depreciator_1 = lambda row: False if row.current_row_indicator_y == False else row.current_row_indicator_x
    dwh_depreciator_2 = lambda row: row.row_expiration_date_y if row.current_row_indicator_y == False else row.row_expiration_date_x

    #Merging paths_dimension table and paths_db table
    dwh_table_actual = dwh_table[dwh_table['current_row_indicator'] == True]
    paths_merged = pd.merge(db_table, dwh_table_actual, how='outer', left_on = 'career_path_id', right_on = 'path_id')

    #Defining df for rows to be inserted
    rows_insert = pd.DataFrame(columns=['path_id', 'path_name', 'hours_to_complete', 'row_effective_date', 'row_expiration_date', 'current_row_indicator'])
    
    #Identifying new records
    if paths_merged['path_name'].isnull().sum() != 0:
        new_paths = paths_merged[paths_merged['path_name'].isna() == True]
        
        new_paths['row_effective_date'] = np.datetime64('today', 'D')
        new_paths['row_expiration_date'] = np.nan
        new_paths['current_row_indicator'] = True

        new_paths = new_paths[['career_path_id', 'career_path_name', 'hours_to_complete_x', 'row_effective_date', 'row_expiration_date', 'current_row_indicator']]
        new_paths = new_paths.rename(columns={'career_path_id': 'path_id', 'career_path_name': 'path_name', 'hours_to_complete_x': 'hours_to_complete'})
        rows_insert = pd.concat([rows_insert, new_paths], axis=0, ignore_index=True)
    
    #Depreciating old records in the dwh table
    paths_merged = paths_merged[paths_merged['path_name'].isna() != True]
    paths_merged['current_row_indicator'] = paths_merged.apply(current_row_indicator_depreciate, axis = 1)
    paths_merged['row_expiration_date'] = paths_merged.apply(expiration_date_depreciate, axis = 1)

    depreciated_paths = paths_merged[paths_merged['current_row_indicator'] == False]

    if not len(depreciated_paths) == 0:
        depreciated_paths = depreciated_paths[['record_id', 'row_expiration_date', 'current_row_indicator']]
        dwh_table_merged = pd.merge(dwh_table, depreciated_paths, how='left', left_on='record_id', right_on='record_id')
        dwh_table_merged['current_row_indicator_x'] = dwh_table_merged.apply(dwh_depreciator_1, axis = 1)
        dwh_table_merged['row_expiration_date_x'] = dwh_table_merged.apply(dwh_depreciator_2, axis = 1)
        dwh_table_merged = dwh_table_merged[['record_id', 'path_id', 'path_name', 'hours_to_complete', 'row_effective_date', 'row_expiration_date_x', 'current_row_indicator_x']]
        dwh_table_merged = dwh_table_merged.rename(columns={'row_expiration_date_x': 'row_expiration_date', 'current_row_indicator_x': 'current_row_indicator'})
        cur.execute('''
        DELETE FROM paths_dim
        ''')
        dwh_table_merged.to_sql('paths_dim', con, index = False, if_exists = 'append')
    
        #Updating the changed rows    
        changed_rows_1 = paths_merged[paths_merged['career_path_name'] != paths_merged['path_name']]
        changed_rows_2 = paths_merged[paths_merged['hours_to_complete_x'] != paths_merged['hours_to_complete_y']]
        changed_rows = pd.concat([changed_rows_1, changed_rows_2], axis=0, ignore_index=True)
        changed_rows = changed_rows.drop_duplicates() 
        changed_rows['path_name'] = changed_rows['career_path_name']
        changed_rows['hours_to_complete_y'] = changed_rows['hours_to_complete_x']
        changed_rows['row_effective_date'] = np.datetime64('today', 'D')
        changed_rows['row_expiration_date'] = np.nan
        changed_rows['current_row_indicator'] = True
        changed_rows = changed_rows[['career_path_id', 'path_name', 'hours_to_complete_y', 'row_effective_date', 'row_expiration_date', 'current_row_indicator']]
        changed_rows = changed_rows.rename(columns={'career_path_id': 'path_id', 'hours_to_complete_y': 'hours_to_complete'})
        rows_insert = pd.concat([rows_insert, changed_rows], axis=0, ignore_index=True)

    #Loading the data into dwh
    rows_insert.to_sql('paths_dim', con, index=False, if_exists='append')

def update_new_cancellations(students_db_table, cancellations_table):
    cancellations_merged = pd.merge(students_db_table, cancellations_table, how = 'outer', left_on = 'uuid', right_on = 'student_id')
    new_cancellations = cancellations_merged[cancellations_merged['job_id_y'].isna() == True]
    new_cancellations_student_dim_part = new_cancellations[['uuid', 'name', 'dob', 'sex', 'num_course_taken', 'time_spent_hrs', 'email', 'street', 'city', 'state', 'postcode', 'age']]
    new_cancellations_cancellation_fact_part = new_cancellations[['uuid', 'job_id_x', 'current_career_path_id']]
    new_student_dim = new_cancellations_student_dim_part.rename(columns ={'uuid': 'student_id', 'dob': 'date_of_birth', 'time_spent_hrs': 'time_spent_week_hrs'})
    new_cancellation_fact = new_cancellations_cancellation_fact_part.rename(columns ={'uuid': 'student_id', 'current_career_path_id': 'career_path_id', 'job_id_x': 'job_id'})

    #Fetching updated Data from the DWH
    jobs_dim_dwh_df = pd.read_sql_query("SELECT * FROM jobs_dim", con)
    paths_dim_dwh_df = pd.read_sql_query("SELECT * FROM paths_dim", con)
    jobs_dim_dwh_df = jobs_dim_dwh_df[jobs_dim_dwh_df['current_row_indicator'] == True]
    paths_dim_dwh_df = paths_dim_dwh_df[paths_dim_dwh_df['current_row_indicator'] == True]

    #Replacing job_ids with current record_ids
    cancellations_merged_jobs = pd.merge(new_cancellation_fact, jobs_dim_dwh_df, how='inner', left_on='job_id', right_on='job_id')
    cancellations_merged_jobs['job_id'] = cancellations_merged_jobs['record_id']
    new_cancellation_fact = new_cancellation_fact[['student_id', 'job_id', 'career_path_id']]

    #Replacing path_ids with current record_ids
    cancellations_merged_paths = pd.merge(new_cancellation_fact, paths_dim_dwh_df, how='inner', left_on='career_path_id', right_on='path_id')
    cancellations_merged_paths['career_path_id'] = cancellations_merged_paths['record_id']
    new_cancellation_fact = new_cancellation_fact[['student_id', 'job_id', 'career_path_id']]

    #Uploading the updated DataFrames
    new_student_dim.to_sql('students_dim', con, index = False, if_exists='append')
    new_cancellation_fact.to_sql('cancellations_fact', con, index = False, if_exists='append')

#Establishing Connection to the DWH
con = sqlite3.connect('prod/students_cancellation_warehouse.db')
cur = con.cursor()

#Fetching Data from the DWH
students_dim_dwh_df = pd.read_sql_query("SELECT * FROM students_dim", con)
cancellations_fact_dwh_df = pd.read_sql_query("SELECT * FROM cancellations_fact", con)
jobs_dim_dwh_df = pd.read_sql_query("SELECT * FROM jobs_dim", con)
paths_dim_dwh_df = pd.read_sql_query("SELECT * FROM paths_dim", con)

#Fetching Data from the Database
students_db_df = pd.read_csv('temporary_files/students.csv')
jobs_db_df = pd.read_csv('temporary_files/jobs.csv')
paths_db_df = pd.read_csv('temporary_files/paths.csv')

#Loading the Data
if len(cancellations_fact_dwh_df) == 0:
    initial_load()
    logger.log(logging.DEBUG, '\nDWH loaded with data for the first time')
else:
    update_jobs_dimension(jobs_dim_dwh_df, jobs_db_df)
    logger.log(logging.DEBUG, '\njobs dimension table up-to-date')
    update_paths_dimension(paths_dim_dwh_df, paths_db_df)
    logger.log(logging.DEBUG, '\npaths dimension table up-to-date')
    update_new_cancellations(students_db_df, cancellations_fact_dwh_df)
    logger.log(logging.DEBUG, '\ncancellations fact table up-to-date')

cur.close()
con.close()