import sqlite3
import pandas as pd

#Connecting to the DWH
con = sqlite3.connect('prod/students_cancellation_warehouse.db')
cur = con.cursor()

#Querying Tables
query_students = 'SELECT * FROM students_dim'
query_jobs = 'SELECT * FROM jobs_dim'
query_paths = 'SELECT * FROM paths_dim'
query_cancellations = 'SELECT * FROM cancellations_fact'

students_df = pd.read_sql_query(query_students, con)
jobs_df = pd.read_sql_query(query_jobs, con)
paths_df = pd.read_sql_query(query_paths, con)
cancellations_df = pd.read_sql_query(query_cancellations, con)

print('printing students')
print(students_df)

print('printing jobs')
print(jobs_df)

print('printing paths')
print(paths_df)

print('printing cancellations')
print(cancellations_df)

cur.close()
con.close()