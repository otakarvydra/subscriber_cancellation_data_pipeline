#Importing Packages
import sqlite3
import pandas as pd

#Establishing Connection
con = sqlite3.connect('C:/Users/Dell/OneDrive/Dokumenty/Data Engineering/Codecademy Course/subscriber_cancellation_pipeline/subscriber-pipeline/subscriber_cancellation_pipeline/databases/cademycode_updated.db')
cur = con.cursor()

#Defining DataFrames
students_df = pd.read_sql_query("SELECT * FROM cademycode_students", con)
paths_df = pd.read_sql_query("SELECT * FROM cademycode_courses", con)
jobs_df = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", con)

con.close() 

print(students_df)
print(paths_df)
print(jobs_df)