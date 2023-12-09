#Importing Packages
import sqlite3
import pandas as pd
import numpy as np
from EnhancedPandas import EnhancedPandas as ep
from datetime import datetime
import logging
import sys

#Setting up the Log
logger = logging.getLogger('students_df_logger')
stream_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler('student_df_cleaning.log')
logger.addHandler(file_handler)
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)

#Connecting to the Database
con = sqlite3.connect('C:/Users/Dell/OneDrive/Dokumenty/Data Engineering/Codecademy Course/subscriber_cancellation_pipeline/subscriber-pipeline/subscriber_cancellation_pipeline/databases/cademycode.db')
cur = con.cursor()
logger.log(logging.INFO, ('connection to the database made at ' + str(datetime.now())))

#Creating the DataFrame
paths_df = pd.read_sql_query("SELECT * FROM cademycode_courses", con)
con.close()

#Removing Duplicates
def remove_duplicates(paths_df):
    duplicated_paths_dfs_df = paths_df[paths_df.duplicated()]

    if len(duplicated_paths_dfs_df) == 0:
        logger.log(logging.INFO, 'no duplicated values were found')
    else:
        logger.log(logging.INFO, 'dropping duplicated paths_dfs:')
        logger.log(logging.INFO, duplicated_paths_dfs_df.to_string())
        paths_df = paths_df.drop_duplicates()

remove_duplicates(paths_df)

def remove_other_missing_data(paths_df):
    missing_dfs = {}
    missing_df = ep.viewna_all(paths_df)
    columns_missing = missing_df[missing_df['missing count'] > 0].reset_index(drop = True)
    if len(columns_missing) == 0:
        print('no columns have missing data')
        return(paths_df, [])
    else:
        print('these columns still posses missing data:')
        print(columns_missing)

    while True:
        missing_df = ep.viewna_all(paths_df)
        print(missing_df)
        input_1 = input('what column do you wish to investigate? (to exit press e) ')
        if input_1.lower() == 'e':
            break

        try:
            paths_df[input_1]
        except KeyError:
            continue

        while True:
            print('Investigating column ' + input_1)
            input_2 = input('''insert column to generate a dataframe that shows the sum of missing data values grouped by the values in the provided column - to exit press e
the available columns are: 'job_id', 'job_category', 'avg_salary' ''')
            if input_2.lower() == 'e':
                break
            try:
                print('results for ' + input_2)    
                print(ep.viewna_column(paths_df, input_1, input_2))         
            except KeyError:
                print('invalid column name provided')
                continue
            
        while True:
            input_3 = input('Do you wish to drop the rows containing the missing values? y/n ')
            if input_3.lower() == 'y':
                empty_values_df = paths_df[paths_df[input_1].isnull() == True]
                missing_dfs[input_1] = empty_values_df
                paths_df = paths_df.dropna(subset=input_1)
                break
            if input_3.lower() == 'n':
                break
            else:
                print('invalid value provided')
                continue
    
    return (paths_df, missing_dfs)

[paths_df, missing_dfs] = remove_other_missing_data(paths_df)

print('printing the dataframe for final inspection')
print(paths_df)

