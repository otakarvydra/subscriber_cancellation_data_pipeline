#Importing Packages
import sqlite3
import pandas as pd
from EnhancedPandas import EnhancedPandas as ep
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

#Connecting to the Database
con = sqlite3.connect('databases/cademycode.db')
cur = con.cursor()
logger.log(logging.DEBUG, '\nRUNNING AUTOMATED CLEANING OF JOBS TABLE')

#Creating the DataFrame
jobs_df = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", con)
con.close()

#Function Definition
def remove_duplicates(jobs_df):
    duplicated_jobs_df = jobs_df[jobs_df.duplicated()]

    if len(duplicated_jobs_df) == 0:
        logger.log(logging.INFO, '\nno duplicated values were found')
        return jobs_df
    else:
        logger.log(logging.INFO, '\ndropping duplicated rows')
        logger.log(logging.INFO, duplicated_jobs_df.to_string())
        file_path = ('dropped_data_log/jobs_df_duplicated_data.csv')
        duplicated_jobs_df.to_csv(file_path, mode = 'a', index = False)
        return jobs_df.drop_duplicates()

def remove_other_missing_data(jobs_df):
    missing_dfs = {}
    missing_df = ep.viewna_all(jobs_df)
    columns_missing = missing_df[missing_df['missing count'] > 0].reset_index(drop = True)
    if len(columns_missing) == 0:
        print('\nno columns have missing data')
        logger.log(logging.DEBUG, '\nmissing data function run - no missing data found')
        return (jobs_df, [])
    else:
        print('\nafter automatically removing structurally missing data between current_career_path_id and time_spent_hrs, these columns still posses missing data:')

    while True:
        missing_df = ep.viewna_all(jobs_df)
        print(missing_df)
        input_1 = input('what column do you wish to investigate? (to exit press e) ')
        if input_1.lower() == 'e':
            break

        try:
            jobs_df[input_1]
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
                print(ep.viewna_column(jobs_df, input_1, input_2))         
            except KeyError:
                print('invalid column name provided')
                continue
            
        while True:
            input_3 = input('Do you wish to drop the rows containing the missing values? y/n ')
            if input_3.lower() == 'y':
                empty_values_df = jobs_df[jobs_df[input_1].isnull() == True]
                missing_dfs[input_1] = empty_values_df
                logger.log(logging.DEBUG, '\ndropping the following rows with empty values')
                logger.log(logging.DEBUG, empty_values_df.to_string())
                jobs_df = jobs_df.dropna(subset=input_1)
                break
            if input_3.lower() == 'n':
                break
            else:
                print('invalid value provided')
                continue
    
    #Logging
    logger.log(logging.DEBUG, '\nmissing data function run')
    return (jobs_df, missing_dfs)

def write_missing_data_file(missing_dfs):
    missing_df = pd.DataFrame()
    try:
        for value in missing_dfs.values():
            missing_df = pd.concat([missing_df, value])
    
        file_path = ('dropped_data_log/jobs_df_missing_data.csv')
        missing_df.to_csv(file_path, mode = 'a', index = False)

    except AttributeError:
        pass

#Removing Duplicates
jobs_df = remove_duplicates(jobs_df)

#Investigating Missing Data
[jobs_df, missing_dfs] = remove_other_missing_data(jobs_df)

#Writing Missing Data to a .csv file
write_missing_data_file(missing_dfs)

#Final visual Inspection of the DataFrame
print('\n' + 'printing the dataframe for final inspection')
print(jobs_df)

#Exporting the clean DataFrame to a temporary file folder
jobs_df.to_csv('temporary_files/jobs.csv', index = False)