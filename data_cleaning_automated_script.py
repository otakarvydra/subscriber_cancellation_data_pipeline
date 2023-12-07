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
students_df = pd.read_sql_query("SELECT * FROM cademycode_students", con)

#Splitting the contact_info column
def split_contact_info_column(students_df):
    def get_email(x):
        return (x.split(':'))[2][2:-2]

    def get_address(x):
        return (x.split(':'))[1][2:-10]

    students_df['email']= list(map(get_email, students_df['contact_info']))
    students_df['address'] = list(map(get_address, students_df['contact_info']))

    #Dropping the original column
    students_df.drop(columns=['contact_info'], inplace=True)

    #Splitting the address column into individual pieces
    address_list = students_df['address'].to_list()
    streets = []
    cities = []
    states = []
    postcodes = []

    for address in address_list:
        address_split = address.split(',')
        street = address_split[0]
        city = address_split[1][1:]
        state = address_split[2][1:]
        postcode = address_split[3][1:]
        streets.append(street)
        cities.append(city)
        states.append(state)
        postcodes.append(postcode)

    students_df['street'] = streets
    students_df['city'] = cities
    students_df['state'] = states
    students_df['postcode'] = postcodes

    students_df = students_df.drop('address', axis = 1)
split_contact_info_column(students_df)

#Converting the Data Types
def convert_data_types(students_df):
    students_df['dob'] = pd.to_datetime(students_df['dob'])
    students_df['sex'] = students_df['sex'].astype(str)
    students_df['job_id'] = pd.to_numeric(students_df['job_id'])
    students_df['num_course_taken'] = pd.to_numeric(students_df['num_course_taken'])
    students_df['current_career_path_id'] = pd.to_numeric(students_df['current_career_path_id'])
    students_df['time_spent_hrs'] = pd.to_numeric(students_df['time_spent_hrs'])
    students_df['email'] = students_df['email'].astype(str)
    students_df['postcode'] = pd.to_numeric(students_df['postcode'])
convert_data_types(students_df)

#Adding Age Column for easier Analysis
def add_age_column(students_df):
    students_dobs = students_df['dob'].to_list()
    students_ages = []

    for student_dob in students_dobs:
        student_age = round((datetime.today() - student_dob).total_seconds() / (365 * 24 * 3600))
        students_ages.append(student_age)

    students_df['age'] = students_ages
add_age_column(students_df)

#Removing Duplicates
def remove_duplicates(students_df):
    duplicated_students_dfs_df = students_df[students_df.duplicated()]

    if len(duplicated_students_dfs_df) == 0:
        logger.log(logging.INFO, 'no duplicated values were found')
    else:
        logger.log(logging.INFO, 'dropping duplicated students_dfs:')
        logger.log(logging.INFO, duplicated_students_dfs_df.to_string())
        students_df = students_df.drop_duplicates()
remove_duplicates(students_df)

#Dealing with Missing Data

    #Structurally Missing Data in the current_career_path_id and time_spent_hrs columns
def remove_struc_missing_data(students_df):
    inpute_current_career_id = lambda row: 999 if (pd.isna(row['current_career_path_id']) and pd.isna(row['time_spent_hrs'])) else row['current_career_path_id']  
    students_df['current_career_path_id'] = students_df.apply(inpute_current_career_id, axis = 1)

    inpute_time_spent_hrs = lambda row: 0 if row['current_career_path_id'] == 999 else row['time_spent_hrs']
    students_df['time_spent_hrs'] = students_df.apply(inpute_time_spent_hrs, axis = 1)
remove_struc_missing_data(students_df)

    #Other Missing Data in any column
def remove_other_missing_data(students_df):
    missing_dfs = {}
    missing_df = ep.viewna_all(students_df)
    columns_missing = missing_df[missing_df['missing count'] > 0].reset_index(drop = True)
    if len(columns_missing) == 0:
        print('after automatically removing structurally missing data between current_career_path_id and time_spent_hrs, no other columns have missing data')
        return
    else:
        print('after automatically removing structurally missing data between current_career_path_id and time_spent_hrs, these columns still posses missing data:')
        print(columns_missing)

    while True:
        missing_df = ep.viewna_all(students_df)
        print(missing_df)
        input_1 = input('what column do you wish to investigate? (to exit press e) ')
        if input_1.lower() == 'e':
            break

        try:
            students_df[input_1]
        except KeyError:
            continue

        while True:
            print('Investigating column ' + input_1)
            input_2 = input('''insert column to generate a dataframe that shows the sum of missing data values grouped by the values in the provided column - to exit press e
the available columns are: 'uuid', 'name', 'dob', 'age', 'sex', 'email', 'street', 'city', 'state', 'postcode', 'job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs' ''')
            if input_2.lower() == 'e':
                break
            try:
                print('results for ' + input_2)    
                print(ep.viewna_column(students_df, input_1, input_2))         
            except KeyError:
                print('invalid column name provided')
                continue
            
        while True:
            input_3 = input('Do you wish to drop the rows containing the missing values? y/n ')
            if input_3.lower() == 'y':
                empty_values_df = students_df[students_df[input_1].isnull() == True]
                missing_dfs[input_1] = empty_values_df
                students_df = students_df.dropna(subset=input_1)
                break
            if input_3.lower() == 'n':
                break
            else:
                print('invalid value provided')
                continue
    
    return (students_df, missing_dfs)

(students_df, missing_dfs) = remove_other_missing_data(students_df)

print(students_df.info())
