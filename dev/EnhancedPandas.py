#This script includes the definition of additional Pandas functions

#Imports
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

class EnhancedPandas():

#viewna_all views the sum of missing values as well as the percentage of the whole column for each column of df
    def viewna_all(df):
        frame_missing = df.isna().sum().reset_index()
        frame_missing = frame_missing.rename(columns = {'index': 'column', 0 : 'missing count'})
        frame_missing['percentage missing'] = 100 * frame_missing['missing count'] / len(df)

        return frame_missing

#viewna_column groups the sum of missing values in column by values of column_2. Is useful for determining what is the cause of the missing data
    def viewna_column(df, column, column_2):
        grouped_frame_missing = df.groupby(column_2)[column].apply(lambda x: x.isnull().sum()).reset_index()
        grouped_frame_filled = df.groupby(column_2)[column].count().reset_index()
        
        #Renaming columns
        grouped_frame_missing = grouped_frame_missing.rename(columns = {str(column) : str(column) + ' missing values'})
        grouped_frame_filled = grouped_frame_filled.rename(columns = {str(column) : str(column) + ' filled in values'})
        
        #Merging the two dataframes
        grouped_frame = pd.merge(grouped_frame_filled, grouped_frame_missing)

        #Adjustment and calculation of the percentage missing
        grouped_frame[str(column) + ' number of records'] = grouped_frame[str(column) + ' filled in values'] + grouped_frame[str(column) + ' missing values']
        grouped_frame = grouped_frame.drop(str(column) + ' filled in values', axis=1).reset_index()
        grouped_frame = grouped_frame.drop('index', axis=1).reset_index()
        grouped_frame['Percentage Missing'] = grouped_frame[str(column) + ' missing values'] / grouped_frame[str(column) + ' number of records'] * 100
        grouped_frame = grouped_frame[[str(column_2), str(column) + ' number of records', str(column) + ' missing values', 'Percentage Missing']]

        return grouped_frame        

#check_missing_time_records loops through column that includes timestamps of given format and checks if there are any missing records   
    def check_missing_time_records(df, column, format):
        record_list = df[column].to_list()
        delta = datetime.strptime(str(record_list[1]), format) - datetime.strptime(str(record_list[0]), format)

        missing_values = []
        for i in (range(len(record_list)-1)):
            value_1 = datetime.strptime(str(record_list[i]), format) 
            try:       
                value_2 = datetime.strptime(str(record_list[i+1]), format)
            except ValueError:
                print('the program finished at ' + str(record_list[i]) + ' ,check if this is the end of the dataset')
                return missing_values
            if value_2 - value_1 != delta:
                tpl = (record_list[i], record_list[i+1])
                missing_values.append(tpl)

        return missing_values

#normalize takes p_key *args and creates a normalized table with p_key as the primary key and values in *args as the table columns 
    def normalize(df, p_key, keep_orig, *args):
        new_frame = pd.DataFrame()
        new_frame[p_key] = df[p_key]
        for arg in args:
            new_frame[arg] = df[arg]
        new_frame = new_frame.drop_duplicates()
        if new_frame[p_key].duplicated().sum() == 0:
            print('no primary key duplicates in the new table')
        else:
            print('warning, primary key duplicates present in the new table')
        if keep_orig == False:
            for arg in args:
                df = df.drop(arg, axis = 1)
        
        return df, new_frame

 #check_bool checks if there is a boolean value in column      
    def check_bool(df, column):
        bool_list = []
        lst = df[column].to_list()
        for element in lst:
            if element.lower() == 'true' or element.lower() == 'false':
                bool_list.append(element)
        
        return bool_list

  #ckeck_numeric checks if there is a numeric value in column  
    def check_numeric(df, column):
        numeric_list = []
        lst = df[column].to_list()
        for element in lst:
            try:
                element = float(element)
                numeric_list.append(element)
            except:
                pass
        
        return numeric_list

#check_empty_values checks if there are strings in column that are commonly used to denote an empty element. ('unknown', 'none', 'empty')
    def check_empty_values(df, column):
        frame_unique = df.drop_duplicates(subset=column).reset_index()
        try:
            frame_unique[column] = frame_unique[column].str.lower()
        except AttributeError:
            pass
        column_list = frame_unique[column].to_list()

        strings_check = ['empty', 'na', 'nan', 'none', 'unknown', 'not applicable']
        strings_present = []
        for string in strings_check:
            if string in column_list:
                strings_present.append(string)
                
        return strings_present

#sort_check sorts column and provides n first and n last values. Any weird elements will either float to the top or bottom
    def sort_check(df, column, n):
        df = df[column]
        frame_unique = df.drop_duplicates()
        frame_sorted = frame_unique.sort_values()
        print('Head:')
        print(frame_sorted.head(n))
        print('Tail:')
        print(frame_sorted.tail(n))

#check_headers checks if there are headers in any of the columns of a given df
    def check_headers(df):
        column_names = df.columns
        columns_with_header = []
        for column in column_names:
            values = df[column].to_list()
            if column in values:
                columns_with_header.append(str(column))
        
        return columns_with_header