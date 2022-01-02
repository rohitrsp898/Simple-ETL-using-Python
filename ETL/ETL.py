# Simple ETL script to extract data from a MySQL, csv and other source and load it into a csv file

import mysql.connector
import logging
import pandas as pd
import glob

path='/home/rohit/ETL/Data/'

logging.basicConfig(filename='/home/rohit/ETL/ETL.log', encoding='utf-8',level=logging.DEBUG,format='%(asctime)s --- %(message)s')
def extract_from_mysql(query):
    """
    Extract data from a MySQL database and return it as a dataframe
    :param query: SQL query to extract data from the MySQL database
    :return: Dataframe with the data from the MySQL database
    """
    # Connect to the database
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',
        database='db1',
        auth_plugin='mysql_native_password'
    )
    # Create a cursor to execute queries
    cursor = connection.cursor()
    # Execute the query
    cursor.execute(query)
    field_names = [i[0] for i in cursor.description]
    #
    # print(field_names)
    # Get the data
    data = cursor.fetchall()
    # Close the connection
    connection.close()
    # Create a dataframe
    df = pd.DataFrame(data, columns=field_names)
    #df.columns = field_names
    #print(df)
    
    return df


def extract_from_csv(path):
    """
    Extract data from a csv file and return it as a dataframe
    :param path: Path to the csv file
    :return: Dataframe with the data from the csv file
    """
    # Create a dataframe
    df = pd.read_csv(path)
    #print(df)
    
    return df

def extract_from_json(path):
    """
    Extract data from a json file and return it as a dataframe
    :param path: Path to the json file
    :return: Dataframe with the data from the json file
    """
    # Create a dataframe
    df = pd.read_json(path)
    #print(df)
    
    return df


def extract():
    extracted_data=pd.DataFrame()       # Create a empty dataframe

    #process all csv files
    for csv in glob.glob(path+"*.csv"):
        extracted_data=extracted_data.append(extract_from_csv(csv),ignore_index=True)
    logging.info("Extracted data from csv files")
    

    for json in glob.glob(path+"*.json"):
        extracted_data=extracted_data.append(extract_from_json(json),ignore_index=True)
    logging.info("Extracted data from json files")
    
    extracted_data=extracted_data.append(extract_from_mysql("select * from data"),ignore_index=True)
    logging.info("Extracted data from MySQL table")

    return extracted_data

def transform(data):

    data['New Salary(INR)']=data['Salary(INR)']*1.2
    logging.info("Transformed data: new column added as New Salary(INR)")

    data['Salary(USD)']=round(data['New Salary(INR)']/75.4,2)
    logging.info("Transformed data: new column added as Salary(USD)")

    logging.info("Transformed data !!")
    return data

def load(data):
    data.to_csv(path+'output.csv',index=False)
    logging.info("Loaded data into csv file")


extracted_data=extract()
transformed_data=transform(extracted_data)
load(transformed_data)





