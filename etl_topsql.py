import requests
import numpy as np
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
import sqlite3
import psycopg2
from sqlalchemy import create_engine
import os



server = "localhost"
host = "localhost"
database = "etl"
port = "5432"
table_name= "sales_table"
log_file = "log_file.txt" 

pwd = "postgres123"
uid = "postgres"

csv_file = r"C:\Users\User\DataEngineerProject\grocery_chain_data.csv"
json_file = r"C:\Users\User\DataEngineerProject\grocery_chain_data.json"
xml_file = r"C:\Users\User\DataEngineerProject\grocery_chain_data.xml"

def extract_from_csv(file_to_process): 
    dataframe = pd.read_csv(file_to_process) 
    return dataframe 
  
def extract_from_json(file_to_process): 
    dataframe = pd.read_json(file_to_process) 
    return dataframe 
  
def extract_from_xml(file_to_process): 
    
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    dataframe = pd.DataFrame(columns=[
        "customer_id", 
        "store_name", 
        "transaction_date", 
        "aisle", 
        "product_name", 
        "quantity", 
        "unit_price", 
        "total_amount"
    ]) 
    for record in root: 
        
        def safe_text(tag):
            el = record.find(tag)
            return el.text if el is not None else None

        customer_id      = safe_text("customer_id")
        store_name       = safe_text("store_name")
        transaction_date = safe_text("transaction_date")
        aisle            = safe_text("aisle")
        product_name     = safe_text("product_name")

        quantity   = safe_text("quantity")
        unit_price = safe_text("unit_price")
        total_amount = safe_text("total_amount")


        
        dataframe = pd.concat([dataframe, pd.DataFrame([{
                "customer_id": customer_id,
                "store_name": store_name,
                "transaction_date": transaction_date,
                "aisle": aisle,
                "product_name": product_name,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount
            }])], ignore_index=True)
 
    return dataframe 

def extract(csv_file, json_file, xml_file): 
    extracted_data = pd.DataFrame(columns=[
        "customer_id", 
        "store_name", 
        "transaction_date", 
        "aisle", 
        "product_name", 
        "quantity", 
        "unit_price", 
        "total_amount"
    ]) # create an empty data frame to hold extracted data  
         
   # extract from single CSV
    df_csv = extract_from_csv(csv_file)
    extracted_data = pd.concat([extracted_data, df_csv], ignore_index=True)
    
    # extract from single JSON
    df_json = extract_from_json(json_file)
    extracted_data = pd.concat([extracted_data, df_json], ignore_index=True)
    
    # extract from single XML
    df_xml = extract_from_xml(xml_file)
    extracted_data = pd.concat([extracted_data, df_xml], ignore_index=True)
    
    return extracted_data

extracted_data = extract(csv_file, json_file, xml_file)
       

def transform(dataframe): 
    dataframe["store_name"] = dataframe["store_name"].astype(str).str.strip()
    dataframe = dataframe[dataframe["store_name"] != ""]
    dataframe = dataframe[dataframe["store_name"].notna()]

    return dataframe.reset_index(drop=True)
    
    
  
def load_data(extracted_data, tbl): 
        try:
            rows_imported = 0
            engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/etl')
            print(f'importing rows {rows_imported} to {rows_imported + len(extracted_data)}... for table {table_name}')
            # save df to postgres
            extracted_data.to_sql(f'stg_{table_name}', engine, if_exists='replace', index=False, chunksize=100000)
            rows_imported += len(extracted_data)
            # add elapsed time to final print out
            print("Data imported successful")
        except Exception as e:
            print("Data load error: " + str(e))

try:
    #call extract function
    extract(csv_file, json_file, xml_file)
except Exception as e:
    print("Error while extracting data: " + str(e))
  
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 
  

log_progress("ETL Job Started") 
  
log_progress("Extract phase Started") 
extracted_data = extract(csv_file, json_file, xml_file)
   
log_progress("Extract phase Ended") 
  
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
   
log_progress("Transform phase Ended") 
  
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(transformed_data, table_name) 
  

log_progress("Load phase Ended") 
  
log_progress("ETL Job Ended") 

  


