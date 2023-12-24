'''*************************************************************************************************************--
-- Desc:This script connects to SQL Server
---Desc:Performs ETL operations using Pandas library
-- Change Log: When,Who,What
-- 2022-03-11,KKancharla, Created ETL pipiline to extract data from SQL Server view "vRptDoctorsShifts" 
-- and write to Mongodb collection "DoctorsShifts"
--**************************************************************************************************************'''
# -*- coding: utf-8 -*-

import pyodbc
import pandas as pd
import pymongo
import csv
import pprint
import numpy as np
import openpyxl
from openpyxl import load_workbook
from pandas import DataFrame
from datetime import datetime, timezone, timedelta

# Connect to SQL Server
conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
            "Server=localhost;"
            "Database=DWClinicReportDataKrishnaKancharla;"
            "Trusted_Connection=yes;")

# Print the query results using pandas dataframe with headers
query  = """
Select * From DWClinicReportDataKrishnaKancharla.dbo.vRptDoctorShifts
"""
df = pd.read_sql(query, conn)
print(df)

#Save the query results to csv file on local machine
df.to_csv('../PythonScripts/vRptDoctorsShifts.csv', index= False)
print('Succesfully saved to csv file')

#Create mongodb connection
#client = pymongo.MongoClient("mongodb://localhost:27017")
#db = client.test 

#Create mongodb connection
client = pymongo.MongoClient("mongodb+srv://kris99:kris@krisk.obbum.mongodb.net/ClinicReportsData?retryWrites=true&w=majority")
db = client.test

#Using pandas df to read the csv file
df = pd.read_csv("vRptDoctorsShifts.csv")

#create variable data and convert df to dictionary
data = df.to_dict(orient = "records")
print(data)

#create new mongodb database
db = client["ClinicReportsData"]
print(db)

#create new mongodb collection and insert records in it
db.DoctorsShifts.insert_many(data)
print(db)


