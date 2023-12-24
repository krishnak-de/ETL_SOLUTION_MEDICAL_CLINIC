'''*************************************************************************************************************--
-- Desc:This script connects to SQL Server
---Desc:Performs ETL operations using Pandas library
-- Change Log: When,Who,What
-- 2022-03-13,KKancharla, Created ETL pipiline to extract data from SQL Server view "vRptPatientVisits" 
-- and write to Mongodb collection "PatientsVisits"
--**************************************************************************************************************'''
# -*- coding: utf-8 -*-

import pyodbc
import pandas as pd
import re
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

#Print query results using pandas dataframe with headers
query  = """
Select * From DWClinicReportDataKrishnaKancharla.dbo.vRptPatientVisits
"""
df = pd.read_sql(query, conn)
print(df)

##Perform transformations to clean the data - create a dictionary to remap any column (ProcedureName) values
dict = {'/': '|', 'ult': 'Ultrasound', 'not':'Not', 'Skel':'Skeletal', 'consultattation':'Consultation', 
              'evaluat':'Evaluation', 'evalua':'Evaluation', 'eval':'Evaluation', ',':' ', 'Lo':'Lower', 'xray':'x-ray',
              'Comprehen':'Comprehensive', 'Intravascul': 'Intravascular'}
#print(dict)

##replace values from the above dictionary in the same dataframe by using inplace=true
df["ProcedureName"].replace(dict, inplace = True)
print(df)

#save the query results to csv file on local machine
df.to_csv('../pythonscripts/vrptpatientsvisits.csv', index=False)
print('succesfully saved to csv file')

#create mongodb connection
client = pymongo.MongoClient("mongodb+srv://kris99:kris@krisk.obbum.mongodb.net/clinicreportsdata?retrywrites=true&w=majority")
db = client.test

#using pandas df to read the csv file
df = pd.read_csv("vrptpatientsvisits.csv")

#create variable 'data' and using 'to_dict' method, convert df to dictionary
data = df.to_dict(orient = "records")
print(data)

#create new mongodb database - "clinicreportsdata"
db = client["clinicreportsdata"]
print(db)

#create new mongodb collection "patientsvisits" and insert records in it
db.patientsvisits.insert_many(data)
print(db)
