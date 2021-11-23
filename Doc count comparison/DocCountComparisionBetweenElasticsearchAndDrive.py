# -*- coding: utf-8 -*-

#@title Install important libraries
!pip install elasticsearch
!pip install gspread==3.6.0
!pip install gspread_formatting

#@title Import Libraries
import elasticsearch
from elasticsearch import Elasticsearch
import pandas as pd
from google.colab import drive
from zipfile import ZipFile
import os
import gspread
from gspread_dataframe import set_with_dataframe
import requests
import json
from gspread_formatting import *
import fnmatch

#@title Google sheet credentials
spreadSheetID = '1J4IyRC1nKO' # Spread sheet Id where the dataframe will be added

# load credentials --> To save a dataframe to googl doc
credentials = {
  "type": "service_account",
  "project_id": "try",
  "private_key_id": "937463",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEuwIBADANBgkqhkiG9w0BAQEFAASCBKUwggShAgEAAoIBAQCoZyhQlIRANy46",
  "client_email": "try@try.iam.gserviceaccount.com",
  "client_id": "123",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/try%try.iam.gserviceaccount.com"
}

#@title Mount to the drive
drive.mount('/drive')

# Directory name
fileDirectory = 'directory1' #@param ['directory1', 'directory2', 'directory3', 'directory4', 'directory5']

readings_index = 'index_1' #@param {type: "string"}
transactions_index = 'index_2' #@param {type: "string"}
organization = "OrgName1" #@param ['OrgName', 'OrgName2', 'OrgName3', 'OrgName4', 'OrgName5']
month = 'OCTOBER' #@param ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER']
year = 2021 #@param {type: "number"}

#@title Change to the current directory
os.chdir(fileDirectory)

#@title Ping Elasticsearch

# Elasticsearch credentials
host = "elasticsearch_link"
user = 'user_name'
pwd = 'user_pswd'

# Initiliaze elasticsearch
es = elasticsearch.Elasticsearch([host], http_auth=(user, pwd))

# Ping elasticsearch
es.ping()

# Query that extracts count of each doc from elasticsearch
query = {"size": 0,
"query": {
  "bool": {
    "must": [
      { "match": { "month": { "query": month} } },
      { "match": { "year": { "query": year} } },
      {"match": {"dataKey.organization.name.keyword": {"query": organization}}}
    ]
  }
},

"aggs" : {   
    "langs" : {
        "terms" : { "field" : "metadata.dataFile.key",  "size" : 500 }
        }
}}

# function to return the doc count for transactions in ES
def getESCountTransactions(index):

  records = es.search(query, index = index)['aggregations']['langs']['buckets']

  listOfRecords = []

  for record in records:
    listOfRecords.append(pd.DataFrame.from_dict(record, orient='index').transpose())

  listOfRecords = pd.concat(listOfRecords).reset_index(drop=True).rename(columns={"key": "dataFileKey", "doc_count": "Doc CountES"})
  
  listOfRecords['file Name ES'] = ''
  listOfRecords['Index ES'] = ''

  for count in listOfRecords.index:
    listOfRecords['file Name ES'][count] = listOfRecords['dataFileKey'][count][81:]
    listOfRecords['Index ES'][count] = transactions_index

  listOfRecords = listOfRecords[['Doc CountES', 'file Name ES', 'Index ES']].sort_values('file Name ES')


  return listOfRecords

# function to return the doc count for readings in ES
def getESCountReadings(index):

  records = es.search(query, index = index)['aggregations']['langs']['buckets']

  listOfRecords = []

  for record in records:
    listOfRecords.append(pd.DataFrame.from_dict(record, orient='index').transpose())

  listOfRecords = pd.concat(listOfRecords).reset_index(drop=True).rename(columns={"key": "dataFileKey", "doc_count": "Doc CountES"})
  
  listOfRecords['file Name ES'] = ''
  listOfRecords['Index ES'] = ''

  for count in listOfRecords.index:
    listOfRecords['file Name ES'][count] = listOfRecords['dataFileKey'][count][81:]
    listOfRecords['Index ES'][count] = readings_index

  listOfRecords = listOfRecords[['Doc CountES', 'file Name ES', 'Index ES']].sort_values('file Name ES')
  return listOfRecords

# Extract zipfile with paths to the files
def extract_zip_files(path): 
  zip_files = []
  for path,dirs,files in os.walk(path):
    for zip_file in files:
      if fnmatch.fnmatch(zip_file,'*.zip'):
        if 'Bezi' in zip_file or 'Iglansoni' in zip_file or 'Kasalazi' in zip_file or 'Londoni' in zip_file or 'Raranya' in zip_file \
        or 'Saranda' in zip_file or 'Sozia' in zip_file or 'Yozu' in zip_file or 'Ziragula' in zip_file or \
        'Changombe' in zip_file or 'Itaswi' in zip_file or 'Komolo' in zip_file or 'Kwamtoro' in zip_file \
        or 'Malambo' in zip_file or 'Idjwi' in zip_file or 'Tunganjika' in zip_file or 'Kigbe' in zip_file or 'Yebu' in zip_file or 'Gambugambu' in zip_file or 'Budo' in zip_file or \
        'Kilankwa' in zip_file or 'Petti' in zip_file or 'Chembaya' in zip_file:
            if str(year) in zip_file:
              if month.lower().capitalize() in zip_file:
                zip_files.append(os.path.join(path,zip_file))
  return zip_files

# Extract only the name of the file without the path
def splitFiles(fileToSplit):
  fileNameWithPath = []
  fileNameWithOutPath = []

# Splitting the file
  for i in fileToSplit:
        fileNameWithPath += i.split('/')

  lst1 = fileNameWithPath
  lst2 = fileNameWithPath

# Creating a dictionary from the split list
  def zipTwoFiles(fileNameWithPath):
    dct = dict(zip(lst1, lst2))
    return dct
 
# Extracting only the keys which contain the path
  for k,v in zipTwoFiles(fileNameWithPath).items():
    if '.zip' in k:   
      fileNameWithOutPath.append(k)

  return fileNameWithOutPath

# opening the zip file in READ mode and return only csv files for transactions for one zip file
def csv_files_transactions(fileDirectory, fileName):
  fileN = []
  fileX = []
  with ZipFile(fileDirectory + fileName, 'r') as zip: 
      # Getting the list of the file names in the zip folder
      fileN+=zip.namelist()
  for f in fileN:
    if 'transactions' in f:
      fileX.append(f)
  return fileX[0]

# Returning doc count for one transaction csv file
def fileCount_transactions(fileDirectory, fileName, datasetType):
  return pd.DataFrame.from_records([{'Doc Count Drive':pd.read_csv(ZipFile(fileDirectory+fileName).open(csv_files_transactions(fileDirectory, fileName))).shape[0],
                    'file Name Drive':fileName, 'Index Drive': datasetType}])

# opening the zip file in READ mode and return only csv files for readings for one zip file
def csv_files_readings(fileDirectory, fileName):
  fileN = []
  fileX = []
  with ZipFile(fileDirectory + fileName, 'r') as zip: 
      # Getting the list of the file names in the zip folder
      fileN+=zip.namelist()
  for f in fileN:
    if 'readings' in f:
      fileX.append(f)
  return fileX[0]

# Returning doc count for one reading csv file
def fileCount_readings(fileDirectory, fileName, datasetType):
  return pd.DataFrame.from_records([{'Doc Count Drive':pd.read_csv(ZipFile(fileDirectory+fileName).open(csv_files_readings(fileDirectory, fileName))).shape[0],
                    'file Name Drive':fileName, 'Index Drive': datasetType}])

# Conctenating transactions and Readings for the Drive
def dataFrameForReadingsAndTransactionsDrive(transactions_index, readings_index):
  fileNames = splitFiles(extract_zip_files(fileDirectory))
  df = pd.DataFrame()
  for f in fileNames:
    df = df.append(fileCount_transactions(fileDirectory, f, transactions_index)).reset_index(drop=True)
    df = df.append(fileCount_readings(fileDirectory, f, readings_index)).reset_index(drop=True)
  return df.sort_values(['Index Drive', 'file Name Drive'], ascending=True).reset_index(drop=True)

# Concatenating transactions and reading for ES
def concatenatingTransactionsReadingES(transactions_index, readings_index):
  dataframe = pd.DataFrame()
  dataframe = dataframe.append(getESCountTransactions(transactions_index))
  dataframe = dataframe.append(getESCountReadings(readings_index))
  return dataframe.sort_values(['Index ES', 'file Name ES'], ascending=True).reset_index(drop=True)

# Concatenating data from transactions and Readings from ES to that from Drive
def concatenatingdataFromESAndDrive(fromES, fromDrive):
  result = pd.concat((fromES, fromDrive), axis=1, join="outer")
  result = result[['Index ES', 'file Name ES', 'Doc CountES', 'Doc Count Drive', 'file Name Drive', 'Index Drive']]
  return result


# Main function that adds the dataframe to google doc
def main():
  # dump JSON Credentials to a file
  json.dump(credentials, open("creds.json",'w'))

  # ACCES GOOGLE SHEET
  gc = gspread.service_account("creds.json")
  sh = gc.open_by_key(spreadSheetID)
  worksheet = sh.get_worksheet(2) 

  # CLEAR SHEET CONTENT
  range_of_cells = worksheet.range('A2:F100') #-> Select the range you want to clear
  for cell in range_of_cells:
      cell.value = ''

  # EXPORT DATAFRAME TO THE GOOGLE SHEET
  worksheet.update_cells(range_of_cells) 

  # MAKE THE FIRST CELL BOLD
  fmt = cellFormat(
    textFormat=textFormat(bold=True)
    )

  format_cell_range(worksheet, 'A1:H1', fmt)

  # APPEND DATA TO SHEET
  set_with_dataframe(worksheet, concatenatingdataFromESAndDrive(concatenatingTransactionsReadingES(transactions_index, readings_index), dataFrameForReadingsAndTransactionsDrive(transactions_index, readings_index)))

# Running the main function
if __name__ == "__main__":
  main()

