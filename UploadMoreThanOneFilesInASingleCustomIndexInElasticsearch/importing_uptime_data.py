# -*- coding: utf-8 -*-

##@title Import Libraries {display-mode: "form"}
# %reload_ext google.colab.data_table
 
# install package for elastic search
!pip install elasticsearch
 
# install package for slack
!pip install slackclient
 
# Import libraries
from IPython.display import clear_output
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from google.colab import drive
from gspread_dataframe import set_with_dataframe
from google.colab import drive
import elasticsearch
import pandas as pd
import numpy as np
import fnmatch
import gspread
import time
import json
import os
from datetime import date
 
clear_output()

# Mount to drive
drive.mount('/gdrive')

# Variables
INDEX= 'prod-uptime-v1' # name of the index in while files will be uploads
TYPE= "_doc" # Type of documents
ID = "_id" # Id of the documents

# Directory containing csv files to upload to Elasticsearch
directory = '/gdrive/MyDrive/nameOftheDirectory'

# Changing to that directory
os.chdir(directory)


# Elasticsearch Credentials
host = "elasticsearch_link"
api_key = "elasticsearch_api_key"
id = "user_id"

# Initializing elasticsearch
es = elasticsearch.Elasticsearch([host], api_key=(id, api_key), timeout=60, max_retries=2, retry_on_timeout=True, verify_certs=True)
 
# Ping elasticsearch
es.ping()

# Extracting site uptimes csv files from all folders in a single directory
def extract_site_uptime_files(path): 
  csv_files = []
  for path,dirs,files in os.walk(path):
    for csv_file in files:
      if fnmatch.fnmatch(csv_file,'*.csv'):
        if 'site uptimes' in csv_file:
          csv_files.append(os.path.join(path,csv_file))
  return csv_files

# Coverting each csv file to a dataframe and adding more fields
import numpy as np

def csv_to_dataframe(csvfile):
  projectId = []
  dataframe = pd.read_csv(csvfile)
  dataframe['year'] = pd.DatetimeIndex(dataframe['period_start']).year
  dataframe['month'] = pd.DatetimeIndex(dataframe['period_start']).month
  dataframe['metadata.dataFile.key'] = csvfile
  dataframe['dataKey.project.name'] = dataframe[['site']].values
  dataframe.drop(pd.Index(np.where(~dataframe['site'].isin(['Bugarama', 'Ighombwe', 'Kalenge', 'Kitaita-Songambele', 
                                                           'Leshata', 'Mavota', 'Murusagamba', 'Nyantakara']))[0]), inplace = True)
  dataframe['indexed'] = date.today().strftime("%A %d. %B %Y")
  dataframe['site'] = dataframe['site'].replace({'Kitaita-Songambele': 'Kitaitaso'})
  dataframe['dataKey.project.name'] = dataframe['dataKey.project.name'].replace({'Kitaita-Songambele': 'Kitaitaso'})
  dataframe['dataKey.project.id'] = ''

  if 'site uptimes PowerGen' in csvfile:
    dataframe['dataKey.organization.name.keyword'] = 'WindGen Power USA Inc. (PowerGen)'
    dataframe['metadata.project.country'] = 'Tanzania'
    dataframe['dataKey.organization.id'] = '434eb44d-53ee-490e-9b2a-c66973956218'
  for row in dataframe['site']:
    if row == 'Bugarama': projectId.append('2e6246f2-4308-4163-96df-8014a4f1d509')
    elif row == 'Ighombwe': projectId.append('ec9ca1a5-8735-469f-9be4-d583d3516547')
    elif row == 'Leshata' : projectId.append('8d6123ab-977c-42d3-aba8-5095c1e23911')
    elif row == 'Nyantakara': projectId.append('d6ea03e0-4888-41c9-8920-f9614cc33a8a')
    elif row == 'Kitaitaso': projectId.append('5e278177-b867-4a51-bf7c-cac03864fbbd')
    elif row == 'Mavota': projectId.append('3f51adbf-c5fb-4f73-9c37-ce8132bcffb8')
    elif row == 'Murusagamba': projectId.append('e75167b3-8756-4f16-8152-b2095e3ee5d6')
    elif row == 'Kalenge': projectId.append('4a45bcd8-4849-40e3-ae38-d8b2360b1ee0')
    else: projectId.append('No Project Id')
  dataframe['dataKey.project.id'] = projectId

  return dataframe

# Creating an index in Kibana in which files will be indexed
def create_index(index_name, id):
  if not es.exists(index=index_name, id=id):
    create_index = es.indices.create(index=index_name, ignore=400)
    create_index = es.indices.put_settings(index=index_name, body={"number_of_replicas": 0})
  return create_index

# Mapping all the fields in Kibana
def create_mappings(index_name, doctype):
  mappings = es.indices.put_mapping(
      index = index_name,
      doc_type = doctype,
      include_type_name = True,
      ignore = 400,
      body={
          doctype: {
        "properties": {
          "@timestamp": {
            "type": "date"
          },
          "SAIDI_h": {
            "type": "double"
          },
          "SAIFI": {
            "type": "double"
          },
          "metadata.dataFile.key": {
            "type": "keyword"
          },
          "dataKey.organization.name.keyword": {
            "type": "keyword"
          },
          "dataKey.organization.id": {
              "type": "keyword"
          },
          "dataKey.project.id": {
              "type": "keyword"
          },
          "metadata.project.country":{
              "type": "keyword"
          },
          "dataKey.project.name": {
              "type": "keyword"
          },
          "downtime_percent": {
            "type": "double"
          },
          "num_meters": {
            "type": "long"
          },
          "period_end": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ssXXX"
          },
          "period_start": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ssXXX"
          },
          "indexed":{
              "type": "keyword",
          },
          "year": {
            "type" : "long"
          },
          "month": {
              "type" : "long"
          },
          "site": {
            "type": "keyword"
          },
          "uptime_percent": {
            "type": "double"
          }
        }
      }
      }
  )
  return mappings

# Converting each dataframe to a json
  def dataframe_to_json(df, index_name, doc_type):
      for record in df.to_dict(orient="records"):
          yield ('{ "index" : { "_index" : "%s", "_type" : "%s"}}'% (index_name, doc_type))
          yield (json.dumps(record))

# Executing the functions
def main():
  create_index(INDEX, ID)
  create_mappings(INDEX, TYPE)

  for csvfile in extract_site_uptime_files(directory):
  
    df = csv_to_dataframe(csvfile)

    if not es.indices.exists(INDEX):
        raise RuntimeError('index does not exists, use `curl -X PUT "elasticsearch_link"` and try again'%INDEX)

    # Checking if the file already exists in elasticsearch before uploading it
    query = {
      "query": {
        "match_phrase": {
          "metadata.dataFile.key": csvfile
        }
      }
    }
    if (es.search(query, index=INDEX)['hits']['total']['value'] == 0):
      r = es.bulk(dataframe_to_json(df, INDEX, TYPE)) # return a dict

      print(not r["errors"])


# Running the main function
if __name__ == "__main__":
  main()