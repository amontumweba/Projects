# -*- coding: utf-8 -*-

#@title Install Libraries
!pip install elasticsearch

#@title Import Libraries
import elasticsearch
from elasticsearch import Elasticsearch
import pandas as pd
from zipfile import ZipFile
import os
import gspread
from gspread_dataframe import set_with_dataframe
import requests
import json
from google.colab import drive
import plotly
import plotly.graph_objs as go
import plotly.offline as offline

OrganizationName = "OrgName" #@param {type: "string"}
ProjectName = "ProjectName" #@param {type: "string"}
Year = "2021" #@param {type: "string"}
# Month = "FEBRUARY" #@param {type: "string"}
startDate = "2021-02-16T00:00:00.000+00:00" #@param {type: "string"}
endDate = "2021-02-23T23:59:59.000+00:00" #@param {type: "string"}
INDEX = 'es_index' #@param {type: "string"}

#@title Mount to Drive
drive.mount('/drive')

#@title Connect to Elasticsearch
host = "host_link"
user = 'user_name'
pwd = 'pswd'

# Initiliaze elasticsearch
es = elasticsearch.Elasticsearch([host], http_auth=(user, pwd))

es.ping()

# Quering elasticsearch
def indexQuery(organizationName, projectName, year, startDate, endDate):
  query = {
        "query": {
          "bool": {
            "must": [],
            "filter": [
              {
                "match_all": {}
              },
              {
                "match_phrase": {
                  "dataKey.organization.name.keyword": organizationName
                }
              },
              {
                "match_phrase": {
                  "dataKey.project.name.keyword": projectName
                }
              },
              {
                "match_phrase": {
                  "year": year
                }
              },
            {
              "range": {
                  "timestamp": {
                    "gte": startDate,
                    "lt": endDate
                  }
                }
              },
              {
                "range": {
                  "timestamp": {
                    "gte": "2018-09-30T10:16:36.132Z",
                    "lte": "2022-09-30T10:16:36.132Z",
                    "format": "strict_date_optional_time"
                  }
                }
              }
            ],
            "should": [],
            "must_not": [
                {
              "bool": {
                "should": [
                  {
                    "term": {
                      "payment.type": "bonus"
                    }
                  },
                  {
                    "term": {
                      "metadata.tags": "sales_account"
                    }
                  },
                  {
                    "term": {
                      "payment.origin": "reversal"
                    }
                  },
                  {
                    "term": {
                      "payment.status": "error"
                    }
                  },
                  {
                    "term": {
                      "payment.status": "reversed"
                    }
                  },
                  {
                    "term": {
                      "metadata.tags": "totalizer"
                    }
                  },
                  {
                    "term": {
                      "metadata.anomalies": "METER_OUTAGE"
                    }
                  }
                ]
              }
            }
        ]
        }
    }
  }
  return query

# Function to Extract meterIds from a field called meter from all pages in Elasticsearch and making it a dataframe
def meterId(index):
  if es != None:
    if es.indices.exists(index = index):
      meterId_lst = []

      # make a search() request to get all docs in the index
      resp = es.search(
          index = index,
          body = indexQuery(OrganizationName, ProjectName, Year, startDate, endDate),
          scroll = '2s' # length of time to keep search context
      )

      # keep track of pass scroll _id
      old_scroll_id = resp['_scroll_id']

      # use a 'while' iterator to loop over dodcument 'hits'
      while len(resp['hits']['hits']):

          # make a request using the Scroll API
          resp = es.scroll(
              scroll_id = old_scroll_id,
              scroll = '2s' # length of time to keep search context
          )

          # keep track of pass scroll _id
          old_scroll_id = resp['_scroll_id']

          # iterate over the document hits for each 'scroll'
          for doc in resp['hits']['hits']:
            meterId_lst.append(pd.DataFrame.from_dict(doc["_source"]['dataKey']['meter'], orient='index').transpose())
            
      dataframe = pd.concat(meterId_lst).reset_index(drop=True).rename(columns={"id": "Meter Id"})
      return dataframe[['Meter Id']].astype(str)

# Function to Extract energy consumption from a field called meter from all pages in Elasticsearch and making it a dataframe
def energyConsumption(index):
  if es != None:
    if es.indices.exists(index = index):
      energyConsumption_lst = []

      # make a search() request to get all docs in the index
      resp = es.search(
          index = index,
          body = indexQuery(OrganizationName, ProjectName, Year, startDate, endDate),
          scroll = '2s' # length of time to keep search context
      )

      # keep track of pass scroll _id
      old_scroll_id = resp['_scroll_id']

      # use a 'while' iterator to loop over dodcument 'hits'
      while len(resp['hits']['hits']):

          # make a request using the Scroll API
          resp = es.scroll(
              scroll_id = old_scroll_id,
              scroll = '2s' # length of time to keep search context
          )

          # keep track of pass scroll _id
          old_scroll_id = resp['_scroll_id']

          # iterate over the document hits for each 'scroll'
          for doc in resp['hits']['hits']:
            energyConsumption_lst.append(pd.DataFrame.from_dict(doc["_source"]['meter'], orient='index').transpose())
            
      dataframe = pd.concat(energyConsumption_lst).reset_index(drop=True).rename(columns={"energyConsumptionKwh": "Energy Consumption Kwh"})
      return dataframe[['Energy Consumption Kwh']]

# Function to Extract customerIds from a field called customer from all pages in Elasticsearch and making it a dataframe
def customerId(index):
  if es != None:
    if es.indices.exists(index = index):
      customerId_lst = []

      # make a search() request to get all docs in the index
      resp = es.search(
          index = index,
          body = indexQuery(OrganizationName, ProjectName, Year, startDate, endDate),
          scroll = '2s' # length of time to keep search context
      )

      # keep track of pass scroll _id
      old_scroll_id = resp['_scroll_id']

      # use a 'while' iterator to loop over dodcument 'hits'
      while len(resp['hits']['hits']):

          # make a request using the Scroll API
          resp = es.scroll(
              scroll_id = old_scroll_id,
              scroll = '2s' # length of time to keep search context
          )

          # keep track of pass scroll _id
          old_scroll_id = resp['_scroll_id']

          # iterate over the document hits for each 'scroll'
          for doc in resp['hits']['hits']:
            customerId_lst.append(pd.DataFrame.from_dict(doc['_source']['dataKey']['customer'], orient='index').transpose())
            
      dataframe = pd.concat(customerId_lst).reset_index(drop=True).rename(columns={"id": "Customer Id"})
      return dataframe[['Customer Id']].astype(str)


#Function to Extract timestamps from a field called timestamp from all pages in Elasticsearch and making it a dataframe
def returningTimestamp(index):
  if es != None:
    if es.indices.exists(index = index):
      timestamp_lst = []

      # make a search() request to get all docs in the index
      resp = es.search(
          index = index,
          body = indexQuery(OrganizationName, ProjectName, Year, startDate, endDate),
          scroll = '2s' # length of time to keep search context
      )

      # keep track of pass scroll _id
      old_scroll_id = resp['_scroll_id']

      # use a 'while' iterator to loop over dodcument 'hits'
      while len(resp['hits']['hits']):

          # make a request using the Scroll API
          resp = es.scroll(
              scroll_id = old_scroll_id,
              scroll = '2s' # length of time to keep search context
          )

          # keep track of pass scroll _id
          old_scroll_id = resp['_scroll_id']

          # iterate over the document hits for each 'scroll'
          for doc in resp['hits']['hits']:
            timestamp_lst.append(doc['_source']['timestamp'])
            
      df = pd.DataFrame()

      df['Timestamp'] = timestamp_lst
      return df

# Concatenating all the dataframes into a single dataframe and saving it as a csv in drive
def concatenatingDataframesAndSavingAsACSV(index):
  df = pd.concat([meterId(index), energyConsumption(index), customerId(index), returningTimestamp(index)], axis=1)
  return df.to_csv(r'/drive/MyDrive/Anomaly Detective/AnomalyDetection.csv', index=False)
concatenatingDataframesAndSavingAsACSV(INDEX)


# Loading data and reading a csv file
AnomalyDetection_df = pd.read_csv('/drive/MyDrive/Anomaly Detective/AnomalyDetection.csv')

# Adding new columns to a dataframe
def anomalyDetectionDf(df):
  df['Date'] = pd.DatetimeIndex(df['Timestamp'])
  df['Customer Id'] = df['Customer Id'].astype(str).str.replace('\.0$', '')
  df = df.sort_values(['Meter Id', 'Customer Id', 'Date'], ascending=(False, False, True)).reset_index(drop=True)
  df['Time Elapsed'] = df.Date.diff()
  df['Time Elapsed'] = df['Time Elapsed'].fillna(pd.Timedelta(seconds=0))
  df['Time Elapsed'] = df['Time Elapsed'].astype(str)
  return df

# Plotting a graph using plotly
def figure(df):
  trace = go.Scatter(x = df['Date'], y=df['Energy Consumption Kwh'], 
                    mode = 'markers', 
                    marker = dict(size=6, color=df['Energy Consumption Kwh'], 
                                  colorscale='Rainbow', 
                                  showscale=True, 
                                  opacity=0.75), 
                    text = "Meter Id: " + df['Meter Id'] + "<br>Customer Id: " + df['Customer Id'] + "<br>Time Elapsed: " + df['Time Elapsed'])
  data = [trace]

  layout = go.Layout(title='Anomaly Detection', 
                    hovermode = 'closest',
                    xaxis=dict(title='Timestamp'), 
                    yaxis=dict(title='Energy Consumption Kwh'))
  fig = go.Figure(data=data, layout=layout)
  return fig.show(renderer='colab')

# Function to plot plotly graph
figure(anomalyDetectionDf(AnomalyDetection_df))

