# -*- coding: utf-8 -*-

# install package for elastic search
!pip install elasticsearch

# install package for slack
!pip install slackclient

# Import libraries
from IPython.display import clear_output
from elasticsearch import Elasticsearch
import elasticsearch
import json

clear_output()

INDEX = 'elasticsearch_index'

##@title Connect to ElasticSearch {display-mode: "form"}

host = "elasticsearch_link"
user = 'user_name'
pwd = 'user_pswd'

# Initiliaze elasticsearch
es = elasticsearch.Elasticsearch([host], http_auth=(user, pwd), port=443, scheme="https", use_ssl=True, timeout=150, max_retries=10, retry_on_timeout=True)

# ping elasticsearch --> "True" means that you are connected
es.ping()

##@title Define Functions {display-mode: "form"}

# function that returns all projects as specfied in the query
def build_query(year, dayOfYear):
  q={
    "query": {
      "bool": {
        "must": [],
        "filter": [
          {
            "match_all": {}
          },
          {
            "bool": {
              "should": [
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "1"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "2"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "3"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "4"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "5"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "6"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "7"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "8"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "9"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "10"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "11"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "12"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "13"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "14"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "15"
                  }
                },
                {
                  "match_phrase": {
                    "dataKey.project.name.keyword": "16"
                  }
                }
              ],
              "minimum_should_match": 1
            }
          },
        {
            "match_phrase": {
              "year": year
            }
          },
          {
            "match_phrase": {
              "dayOfYear": dayOfYear
            }
          },
          {
            "range": {
              "timestamp": {
                "gte": "2021-01-31T00:00:00.000Z",
                "lte": "2021-02-26T00:00:00.000Z",
                "format": "strict_date_optional_time"
              }
            }
          }
        ],
        "should": [],
        "must_not": []
      }
    }}
  
  return q

# function to iterate through months from the start march 2020 year till now
def main():
  for year in range(2021, 2022):
    for dayOfYear in range(1, 366):
      query = json.dumps(build_query(year, dayOfYear))
      
      # check first if the index exist
      if es.indices.exists(index = INDEX):
        # run delete by query based on the built index above, Note: Ignoring conflicts
        es.delete_by_query(index = INDEX, body = query, conflicts='proceed', wait_for_completion =True, slices='auto')
        print("Completed for dayOfYear: " + str(year) + str(dayOfYear))
      else:
        print("Index does not exist")

# Run the main function
if __name__ == "__main__":
  main()

