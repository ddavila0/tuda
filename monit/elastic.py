import requests
import json
import pandas as pd

class elastic():
    """ELASTIC: ES Local Analysis Search Tool Implemented as a Class"""
    
    def __init__(query, fields, path_to_token,
                  min_timestamp=time.time()-3600*6,
                  max_timestamp=time.time()):
        self.query = query
        self.token = self.read_token(path_to_token)
        self.headers = {"Authorization": "Bearer "+self.token}
        self.api_url = ("https://monit-grafana.cern.ch/"
                        +"api/datasources/proxy/9014/_search")

    def read_fields(self, fname):
        """Read desired fields from an external file, return list of
           fields
        """
        fields = []
        with open(fname, "r") as fin:
            lines = fin.readlines()
            for line in lines:
                fields.append("data."+line[:-1])
        return fields

    def read_token(self, fname):
        """Read token from an outside file, return token string"""
        with open(fname, "r") as fin:
            line = fin.readline()
            # Remove the "newline" character if found
            if line[len(line)-1] == "\n":
                line = line[:-1]
            self.token = line
        return self.token

    def get_data_string(self, num_records=10000, offset=0):
        """Return stringified JSON of ES request"""
        data = {
                "size":num_records,
                "from":offset,
                "query":{
                    "bool":{
                        "filter":[
                            {"range":{
                                "data.RecordTime":{
                                    "gte":str(int(self.min_timestamp*1e3)),
                                    "lte":str(int(self.max_timestamp*1e3)),
                                    "format":"epoch_millis"
                                    }
                                }
                            },
                            {"query_string":{
                                "analyze_wildcard":"true",
                                "query":self.query
                                }
                            }
                        ]
                    }
                },
                "_source":fields
        }

        return json.dumps(data)

    def fetch(self, max_queries=15, records_per_query=10000):
        """Query ES, return pandas dataframe"""
        dfs = []
        query_counter = 0
        while query_counter <= max_queries:
            # Query elastic search
            data_string = self.get_data_string(
                              num_records=records_per_query,
                              offset=query_counter*records_per_query
                          )
            response = requests.get(self.api_url, headers=self.headers, 
                                    data=data_string)
            # Get the data from the response
            data = response.json()
            # Create a pandas DataFrame with the data retreived
            clean_records = []
            no_data_count = 0
            for record in data["hits"]["hits"]:
                try:
                    clean_record = record["_source"]["data"]
                    clean_records.append(clean_record)
                except:
                    no_data_count += 1
            # Store as pandas dataframe
            dfs.append(pd.DataFrame(clean_records))
            # Check progress
            if (len(clean_records) < records_per_query or
                query_counter >= max_queries):
                break
            query_counter += 1

        return pd.concat(dfs).reset_index()

def fetch_classads(path_to_token, fields, min_timestamp, max_timestamp,
                   query=None):
    """Fetch ES classads records for a given time period"""
    if not query:
        # Default lucene query
        query =         "data.Type:analysis"
        query = query + " AND  data.Status:Completed"
        query = query + " AND  data.JobUniverse:5"

    es = elastic(query, fields, path_to_token)

    return elastic.fetch()

def fetch_xrootd(path_to_token, fields, min_timestamp, max_timestamp,
                 query=None):
    """Fetch ES xrootd records for a given time period"""
    if not query:
        # Default lucene query
        query =         "data.lfn:/store/data*"
        query = query + " AND  data.server_host:/xcache-*/"
        query = query + " AND  data.vo:cms"

    es = elastic(query, fields, path_to_token)

    return elastic.fetch()
