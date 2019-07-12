import requests
import json
import time
import pandas as pd

MONIT_GRAPHANA_API = ("https://monit-grafana.cern.ch/"
                      + "api/datasources/proxy/9014/_search")

class elastic():
    """ELASTIC: ES Local Analysis Search Tool Implemented as a Class"""
    
    def __init__(self, query, fields, aliases, path_to_token, 
                 api_url=MONIT_GRAPHANA_API,
                 min_timestamp=time.time()-3600*6,
                 max_timestamp=time.time()):
        self.query = query
        self.fields = fields
        self.aliases = aliases
        self.token = self.read_token(path_to_token)
        self.api_url = api_url
        self.min_timestamp = str(int(min_timestamp*1e3))
        self.max_timestamp = str(int(max_timestamp*1e3))
        self.headers = {"Authorization": "Bearer "+self.token}

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

    def get_data_string(self, num_records=10000):
        """Return stringified JSON of ES request"""
        data = {
            "size":num_records,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {
                            "data.RecordTime": {
                                "gte":self.min_timestamp,
                                "lte":self.max_timestamp,
                                "format":"epoch_millis"
                                }
                            }
                        },
                        {"query_string": {
                            "analyze_wildcard":"true",
                            "query":self.query
                            }
                        }
                    ]
                }
            },
            "_source":self.fields
        }

        return json.dumps(data)

    def fetch(self, max_queries=150, timeout="10m"):
        """Query ES, return pandas dataframe"""
        # Get scroll context
        init_request_string = self.get_data_string()
        main_request = {"scroll": timeout}
        scroll_id = ""
        dfs = []
        for query in range(0, max_queries):
            hits = None
            # Query elastic search
            if query == 0:
                response = requests.get(self.api_url+"?scroll="+timeout,
                                        headers=self.headers,
                                        data=init_request_string)
            else:
                main_request["scroll_id"] = scroll_id
                response = requests.get(self.api_url+"/scroll",
                                        headers=self.headers,
                                        data=json.dumps(main_request))
            # Handle response
            response_json = response.json()
            scroll_id = response_json["_scroll_id"]
            hits = response_json["hits"]["hits"]
            if len(hits) == 0:
                break
            else:
                # Create a pandas DataFrame with the data retreived
                clean_records = []
                no_data_count = 0
                for record in hits:
                    try:
                        clean_record = record["_source"]["data"]
                        clean_records.append(clean_record)
                    except:
                        no_data_count += 1
                # Store as pandas dataframe
                dfs.append(pd.DataFrame(clean_records))
                print("Retrieved DataFrame "
                      +"with shape {}".format(dfs[query].shape))

                if dfs[query].shape[0] == 0:
                    break
                else:
                    query += 1

        df = pd.concat(dfs).reset_index()
        new_names = dict(zip(df.columns, ["index"]+self.aliases))
        df.rename(columns=new_names, inplace=True)
        return df

def fetch_classads(path_to_token, fields, aliases, min_timestamp,
                   max_timestamp, query=None):
    """Fetch ES classads records for a given time period"""
    if not query:
        # Default lucene query
        query = ("data.Type:analysis AND data.Status:Completed "
                 + "AND data.JobUniverse:5 AND data.VO:cms")

    es = elastic(query, fields, aliases, path_to_token)

    return es.fetch()

def fetch_xrootd(path_to_token, fields, aliases, min_timestamp,
                 max_timestamp, query=None):
    """Fetch ES xrootd records for a given time period"""
    if not query:
        # Default lucene query
        query = ("data.lfn:/store/data* AND data.vo:cms "
                 + "AND (data.server_host:xrd-cache-* "
                 + "OR data.server_host:xcache-*)")

    es = elastic(query, fields, aliases, path_to_token)

    return es.fetch()

if __name__ == "__main__":
    from dateutil import parser as parser
    from monit import get_timestamp
    from aggs import agg_utils

    min_date = parser.parse("Jun 23 2019 00:00:00 UTC")
    max_date = parser.parse("Jun 29 2019 23:59:59 UTC")
    min_timestamp = get_timestamp(min_date)
    max_timestamp = get_timestamp(max_date)

    with open("./configs/classads.json", "r") as fin:
        config = json.load(fin)

    classads_df = fetch_classads("./token", 
                                 config["fields"], config["aliases"],
                                 min_timestamp, max_timestamp)

    classads_df["workflow_id"] = (classads_df.workflow_id.map(str)
                                  + "_"
                                  + classads_df.num_retries.astype(str))
    classads_df["job_id"] = (classads_df.crab_id.map(str)
                             + "/"
                             + classads_df.workflow_id)

    results = agg_utils.run_aggs(classads_df, "classads")
    print(results)
