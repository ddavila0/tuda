# Monicron
Monicron is an easily configurable system that is designed to run daily aggregations over data stored in HDFS at CERN.

### Contents
1. [Establishing a Monicron Development Environment](#establishing-a-monicron-development-environment)
2. [Adding a HDFS Source](#adding-a-hdfs-source)

## Establishing a Monicron Development Environment
1. Clone or otherwise download this repository
2. Install Docker (instructions can be found [here](https://docs.docker.com/install/))
3. Download the CERN HDFS Docker Image (following these [instructions](https://hadoop-user-guide.web.cern.ch/hadoop-user-guide/getstart/client_docker.html))
```console
$ docker login gitlab-registry.cern.ch
```
Specifically, pull the "master" image.
```console
$ docker pull gitlab-registry.cern.ch/db/cerndb-infra-hadoop-conf:master
```
4. Ensure that ports 5001 to 5300 are open so that Spark may use them
```console
$ sudo firewall-cmd --permanent --add-port=5001-5300/tcp
$ sudo firewall-cmd --reload
```
5. Launch the Docker container
```console
$ ./run-docker.sh
Building monicron image...
...
Starting container...
Intialized container named 264a20792e179c81e2d5f7e72855841348f672fdc9c9ec9d6d22279d9ecdd575
```
This will print out the Docker container's name as in the example above where, in this case, the container name is `264a2079...`

6. Attach your session to the Docker container using the container name `<container>` produced after the previous step
```console
$ docker attach <container>
```
7. Initialize a Kerberos certificate
```console
$ kinit <user>
```
You can detach from the container by pressing `Ctrl`+`p` followed immediately by `Ctrl`+`q`.

## Adding a HDFS Source
1. Write a fetching function to a file in [/fetchers](https://github.com/jkguiang/tuda/tree/master/monit/fetchers). Here's an 
example:
```python
from .fetch_utils import fetch_wrapper, SPARK_SESSION as spark

@fetch_wrapper(tag="foo", cache="socal")
def fetch_foo(hdfs_path):
    """Fetcher function designed to retrieve certain files from a given HDFS path
       and return a Spark Dataframe of those files
    """  
    # Get job reports
    jobreports = spark.read.json(hdfs_path)
    # Get dataset
    df = (jobreports
            # A dummy filter
            .filter(col('data.some_col') == "some_val")
         )

    return df
```
Note that we have associated the function `fetch_foo` with the tag "foo" and the cache "socal" through the decorator `@fetch_wrapper`. This will be important later, but this essentially allows for a plugin system such that new fetchers and aggregations may be added at will for any cache and tied together by a single configuration file.

2. Write some aggregations to a file in [/aggs](https://github.com/jkguiang/tuda/tree/master/monit/aggs). For example, for a 
source "foo," an aggregation would look like this:
```python
from .agg_utils import agg_wrapper

@agg_wrapper(tags=["foo"])
def some_aggregation(df):
    """Aggregation function that returns some (hopefully) useful value
    
    Parameters
    ----------
    df: pandas dataframe
    """
 
    return df.some_col.pd_agg_func()
```
It is important to note here that each aggregation function will have its results written to a `data.json` file, where its 
entry in that file will inherit its name. In this case, were the above function to be run, an entry named "some_aggregation" 
would be recorded in `data.json`.

Additionally, you may also design an aggregation that performs calculations on other aggregations. This simply needs to be 
marked as a "post_agg" as such:
```python
@agg_wrapper(tag=["foo"], post_agg=True)
def some_post_aggregation(aggs):
    """Aggregation function that aggregates previously made aggregations (i.e. values returned
       by functions where post_agg == False, which is the default configuration)
    
    Parameters
    ----------
    aggs: dictionary of aggregations
    """

    return aggs["some_aggregation"]/aggs["another_aggregation"]
```
By default, the `post_agg` keyword argument is set to `False`.

Finally, an aggregation may point to several sources. This is done by simply including more names in the list supplied to the 
`tags` keyword argument:
```python
@agg_wrapper(tags=["foo", "bar", "baz"])
```
You may have noticed that we do not specify a cache here. One is free to do that by supplying a cache name to the cache keyword in the `@agg_wrapper` decorator, but it is empty by default under the assumption that aggregations are likely to be re-used for many different caches.

3. Write a configuration file to [/configs](https://github.com/jkguiang/tuda/tree/master/monit/configs):
```python
{
    "tag": "foo_blah",
    "hdfs_base": "/path/to/foo/in/hdfs",
    "hdfs_ext": "json.gz",
    "cache_name": "socal",
    "source_name": "foo",
    "namespace": "blah" # Optional, but allows for greater division of data for a single source
}
```



