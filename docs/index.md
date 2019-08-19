# What is TUDA?

TUDA is a project developed with funding from Google Summer of Code through the CMS Data Access 
[project](https://summerofcode.withgoogle.com/projects/#5810325671116800) for [CERN-HSF](http://hepsoftwarefoundation.org/). More specifically, these "tools" allow for the aggregation and analysis of data pertaining to the usage of the US-CMS cache infrastructure.

![GSoC Logo](assets/gsoc.png)

# Monicron
Formost among these tools is the [Monicron](https://github.com/jkguiang/tuda/tree/master/monit) application, which aggregates data in regular, predefined intervals, then pushes these aggregations (defined [here](https://github.com/jkguiang/tuda/tree/master/monit/notebooks)) to a database such that graphical dashboards may be used to visualize the aggregated data, allowing for a single user to monitor the health of the US CMS cache infrastructure "at a glance."

### Technologies
Monicron uses many different technologies that are best introduced by walking through its workflow. First, Pyspark is used to query the Hadoop File System (HDFS) hosted by CERN for data of various categories. Once the data has been collected, it is converted into a Pandas dataframe for final aggregation. A [plugin](https://github.com/jkguiang/tuda/tree/master/monit#adding-a-hdfs-source) system was devised to allow the application to be extentable to other parts of the US CMS cache infrastructure, an extension that is planned to take place in the near future. These aggregations are cached locally, then pushed through StompAMQ to an Elasticsearch database maintained by the MONiT Team at CERN in order to take advantage of the Grafana service set up by them. The entire application is deployed within a Docker container on a VM hosted by CERN Openstack.

![Technologies](assets/technologies.png)

### Insights
Although only recently implimented, Monicron has already provided new and useful information. Those described here are only a handful of what Monicron delivers.

# Other Tools
While producing Monicron, various analyses were performed in order to validate steps towards Monicron or otherwise satisfy general curiousity. There are two contained in this repository:
1. **[ClassAds vs. XRootD Analysis](https://nbviewer.jupyter.org/github/jkguiang/tuda/blob/master/analysis/SanityChecks_XRootD-vs-ClassAds.ipynb):** Early on, an analysis of the agreement between two separate monitoring sources, ClassAds and XRootD covering clientside and serverside cache access information respectively, was performed in order to determine if the two could be reliably coupled in order to form a complete description of cache access patterns. It was found that the two _generally_ agreed, however they did not _completely_ agree for reasons that remain a mystery at the time of writing.

2. **[ClassAds Classifier](https://nbviewer.jupyter.org/github/jkguiang/tuda/blob/master/analysis/BDT.ipynb):** In direct response to the analysis described above, a Boosted Decision Tree (BDT) was trained with the intent of creating a classifier that would determine which ClassAds are likely to not be accounted for in XRootD and vice versa. The BDT did not yield any particularly interesting results, but it established a framework for future efforts.

# Presentations
Due to the collaborative nature of any work done at CERN, Monicron has already been the subject of some discussion. Presentations given on Monicron and related TUDA projects are listed here:
1. [Jul. 31st, 2019](http://uaf-10.t2.ucsd.edu/~jguiang/presentations/monicron/monicron_07-31-2019.pdf): Given at the OSG weekly meeting.
2. [Aug. 7th, 2019](http://uaf-10.t2.ucsd.edu/~jguiang/presentations/monicron/monicron_08-07-2019.pdf): Given at the CMS Computer Monitoring biweekly meeting.
