# What is TUDA?

TUDA is a project developed with funding from Google Summer of Code through the CMS Data Access 
[project](https://summerofcode.withgoogle.com/projects/#5810325671116800) for [CERN-HSF](http://hepsoftwarefoundation.org/). More specifically, these "tools" allow for the aggregation and analysis of data pertaining to the usage of the US-CMS cache infrastructure.

![GSoC Logo](/docs/assets/gsoc.png)

# Monicron
Formost among these tools is the [monicron](https://github.com/jkguiang/tuda/tree/master/monit) application, which aggregates data in regular, predefined intervals, then pushes these aggregations to a database such that graphical dashboards may be used to visualize the aggregated data, allowing for a single user to monitor the health of the US CMS cache infrastructure "at a glance."

### Technical Description
Monicron uses many different technologies that are best introduced by walking through its workflow. First, Pyspark is used to query the Hadoop File System (HDFS) hosted by CERN for data of various categories. Once the data has been collected, it is converted into a Pandas dataframe for final aggregation. A [plugin](https://github.com/jkguiang/tuda/tree/master/monit#adding-a-hdfs-source) system was devised to allow the application to be extentable to other parts of the US CMS cache infrastructure, an extension that is planned to take place in the near future. These aggregations are cached locally, then pushed through StompAMQ to an Elasticsearch database maintained by the MONiT Team at CERN in order to take advantage of the Grafana service set up by them. The entire application is deployed within a Docker container on a VM hosted by CERN Openstack.

![tech](/docs/assets/technologies.png)

# Insights
Although the TUDA project is still quite new, new and useful information has already been revealed after careful analysis of the aggregations made by Monicron.

# Presentations
Due to the collaborative nature of any work done at CERN, Monicron has already been the subject of some discussion. Presentations given on Monicron and related TUDA projects are listed here:
1. [Jul. 31st, 2019](http://uaf-10.t2.ucsd.edu/~jguiang/presentations/monicron/monicron_07-31-2019.pdf): Given at the OSG weekly meeting.
2. [Aug. 15th, 2019](http://uaf-10.t2.ucsd.edu/~jguiang/presentations/monicron/monicron_08-15-2019.pdf): Given at the CMS Computer Monitoring biweekly meeting.
