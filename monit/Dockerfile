FROM gitlab-registry.cern.ch/db/cerndb-infra-hadoop-conf:master

RUN yum update -y && yum install -y python2-pip 
RUN pip install --upgrade pip
COPY requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt

RUN mkdir /aggregations
RUN mkdir /monicron

COPY monicron.py /monicron/monicron.py
COPY monicron.test.sh /monicron/monicron.test.sh
COPY hdfetchs.py /monicron/hdfetchs.py
COPY aggs/. /monicron/aggs
COPY configs/. /monicron/configs
COPY fetchers/. /monicron/fetchers

WORKDIR /monicron

ENV OUT_DIR /aggregations
