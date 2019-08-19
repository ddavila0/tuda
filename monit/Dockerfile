FROM gitlab-registry.cern.ch/db/cerndb-infra-hadoop-conf:master

RUN yum install -y python2-pip 
RUN pip install --upgrade pip
COPY requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt

RUN mkdir /monicron
RUN mkdir /aggregations
RUN mkdir /logs

COPY jguiang.keytab /monicron/jguiang.keytab
COPY creds.json /monicron/creds.json

COPY populate.py /monicron/populate.py
COPY populate.sh /monicron/populate.sh

COPY run-jobs.sh /monicron/run-jobs.sh
COPY monicron.py /monicron/monicron.py
COPY monicron.sh /monicron/monicron.sh
COPY hdfetchs.py /monicron/hdfetchs.py
COPY utils.py /monicron/utils.py
COPY aggs/. /monicron/aggs
COPY configs/. /monicron/configs
COPY fetchers/. /monicron/fetchers

RUN kinit -k -t /monicron/jguiang.keytab jguiang@CERN.CH

WORKDIR /monicron

ENV OUT_DIR /aggregations
ENV WMA_BROKER /monicron/creds.json
