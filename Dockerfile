FROM gitlab-registry.cern.ch/db/cerndb-infra-hadoop-conf:master

RUN yum install supervisor cronie -y
ADD supervisor/supervisord.conf /etc

RUN yum install -y vim
RUN yum install -y python2-pip 
RUN pip install --upgrade pip
COPY requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt

RUN mkdir /monicron

COPY monicron.py /monicron/monicron.py
COPY monicron.sh /monicron/monicron.sh
COPY hdfetchs.py /monicron/hdfetchs.py
COPY utils.py /monicron/utils.py
COPY aggs/. /monicron/aggs
COPY configs/. /monicron/configs
COPY fetchers/. /monicron/fetchers

WORKDIR /monicron

CMD exec /usr/bin/supervisord -c /etc/supervisord.conf
