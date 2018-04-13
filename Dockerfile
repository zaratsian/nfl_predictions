FROM centos

######################################################################################################
#
#   Vars
#
######################################################################################################

ARG JAVA_VER=java-1.8.0-openjdk-devel

ARG SPARK_URL=https://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz
ARG SPARK_VER=spark-2.2.0-bin-hadoop2.7

######################################################################################################
#
#   Dependancies
#
######################################################################################################

RUN yum install -y ${JAVA_VER}
RUN echo "export JAVA_HOME=/usr/lib/jvm/java" >> /root/.bashrc

RUN yum install -y epel-release
RUN yum update -y

RUN yum install -y wget
RUN yum install -y unzip
RUN yum install -y git

######################################################################################################
#
#   Install Spark
#
######################################################################################################

RUN wget ${SPARK_URL} -O /spark.tgz
RUN tar -xzvf spark.tgz
RUN mv ${SPARK_VER} /spark
RUN rm /spark.tgz

######################################################################################################
#
#   Install Python Dependancies 
#
######################################################################################################

RUN curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
RUN python get-pip.py
RUN rm get-pip.py
RUN pip install flask
RUN pip install requests
RUN pip install numpy

######################################################################################################
#
#   Assets
#
######################################################################################################

ADD assets /assets

#CMD source /root/.bashrc; cd /spark; /zeppelin/bin/zeppelin-daemon.sh start; superset runserver
