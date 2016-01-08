FROM DOCKER_VAR_OS
MAINTAINER astroshim <hsshim@nflabs.com>

# Update the image with the latest packages
RUN yum update -y; yum clean all
RUN yum install -y \
wget \
tar \
curl \
which \
sudo \
bzip2 \
openssh-server openssh-clients rsync \
&& \
yum clean all

# Remove old jdk
RUN yum remove java; yum remove jdk

# Install oracle jdk7
RUN wget --continue --no-check-certificate --header "Cookie: oraclelicense=accept-securebackup-cookie" -O jdk-linux-x64.rpm "http://download.oracle.com/otn-pub/java/jdk/7u51-b13/jdk-7u51-linux-x64.rpm"
RUN rpm -Uvh jdk-linux-x64.rpm
RUN rm jdk-linux-x64.rpm

ENV JAVA_HOME /usr/java/default
ENV PATH $PATH:$JAVA_HOME/bin

# get hadoop 
RUN mkdir -p /tmp/hadoop
ADD hadoop.tar.gz /tmp/hadoop
RUN ln -s /tmp/hadoop/hadoop* /usr/local/hadoop

ENV HADOOP_PREFIX /usr/local/hadoop
ENV HADOOP_COMMON_HOME /usr/local/hadoop
ENV HADOOP_HDFS_HOME /usr/local/hadoop
ENV HADOOP_MAPRED_HOME /usr/local/hadoop
ENV HADOOP_YARN_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR /usr/local/hadoop/etc/hadoop

RUN sed -i '/^export JAVA_HOME/ s:.*:export JAVA_HOME=/usr/java/default\nexport HADOOP_PREFIX=/usr/local/hadoop\nexport HADOOP_HOME=/usr/local/hadoop\n:' $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh
RUN sed -i '/^export HADOOP_CONF_DIR/ s:.*:export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop/:' $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

RUN mkdir $HADOOP_PREFIX/input
RUN cp $HADOOP_PREFIX/etc/hadoop/*.xml $HADOOP_PREFIX/input

# add config
ADD conf/core-site.xml $HADOOP_PREFIX/etc/hadoop/core-site.xml
ADD conf/hdfs-site.xml $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml
ADD conf/mapred-site.xml $HADOOP_PREFIX/etc/hadoop/mapred-site.xml
ADD conf/yarn-site.xml $HADOOP_PREFIX/etc/hadoop/yarn-site.xml

# format of hdfs
RUN mkdir /data/
RUN chmod 777 /data/
RUN $HADOOP_PREFIX/bin/hdfs namenode -format

RUN ls -la /usr/local/hadoop/etc/hadoop/*-env.sh
RUN chmod +x /usr/local/hadoop/etc/hadoop/*-env.sh
RUN ls -la /usr/local/hadoop/etc/hadoop/*-env.sh

# update buildstep
COPY buildstep.sh /buildstep.sh
RUN chown root.root /buildstep.sh
RUN chmod 700 /buildstep.sh

# update boot script
COPY bootstrap.sh /etc/bootstrap.sh
RUN chown root.root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

# open port for hdfs
EXPOSE 50010 50020 50070 50075 50090
# open port for mapred
EXPOSE 9000 9001
# open port for yarn
EXPOSE 8030 8031 8032 8033 8040 8042 8088
# open port for spark
EXPOSE 8080 7077 7072 8081 8082

ENTRYPOINT ["/etc/bootstrap.sh"]
