FROM DOCKER_VAR_OS
MAINTAINER astroshim <hsshim@nflabs.com>

# update the image with the latest packages
RUN yum update -y; yum clean all
RUN yum install -y \
wget \
tar \
bzip2 \
openssh-server \
openssh-clients \
curl \
&& \
yum clean all

# remove old jdk
RUN yum remove java; yum remove jdk

# install oracle jdk7
RUN wget --continue --no-check-certificate --header "Cookie: oraclelicense=accept-securebackup-cookie" -O jdk-linux-x64.rpm "http://download.oracle.com/otn-pub/java/jdk/7u51-b13/jdk-7u51-linux-x64.rpm"
RUN rpm -Uvh jdk-linux-x64.rpm
RUN rm jdk-linux-x64.rpm

ENV JAVA_HOME /usr/java/default
ENV PATH $PATH:$JAVA_HOME/bin

# update buildstep
COPY buildstep.sh /buildstep.sh
RUN chown root.root /buildstep.sh
RUN chmod 700 /buildstep.sh

# update boot script
COPY bootstrap.sh /etc/bootstrap.sh
RUN chown root.root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

# ---------------------------------
# install new item
# ---------------------------------
#
# - insert new item scripts.
# 
# - open port for new item
# EXPOSE xxxx
#
# ---------------------------------

# open port for spark
EXPOSE 8080 7077 8888 8081

ENTRYPOINT ["/etc/bootstrap.sh"]
