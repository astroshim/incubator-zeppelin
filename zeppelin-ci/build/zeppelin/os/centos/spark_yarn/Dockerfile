FROM DOCKER_VAR_OS
MAINTAINER astroshim <hsshim@nflabs.com>

# set env 
ENV MAVEN_VERSION 3.3.3

# get utils.
RUN yum install -y \
bzip2 \
wget \
git \
python-setuptools python-dev python-numpy \
npm \
tar \
unzip \
&& \
yum clean all

# remove old jdk
RUN yum remove java; yum remove jdk

# install oracle jdk7
RUN wget --continue --no-check-certificate --header "Cookie: oraclelicense=accept-securebackup-cookie" -O jdk-linux-x64.rpm "http://download.oracle.com/otn-pub/java/jdk/7u80-b15/jdk-7u80-linux-x64.rpm"
RUN rpm -Uvh jdk-linux-x64.rpm
RUN rm jdk-linux-x64.rpm
ENV JAVA_HOME /usr/java/default
ENV PATH $PATH:$JAVA_HOME/bin

# install maven
COPY apache-maven-$MAVEN_VERSION-bin.tar.gz /tmp/
RUN tar xzf /tmp/apache-maven-$MAVEN_VERSION-bin.tar.gz -C /usr/share \
  && mv /usr/share/apache-maven-$MAVEN_VERSION /usr/share/maven \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn
ENV MAVEN_HOME /usr/share/maven
COPY maven-settings.xml /usr/share/maven/conf/settings.xml

# copy zeppelin config.
COPY zeppelin-env.sh /tmp/

# copy spark config.
RUN mkdir -p /tmp/spark_conf
COPY conf/* /tmp/spark_conf/

# fake display with firefox 
ENV DISPLAY :99
RUN rpm -Uvh http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
RUN yum -y install xorg-x11-server-Xvfb x11vnc
RUN yum -y install xorg-x11-fonts-cyrillic
RUN yum -y install libXfont-devel
RUN yum -y install xorg-x11-fonts-100dpi
RUN yum -y install xorg-x11-fonts-75dpi
RUN yum -y install xorg-x11-apps
RUN yum -y install ImageMagick

# firefox dependencies
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/local/firefox:/usr/lib:/usr/local/lib:/usr/lib64:/usr/local/lib64:/lib64:/lib 
RUN yum install -y alsa-lib.x86_64
RUN yum install -y libmozalloc.so

# Py4j for PySpark
RUN easy_install py4j

# update reposhare
RUN mkdir -p /reposhare

# update buildstep
COPY buildstep.sh /buildstep.sh
RUN chown root.root /buildstep.sh
RUN chmod 700 /buildstep.sh

# update boot script
COPY build.sh /build.sh
RUN chown root.root /build.sh
RUN chmod 700 /build.sh

EXPOSE 8080 8081

ENTRYPOINT ["/build.sh"]
