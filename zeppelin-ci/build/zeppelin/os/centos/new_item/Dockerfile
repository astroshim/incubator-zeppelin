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
java-1.7.0-openjdk \
java-1.7.0-openjdk-devel \
unzip \
&& \
yum clean all

# set java home
ENV JAVA_HOME /etc/alternatives/jre_openjdk/
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

# Py4j for PySpark
RUN easy_install py4j

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


# ---------------------------------
# install new item
# ---------------------------------
#
# - insert new item scripts.
#
# ---------------------------------


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
