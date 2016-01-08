#!/bin/bash
set -e
source $1
REPO_HOME=$2


# ----------------------------------------------------------------------
# Get Spark
# ----------------------------------------------------------------------
echo "@ Confirm spark binary"
IFS=' ' read -r -a SPARK_BIN_ARR <<< "$SPARK_VERSION"
for i in "${SPARK_BIN_ARR[@]}"
do
    SPARK_VER=$i
	HADOOP_PRO=${HADOOP_VERSION%.*}
    SPARK_BIN=spark-$SPARK_VER-bin-hadoop$HADOOP_PRO.tgz

    # download
    if [ ! -f $REPO_HOME/$SPARK_BIN ]; then
		set +e
        echo " - Doesn't exist spark : $REPO_HOME/$SPARK_BIN"
		echo ""
        wget -P $REPO_HOME http://mirror.tcpdiag.net/apache/spark/spark-$SPARK_VER/$SPARK_BIN

		set -e
		if [ ! -f $REPO_HOME/$SPARK_BIN ]; then
            echo " - Retry Downloading..."
            wget -P $REPO_HOME http://apache.mirror.cdnetworks.com/spark/spark-$SPARK_VER/$SPARK_BIN
        fi
    fi

    # setup
    echo " - Setup : $SPARK_VER";
    if [ ! -f $REPOSHARE_PATH/$SPARK_BIN ]; then
        cp -f $REPO_HOME/$SPARK_BIN $REPOSHARE_PATH/
    fi
done


# ----------------------------------------------------------------------
# Get Firefox
# ----------------------------------------------------------------------
echo "@ Confirm firefox binary"
FIREFOX_BIN=firefox-31.0.tar.bz2
if [ ! -f $REPO_HOME/$FIREFOX_BIN ]; then
    echo " - Doesn't exist firefox : $REPO_HOME/$FIREFOX_BIN"
	echo ""
    wget -P $REPO_HOME http://ftp.mozilla.org/pub/firefox/releases/31.0/linux-x86_64/en-US/$FIREFOX_BIN
fi

if [ ! -f $REPOSHARE_PATH/$FIREFOX_BIN ]; then
    cp -f $REPO_HOME/$FIREFOX_BIN $REPOSHARE_PATH/
fi


# ----------------------------------------------------------------------
# Get Maven
# ----------------------------------------------------------------------
echo "@ Confirm maven binary"
MAVEN_BIN=apache-maven-3.3.3-bin.tar.gz
if [ ! -f $REPO_HOME/$MAVEN_BIN ]; then
	echo " # Doesn't exist maven."; echo""
	wget -P $REPO_HOME http://archive.apache.org/dist/maven/maven-3/3.3.3/binaries/$MAVEN_BIN
fi
cp -f $REPO_HOME/$MAVEN_BIN $BUILD_PATH/


# ----------------------------------------------------------------------
# Get Hadoop
# ----------------------------------------------------------------------
if [ $type = "spark_yarn" ]; then
	echo "@ Confirm hadoop-$HADOOP_VERSION binary"
    HADOOP_BIN=hadoop-$HADOOP_VERSION.tar.gz
    if [ ! -f $REPO_HOME/$HADOOP_BIN ]; then
        echo " - Doesn't exist hadoop : $REPO_HOME/$HADOOP_BIN"
        echo ""
        wget -P $REPO_HOME https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/$HADOOP_BIN
    fi
fi
cp -f $REPO_HOME/$HADOOP_BIN $BUILD_PATH/hadoop.tar.gz


echo ""
# ----------------------------------------------------------------------
# End of Script
# ----------------------------------------------------------------------
