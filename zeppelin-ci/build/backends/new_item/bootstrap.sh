#!/bin/bash
set -e
source /reposhare/$ZCI_ENV

# ----------------------------------------------------------------------
# Init
# ----------------------------------------------------------------------
BUILDSTEP_TIMEOUT=3600          #<- sec
BUILDSTEP_DIR=/reposhare/buildstep/$BUILD_TYPE
BUILDSTEP_ZEP=${CONT_NAME}_zeppelin.bs
BUILDSTEP_BAK=${CONT_NAME}_backend.bs
SPARK_SHARE=/reposhare/$BUILD_TYPE

/buildstep.sh init $BUILDSTEP_DIR $BUILDSTEP_TIMEOUT
/buildstep.sh log $BUILDSTEP_BAK "# Start, backend build ..."

if [ -f $BUILDSTEP_DIR/$BUILDSTEP_ZEP ]; then
    mv $BUILDSTEP_DIR/$BUILDSTEP_ZEP $BUILDSTEP_DIR/.$BUILDSTEP_ZEP.bak
fi


# ----------------------------------------------------------------------
# Setup spark & firefox
# ----------------------------------------------------------------------
# setup firefox
/buildstep.sh log $BUILDSTEP_BAK "- $BUILDSTEP_BAK : setup firefox"
FIREFOX_BIN=firefox-31.0.tar.bz2
if [ ! -d /reposhare/$FIREFOX_BIN ]; then
    tar xjf /reposhare/$FIREFOX_BIN -C /reposhare
fi

# setup spark
mkdir -p $SPARK_SHARE
IFS=' ' read -r -a SPARK_BIN_ARR <<< "$SPARK_VERSION"
for i in "${SPARK_BIN_ARR[@]}"
do
    SPARK_VER=$i
	HADOOP_PRO=${HADOOP_VERSION%.*}
    SPARK_DAT=spark-$SPARK_VER-bin-hadoop$HADOOP_PRO
    SPARK_BIN=$SPARK_DAT.tgz

    # download
    if [ ! -f /reposhare/$SPARK_BIN ]; then
        /buildstep.sh log $BUILDSTEP_BAK "- $BUILDSTEP_BAK : Doesn't exist spark -> downloading : /reposhare/$SPARK_BIN"
        tmp_path=/tmp/$BUILD_TYPE
        mkdir -p $tmp_path
        wget -P $tmp_path http://mirror.tcpdiag.net/apache/spark/spark-$SPARK_VER/$SPARK_BIN
        mv $tmp_path/$SPARK_BIN /reposhare
    fi

    # setup
    /buildstep.sh log $BUILDSTEP_BAK "- $BUILDSTEP_BAK : setup version $SPARK_VER";
    if [ ! -d $SPARK_SHARE/$SPARK_DAT ]; then
        tar xzf /reposhare/$SPARK_BIN -C $SPARK_SHARE
    fi
done

export SPARK_MASTER_PORT=7077
export SPARK_LOCAL_IP=`awk 'NR==1 {print $1}' /etc/hosts`

sed '1d' /etc/hosts > /tmp/hosts
cat /tmp/hosts > /etc/hosts
rm /tmp/hosts
echo "$SPARK_LOCAL_IP `hostname`" >> /etc/hosts

/buildstep.sh log $BUILDSTEP_BAK "- $BUILDSTEP_BAK : Setup Succeed"


# ----------------------------------------------------------------------
# Starting or Setting new_item
# ----------------------------------------------------------------------
#
# <- insert new item scripts.
#
# ----------------------------------------------------------------------


# ----------------------------------------------------------------------
# Run spark (start & stop)
# ----------------------------------------------------------------------
IFS=' ' read -r -a SPARK_VERSIONS <<< "$SPARK_VERSION"
for i in "${SPARK_VERSIONS[@]}"
do
    ##### set spark env
    SPARK_VER=$i
	HADOOP_PRO=${HADOOP_VERSION%.*}
    SPARK_DAT=spark-$SPARK_VER-bin-hadoop$HADOOP_PRO
    export SPARK_HOME="$SPARK_SHARE/$SPARK_DAT"
    cd $SPARK_HOME/sbin

	# --------------------------------------------------
	# Setting SPARK_HOME for new_item
	# --------------------------------------------------
	#
	# <- insert new item scripts.
	#
	# --------------------------------------------------

    ##### Build Step 1
    /buildstep.sh waitfor $BUILDSTEP_ZEP "- $BUILDSTEP_ZEP : started $BUILD_TYPE build spark $SPARK_VER"
    /buildstep.sh log $BUILDSTEP_BAK "- $BUILDSTEP_BAK : started $BUILD_TYPE backend spark $SPARK_VER"

    ##### starting
    ./start-master.sh

    set +e
    echo $SPARK_VER | grep "^1.[123].[0-9]" > /dev/null
    let ret=$?

    set -e
    if [ $ret -eq 0 ]; then   # spark 1.3 or prior
        ./start-slave.sh 1 `hostname`:$SPARK_MASTER_PORT
    else
        ./start-slave.sh spark://`hostname`:$SPARK_MASTER_PORT
    fi

    ##### Build Step 2
    /buildstep.sh log $BUILDSTEP_BAK "- $BUILDSTEP_BAK : wait for zeppelin - $BUILD_TYPE $SPARK_VER"
    /buildstep.sh waitfor $BUILDSTEP_ZEP "- $BUILDSTEP_ZEP : finished $BUILD_TYPE build spark $SPARK_VER"

    ##### stopping spark
    ./spark-daemon.sh stop org.apache.spark.deploy.worker.Worker 1
    ./stop-master.sh

    ##### Build Step 3
    /buildstep.sh log $BUILDSTEP_BAK "- $BUILDSTEP_BAK : closed $BUILD_TYPE backend spark $SPARK_VER"
done


# ----------------------------------------------------------------------
# Tail
# ----------------------------------------------------------------------
CMD=${1:-"exit 0"}
if [[ "$CMD" == "-d" ]];
then
	service sshd stop
	/usr/sbin/sshd -D -d
else
	/bin/bash -c "$*"
fi


# ----------------------------------------------------------------------
# End of Script
# ----------------------------------------------------------------------
