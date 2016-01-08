#!/bin/bash
set -e
SPARK_SHARE=/reposhare/$BUILD_TYPE

echo "# ZCI-ENV FILE : $ZCI_ENV"
source /reposhare/$ZCI_ENV

# ----------------------------------------------------------------------
# Functions
# ----------------------------------------------------------------------
function spark_conf		#<- only spark_yarn
{
	home=$1

	if [[ $BUILD_TYPE == "spark_yarn" ]]; then
		echo "- copy spakr conf ."
		\cp -f /tmp/spark_conf/*  $home/conf/
	fi
}

function first_build
{
	SPARK_VER=$1
	SPARK_PRO=$2
	HADOOP_VER=$3
	SPARK_DAT=spark-$SPARK_VER-bin-hadoop$HADOOP_VER

	mvn package -DskipTests -Pspark-$SPARK_PRO -Phadoop-$HADOOP_VER -Ppyspark -Pscalding -B
	echo "- Done : mvn package Install spark-$SPARK_PRO, hadoop-$HADOOP_VER" 

	mvn package -Pbuild-distr -Pspark-$SPARK_PRO -Phadoop-$HADOOP_VER -Ppyspark -Pscalding -B
	echo "- Done : mvn package build-distr spark-$SPARK_PRO, hadoop-$HADOOP_VER"

	echo "- Build mvn package with spark-$SPARK_PRO, hadoop-$HADOOP_VER"
	\cp -f /tmp/zeppelin-env.sh /zeppelin/conf/
	echo "export SPARK_HOME=$SPARK_SHARE/$SPARK_DAT" >> conf/zeppelin-env.sh
	spark_conf "$SPARK_SHARE/$SPARK_DAT"

	mvn verify -Drat.skip=true -Pusing-packaged-distr -Pspark-$SPARK_PRO -Phadoop-$HADOOP_VER -Ppyspark -Pscalding -B
	echo "# Done : mvn verify -Pusing-packaged-distr -Pspark-$SPARK_PRO -Phadoop-$HADOOP_VER -Ppyspark -B"
}

function skiptests_etc_build
{
	SPARK_VER=$1
	SPARK_PRO=$2
	HADOOP_VER=$3
	SPARK_DAT=spark-$SPARK_VER-bin-hadoop$HADOOP_VER

	echo "- Build mvn package with skipTests"
    rm -rf /zeppelin/interpreter/spark
	mvn package -DskipTests -Pspark-$SPARK_PRO -Phadoop-$HADOOP_VER -Ppyspark -B -pl 'zeppelin-interpreter,spark-dependencies,spark'

	echo "- Build mvn package with spark-$SPARK_PRO, hadoop-$HADOOP_VER"
	\cp -f /tmp/zeppelin-env.sh /zeppelin/conf/
	echo "export SPARK_HOME=$SPARK_SHARE/$SPARK_DAT" >> conf/zeppelin-env.sh
	spark_conf "$SPARK_SHARE/$SPARK_DAT"

	mvn package -Pspark-$SPARK_PRO -Phadoop-$HADOOP_VER -B -pl 'zeppelin-interpreter,zeppelin-zengine,zeppelin-server' -Dtest=org.apache.zeppelin.rest.*Test -DfailIfNoTests=false
}

# only 1.2 and 1.1
function etc_build
{
    SPARK_VER=$1
    SPARK_PRO=$2
    HADOOP_VER=$3
    SPARK_DAT=spark-$SPARK_VER-bin-hadoop$HADOOP_VER

	echo "- Build mvn package without skipTests"
    rm -rf /zeppelin/interpreter/spark
	mvn package -Pspark-$SPARK_PRO -Phadoop-$HADOOP_VER -Ppyspark -B -pl 'zeppelin-interpreter,spark-dependencies,spark'

	echo "- Build mvn package with spark-$SPARK_PRO, hadoop-$HADOOP_VER"
    \cp -f /tmp/zeppelin-env.sh /zeppelin/conf/
    echo "export SPARK_HOME=$SPARK_SHARE/$SPARK_DAT" >> conf/zeppelin-env.sh
	mvn package -Pspark-$SPARK_PRO -Phadoop-$HADOOP_VER -B -pl 'zeppelin-interpreter,zeppelin-zengine,zeppelin-server' -Dtest=org.apache.zeppelin.rest.*Test -DfailIfNoTests=false
}


# ----------------------------------------------------------------------
# Init
# ----------------------------------------------------------------------
BUILDSTEP_TIMEOUT=300
BUILDSTEP_DIR=/reposhare/buildstep/$BUILD_TYPE
BUILDSTEP_ZEP=${CONT_NAME}_zeppelin.bs
BUILDSTEP_BAK=${CONT_NAME}_backend.bs

/buildstep.sh init $BUILDSTEP_DIR $BUILDSTEP_TIMEOUT
/buildstep.sh log $BUILDSTEP_ZEP "# Start, zeppelin build ..."

# firefox 
ln -s /reposhare/firefox/firefox /usr/bin/firefox


# ----------------------------------------------------------------------
# Open XVFB
# ----------------------------------------------------------------------
/buildstep.sh log $BUILDSTEP_ZEP "- $BUILDSTEP_ZEP : Info, Launch a XVFB session on display"
/buildstep.sh log $BUILDSTEP_ZEP "- $BUILDSTEP_ZEP : Info, DISPLAY PORT = $DISPLAY"
dbus-uuidgen > /var/lib/dbus/machine-id
Xvfb $DISPLAY -ac -screen 0 1280x1024x24 &


# ----------------------------------------------------------------------
# Cloning zeppelin
# ----------------------------------------------------------------------
/buildstep.sh log $BUILDSTEP_ZEP "- $BUILDSTEP_ZEP : Info, Cloning zeppelin"
git clone -b $BRANCH $REPO /zeppelin
cd /zeppelin


# ----------------------------------------------------------------------
# Build Script
# ----------------------------------------------------------------------
arg_num=0
IFS=' '
read -r -a SPARK_VERSIONS <<< "$SPARK_VERSION"
for i in "${SPARK_VERSIONS[@]}"
do
	SPARK_VERSION=$i
	SPARK_PROFILE=${SPARK_VERSION%.*}
	HADOOP_PROFILE=${HADOOP_VERSION%.*}

	##### Build Step 1
	/buildstep.sh log $BUILDSTEP_ZEP "- $BUILDSTEP_ZEP : started $BUILD_TYPE build spark $SPARK_VERSION"

	##### Build Step 2 ( build spark 1.x )
	if [[ $arg_num == 0 ]]; then
		first_build $SPARK_VERSION $SPARK_PROFILE $HADOOP_PROFILE
	else
		if [[ $SPARK_PROFILE == "1.2" || $SPARK_PROFILE == "1.1" ]]; then
			etc_build $SPARK_VERSION $SPARK_PROFILE $HADOOP_PROFILE
		else
			skiptests_etc_build $SPARK_VERSION $SPARK_PROFILE $HADOOP_PROFILE
		fi
	fi
	let "arg_num+=1"

	##### Build Step 3
	/buildstep.sh log $BUILDSTEP_ZEP "- $BUILDSTEP_ZEP : finished $BUILD_TYPE build spark $SPARK_VERSION"
	/buildstep.sh log $BUILDSTEP_ZEP "- $BUILDSTEP_ZEP : wait for backend - spark $SPARK_VERSION"
	/buildstep.sh waitfor $BUILDSTEP_BAK "- $BUILDSTEP_BAK : closed $BUILD_TYPE backend spark $SPARK_VERSION"
done
echo "Done!"


# ----------------------------------------------------------------------
# End of Script
# ----------------------------------------------------------------------
