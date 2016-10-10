#!/usr/bin/python
import sys, getopt, traceback, json, re
import os, time


'''
export SPARK_HOME=/home/nflabs/spark-1.6.2-bin-hadoop2.3
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.9-src.zip
'''

f = open('/home/nflabs/debug','w')

#os.environ['SPARK_HOME'] = "/home/nflabs/spark-1.6.2-bin-hadoop2.3"
#os.environ['PYTHONPATH'] = "$SPARK_HOME/python/lib/py4j-0.9-src.zip"
#sys.path.append("/home/nflabs/spark-1.6.2-bin-hadoop2.3/python")
#sys.path.append("/home/nflabs/spark-1.6.2-bin-hadoop2.3/python/lib")

'''
#sys.path.append("/home/nflabs/zeppelin/local-repo/net/sf/py4j/py4j/0.8.2.1")
sys.path.append("/home/nflabs/zeppelin/spark-dependencies/target/spark-dist/spark-1.6.1/python")
#sys.path.append("/home/nflabs/zeppelin/spark-dependencies/target/spark-dist/spark-1.6.1/python/lib")
sys.path.append("/home/nflabs/zeppelin/spark-dependencies/target/spark-dist/spark-1.6.1/python/lib/py4j-0.9-src.zip")
'''

sys.path.append("/home/nflabs/zeppelin/spark-dependencies/target/spark-dist/spark-2.0.0/python")
sys.path.append("/home/nflabs/zeppelin/spark-dependencies/target/spark-dist/spark-2.0.0/python/lib/py4j-0.10.1-src.zip")


f.write('step 1\n')
try:
  from py4j.java_gateway import java_import, JavaGateway, GatewayClient
  from py4j.protocol import Py4JJavaError
  from pyspark.conf import SparkConf
  from pyspark.context import SparkContext
  from pyspark.rdd import RDD
  from pyspark.files import SparkFiles
  from pyspark.storagelevel import StorageLevel
  from pyspark.accumulators import Accumulator, AccumulatorParam
  from pyspark.broadcast import Broadcast
  from pyspark.serializers import MarshalSerializer, PickleSerializer
  import ast
  import traceback

  # for back compatibility
  from pyspark.sql import SQLContext, HiveContext, Row


  f.write('step 2\n')
  count = 0
  while True:
    print 'The count is:', count
    count = count + 1
    time.sleep(1)

except ImportError as e:
  f.write('step 33\n')
  print ("Error importing Spark Modules", e)

f.write('step 4\n')
f.close()
sys.exit(1)


'''
from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from py4j.protocol import Py4JJavaError
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.files import SparkFiles
from pyspark.storagelevel import StorageLevel
from pyspark.accumulators import Accumulator, AccumulatorParam
from pyspark.broadcast import Broadcast
from pyspark.serializers import MarshalSerializer, PickleSerializer
import ast
import traceback
import os

# for back compatibility
from pyspark.sql import SQLContext, HiveContext, Row
'''




print "Good bye!"

