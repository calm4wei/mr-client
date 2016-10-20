#!/bin/bash

/usr/bin/hadoop jar earth-1.0-SNAPSHOT.jar earth/mr/WordCount /user/cstor/mr/input/text.txt /user/cstor/mr/output2

bin/hadoop jar test/earth-1.0-SNAPSHOT/earth-1.0-SNAPSHOT.jar earth/mr/WordCount /user/hjg/input/file01 /user/test/output

bin/hadoop jar test/earth-1.0-SNAPSHOT/earth-1.0-SNAPSHOT.jar earth/mr/ETLSeedData


export RAZOR_MR_HOME=$(dirname $(cd $(dirname $0); pwd))
export HADOOP_CONF=/etc/hadoop/conf.cloudera.yarn
export HBASE_CONF=/etc/hbase/conf
export HADOOP_CLASSPATH=/opt/cloudera/parcels/CDH/lib/hadoop/client/*
export LD_LIBRARY_PATH=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native
export HBASE_CLASSPATH=/opt/cloudera/parcels/CDH/lib/hbase/lib/*
export HBASE_NAMESPACE='razor'

CLASSPATH="$CONF_PATH:$HADOOP_CLASSPATH:$HBASE_CLASSPATH:$CLASSPATH"
$JAVA -Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8 -cp $CLASSPATH $JAVA_OPTS $CLASS