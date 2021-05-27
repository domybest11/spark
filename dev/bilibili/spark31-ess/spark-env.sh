#!/usr/bin/env bash

export SPARK_HOME=/data/app/jssz-spark31-ess
export HADOOP_CONF_DIR=/etc/hadoop
export SPARK_LOG_DIR=/data/log/jssz-spark31-ess

export PATH=$PATH:${SPARK_HOME}/bin
export SPARK_YARN_MODE=true
export SPARK_DIST_CLASSPATH="/data/service/hadoop/share/hadoop/common/lib/*"
export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:/data/service/hadoop/share/hadoop/common/*"
export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:/data/service/hadoop/share/hadoop/hdfs/lib/*"
export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:/data/service/hadoop/share/hadoop/hdfs/*"
export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:/data/service/hadoop/share/hadoop/mapreduce/lib/*"
export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:/data/service/hadoop/share/hadoop/mapreduce/*"
export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:/data/service/hadoop/share/hadoop/yarn/lib/*"
export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:/data/service/hadoop/share/hadoop/yarn/*"
export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:/data/service/hadoop/share/hadoop/tools/lib/*"
export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:/data/sdk/lzo/jar/*"

export SPARK_DAEMON_MEMORY=8G
#export SPARK_SHUFFLE_OPTS=""