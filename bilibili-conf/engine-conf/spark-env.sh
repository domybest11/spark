#!/usr/bin/env bash

export SPARK_HOME=/data/service/spark3.1
export SPARK_YARN_MODE=true
export PATH=$PATH:$SPARK_HOME/bin
export YARN_CONF_DIR=/etc/kerberos-hadoop
export HADOOP_CONF_DIR=/etc/kerberos-hadoop
export LZO_CLASSPATH=/data/sdk/lzo/jar/*
export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:${LZO_CLASSPATH}"

export CONF_USE_CONF=false
export CONF_VERSION=spark-sink
export CONF_HOST=config.bilibili.co
export CONF_PATH=/tmp/spark-conf
export CONF_RELOAD=false
export CONF_TOKEN=231ef1c5882e85157fb2300cf695bf7c
export TREE_ID=143974
export DEPLOY_ENV=prod
export ZONE=sz001
export SPRING_CONFIG_LOCATION=/tmp/spark



