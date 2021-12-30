#!/bin/bash
source /etc/profile

export SPARK_HOME=/data/service/spark3.1
export SPARK_NO_DAEMONIZE=1

cd ${SPARK_HOME}

# Copied from sbin/start-shuffle-service.sh
. "${SPARK_HOME}/sbin/spark-config.sh"
. "${SPARK_HOME}/bin/load-spark-env.sh"
exec "${SPARK_HOME}/sbin"/spark-daemon.sh start org.apache.spark.deploy.ExternalShuffleService 1
