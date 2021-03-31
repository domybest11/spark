#!/bin/bash
source /etc/profile

pwd

cp -v ./bilibili/jssz-spark31-ess/log4j.properties ./conf
cp -v ./bilibili/jssz-spark31-ess/metrics.properties ./conf
cp -v ./bilibili/jssz-spark31-ess/spark-defaults.conf ./conf
cp -v ./bilibili/jssz-spark31-ess/spark-env.sh ./conf
chown -R yarn:hadoop .

mkdir -p /data/log/jssz-spark31-ess
chown -R yarn:hadoop /data/log/jssz-spark31-ess

# Update supervisor conf
readonly SC_SRC=./bilibili/jssz-spark31-ess/supervisor.conf
readonly SC_DST=/etc/supervisor/conf.d/jssz-spark31-ess.conf
touch ${SC_DST}
diff -q ${SC_SRC} ${SC_DST} &>/dev/null || {
    cp -v ${SC_SRC} ${SC_DST}
    supervisorctl update
}