#!/bin/bash
set -e

DEFAULT_UAT_CONFIG_HOST="uat-config.bilibili.co"
DEFAULT_PROD_CONFIG_HOST="config.bilibili.co"
CHECK_CONFIG_API="/config/v2/check"
GET_CONFIG_API="/config/v2/get"


for var_name in  "ZONE" "TREE_ID" "CONF_PATH" "CONF_TOKEN" "DEPLOY_ENV" "CONF_VERSION"
do
    if [[ -z ${!var_name} ]]; then
        echo "${var_name} is required"
        exit 1
    fi
done

if [[ -z ${CONF_HOST} ]]; then
    if [[ ${DEPLOY_ENV} == "prod" || ${DEPLOY_ENV} == "pre" ]]; then
       CONF_HOST=${DEFAULT_PROD_CONFIG_HOST}
    else
       CONF_HOST=${DEFAULT_UAT_CONFIG_HOST}
    fi
fi

if [[ -z ${HOSTNAME} ]]; then
    HOSTNAME=$(hostname)
fi

if [[ -z ${POD_IP} ]]; then
    IP="127.0.0.1"
else
    IP=${POD_IP}
fi

SERVICE=${TREE_ID}_${DEPLOY_ENV}_${ZONE}
echo $SERVICE
BASE_QUERY="hostname=${HOSTNAME}&build=${CONF_VERSION}&service=${SERVICE}&token=${CONF_TOKEN}&ip=${IP}"
echo $BASE_QUERY

CHECK_QUERY="${BASE_QUERY}&version=-1"
echo http://${CONF_HOST}${CHECK_CONFIG_API}
RESPONSE=`curl http://${CONF_HOST}${CHECK_CONFIG_API}?${CHECK_QUERY}`
echo $RESPONSE
VERSION=`echo $RESPONSE | python -c 'import json,sys;obj=json.load(sys.stdin);obj2=obj["data"]["version"];print obj2'`
echo $VERSION

GET_QUERY="${BASE_QUERY}&version=${VERSION}&ids=null"
RESPONSE=`curl http://${CONF_HOST}${GET_CONFIG_API}?${GET_QUERY}`
# echo $RESPONSE

HOST=`hostname`

QUEUE=`echo $RESPONSE | python -c 'import json,sys;obj=json.load(sys.stdin);obj2=obj["data"]["content"]; obj3=obj2.replace("[","").replace("]","");obj4=json.loads(obj3);print obj4["config"]' | grep $HOST | awk -F '=' '{print $2} '`
echo " queue is $QUEUE "

readonly _APP_NAME=SparkThriftServer
readonly SPARK_HOME=/data/app/jssz-spark24-ess
PORT=10003

# 启动spark thrift server
bash ${SPARK_HOME}/sbin/start-thriftserver.sh --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 --queue $QUEUE --name ${_APP_NAME}_${HOST}:${PORT} --hiveconf hive.server2.thrift.port=$PORT

echo "start thriftserver successed...."
