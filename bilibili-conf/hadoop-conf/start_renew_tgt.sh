#!/usr/bin/env bash

function print_usage(){
  echo "Usage: [principal][keytab]"
  echo "       principal is the use in kdc"
  echo "       keytab is master key file for the principal"
}

if [ $# -ne 3 ]; then
  print_usage
  exit 1
fi
PRINCIPAL=$1
KEYTAB=$2
USE_GROUP_KEYTAB=$3

has_renew_process=1
pid_file="/tmp/renew_pid_$PRINCIPAL"
if [ -f $pid_file ];then
 pid=`cat $pid_file`
 if ps -ef | grep $pid | grep -v grep | grep -v sleep > /dev/null 2>&1;then
   has_renew_process=0
 fi
else
 flock -xn $pid_file -c "touch $pid_file"
fi
if [ $has_renew_process -eq 1 ];then
  if [ "$USE_GROUP_KEYTAB" == "true" ];then
    nohup bash ${HADOOP_CONF_DIR}/renew_tgt.sh $PRINCIPAL $KEYTAB true >/dev/null 2>&1 &
    echo $! > $pid_file
  else
    nohup bash ${HADOOP_CONF_DIR}/renew_tgt.sh $PRINCIPAL $KEYTAB false >/dev/null 2>&1 &
    echo $! > $pid_file
  fi
fi

chmod 777 $pid_file
