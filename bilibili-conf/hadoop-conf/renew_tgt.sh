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

function check_if_need_kinit() {
    # check principal is match
    principal=$(klist | awk '{print $3}' | sed -n '2p' | cut -d@ -f1)
    if [ "$principal" == "$PRINCIPAL" ]; then
        expiredTime=$(klist | sed -n "5,1p" | awk -F'  ' '{print $2}')
        expiredTimeFT=$(date -d "$expiredTime" +"%Y-%m-%d %H:%M:%S")
        expiredTimestamp=$(date -d "$expiredTimeFT" +%s)
        currentTimestamp=$(date +%s)
        if [ $expiredTimestamp -gt $currentTimestamp ]; then
          let free=$expiredTimestamp-$currentTimestamp
          let maxLeftTime=60*60*15
          # 剩余时间大于所设阈值，则不需要 kinit
          if [ $free -gt $maxLeftTime ]; then
             nowTime=$(date "+%Y-%m-%d %H:%M:%S")
             echo "NowTime: $nowTime, ExpiredTime: $expiredTimeFT, no need to kinit!"
             needKinit=1
          fi
        fi
    else
      echo "default principal $principal in tgt is not match $USERNAME, need kinit!"
    fi
}

function renew_tgt() {
  has_renew_process=1
  while [ $has_renew_process -eq 1 ]
  do
    needKinit=0
    check_if_need_kinit
    if [ $needKinit -eq 0 ]; then
      kinit -kt "$KEYTAB" "$PRINCIPAL" >/dev/null 2>&1
      if [ "$USE_GROUP_KEYTAB" == "true" ];then
        chmod 777 /tmp/krb5_$PRINCIPAL
      fi
      echo "kinit by $KEYTAB  $PRINCIPAL"
    fi
    sleep 60
  done
}

export KRB5CCNAME="/tmp/krb5_$PRINCIPAL"
renew_tgt