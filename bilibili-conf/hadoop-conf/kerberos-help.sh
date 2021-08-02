
# support kerberos
source /etc/profile.d/hdfs-group.sh
NEED_KINIT=0
function check_if_need_kinit() {
  klist > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    principal=$(klist | awk '{print $3}' | sed -n '2p' | cut -d@ -f1)
    expiredTime=$(klist | sed -n "5,1p" | awk -F'  ' '{print $2}')
    expiredTimeFT=$(date -d "$expiredTime" +"%Y-%m-%d %H:%M:%S")
    expiredTimestamp=$(date -d "$expiredTimeFT" +%s)
    currentTimestamp=$(date +%s)
    if [ $expiredTimestamp -gt $currentTimestamp ]; then
      let free=$expiredTimestamp-$currentTimestamp
      let maxLeftTime=60*60*6
      # 剩余时间大于所设阈值，则不需要 kinit
      if [ $free -gt $maxLeftTime ]; then
         nowTime=$(date "+%Y-%m-%d %H:%M:%S")
         NEED_KINIT=1
      fi
    fi
  fi
}
if grep -A 2 "hadoop.security.authentication" "$HADOOP_CONF_DIR/core-site.xml" | grep "kerberos" >/dev/null;then
  krb5ccname=`echo $KRB5CCNAME | awk -F'_' '{print $2}'`
  if [ "$krb5ccname" == "" ];then
    USERNAME=$(whoami)
    KEYTAB=/home/$USERNAME/$USERNAME.keytab
    PRINCIPAL=$USERNAME@BILIBILI.CO
    use_group_keytab=1
    if [ ! -f "$KEYTAB" ]; then
      if [ -z "$GROUP_NAME" ]; then
        exit 1
      else
        USERNAME="$GROUP_NAME"
        KEYTAB=/home/$USERNAME/$USERNAME.keytab
        PRINCIPAL=$USERNAME@BILIBILI.CO
        use_group_keytab=0
      fi
    fi
    export KRB5CCNAME="/tmp/krb5_$USERNAME"
    check_if_need_kinit
    if [ $NEED_KINIT -eq 0 ];then
      kinit -kt "$KEYTAB" "$PRINCIPAL" >/dev/null 2>&1
      if [ $? -ne 0 ]; then
        exit 1
      fi
      if [ $use_group_keytab -eq 0 ];then
        chmod 777 /tmp/krb5_$USERNAME
      fi
    fi

    if [ $use_group_keytab -eq 0 ];then
      bash ${HADOOP_CONF_DIR}/start_renew_tgt.sh $USERNAME $KEYTAB true >/dev/null 2>&1
    else
      bash ${HADOOP_CONF_DIR}/start_renew_tgt.sh $USERNAME $KEYTAB false >/dev/null 2>&1
    fi
  fi
fi
