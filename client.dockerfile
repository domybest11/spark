FROM hub.bilibili.co/compile/debian:9.8

ENV TERM linux
ENV LANG C.UTF-8

ADD http://artifactory.bilibili.co/artifactory/solomon/yuejianhui/sources.list /etc/apt/sources.list
RUN DEBIAN_FRONTEND=noninteractive apt-get update --allow-insecure-repositories
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y -q vim curl libnss-ldapd libpam-ldapd krb5-user krb5-config libpam-krb5 libpam-ccreds procps --no-install-recommends --allow-unauthenticated

RUN groupadd -g 1200 hadoop \
    && groupadd -g 1500 bigdata_group \
    && useradd hdfs -g 1200 -u 1200 -d /home/hdfs -s /bin/bash -m \
    && useradd hive -g 1200 -u 1205 -d /home/hive -s /bin/bash -m \
    && useradd presto -g 1200 -u 1209 -d /home/presto -s /bin/bash -m \
    && useradd ai -d /home/ai -s /bin/bash -m \
    && chmod -R 755 /home/ai \
    && mkdir -p /data/src \
        /data/sdk \
        /data/service \
        /data/config/hive \
        /hdfs_source/ \
        /data/log/hive \
        /etc/hadoop \
        /etc/security/keytabs \
        touch /data/log/hive/hive.log \
    && echo "127.0.0.5 localhost" >> /etc/hosts \
    && ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo 'Asia/Shanghai' > /etc/timezone

#install lzo
RUN curl http://10.70.82.16:8000/hadoop/lzo-2.10.tar.gz -o /var/tmp/lzo-2.10.tar.gz \
    && tar zxf /var/tmp/lzo-2.10.tar.gz -C /data/src/ \
    && ln -sfn /data/src/lzo-2.10 /data/sdk/lzo

#install jdk
RUN curl http://10.70.82.16:8000/hadoop/jdk1.8.0.tar.gz -o /var/tmp/jdk1.8.0.tar.gz \
    && tar zxf /var/tmp/jdk1.8.0.tar.gz -C /data/src/ \
    && ln -sfn /data/src/jdk1.8.0 /data/sdk/jdk

#install hadoop
COPY hadoop-dist/target/hadoop-2.8.4.tar.gz /var/tmp/hadoop-2.8.4.tar.gz
RUN tar zxf /var/tmp/hadoop-2.8.4.tar.gz -C /data/src/ \
    && ln -sfn /data/src/hadoop-2.8.4 /data/service/hadoop

#install hadoop conf
COPY hadoop-client-proxy-ns2.tar.gz /var/tmp/hadoop-client-proxy-ns2.tar.gz
RUN tar zxf /var/tmp/hadoop-client-proxy-ns2.tar.gz -C /data/src/ \
    && cp -r /data/src/hadoop-client-proxy-ns2/* /etc/hadoop

#install hive
COPY packaging/target/apache-hive-2.3.4-bin.tar.gz  /var/tmp/apache-hive-2.3.4-bin.tar.gz
RUN  tar zxf /var/tmp/apache-hive-2.3.4-bin.tar.gz -C /data/src/ \
    && ln -sfn /data/src/apache-hive-2.3.4-bin /data/service/hive

#install hive conf
COPY jssz-bigdata-hive-client-config.tar.gz   /var/tmp/jssz-bigdata-hive-client-config.tar.gz
RUN tar zxf /var/tmp/jssz-bigdata-hive-client-config.tar.gz  -C /data/src/ \
    && cp -r /data/src/jssz-bigdata-hive-client-config/* /data/config/hive \
    && rm -f /var/tmp/jssz-bigdata-hive-client-config.tar.gz

#install spark31 & conf
COPY dest/spark-3.1.1-bili-SNAPSHOT-bin--hadoop-2.8.tgz /var/tmp/spark-3.1.1-bili-SNAPSHOT-bin--hadoop-2.8.tgz
RUN tar zxf /var/tmp/spark-3.1.1-bili-SNAPSHOT-bin--hadoop-2.8.tgz -C /data/src/ \
    && ln -sfn /data/src/spark-3.1.1-bili-SNAPSHOT-bin--hadoop-2.8 /data/service/spark3.1

# install kerberos tools
RUN curl http://10.70.82.16:8000/hadoop/krb5.conf -o /etc/krb5.conf

# Clear *.tar.gz
RUN rm -rf /var/tmp/*

# env
RUN curl http://10.70.82.16:8000/hadoop/hdfs-env.sh -o /etc/profile.d/hdfs-env.sh   \
    && cp /etc/profile.d/hdfs-env.sh /hdfs_source/   \
    && echo 'export JAVA_HOME="/data/sdk/jdk"' >> ~/.bashrc \
    && echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.bashrc \
    && echo 'export PATH="/data/service/hadoop/bin:$PATH"' >> ~/.bashrc \
    && echo 'export PATH="/data/service/hive/bin:$PATH"' >> ~/.bashrc \
    && echo 'export HIVE_CONF_DIR="/data/config/hive"' >> ~/.bashrc \
    && echo 'source /etc/profile.d/hdfs-env.sh' >> ~/.bashrc

WORKDIR /root


