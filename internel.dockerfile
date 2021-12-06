FROM hub.bilibili.co/compile/hadoop-internal

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

# env
RUN echo 'export PATH="/data/service/hive/bin:$PATH"' >> ~/.bashrc \
    && echo 'export HIVE_CONF_DIR="/data/config/hive"' >> ~/.bashrc
