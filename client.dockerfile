FROM hub.bilibili.co/compile/hadoop-encode:latest
ENV TERM linux

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV LC_ALL en_US.UTF-8

#RUN groupadd  -g 1200 hadoop \
#&& useradd hdfs -g 1200 -u 1200 -d /home/hdfs

RUN mkdir -p /data/service/ \
			 /data/sdk/ \
         /data/service/sparklib/ \
         /etc/hadoop/ \
         /etc/kerberos-hadoop \
         /etc/security/keytabs

COPY spark-3.1.1-bili-SNAPSHOT-bin--hadoop-2.8.tgz /data/src/
#WORKDIR /data/src/
RUN tar zxvf /data/src/spark-3.1.1-bili-SNAPSHOT-bin--hadoop-2.8.tgz -C /data/src/ \
&& rm -rf /data/src/spark-3.1.1-bili-SNAPSHOT-bin--hadoop-2.8/conf/* \
&& cp /data/src/spark-3.1.1-bili-SNAPSHOT-bin--hadoop-2.8/bilibili-conf/thrifserver-conf/* /data/src/spark-3.1.1-bili-SNAPSHOT-bin--hadoop-2.8/conf/ \
&& cp /data/src/spark-3.1.1-bili-SNAPSHOT-bin--hadoop-2.8/bilibili-conf/hadoop-conf/* /etc/kerberos-hadoop/ \
&& cp /data/src/spark-3.1.1-bili-SNAPSHOT-bin--hadoop-2.8/bilibili-conf/hadoop-conf/* /etc/hadoop/ \
&& ln -s /data/src/spark-3.1.1-bili-SNAPSHOT-bin--hadoop-2.8 /data/service/spark3.1 \
&& chmod 777 /data/service/spark3.1/sbin/start-thriftserver.sh \
&& chmod 777 /etc/hadoop/rack-topology.sh


RUN curl http://10.70.82.16:8000/hadoop/sts/jmx_prometheus_javaagent-0.11.0.jar -o /data/service/sparklib/jmx_prometheus_javaagent-0.11.0.jar \
&& chmod 777 /data/service/sparklib

RUN curl http://10.70.82.16:8000/hadoop/sts/spark.service.keytab -o /etc/security/keytabs/spark.service.keytab

USER root

ENV JAVA_HOME="/data/sdk/jdk"
ENV CLASSPATH=$CLASSPATH:.:$JAVA_HOME/lib:$JAVA_HOME/jre/lib

ENV PATH=$JAVA_HOME/bin:$JAVA_HOME/jre/bin:$PATH