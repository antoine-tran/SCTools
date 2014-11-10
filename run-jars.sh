# The script to run hedera using Java hadoop map reduce framework. 
# For running via other languages (Pig or Python), please check
# the documentation on http://antoine-tran.github.io/hedera/wikirevision.html

#!/bin/sh

rm libst/*

mvn validate install -DskipTests
rm libst/hadoop*
rm libst/jetty*
rm libst/jasper*
rm libst/jersey*

LIB=$(pwd)/libst
LOCAL_LIB=$(pwd)/lib

LOG4J=-Dlog4j.configuration=file:etc/log4j.properties

# path to your jar file here. Default is the maven artifact in your local repo
export JAR_PATH="target/streamcorpus-dexter-0.1-SNAPSHOT.jar"

for jarf in $LIB/*.jar
do
CLPA=$CLPA:$jarf
HCLPA=$HCLPA,$jarf
done
for jarf in $LOCAL_LIB/*.jar
do
CLPA=$CLPA:$jarf
HCLPA=$HCLPA,$jarf
done

# CLPA=${CLPA:1:${#CLPA}-1}
# HCLPA=${HCLPA:1:${#HCLPA}-1}
CLPD=$CLPA:$JAR_PATH
HCLPDA=$HCLPA,$JAR_PATH


# Optional: set up the version of the old mapred API here (We use CDH 4.6.0 for testing)
export HADOOP_MAPRED_HOME="/opt/cloudera/parcels/CDH/lib/hadoop-0.20-mapreduce"
export HADOOP_HOME="/opt/cloudera/parcels/CDH/lib/hadoop-0.20-mapreduce"
export HADOOP_CLIENT_OPTS="-Xmx2048m $HADOOP_CLIENT_OPTS"
export HADOOP_CLASSPATH="$CLPD:$HADOOP_CLASSPATH:/opt/cloudera/parcels/CDH/lib/hadoop-0.20-mapreduce/conf"

#setup TERRIER_HOME
if [ ! -n "$TERRIER_HOME" ]
then
  #find out where this script is running
  TEMPVAR=`dirname $0`
  #terrier folder is folder above
  TERRIER_HOME=`pwd $TEMPVAR`
  echo "Setting TERRIER_HOME to $TERRIER_HOME"
fi
#setup TERRIER_ETC
if [ ! -n "$TERRIER_ETC" ]
then
  TERRIER_ETC=$TERRIER_HOME/etc
fi

hadoop jar target/streamcorpus-dexter-0.1-SNAPSHOT.jar $1 -libjars ${HCLPDA} -D terrier.home="$TERRIER_HOME" $LOG4J -D mapred.output.compress=true -D mapred.output.compression.type=BLOCK -D mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec -D mapred.compress.map.output -D mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec -D mapred.task.timeout=1200000 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13} ${14} ${15} ${16} ${17} ${18} ${19} ${20}
