#!/bin/bash


export SPARK_HOME=${SPARK_HOME:-/usr/local/spark-current}
export PATH=$SPARK_HOME:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

JAR=`readlink -f "%s"`
CLASS=%s

if [ -z $PARALLELISM ]; then
    echo "PARALLELISM not defined."
    exit 1
fi

if [ -z $MASTER ]; then
    echo "MASTER not defined."
    exit 1
fi

$SPARK_HOME/bin/spark-submit --verbose \
    --conf spark.default.parallelism=$PARALLELISM \
    --class $CLASS \
    $JAR $@
