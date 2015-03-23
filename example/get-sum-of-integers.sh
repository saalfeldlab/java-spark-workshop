#!/bin/bash


export SPARK_HOME=${SPARK_HOME:-/usr/local/spark-current}
export PATH=$SPARK_HOME:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
OWN_DIR_ABS_PATH=`realpath "$OWN_DIR"`
JAR=`realpath "$OWN_DIR_ABS_PATH/../../../target/flintstone-0.0.1-SNAPSHOT.jar"`
CLASS=org.janelia.flintstone.Example

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
