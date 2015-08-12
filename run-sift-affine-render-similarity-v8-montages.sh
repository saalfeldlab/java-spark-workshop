#!/bin/bash

INFLAME=$PWD/inflame.sh
JAR=$PWD/target/render-align-0.0.1-SNAPSHOT.jar # this jar must be accessible from the cluster
CLASS=org.janelia.saalfeldlab.renderalign.SimilarityRenderMontages
N_NODES=10

SERVER="http://tem-services.int.janelia.org:8080/render-ws/v1"
OWNER="flyTEM"
PROJECT="FAFB00"
STACK="v8_montage"
#X="-109.0"
#Y="370.0"
#WIDTH="16384"
#HEIGHT="16384"
#WIDTH="239959.0"
#HEIGHT="148704.0"
SCALE="0.015625"
OUTPUT="/nobackup/saalfeld/tmp/spark-export/"

#ARGV="-S $SERVER -u $OWNER -p $PROJECT -s $STACK -x $X -y $Y -w $WIDTH -h $HEIGHT -t $SCALE -o $OUTPUT"
ARGV="-S $SERVER -u $OWNER -p $PROJECT -s $STACK -t $SCALE -o $OUTPUT"

mkdir -p $OUTPUT

$INFLAME $N_NODES $JAR $CLASS $ARGV
