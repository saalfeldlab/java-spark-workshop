OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`


FLINTSTONE=$ABS_DIR/flintstone/flintstone.sh
JAR=$ABS_DIR/target/java-spark-workshop-0.0.1-SNAPSHOT.jar # this jar must be accessible from the cluster
CLASS=org.janelia.workshop.spark.IntegerSum

N_NODES=2
N=10000
ARGV=$N

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
