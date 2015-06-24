INFLAME=$PWD/inflame.sh
JAR=$PWD/target/java-spark-workshop-0.0.1-SNAPSHOT.jar # this jar must be accessible from the cluster
CLASS=org.janelia.workshop.spark.practices.PairwiseImageSelection
N_NODES=40

N_IMAGES=1000
SIDE_LENGTH=1000
RANGE=300
ARGV="$N_IMAGES $SIDE_LENGTH $RANGE"

$INFLAME $N_NODES $JAR $CLASS $ARGV
