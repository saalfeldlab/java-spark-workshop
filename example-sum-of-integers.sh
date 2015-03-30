INFLAME=$PWD/inflame.sh
JAR=$PWD/target/java-spark-workshop-0.0.1-SNAPSHOT.jar # this jar must be accessible from the cluster
CLASS=org.janelia.workshop.spark.IntegerSum

N_NODES=5
N=10000
ARGV=$N

$INFLAME $N_NODES $JAR $CLASS $ARGV
