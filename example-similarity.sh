# THIS WILL WRITE INTO $HOME/jsw-output

INFLAME=$PWD/inflame.sh
JAR=$PWD/target/java-spark-workshop-0.0.1-SNAPSHOT.jar # this jar must be accessible from the cluster
CLASS=org.janelia.workshop.spark.SparkSimilarities
N_NODES=5

INPUT_FILE=/tier2/saalfeld/hanslovskyp/java-spark-workshop/example-files.txt
MIN=0
MAX=1000
NUMBER_OF_LEVELS_TO_DOWNSAMPLE=3
OUTPUT_DIR=$HOME/jsw-output/
OUTPUT_FILE=$OUTPUT_DIR/matrix.tif
RANGE=200
ARGV="-f $INPUT_FILE -F -o $OUTPUT_FILE -m $MIN -M $MAX -r $RANGE -s $NUMBER_OF_LEVELS_TO_DOWNSAMPLE"

mkdir -p $OUTPUT_DIR

$INFLAME $N_NODES $JAR $CLASS $ARGV
