# THIS WILL WRITE INTO $HOME/jsw-output

OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`


FLINTSTONE=$ABS_DIR/flintstone/flintstone.sh
JAR=$ABS_DIR/target/java-spark-workshop-0.0.1-SNAPSHOT.jar # this jar must be accessible from the cluster
CLASS=org.janelia.workshop.spark.Downsampling
N_NODES=5

INPUT_FORMAT=/tier2/saalfeld/hanslovskyp/java-spark-workshop/example-stack-enumerated/%05d.tif
MIN=0
MAX=1000
NUMBER_OF_LEVELS_TO_DOWNSAMPLE=5
OUTPUT_DIR=$HOME/jsw-output/downscaled-$NUMBER_OF_LEVELS_TO_DOWNSAMPLE-levels
OUTPUT_FORMAT=$OUTPUT_DIR/%05d.tif
ARGV="$INPUT_FORMAT $MIN $MAX $NUMBER_OF_LEVELS_TO_DOWNSAMPLE $OUTPUT_FORMAT"

mkdir -p $OUTPUT_DIR

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
