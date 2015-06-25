# this example can also be run like:
# ../src/main/shell/inflame.sh <N_NODES> ../target/flintstone-0.0.1-SNAPSHOT.jar org.janelia.flintstone.Example <N>

N=${N:-20}
ARGV=$N
N_NODES=${N_NODES:-5}

IGNITE=../src/main/shell/ignite.sh
SCRIPT=get-sum-of-integers.sh

$IGNITE $N_NODES $SCRIPT $ARGV

