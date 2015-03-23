N=${N:-20}
ARGV=$N
N_NODES=${N_NODES:-5}

IGNITE=../src/main/shell/ignite.sh
SCRIPT=get-sum-of-integers.sh

$IGNITE $N_NODES $SCRIPT $ARGV

