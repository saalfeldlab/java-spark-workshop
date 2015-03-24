#!/bin/bash

OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
OWN_DIR_ABS_PATH=`readlink -f "$OWN_DIR"`
DEFAULT_WRAPPER_FILE=$OWN_DIR_ABS_PATH/qsub-argument-wrap-script.sh

USAGE="[WRAPPER_FILE=<WRAPPER_FILE>] $0 <script-file> <argv for script-file>
<WRAPPER_FILE> defaults to $DEFAULT_WRAPPER_FILE"
N_NODES="${N_NODES:-1}"
TMP="${TMP:-$HOME/tmp}"

if [ $# -lt 1 ]; then
    echo 'Need script name argument.' 1>&2
    echo "$USAGE" 1>&2
    exit 1
fi


WRAPPER_FILE=${WRAPPER_FILE:-$DEFAULT_WRAPPER_FILE}
SCRIPT_FILE="$1"
shift
mkdir -p "$TMP"
FILENAME=`mktemp --tmpdir="$TMP"`
JOB=`qsub -jc spark -pe spark $(($N_NODES + 1)) -q hadoop2 -j y -o ~/.sparklogs "$WRAPPER_FILE" "$FILENAME" "$SCRIPT_FILE" $@ | awk '{print $3}'`
echo $JOB > $FILENAME
