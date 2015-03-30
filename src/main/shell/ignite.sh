#!/bin/bash

USAGE="$0 <N> <SCRIPT> <ARGV>"

if [[ "$1" -eq "-h" ]]; then
    echo $USAGE
else
    

    N_NODES=$1; shift
    SCRIPT=`readlink -f "$1"`; shift
    ARGV=$@

    OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
    OWN_DIR_ABS_PATH=`readlink -f "$OWN_DIR"`
    ROOT_DIR=${ROOT_DIR:-$OWN_DIR_ABS_PATH}
    QSUB_WRAPPER=$ROOT_DIR/qsub-wrapper.sh

    N_NODES=$N_NODES $QSUB_WRAPPER $SCRIPT $ARGV
fi
