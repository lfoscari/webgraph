#!/bin/bash -e

if [[ "$3" == "" ]]; then
	"$(basename $0) INPUTSDIR OUTPUTSDIR NTHREADS" 1>&2
	echo "Reads inputs in INPUTSDIR and outputs in OUTPUTSDIR and extracts the sorted addresses." 1>&2
	exit 1
fi

INPUTSDIR=$1
OUTPUTSDIR=$2
NTHREADS=$3
SCRIPTDIR=$(dirname $(realpath $0))

INPUTS=$(mktemp)
bash $SCRIPTDIR/extract.sh $INPUTSDIR $NTHREADS input address > $INPUTS

OUTPUTS=$(mktemp)
bash $SCRIPTDIR/extract.sh $OUTPUTSDIR $NTHREADS output address > $OUTPUTS

LC_ALL=C sort -S2G -mu $INPUTS $OUTPUTS
rm -f $INPUTS $OUTPUTS
