#!/bin/bash -e

if [[ "$3" == "" ]]; then
	echo "$(basename $0) INPUTSDIR OUTPUTSDIR NTHREADS" 1>&2
	echo "Reads inputs in INPUTSDIR  and outputs in OUTPUTSDIR and processes them using NTHREADS parallel sorts." 1>&2
	echo "Files are processed as input files unless OUTPUT is specified." 1>&2
	echo "FILES MUST END WITH A NEWLINE. Fix them with \"sed -i -e '\$a\\' *\"." 1>&2
	exit 1
fi

INPUTSDIR=$1
OUTPUTSDIR=$2
NTHREADS=$(expr $3 / 2)

TMPIN=$(mktemp)
bash inputoutput $INPUTSDIR $NTHREADS > $TMPIN &

TMPOUT=$(mktemp)
bash inputoutput $INPUTSDIR $NTHREADS OUTPUT > $TMPOUT &

LC_ALL=C sort -S2G -m $TMPIN $TMPOUT