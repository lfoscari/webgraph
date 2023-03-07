#!/bin/bash -xe

if [[ "$2" == "" ]]; then
	echo "$(basename $0) INPUTSDIR OUTPUTSDIR NTHREADS" 1>&2
	echo "Reads inputs in INPUTSDIR and outputs in OUTPUTSDIR and processes them using NTHREADS parallel sorts." 1>&2
	echo "FILES MUST END WITH A NEWLINE. Fix them with \"sed -i -e '\$a\\' *\"." 1>&2
	exit 1
fi

INPUTSDIR=$1
OUTPUTSDIR=$2
NTHREADS=$(expr $3 / 2)

INPUTFILE=$(mktemp)
bash extract.sh $INPUTSDIR $NTHREADS input address > $INPUTFILE &

OUTPUTFILE=$(mktemp)
bash extract.sh $OUTPUTSDIR $NTHREADS output address > $OUTPUTFILE &

LC_ALL=C sort -S2G -m $INPUTFILE $OUTPUTFILE