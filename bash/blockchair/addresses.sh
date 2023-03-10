#!/bin/bash -e

if [[ "$3" == "" ]]; then
	"$(basename $0) INPUTSDIR OUTPUTSDIR NTHREADS" 1>&2
	echo "Reads inputs in INPUTSDIR and outputs in OUTPUTSDIR and processes them using NTHREADS parallel sorts." 1>&2
	echo "FILES MUST END WITH A NEWLINE. Fix them with \"sed -i -e '\$a\\' *\"." 1>&2
	exit 1
fi

INPUTSDIR=$1
OUTPUTSDIR=$2
NTHREADS=$3
SCRIPTDIR=$(dirname $(realpath $0))

LC_ALL=C sort -S2G -mu \
  $(bash $SCRIPTDIR/extract.sh $INPUTSDIR $NTHREADS input address) \
  $(bash $SCRIPTDIR/extract.sh $OUTPUTSDIR $NTHREADS output address)
