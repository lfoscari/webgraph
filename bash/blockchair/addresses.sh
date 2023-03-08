#!/bin/bash -e

if [[ "$2" == "" ]]; then
	"$(basename $0) INPUTSDIR OUTPUTSDIR NTHREADS" 1>&2
	echo "Reads inputs in INPUTSDIR and outputs in OUTPUTSDIR and processes them using NTHREADS parallel sorts." 1>&2
	echo "FILES MUST END WITH A NEWLINE. Fix them with \"sed -i -e '\$a\\' *\"." 1>&2
	exit 1
fi

INPUTSDIR=$1
OUTPUTSDIR=$2
NTHREADS=$(expr $3 / 2)
SCRIPTDIR=$(dirname $(realpath $0))

[[ -f $INPUTSDIR/input.pipe ]] && rm -f $INPUTSDIR/input.pipe
mkfifo $INPUTSDIR/input.pipe
bash $SCRIPTDIR/extract.sh $INPUTSDIR $NTHREADS input address >$INPUTSDIR/input.pipe &

[[ -f $OUTPUTSDIR/output.pipe ]] && rm -f $OUTPUTSDIR/output.pipe
mkfifo $OUTPUTSDIR/output.pipe
bash $SCRIPTDIR/extract.sh $OUTPUTSDIR $NTHREADS output address >$OUTPUTSDIR/output.pipe &

LC_ALL=C sort -S2G -m $INPUTTMP.pipe $OUTPUTTMP.pipe

rm -f $INPUTSDIR/input.pipe $OUTPUTSDIR/output.pipe
