#!/bin/bash

set -e
set -o pipefail

if [[ "$2" == "" ]]; then
	echo "$(basename "$0") DIR NTHREADS" 1>&2
	echo "Reads inputs tsv files in DIR and processes them using NTHREADS parallel sorts." 1>&2
	echo "Produces a file containing for each line the transaction id of the input, the transaction id of the output and the amount of coins." 1>&2
	echo "Use this class to build a transaction graph by passing the output to a ScatteredLabelledArcsASCIIGraph with the correct transaction map."
	exit 1
fi

DIR=$1
NTHREADS=$2

function file_ends_with_newline() {
	[[ $(tail -c1 "$1" | wc -l) -gt 0 ]]
}

FILES=$(mktemp)
find $DIR -type f >$FILES

# Check that all files end with a newline
while read FILE; do
	if ! file_ends_with_newline $FILE; then
		echo "File $FILE does not end with a newline" 1>&2
		exit 1
	fi
done <$FILES

NFILES=$(cat $FILES | wc -l)

# To avoid empty splits, there must be at least as many threads as files
if ((NFILES < NTHREADS)); then
	NTHREADS=$NFILES
	echo "Not enough files: number of threads set to $NFILES" 1>&2
fi

SPLITBASE=$(mktemp)
split -n l/$NTHREADS $FILES $SPLITBASE
SPLITS=$(for file in ${SPLITBASE}?*; do echo $file; done)

for SPLIT in $SPLITS; do
	mkfifo $SPLIT.pipe
  (tail -q -n+2 $(cat $SPLIT) | cut -f2,5,13 | awk '{ print $1 "\t" $3 "\t" $2 }' | LC_ALL=C sort -S2G >$SPLIT.pipe) &
done

LC_ALL=C sort -S2G -m $(for SPLIT in $SPLITS; do echo $SPLIT.pipe; done)

rm -f $FILES
rm -f ${SPLITBASE}*
