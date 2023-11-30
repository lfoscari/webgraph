#!/bin/bash

set -e
set -o pipefail

if [[ "$3" == "" ]]; then
	echo "$(basename "$0") DIR NTHREADS {INPUT|OUTPUT} [address|transaction|transaction-address]" 1>&2
	echo "Reads tsv files in DIR and processes them using NTHREADS parallel sorts." 1>&2
	echo "Files are processed as inputs if \"input\" is specified, otherwise as outputs if \"output\" is specified." 1>&2
	echo "By specifying either \"address\", \"transaction\" or \"transaction-address\" it is possible to choose to extract only the addresses, only the transactions or extract both, in the first case sorting the data by transaction, otherwise by address. If you want to build an address graph use \"transaction-address\"." 1>&2
	exit 1
fi

DIR=$1
NTHREADS=$2
SOURCE=$(echo "$3" | tr '[:upper:]' '[:lower:]')
TARGET=$(echo "$4" | tr '[:upper:]' '[:lower:]')

if [[ "$SOURCE" != "output" && "$SOURCE" != "input" ]]; then
	echo "Source \"$SOURCE\" must be either \"input\" or \"output\""
	exit 1
fi

if [[ "$TARGET" != "" && "$TARGET" != "address" && "$TARGET" != "transaction" && "$TARGET" != "transaction-address" ]]; then
	echo "Target \"$TARGET\" must be either empty, \"address\", \"transaction\" or \"transaction-address\""
	exit 1
fi

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
	if [[ "$SOURCE" == "output" ]]; then
		if [[ "$TARGET" == "address" ]]; then
			(tail -q -n+2 $(cat $SPLIT) | cut -f7,10 | awk '{ if ($2 == 0) print $1 }' | LC_ALL=C sort -S2G >$SPLIT.pipe) &
		elif [[ "$TARGET" == "transaction" ]]; then
			(tail -q -n+2 $(cat $SPLIT) | cut -f2,10 | awk '{ if ($2 == 0) print $1 }' | LC_ALL=C sort -S2G >$SPLIT.pipe) &
		elif [[ "$TARGET" == "transaction-address" ]]; then
			(tail -q -n+2 $(cat $SPLIT) | cut -f2,7,10 | awk '{ if ($3 == 0) print $1 "\t" $2 }' | LC_ALL=C sort -S2G >$SPLIT.pipe) &
		fi
	elif [[ "$SOURCE" == "input" ]]; then
		if [[ "$TARGET" == "address" ]]; then
			(tail -q -n+2 $(cat $SPLIT) | cut -f7 | LC_ALL=C sort -S2G >$SPLIT.pipe) &
		elif [[ "$TARGET" == "transaction" ]]; then
			(tail -q -n+2 $(cat $SPLIT) | cut -f13 | LC_ALL=C sort -S2G >$SPLIT.pipe) &
    elif [[ "$TARGET" == "transaction-address" ]]; then
			(tail -q -n+2 $(cat $SPLIT) | cut -f7,13 | awk '{ print $2 "\t" $1 }' | LC_ALL=C sort -S2G >$SPLIT.pipe) &
		fi
	fi
done

LC_ALL=C sort -S2G -m $(for SPLIT in $SPLITS; do echo $SPLIT.pipe; done)

rm -f $FILES
rm -f ${SPLITBASE}*
