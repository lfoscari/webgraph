#!/bin/bash -e

# TODO: rewrite
if [[ "$3" == "" ]]; then
	echo "$(basename $0) DIR NTHREADS {INPUT|OUTPUT} [ADDRESS|TRANSACTION]" 1>&2
	echo "Reads files in DIR and processes them using NTHREADS parallel sorts." 1>&2
	echo "Files are processed as input files if \"input\" is specified or as output files if \"output\" is specified." 1>&2
	echo "If no further option is specified, addresses and transactions will be extracted from the files." 1>&2
	echo "By specifying either \"address\" or \"transaction\" is possible to choose." 1>&2
	echo "FILES MUST END WITH A NEWLINE. Fix them with \"sed -i -e '\$a\\' *\"." 1>&2
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

if [[ "$TARGET" != "" && "$TARGET" != "address" && "$TARGET" != "transaction" ]]; then
	echo "Target \"$TARGET\" must be either empty, \"address\" or \"transaction\""
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
		if [[ "$TARGET" == "" ]]; then
			(tail -q -n+2 $(cat $SPLIT) | cut -f2,7,10 | awk '{ if ($3 == 0) print $1 "\t" $2 }' | LC_ALL=C sort -S2G >$SPLIT.pipe) &
		elif [[ "$TARGET" == "address" ]]; then
			(tail -q -n+2 $(cat $SPLIT) | cut -f7,10 | awk '{ if ($2 == 0) print $1 }' | LC_ALL=C sort -S2G >$SPLIT.pipe) &
		elif [[ "$TARGET" == "transaction" ]]; then
			(tail -q -n+2 $(cat $SPLIT) | cut -f2,10 | awk '{ if ($2 == 0) print $1 }' | LC_ALL=C sort -S2G >$SPLIT.pipe) &
		fi
	elif [[ "$SOURCE" == "input" ]]; then
		if [[ "$TARGET" == "" ]]; then
			(tail -q -n+2 $(cat $SPLIT) | cut -f7,13 | awk '{ print $2 "\t" $1 }' | LC_ALL=C sort -S2G >$SPLIT.pipe) &
		elif [[ "$TARGET" == "address" ]]; then
			(tail -q -n+2 $(cat $SPLIT) | cut -f7 | LC_ALL=C sort -S2G >$SPLIT.pipe) &
		elif [[ "$TARGET" == "transaction" ]]; then
			(tail -q -n+2 $(cat $SPLIT) | cut -f13 | LC_ALL=C sort -S2G >$SPLIT.pipe) &
		fi
	fi
done

LC_ALL=C sort -S2G -m $(for SPLIT in $SPLITS; do echo $SPLIT.pipe; done)

rm -f $FILES
rm -f ${SPLITBASE}*
