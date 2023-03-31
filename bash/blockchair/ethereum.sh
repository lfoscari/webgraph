#!/bin/bash

set -e
set -o pipefail

if [[ "$1" == "" ]]; then
	echo "$(basename "$0") DIR DEST" 1>&2
	echo "Reads files in DIR and processes them using NTHREADS parallel sorts." 1>&2
	echo "Extracts transaction recipients and splits them into synthetic coinbase, calls to a smart contract and user-to-user transactions." 1>&2
	exit 1
fi

DIR=$1
DEST="ethereum_split"
[[ $2 != "" ]] && DEST=$2

mkdir -p $DEST

:> $DEST/synthetic_coinbase.tsv
:> $DEST/call.tsv
:> $DEST/call_tree.tsv

cat $DIR/* | cut -f 3,6,7,8 | awk \ '
	$2 ~ /synthetic_coinbase/ { print $4 > "'$DEST'/synthetic_coinbase.tsv" }
	$2 ~ /call/ { print $1 $3 $4 > "'$DEST'/call.tsv" }
	$2 ~ /call_tree/ { print $1 $3 $4 > "'$DEST'/call_tree.tsv" }'

echo "Results saved in $DEST"