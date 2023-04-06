#!/bin/bash

set -e
set -o pipefail

if [[ "$2" == "" ]]; then
	echo "$(basename "$0") DIR DEST [MEMORY]" 1>&2
	echo "Reads files in DIR and saves the results in the DEST directory." 1>&2
	echo "Extracts transaction recipients and splits them into synthetic coinbase, calls to a smart contract and user-to-user transactions." 1>&2
	echo "Optionally you can set the amount of memory used by the script in GB."
	exit 1
fi

DIR=$1
DEST=$2
MEM=2
[[ $3 != "" ]] && MEM=$3

mkdir -p $DEST

:> $DEST/synthetic_coinbase.tsv
:> $DEST/call.tsv
:> $DEST/call_tree.tsv

:> $DEST/transactions.tsv
:> $DEST/addresses.tsv

cat $DIR/* | cut -f 3,6,7,8 | awk \ '
	$2 ~ /synthetic_coinbase/ {
		print $4 > "'$DEST'/synthetic_coinbase.tsv";
		print $4 > "'$DEST'/addresses.tsv"
	}
	$2 ~ /call/ {
		print $3 "\t" $4 "\t" $1 > "'$DEST'/call.tsv";
		print $3 "\n" $4 > "'$DEST'/addresses.tsv";
		print $1 > "'$DEST'/transactions.tsv"
	}
	$2 ~ /call_tree/ {
		print $3 "\t" $4 "\t" $1 > "'$DEST'/call_tree.tsv";
	 	print $3 "\n" $4 > "'$DEST'/addresses.tsv";
	 	print $1 > "'$DEST'/transactions.tsv"
	}'

echo "Deduping addresses and transactions"
LC_ALL=C sort -u -S"${MEM}G" -o $DEST/transactions.tsv $DEST/transactions.tsv
LC_ALL=C sort -u -S"${MEM}G" -o $DEST/addresses.tsv $DEST/addresses.tsv

echo "Computing minimal hash functions (optional)"
java -Djava.io.tmpdir="$DEST" it.unimi.dsi.sux4j.mph.GOV3Function -b -s 10 $DEST/transaction.map $DEST/transactions.tsv
java -Djava.io.tmpdir="$DEST" it.unimi.dsi.sux4j.mph.GOV3Function -b -s 10 $DEST/address.map $DEST/addresses.tsv

echo "Results saved in $DEST"