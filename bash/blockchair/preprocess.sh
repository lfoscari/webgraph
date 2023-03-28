#!/bin/bash

set -e
set -o pipefail

if [[ "$3" == "" ]]; then
	echo "$(basename $0) INPUTSDIR OUTPUTSDIR NTHREADS" 1>&2
	echo "Reads inputs in INPUTSDIR and outputs in OUTPUTSDIR and creates inputs and outputs ready to be parsed, alongside address and transaction maps." 1>&2
	exit 1
fi

INPUTSDIR=$1
OUTPUTSDIR=$2
NTHREADS=$3

SECONDS=0
TMPDIR=$(mktemp -d -p "$(pwd)")

echo "Extracting inputs"
bash ~/transactiongraph/bash/blockchair/extract.sh $INPUTSDIR $NTHREADS input > inputs.tsv

echo "Extracting outputs"
bash ~/transactiongraph/bash/blockchair/extract.sh $OUTPUTSDIR $NTHREADS output > outputs.tsv

echo "Extracting addresses"
bash ~/transactiongraph/bash/blockchair/addresses.sh $INPUTSDIR $OUTPUTSDIR $NTHREADS > addresses.tsv

echo "Extracting transactions"
bash ~/transactiongraph/bash/blockchair/transactions.sh $INPUTSDIR $OUTPUTSDIR $NTHREADS > transactions.tsv

echo "Computing address map"
java -Djava.io.tmpdir="$TMPDIR" it.unimi.dsi.sux4j.mph.GOV3Function -b -s 10 address.map addresses.tsv

echo "Computing transaction map"
java -Djava.io.tmpdir="$TMPDIR" it.unimi.dsi.sux4j.mph.GOV3Function -b -s 10 transaction.map transactions.tsv

echo "$((SECONDS / 60 / 60)) hours, $((SECONDS / 60 % 60)) minutes and $((SECONDS % 60)) seconds elapsed"