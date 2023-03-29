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
_TMPDIR=$(mktemp -d -p "$(pwd)")

if [[ -f inputs.tsv ]]; then
	echo "Inputs already computed"
else
	echo "Extracting inputs"
	TMPDIR=$_TMPDIR bash ~/transactiongraph/bash/blockchair/extract.sh $INPUTSDIR $NTHREADS input > inputs.tsv
fi

if [[ -f outputs.tsv ]]; then
	echo "Outputs already computed"
else
	echo "Extracting outputs"
	TMPDIR=$_TMPDIR bash ~/transactiongraph/bash/blockchair/extract.sh $OUTPUTSDIR $NTHREADS output > outputs.tsv
fi

if [[ -f addresses.tsv ]]; then
	echo "Addresses already computed"
else
	echo "Extracting addresses"
	TMPDIR=$_TMPDIR bash ~/transactiongraph/bash/blockchair/addresses.sh $INPUTSDIR $OUTPUTSDIR $NTHREADS > addresses.tsv
fi

if [[ -f transactions.tsv ]]; then
	echo "Transactions already computed"
else
	echo "Extracting transactions"
	TMPDIR=$_TMPDIR bash ~/transactiongraph/bash/blockchair/transactions.sh $INPUTSDIR $OUTPUTSDIR $NTHREADS > transactions.tsv
fi

if [[ -f address.map ]]; then
	echo "Address map already computed"
else
	echo "Computing address map"
	TMPDIR=$_TMPDIR java -Djava.io.tmpdir="$_TMPDIR" it.unimi.dsi.sux4j.mph.GOV3Function -b -s 10 address.map addresses.tsv
fi

if [[ -f transaction.map ]]; then
	echo "Transaction map  already computed"
else
	echo "Computing transaction map"
	TMPDIR=$_TMPDIR java -Djava.io.tmpdir="$_TMPDIR" it.unimi.dsi.sux4j.mph.GOV3Function -b -s 10 transaction.map transactions.tsv
fi

echo "$((SECONDS / 60 / 60)) hours, $((SECONDS / 60 % 60)) minutes and $((SECONDS % 60)) seconds elapsed"
rm -rf $_TMPDIR