#!/bin/bash -e

if [[ "$3" == "" ]]; then
	echo "$(basename $0) DIR|FILE OFFSET MAPFILE" 1>&2
	echo "Reads TSVs in DIR (or the single FILE) and computes an immutable function with the rank as the values and the OFFSETH-th column as keys." 1>&2
	exit 1
fi

DIR=$1
OFFSET=$2
MAP=$3

function file_ends_with_newline() {
	[[ $(tail -c1 "$1" | wc -l) -gt 0 ]]
}

FILES=$(find "$DIR" -type f)
TMP=$(mktemp)

for FILE in $FILES; do
	if ! file_ends_with_newline "$FILE"; then
		echo "File $FILE does not end with a newline" 1>&2
		exit 1
	fi
done

tail -n +2 -q ${FILES[@]} | cut -f"$OFFSET" > "$TMP"
java it.unimi.dsi.sux4j.mph.GOV3Function -b "$MAP" "$TMP"