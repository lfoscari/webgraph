#!/bin/bash

set -e
set -o pipefail

if [[ "$3" == "" ]]; then
  echo "$(basename $0) FILES_DIR BLOCKCHAIR_URL BLOCKCHAIR_KEY" 1>&2
  echo "Ensures that in the FILES_DIR directory are present all the files listed at BLOCKCHAIR_URL in tsv format." 1>&2
  exit 1
fi

FILES_DIR=$1 # e.g. "inputs"
BASE_URL=$2 # e.g. "https://gz.blockchair.com/bitcoin/$FILES_DIR/"
BC_KEY=$3 # e.g. "202101GjMhj8R3FF"

# Ensure BASE_URL ends in a slash
[[ ${BASE_URL:${#BASE_URL}-1:1} != "/" ]] && BASE_URL="$BASE_URL/"

# Check which tsvs we have already downloaded
if find $FILES_DIR -name "*.tsv" -mindepth 1 -maxdepth 1 | read; then
  old=($(ls $FILES_DIR/*.tsv | xargs basename))
else
  old=()
fi

# Download the full updated list from the website
echo "Downloading updated list of blocks"
updated=($(curl -# "$BASE_URL?key=$BC_KEY" | sed -n 's/<a href="\(.*tsv\)\.gz">.*/\1/gp'))

# Find the tsvs not already present
diff=$(echo ${old[@]} ${updated[@]} | tr ' ' '\n' | sort | uniq -u)
echo "Downloading ${#diff[@]} new file(s)"

# Download and unpack them
for file in $diff
do
  echo "Downloading $file"
  curl -# $BASE_URL$file.gz?key=$BC_KEY --output $FILES_DIR/$file.gz
  gzip -d $FILES_DIR/$file.gz
done