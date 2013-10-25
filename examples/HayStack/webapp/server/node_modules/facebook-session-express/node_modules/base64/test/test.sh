#!/bin/bash
#

set -e

FILES=$(find ./data -type f)

for file in $FILES; do
    perlf=${file}-perl-out
    nodef=${file}-node-out

    echo "testing $file"

    perl perl-base64-encode.pl $file > $perlf
    node node-base64-encode.js $file > $nodef

    cmp $perlf $nodef

    node node-base64-decode.js $nodef > $nodef-dec

    cmp $file $nodef-dec

    rm -rf $perlf $nodef $nodef-dec
done

echo "all tests passed. good to go."
