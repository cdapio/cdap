#! /bin/sh
git stash -q --keep-index
./tools/jshint/bin/hint ./client/core/models/ ./client/core/embeddables/ ./client/core/controllers/ ./client/core/ ./client/main.js ./client/local/views ./client/sandbox/views ./client/enterprise/views ./server/local/main.js ./server/sandbox/main.js ./server/enterprise/main.js --config ./jshint.json
RESULT=$?
git stash pop -q
[ $RESULT -ne 0 ] && exit 1
exit 0