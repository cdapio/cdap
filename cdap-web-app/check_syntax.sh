#!/usr/bin/env bash

./tools/jshint/bin/hint ./client/core/models/ ./client/core/embeddables/ ./client/core/controllers/ ./client/core/ ./client/main.js ./client/local/views ./client/sandbox/views ./client/enterprise/views ./server/common/server.js ./server/common/api.js ./server/local/main.js ./server/sandbox/main.js ./server/enterprise/main.js --config ./jshint.json
