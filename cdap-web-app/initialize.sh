#!/usr/bin/env bash

npm install jshint -g
npm install groc -g
npm install one -g

ln -s ./check_syntax.sh .git/hooks/pre-commit

NODE_ENV=development

npm link ./server/

rm -rf ./node_modules/
