#! /bin/sh

npm install jshint -g
npm install groc -g
npm install one -g

NODE_ENV=development

npm link ./server/

rm -rf ./node_modules/