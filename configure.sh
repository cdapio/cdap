#! /bin/sh

npm install jshint -g
npm install groc -g

gem install foreman

NODE_ENV=development

npm link ./server/

rm -rf ./node_modules/