#! /bin/sh

/usr/bin/ssh-agent
ssh-add ./deploy_key

rsync -v -r --delete ./build deploy@staging.continuuity.com:~/

ssh deploy@staging.continuuity.com 'sudo npm link build/server'
ssh deploy@staging.continuuity.com 'sudo killall node'
ssh deploy@staging.continuuity.com 'sudo NODE_ENV=staging build/node build/server/main.js </dev/null >/dev/null 2>&1 &'