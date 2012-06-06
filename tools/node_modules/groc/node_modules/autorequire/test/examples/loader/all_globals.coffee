module.exports = {}
# We need to copy them to ensure that we don't have a munged global object when passed back out of
# the sandbox.
for k,v of global
  module.exports[k] = v

global.innerProperty = 'Inner global'
