module.exports =
  vows:   require 'vows'
  assert: require 'assert'
  path:   require 'path'
  semver: require 'semver'

  autorequire: require '..'

# We're not afraid to mangle the assert package!  We're in tests after all.
module.exports.assert.keysEqual = (obj, keys) ->
  @deepEqual (k for k of obj).sort(), keys.sort()

module.exports.assert.valuesEqual = (obj, values) ->
  @deepEqual (v for k,v of obj).sort(), values.sort()

# Consider submitting this upstream
module.exports.assert.exclude = (actual, expected, message='expected {actual} to exclude {expected}') ->
  if (actual.indexOf?(expected) >= 0) or (actual.hasOwnProperty? expected)
    module.exports.assert.fail(actual, expected, message, 'exclude', module.exports.assert.exclude)
