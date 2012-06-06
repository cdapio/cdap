_      = require 'underscore'
test   = require './common'
Module = require 'module'


EXAMPLE_PATH     = test.path.join(__dirname, 'examples', 'loader')
ALL_GLOBALS_PATH = test.path.join(EXAMPLE_PATH, 'all_globals.coffee')
RELATIVE_PATH    = test.path.join(EXAMPLE_PATH, 'relative.js')

# Even though older versions did not promise to provide many of these properties, they still do.
commonProperties =
  'should include global': (moduleGlobals) ->
    test.assert.notEqual typeof moduleGlobals.global, 'undefined'

  'should include process': (moduleGlobals) ->
    test.assert.equal moduleGlobals.process, process

  'should include require': (moduleGlobals) ->
    test.assert.equal typeof moduleGlobals.require, 'function'
    test.assert.equal moduleGlobals.require('./relative'), 'Relative require'

  'should include require.resolve': (moduleGlobals) ->
    test.assert.equal moduleGlobals.require.resolve('./relative'), RELATIVE_PATH

  'should include require.paths': (moduleGlobals) ->
    test.assert.notEqual typeof moduleGlobals.require.paths, 'undefined'

  'should include __filename': (moduleGlobals) ->
    test.assert.equal moduleGlobals.__filename, ALL_GLOBALS_PATH

  'should include __dirname': (moduleGlobals) ->
      test.assert.equal moduleGlobals.__dirname, EXAMPLE_PATH

  'should include module': (moduleGlobals) ->
    test.assert.notEqual typeof moduleGlobals.module, 'undefined'

  # v0.4.5+, v0.5.0+
  'should include setTimeout': (moduleGlobals) ->
    test.assert.equal moduleGlobals.setTimeout, setTimeout

  'should include clearTimeout': (moduleGlobals) ->
    test.assert.equal moduleGlobals.clearTimeout, clearTimeout

  'should include setInterval': (moduleGlobals) ->
    test.assert.equal moduleGlobals.setInterval, setInterval

  'should include clearInterval': (moduleGlobals) ->
    test.assert.equal moduleGlobals.clearInterval, clearInterval

  # v0.4.7+, v0.5.0+
  'should include console': (moduleGlobals) ->
    test.assert.equal moduleGlobals.console, console

  # v0.4.9+, v0.5.0+
  'should include require.cache': (moduleGlobals) ->
    test.assert.notEqual typeof moduleGlobals.require.cache, 'undefined'

  # v0.4.10+
  'should include exports': (moduleGlobals) ->
    test.assert.equal typeof moduleGlobals.exports, 'object'

  # v0.5.2+
  'should include Buffer': (moduleGlobals) ->
    test.assert.equal moduleGlobals.Buffer, Buffer

# Some behavior actually does change:
if test.semver.gte process.version, 'v0.5.1'
  _(commonProperties).extend
    'should include module.require': (moduleGlobals) ->
      test.assert.equal typeof moduleGlobals.module.require, 'function'
      test.assert.equal moduleGlobals.module.require('./relative'), 'Relative require'

if test.semver.gte process.version, 'v0.5.2'
  delete commonProperties['should include require.paths']

  _(commonProperties).extend
    'should include a deprecation notice for require.paths': (moduleGlobals) ->
      test.assert.throws (-> moduleGlobals.require.paths), Error

test.vows.describe('Loader').addBatch
  'a sandboxed require()':
    topic: ->
      # stay consistent with SANDBOXED require(), we're less concerned with the traditional one.
      oldContextLoad = Module._contextLoad
      Module._contextLoad = true
      result = require test.path.join(EXAMPLE_PATH, 'all_globals')
      Module._contextLoad = oldContextLoad

      result

    'properties': commonProperties

  'global':
    topic: ->
      convention = new test.autorequire.conventions.Default

      test.autorequire.Loader.loadModule 'all_globals', ALL_GLOBALS_PATH, null, convention

    'properties': commonProperties

  'global inheritance':
    topic: ->
      global.inheritedProperty = 'From parent'
      convention = new test.autorequire.conventions.Default

      test.autorequire.Loader.loadModule 'all_globals', ALL_GLOBALS_PATH, null, convention

    'properties should propagate into required modules': (moduleGlobals) ->
      test.assert.equal moduleGlobals.inheritedProperty, 'From parent'

    'global properties should not escape the sandbox': (moduleGlobals) ->
      test.assert.equal typeof global.innerProperty, 'undefined'

.export module
