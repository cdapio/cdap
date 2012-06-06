path   = require 'path'
Module = require 'module'
vm     = require 'vm'

# Behavior to mirror Node's v0.5.2 to present implementation of
# [`Module.prototype._compile`](https://github.com/joyent/node/blob/v0.5.2/lib/module.js#L346).
module.exports =
  # Clean up shebang lines in input sources.
  _cleanContent: (content) ->
    content.replace /^\#\!.*/, ''

  # Builds the require() function for this module, and any properties on it.
  _buildRequire: ->
    self    = @
    require = (path) => self.require path

    require.resolve = (request) -> Module._resolveFilename(request, self)[1]

    Object.defineProperty require, 'paths', get: ->
      throw new Error 'require.paths is removed. Use node_modules folders, or the NODE_PATH environment variable instead.'

    require.main = process.mainModule

    require.extensions = Module._extensions
    require.registerExtension = ->
      throw new Error 'require.registerExtension() removed. Use require.extensions instead.'

    require.cache = Module._cache

    require

  # Builds the default sandbox for a module.
  _buildSandbox: (filename) ->
    sandbox = vm.createContext {}
    for k, v of global
      sandbox[k] = v

    sandbox.require    = @_buildRequire()
    sandbox.exports    = @exports
    sandbox.__filename = filename
    sandbox.__dirname  = path.dirname filename
    sandbox.module     = @
    sandbox.global     = sandbox
    sandbox.root       = root

    sandbox
