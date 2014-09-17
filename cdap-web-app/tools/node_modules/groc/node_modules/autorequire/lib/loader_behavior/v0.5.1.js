(function() {
  var Module, path, vm;

  path = require('path');

  Module = require('module');

  vm = require('vm');

  module.exports = {
    _cleanContent: function(content) {
      return content.replace(/^\#\!.*/, '');
    },
    _buildRequire: function() {
      var require, self;
      self = this;
      require = function(path) {
        return self.require(path);
      };
      require.resolve = function(request) {
        return Module._resolveFilename(request, self)[1];
      };
      require.paths = Module._paths;
      require.main = process.mainModule;
      require.extensions = Module._extensions;
      require.registerExtension = function() {
        throw new Error('require.registerExtension() removed. Use require.extensions instead.');
      };
      require.cache = Module._cache;
      return require;
    },
    _buildSandbox: function(filename) {
      var k, sandbox, v;
      sandbox = vm.createContext({});
      for (k in global) {
        v = global[k];
        sandbox[k] = v;
      }
      sandbox.require = this._buildRequire();
      sandbox.exports = this.exports;
      sandbox.__filename = filename;
      sandbox.__dirname = path.dirname(filename);
      sandbox.module = this;
      sandbox.global = sandbox;
      sandbox.root = root;
      return sandbox;
    }
  };

}).call(this);
