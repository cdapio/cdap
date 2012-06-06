(function() {
  var Loader, Module, assert, behavior, fs, k, path, v, version, vm;
  var __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor; child.__super__ = parent.prototype; return child; };

  assert = require('assert');

  fs = require('fs');

  path = require('path');

  vm = require('vm');

  Module = require('module');

  Loader = (function() {

    __extends(Loader, Module);

    function Loader(componentName, autorequireParent, convention) {
      Loader.__super__.constructor.call(this, componentName);
      this.convention = convention;
      this.autorequireParent = autorequireParent;
    }

    Loader.loadModule = function(componentName, modulePath, autorequireParent, convention) {
      var loader;
      loader = new this(componentName, autorequireParent, convention);
      loader.load(modulePath);
      return loader.exports;
    };

    Loader.prototype._compile = function(content, filename) {
      var sandbox;
      if (this.id === '.') {
        throw new Error('Compiling a root module is not supported by autorequire.');
      }
      content = this._cleanContent(content);
      sandbox = this._buildSandbox(filename);
      if (this.convention.modifySandbox) {
        sandbox = this.convention.modifySandbox(sandbox, this);
      }
      if (this.convention.modifySource) {
        content = this.convention.modifySource(content, this);
      }
      vm.runInContext(content, sandbox, filename, true);
      this.sandbox = sandbox;
      if (this.convention.modifyExports) {
        return this.exports = this.convention.modifyExports(this.exports, this);
      }
    };

    Loader.prototype.load = function(filename) {
      var extension;
      assert.ok(!this.loaded);
      this.filename = filename;
      this.paths = Module._nodeModulePaths(path.dirname(filename));
      extension = path.extname(filename);
      if (!Module._extensions[extension]) extension = '.js';
      (this._extensions[extension] || Module._extensions[extension])(this, filename);
      return this.loaded = true;
    };

    Loader.prototype._extensions = {
      '.coffee': function(module, filename) {
        var content;
        content = require('coffee-script').compile(fs.readFileSync(filename, 'utf8'), {
          filename: filename,
          bare: true
        });
        return module._compile(content, filename);
      }
    };

    return Loader;

  })();

  version = (function() {
    var _i, _len, _ref, _results;
    _ref = process.version.match(/v(\d+)\.(\d+)\.(\d+)/).slice(1, 4);
    _results = [];
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      v = _ref[_i];
      _results.push(parseInt(v));
    }
    return _results;
  })();

  behavior = (function() {
    if (version[1] === 6) {
      return require('./loader_behavior/v0.5.2');
    } else if (version[1] === 5 && version[2] >= 2) {
      return require('./loader_behavior/v0.5.2');
    } else if (version[1] === 5 && version[2] === 1) {
      return require('./loader_behavior/v0.5.1');
    } else if (version[1] === 5 && version[2] === 0) {
      return require('./loader_behavior/v0.4.0');
    } else if (version[1] === 4) {
      return require('./loader_behavior/v0.4.0');
    } else {
      throw new Error("No autorequire.Loader behavior defined for node " + process.version);
    }
  })();

  for (k in behavior) {
    v = behavior[k];
    Loader.prototype[k] = v;
  }

  module.exports = Loader;

}).call(this);
