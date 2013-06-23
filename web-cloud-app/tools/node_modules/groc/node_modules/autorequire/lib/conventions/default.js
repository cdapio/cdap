(function() {
  var Default, ModuleGroupFactory;

  ModuleGroupFactory = require('../module_group_factory');

  Default = (function() {

    function Default() {}

    Default.prototype.buildRootModuleGroup = function(rootPath) {
      return ModuleGroupFactory.buildModuleGroup(this, rootPath);
    };

    Default.prototype.specialCaseModuleNames = {};

    Default.prototype.directoryToProperty = function(directoryName, parentPath) {
      return this.specialCaseModuleNames[directoryName] || this.camelCase(directoryName);
    };

    Default.prototype.fileToProperty = function(fileName, parentPath) {
      var baseName;
      baseName = this.stripFileExtension(fileName);
      return this.specialCaseModuleNames[baseName] || this.camelCase(baseName);
    };

    Default.prototype.modifySandbox = function(sandbox, module) {
      module._globalLazyLoads = this.globalLazyLoads(module);
      module._require = sandbox.require;
      return sandbox;
    };

    Default.prototype.modifySource = function(source, module) {
      return "for (var key in module._globalLazyLoads) {\n  try {\n    Object.defineProperty(global, key, {\n      enumerable: false, configurable: true, get: module._globalLazyLoads[key]\n    });\n  } catch (err) {}\n}" + source;
    };

    Default.prototype.modifyExports = function(exports, module) {
      return exports;
    };

    Default.prototype.globalModules = ['assert', 'buffer', 'child_process', 'constants', 'crypto', 'dgram', 'dns', 'events', 'freelist', 'fs', 'http', 'https', 'net', 'os', 'path', 'querystring', 'readline', 'repl', 'stream', 'string_decoder', 'sys', 'timers', 'tls', 'tty', 'url', 'util', 'vm'];

    Default.prototype.extraGlobalModules = [];

    Default.prototype.globalLazyLoads = function(module) {
      var result;
      result = {};
      this.appendGlobalModules(result, module);
      this.appendProjectModules(result, module);
      return result;
    };

    Default.prototype.appendGlobalModules = function(lazyLoads, module) {
      var mod, _i, _len, _ref, _results;
      var _this = this;
      _ref = this.globalModules.concat(this.extraGlobalModules);
      _results = [];
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        mod = _ref[_i];
        _results.push((function(mod) {
          return lazyLoads[_this.directoryToProperty(mod)] = function() {
            return module._require(mod);
          };
        })(mod));
      }
      return _results;
    };

    Default.prototype.appendProjectModules = function(lazyLoads, module) {
      var key, moduleGroup, _fn, _results;
      moduleGroup = module.autorequireParent;
      _results = [];
      while (moduleGroup) {
        _fn = function(key, moduleGroup) {
          return lazyLoads[key] = function() {
            return moduleGroup[key];
          };
        };
        for (key in moduleGroup) {
          if (key in lazyLoads) {
            throw new Error("Ambiguous property '" + key + "'");
          }
          if (key === module.id) continue;
          _fn(key, moduleGroup);
        }
        _results.push(moduleGroup = moduleGroup.__parent);
      }
      return _results;
    };

    Default.prototype.stripFileExtension = function(fileName) {
      return fileName.match(/(.+?)(\.[^.]*$|$)/)[1];
    };

    Default.prototype.camelCaps = function(pathComponent) {
      return pathComponent.split(/[-_]+/).map(function(val) {
        return val[0].toLocaleUpperCase() + val.slice(1);
      }).join('');
    };

    Default.prototype.camelCase = function(pathComponent) {
      var result;
      result = this.camelCaps(pathComponent);
      return result[0].toLocaleLowerCase() + result.slice(1);
    };

    Default.prototype.underscore = function(pathComponent) {
      return pathComponent.split(/[-_]+/).join('_');
    };

    return Default;

  })();

  module.exports = Default;

}).call(this);
