(function() {
  var Loader, ModuleGroupFactory, fs, path, utils;
  var __slice = Array.prototype.slice;

  fs = require('fs');

  path = require('path');

  utils = require('./utils');

  Loader = require('./loader');

  ModuleGroupFactory = (function() {

    ModuleGroupFactory.buildModuleGroup = function() {
      var args, newFactory;
      args = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
      newFactory = (function(func, args, ctor) {
        ctor.prototype = func.prototype;
        var child = new ctor, result = func.apply(child, args);
        return typeof result === "object" ? result : child;
      })(this, args, function() {});
      return newFactory.build();
    };

    function ModuleGroupFactory(convention, path, name, parent) {
      if (name == null) name = '__root';
      this.convention = convention;
      this.path = path;
      this.name = name;
      this.parent = parent;
    }

    ModuleGroupFactory.prototype.build = function() {
      this.moduleGroup = {};
      this.appendIntrospectiveProperties();
      this.enumerateModuleDirectory();
      return this.moduleGroup;
    };

    ModuleGroupFactory.prototype.appendIntrospectiveProperties = function() {
      Object.defineProperty(this.moduleGroup, '__dirname', {
        value: this.path,
        enumerable: false
      });
      Object.defineProperty(this.moduleGroup, '__name', {
        value: this.name,
        enumerable: false
      });
      return Object.defineProperty(this.moduleGroup, '__parent', {
        value: this.parent,
        enumerable: false
      });
    };

    ModuleGroupFactory.prototype.enumerateModuleDirectory = function() {
      var fullPath, pathComponent, pathStats, _i, _len, _ref, _results;
      var _this = this;
      _ref = fs.readdirSync(this.path);
      _results = [];
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        pathComponent = _ref[_i];
        if (!(pathComponent[0] !== '.')) continue;
        fullPath = path.join(this.path, pathComponent);
        pathStats = fs.statSync(fullPath);
        _results.push((function(fullPath) {
          var componentName, groupName;
          if (pathStats.isDirectory()) {
            groupName = _this.convention.directoryToProperty(pathComponent, _this.path);
            return utils.lazyLoad(_this.moduleGroup, groupName, function() {
              return _this.buildSubGroup(groupName, fullPath);
            });
          } else if (pathStats.isFile()) {
            componentName = _this.convention.fileToProperty(pathComponent, _this.path);
            return utils.lazyLoad(_this.moduleGroup, componentName, function() {
              return _this.loadModule(componentName, fullPath);
            });
          }
        })(fullPath));
      }
      return _results;
    };

    ModuleGroupFactory.prototype.buildSubGroup = function(groupName, path) {
      return this.constructor.buildModuleGroup(this.convention, path, "" + this.name + "." + groupName, this.moduleGroup);
    };

    ModuleGroupFactory.prototype.loadModule = function(componentName, path) {
      return Loader.loadModule(componentName, path, this.moduleGroup, this.convention);
    };

    return ModuleGroupFactory;

  })();

  module.exports = ModuleGroupFactory;

}).call(this);
