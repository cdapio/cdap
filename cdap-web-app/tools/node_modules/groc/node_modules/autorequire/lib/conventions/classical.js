(function() {
  var Classical, Default;
  var __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor; child.__super__ = parent.prototype; return child; };

  Default = require('./default');

  Classical = (function() {

    __extends(Classical, Default);

    function Classical() {
      Classical.__super__.constructor.apply(this, arguments);
    }

    Classical.prototype.fileToProperty = function(fileName, parentPath) {
      var baseName;
      baseName = this.stripFileExtension(fileName);
      return this.specialCaseModuleNames[baseName] || this.camelCaps(baseName);
    };

    Classical.prototype.modifyExports = function(exports, module) {
      if (!module.sandbox[module.id]) {
        throw new TypeError("Expected " + module.filename + " to define " + module.id);
      }
      return module.sandbox[module.id];
    };

    return Classical;

  })();

  module.exports = Classical;

}).call(this);
