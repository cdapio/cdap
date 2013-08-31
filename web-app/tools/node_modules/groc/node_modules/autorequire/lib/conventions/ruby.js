(function() {
  var Classical, Ruby;
  var __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor; child.__super__ = parent.prototype; return child; };

  Classical = require('./classical');

  Ruby = (function() {

    __extends(Ruby, Classical);

    function Ruby() {
      Ruby.__super__.constructor.apply(this, arguments);
    }

    Ruby.prototype.directoryToProperty = function(directoryName, parentPath) {
      return this.underscore(directoryName);
    };

    return Ruby;

  })();

  module.exports = Ruby;

}).call(this);
