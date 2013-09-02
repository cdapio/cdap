(function() {
  var path;

  path = require('path');

  module.exports = {
    lazyLoad: function(object, property, getter) {
      return Object.defineProperty(object, property, {
        enumerable: true,
        configurable: true,
        get: function() {
          var result;
          result = getter();
          Object.defineProperty(object, property, {
            enumerable: true,
            value: result
          });
          return result;
        }
      });
    },
    STACK_PATH_EXTRACTOR: /\((.+)\:\d+\:\d+\)/,
    getCallingDirectoryFromStack: function(offset) {
      var match, stackLines;
      if (offset == null) offset = 1;
      stackLines = new Error().stack.split("\n");
      match = stackLines[offset + 2].match(this.STACK_PATH_EXTRACTOR);
      return (match && path.dirname(match[1])) || process.cwd();
    }
  };

}).call(this);
