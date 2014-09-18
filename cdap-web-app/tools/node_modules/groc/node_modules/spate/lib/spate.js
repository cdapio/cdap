(function() {
  var spate;

  spate = {
    pool: function(workItems, options, workerFunc) {
      var hasResponded, result;
      if (options == null) options = {};
      if (typeof options.maxConcurrency !== 'number') {
        throw new TypeError('maxConcurrency is a required option for spate.pool.');
      }
      result = {};
      hasResponded = false;
      result.exec = function(callback) {
        var i, itemsLeft, numInFlight, processItem, respond, _results;
        numInFlight = 0;
        itemsLeft = (function() {
          var _i, _len, _results;
          _results = [];
          for (_i = 0, _len = workItems.length; _i < _len; _i++) {
            i = workItems[_i];
            _results.push(i);
          }
          return _results;
        })();
        itemsLeft.reverse();
        processItem = function() {
          var item;
          if (hasResponded) return;
          item = itemsLeft.pop();
          if (item != null) {
            numInFlight += 1;
            return workerFunc(item, function(error) {
              numInFlight -= 1;
              if (error != null) return respond(error);
              return processItem();
            });
          } else if (numInFlight === 0) {
            return respond();
          }
        };
        respond = function(error) {
          if (!hasResponded) {
            hasResponded = true;
            return callback(error);
          }
        };
        _results = [];
        while (!hasResponded && itemsLeft.length > 0 && numInFlight < options.maxConcurrency) {
          _results.push(processItem());
        }
        return _results;
      };
      return result;
    }
  };

  module.exports = spate;

}).call(this);
