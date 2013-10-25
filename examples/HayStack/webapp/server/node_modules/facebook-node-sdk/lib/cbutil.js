
var assert = require('assert');

/**
 * This function wrap a function that taken a callback:
 * - to callback when we catch error and
 * - to wrap the callback:
 *   - to avoid throwing error in the callback and
 *   - to avoid being called twice.
 *
 * @param fn function that taken a callback.
 * @param addError (optional) If this is true callback is not taken an error. And unshift callback arguments to pass an error.
 * @param callbackIndex (optional) argument index of callback
 */
exports.wrap = function wrap(fn, /* opt */addError, /* opt */callbackIndex) {
  if (fn.name === '__cbUtilWrapped__') {
    return fn;
  }
  return function __cbUtilWrapped__() {
    var index = (callbackIndex === undefined) ? arguments.length - 1 : callbackIndex;
    var callback = arguments[index];
    if (typeof callback !== 'function') {
      throw new Error('Callback is not a function.');
    }
    callback = wrapCallback(callback, addError);
    arguments[index] = callback;
    try {
      fn.apply(this, arguments);
    }
    catch (err) {
      callback(err, null);
    }
  };
};

exports.errorLog = function(msg) {
  console.error(msg);
};

exports.returnToCallback = function(callback, handleError, fn) {
  return function() {
    try {
      if (handleError) {
        callback(null, fn.apply(this, arguments));
      }
      else {
        var args = Array.prototype.slice.call(arguments);
        var e = args.shift();
        if (e) {
          throw e;
        }
        callback(null, fn.apply(this, args));
      }
    }
    catch (err) {
      callback(err, null);
    }
  };
};

function wrapCallback(callback, /* opt */addError) {
  assert.ok(callback.name !== '__cbUtilwrappedCallback__');
  var called = false;
  return function __cbUtilWrappedCallback__() {
    if (called === true) {
      exports.errorLog(new Error('Cannot call callback twice.').stack);
    }
    else {
      called = true;
      try {
        if (addError === true) {
          // to array
          var args = Array.prototype.slice.call(arguments);
          args.unshift(null);
          callback.apply(this, args);
        }
        else {
          callback.apply(this, arguments);
        }
      }
      catch (err) {
        exports.errorLog('Callback cannot throw error: ' + err.stack);
      }
    }
  };
};

