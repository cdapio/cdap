var promise = require('q');
var fs = require('fs');
module.exports = {
  readFile: readFile
};

function readFile(path, format) {
  var deferred = promise.defer();
  if (format) {
    fs.readFile(path, format, function(err, result) {
      deferred.resolve(result);
    });
  } else {
    fs.readFile(path, function(err, result) {
      deferred.resolve(result);
    });
  }
  return deferred.promise;
}