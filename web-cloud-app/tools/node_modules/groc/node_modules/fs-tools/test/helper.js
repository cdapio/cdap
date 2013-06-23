'use strict';


var Path = require('path');
var Assert = require('assert');
var Helper = module.exports = {};

Helper.SANDBOX_DIR = require('fs').realpathSync(__dirname + '/..') + '/tmp/sandbox';

var fns2type = {
  isFile: 'file',
  isDirectory: 'directory',
  isBlockDevice: 'block device',
  isCharacterDevice: 'character device',
  isSymbolicLink: 'symlink',
  isFIFO: 'FIFO',
  isSocket: 'socket'
};

function getPathType(stats) {
  var fn;

  if (!stats || 'object' !== typeof stats) {
    throw new Error('Stats object required');
  }

  /*jslint forin:true*/
  for (fn in fns2type) {
    if ('function' === typeof stats[fn] && stats[fn]()) {
      return fns2type[fn];
    }
  }

  throw new Error('Expected valid stats object');
}

Object.getOwnPropertyNames(fns2type).forEach(function (fn) {
  Assert[fn] = function (stats, msg) {
    var expected = fns2type[fn], result = getPathType(stats);
    msg = msg || "Expected '" + expected + "' but got '" + result + "'.";
    Assert.ok(expected === result, msg);
  };
});


Assert.hasPermsMode = function hasPermsMode(stats, expected, msg) {
  var result = stats.mode.toString(8).slice(-4);
  msg = msg || "Expected '" + expected + "' mode, but got '" + result + "'.";
  Assert.ok(expected === result, msg);
};


Assert.pathExists = function pathExists(path, msg) {
  msg = msg || "Expect path '" + path + "' to exist.";
  Assert.ok(Path.existsSync(path), msg);
};


Assert.pathNotExists = function pathNotExists(path, msg) {
  msg = msg || "Doe not expect path '" + path + "' to exist.";
  Assert.ok(!Path.existsSync(path), msg);
};
