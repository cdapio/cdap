'use strict';


var FsTools = require('../');
var Helper = require('./helper');
var Assert = require('assert');

var SANDBOX = Helper.SANDBOX_DIR + '/walk';

require('vows').describe('walk()').addBatch({
  'walking through directory': {
    topic: function () {
      var callback = this.callback, result;

      result = { total: 0, files: 0, symlinks: 0 };
      FsTools.walk(SANDBOX, function (path, stats, next) {
        result.total += 1;

        if (stats.isFile()) {
          result.files += 1;
        }

        if (stats.isSymbolicLink()) {
          result.symlinks += 1;
        }

        next();
      }, function (err) {
        callback(err, result);
      });
    },
    'finishes without errors': function (err, result) {
      Assert.ok(!err, 'Has no errors');
    },
    'calls iterator on all entries': function (err, result) {
      Assert.equal(result.total, 8);
    },
    'provides lstats info': function (err, result) {
      Assert.equal(result.files, 4);
      Assert.equal(result.symlinks, 4);
    }
  },

  'walking through directory with pattern': {
    topic: function () {
      var callback = this.callback, result;

      result = 0;
      FsTools.walk(SANDBOX, /file$/, function (path, stats, next) {
        result += 1;
        next();
      }, function (err) {
        callback(err, result);
      });
    },
    'calls itertor on matching entries only': function (err, result) {
      Assert.ok(!err, 'Has no errors');
      Assert.equal(result, 4);
    }
  }
}).export(module);
