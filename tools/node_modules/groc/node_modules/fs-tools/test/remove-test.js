'use strict';


var FsTools = require('../');
var Helper = require('./helper');
var Assert = require('assert');

var SANDBOX = Helper.SANDBOX_DIR + '/remove';

require('vows').describe('remove()').addBatch({
  'removing single file': {
    topic: function () {
      FsTools.remove(SANDBOX + '/foo/bar/baz/file', this.callback);
    },
    'removes exactly one file': function (err) {
      Assert.ok(!err, 'Has no error');
      Assert.pathNotExists(SANDBOX + '/foo/bar/baz/file');
      Assert.pathExists(SANDBOX + '/foo/bar/baz');
      Assert.pathExists(SANDBOX);
    }
  },

  'removing symbolic link': {
    topic: function () {
      FsTools.remove(SANDBOX + '/foo/bar/baz/link', this.callback);
    },
    'removes symbolic link, and not the file/dir it points to': function (err) {
      Assert.ok(!err, 'Has no error');
      Assert.pathNotExists(SANDBOX + '/foo/bar/baz/link');
      Assert.pathExists(SANDBOX + '/foo/bar/baz');
      Assert.pathExists(SANDBOX);
    }
  }
}).addBatch({
  'removing directory': {
    topic: function () {
      FsTools.remove(SANDBOX, this.callback);
    },
    'removes directory recursively': function (err) {
      Assert.ok(!err, 'Has no error');
      Assert.pathNotExists(SANDBOX);
    }
  }
}).export(module);
